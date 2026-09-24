// Package cqrs provides a lightweight, zero-allocation, thread-safe
// in-memory CQRS (Command Query Responsibility Segregation)
// and Event Streaming toolkit for Go.
package cqrs

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
)

// ErrUnimplementedHandler - error value for unimplemented handler cases.
var ErrUnimplementedHandler = errors.New("cqrs: unimplemented handler")

type commandBus struct {
	router  map[CommandType]CommandHandlerFunc
	onPanic PanicHook
	closed  atomic.Bool
}

func (cb *commandBus) OnPanic(h PanicHook) { cb.onPanic = h }

func (cb *commandBus) Close() error {
	if !cb.closed.CompareAndSwap(false, true) {
		return nil
	}
	cb.router = make(map[CommandType]CommandHandlerFunc)
	return nil
}

func (cb *commandBus) HandleCommand(ctx context.Context, c Command) error {
	if cb.closed.Load() {
		return io.ErrClosedPipe
	}
	f, ok := cb.router[c.Type()]
	if !ok {
		return ErrUnimplementedHandler
	}
	_, err := f(ctx, c)
	return err

}

func (cb *commandBus) OnCommand(t CommandType, f CommandHandlerFunc) {
	if f == nil {
		return
	}
	cb.router[t] = func(ctx context.Context, c Command) ([]Event, error) {
		defer func() {
			if r := recover(); r != nil &&
				cb.onPanic != nil {
				cb.onPanic(r)
			}
		}()
		return f(ctx, c)
	}
}

type queryBus struct {
	router  map[QueryType]QueryHandlerFunc
	onPanic PanicHook
	closed  atomic.Bool
}

func (qb *queryBus) OnPanic(h PanicHook) { qb.onPanic = h }

func (qb *queryBus) Close() error {
	if !qb.closed.CompareAndSwap(false, true) {
		return nil
	}
	qb.router = make(map[QueryType]QueryHandlerFunc)
	return nil
}

func (qb *queryBus) OnQuery(t QueryType, f QueryHandlerFunc) {
	if f == nil {
		return
	}
	qb.router[t] = func(ctx context.Context, q Query) (Result, error) {
		defer func() {
			if r := recover(); r != nil &&
				qb.onPanic != nil {
				qb.onPanic(r)
			}
		}()
		return f(ctx, q)
	}
}

func (qb *queryBus) HandleQuery(ctx context.Context, q Query) (Result, error) {
	if qb.closed.Load() {
		return nil, io.ErrClosedPipe
	}
	f, ok := qb.router[q.Type()]
	if !ok {
		return nil, ErrUnimplementedHandler
	}
	return f(ctx, q)
}

type eventBus struct {
	router  map[EventType][]EventHandlerFunc
	onPanic PanicHook
	closed  atomic.Bool
}

func (eb *eventBus) Close() error {
	if !eb.closed.CompareAndSwap(false, true) {
		return nil
	}
	eb.router = make(map[EventType][]EventHandlerFunc)
	return nil
}

func (eb *eventBus) OnPanic(h PanicHook) { eb.onPanic = h }

func (eb *eventBus) OnEvent(t EventType, f EventHandlerFunc) {
	if f == nil {
		return
	}
	eb.router[t] = append(eb.router[t], func(ctx context.Context, e Event) {
		defer func() {
			if r := recover(); r != nil &&
				eb.onPanic != nil {
				eb.onPanic(r)
			}
		}()
		f(ctx, e)
	})
}

func (eb *eventBus) HandleEvent(ctx context.Context, e Event) {
	if eb.closed.Load() {
		return
	}
	handlers := eb.router[e.Type()]
	if len(handlers) == 0 {
		return
	}
	for _, f := range handlers {
		f(ctx, e)
	}
}

type app struct {
	cb      CommandBusPort
	qb      QueryBusPort
	eb      EventBusPort
	es      EventSourcePort
	onClose Hook
}

func (app *app) OnClose(h Hook) { app.onClose = h }

func (app *app) OnPanic(h PanicHook) {
	app.cb.OnPanic(h)
	app.qb.OnPanic(h)
	app.eb.OnPanic(h)
}

func (app *app) Close() error {
	if app.onClose != nil {
		app.onClose()
	}
	return errors.Join(
		app.cb.Close(),
		app.qb.Close(),
		app.eb.Close(),
		app.es.Close(),
	)
}

// New returns new CQRS application facade instance.
func New() App {
	return &app{
		cb: &commandBus{router: make(map[CommandType]CommandHandlerFunc)},
		qb: &queryBus{router: make(map[QueryType]QueryHandlerFunc)},
		eb: &eventBus{router: make(map[EventType][]EventHandlerFunc)},
		es: &eventSource{streams: make(map[chan Event]context.CancelFunc)},
	}
}

func (app *app) OnCommand(t CommandType, f CommandHandlerFunc) {
	app.cb.OnCommand(t, func(ctx context.Context, c Command) ([]Event, error) {
		events, err := f(ctx, c)
		if err != nil {
			return nil, err
		}
		if len(events) == 0 {
			return nil, nil
		}
		for _, event := range events {
			// Projections and local handlers run synchronously first.
			app.eb.HandleEvent(ctx, event)
			// Stream subscribers receive the event afterwards.
			app.es.HandleEvent(ctx, event)
		}
		return events, nil
	})
}

func (app *app) OnQuery(t QueryType, f QueryHandlerFunc) { app.qb.OnQuery(t, f) }

func (app *app) OnEvent(t EventType, f EventHandlerFunc) { app.eb.OnEvent(t, f) }

func (app *app) Build(projection Projection) {
	if projection == nil || len(projection.Types()) == 0 {
		return
	}
	for _, t := range projection.Types() {
		app.eb.OnEvent(t, projection.HandleEvent)
	}
}

func (app *app) HandleCommand(ctx context.Context, command Command) error {
	return app.cb.HandleCommand(ctx, command)
}

func (app *app) HandleQuery(ctx context.Context, query Query) (Result, error) {
	return app.qb.HandleQuery(ctx, query)
}

func (app *app) HandleEvent(ctx context.Context, event Event) {
	app.eb.HandleEvent(ctx, event)
}

func (app *app) EventStream(ctx context.Context) (<-chan Event, error) {
	return app.es.EventStream(ctx)
}

type eventSource struct {
	streams map[chan Event]context.CancelFunc
	wg      sync.WaitGroup
	mu      sync.RWMutex
	closed  bool
}

func (es *eventSource) EventStream(ctx context.Context) (<-chan Event, error) {
	es.mu.Lock()
	if es.closed {
		return nil, io.ErrClosedPipe
	}
	stream := make(chan Event, 50)
	streamCtx, streamCancel := context.WithCancel(ctx)
	es.streams[stream] = streamCancel
	es.mu.Unlock()
	es.wg.Go(func() {
		defer func() {
			es.mu.Lock()
			delete(es.streams, stream)
			close(stream)
			es.mu.Unlock()
			streamCancel()
		}()
		<-streamCtx.Done()
	})
	return stream, nil
}

func (es *eventSource) Close() error {
	es.mu.Lock()
	if es.closed {
		es.mu.Unlock()
		return nil
	}
	es.closed = true
	for _, cancel := range es.streams {
		cancel()
	}
	es.mu.Unlock()
	es.wg.Wait()
	return nil
}

func (es *eventSource) HandleEvent(ctx context.Context, event Event) {
	es.mu.RLock()
	if es.closed {
		es.mu.RUnlock()
		return
	}
	es.wg.Add(1)
	defer es.wg.Done()
	var slowConsumers []chan Event
	for stream := range es.streams {
		select {
		case stream <- event:
		default:
			slowConsumers = append(slowConsumers, stream)
		}
	}
	es.mu.RUnlock()
	if len(slowConsumers) > 0 {
		es.mu.Lock()
		for _, stream := range slowConsumers {
			if cancel, ok := es.streams[stream]; ok {
				cancel()
			}
		}
		es.mu.Unlock()
	}
}
