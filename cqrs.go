// Package cqrs provides a lightweight, zero-allocation, thread-safe
// in-memory CQRS (Command Query Responsibility Segregation)
// and Event Streaming toolkit for Go.
package cqrs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
)

// ErrUnimplementedHandler - error value for unimplemented handler cases.
var ErrUnimplementedHandler = errors.New("cqrs: unimplemented handler")

type commandBus map[CommandType]CommandHandlerFunc

func (cb commandBus) Close() error {
	cb = make(commandBus)
	return nil
}

func (cb commandBus) HandleCommand(ctx context.Context, c Command) error {
	f, ok := cb[c.Type()]
	if !ok {
		return ErrUnimplementedHandler
	}
	if _, err := f(ctx, c); err != nil {
		return err
	}
	return nil
}

func (cb commandBus) OnCommand(t CommandType, f CommandHandlerFunc) {
	if f == nil {
		return
	}
	cb[t] = func(ctx context.Context, c Command) (events []Event, err error) {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("%v", r)
			}
		}()
		return f(ctx, c)
	}
}

type queryBus map[QueryType]QueryHandlerFunc

func (qb queryBus) Close() error {
	qb = make(queryBus)
	return nil
}

func (qb queryBus) OnQuery(t QueryType, f QueryHandlerFunc) {
	if f == nil {
		return
	}
	qb[t] = func(ctx context.Context, q Query) (result Result, err error) {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("%v", r)
			}
		}()
		return f(ctx, q)
	}
}

func (qb queryBus) HandleQuery(ctx context.Context, q Query) (Result, error) {
	f, ok := qb[q.Type()]
	if !ok {
		return nil, ErrUnimplementedHandler
	}
	return f(ctx, q)
}

type eventBus map[EventType][]EventHandlerFunc

func (eb eventBus) Close() error {
	eb = make(eventBus)
	return nil
}

func (eb eventBus) OnEvent(t EventType, f EventHandlerFunc) {
	if f == nil {
		return
	}
	eb[t] = append(eb[t], func(ctx context.Context, e Event) (err error) {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("%v", r)
			}
		}()
		return f(ctx, e)
	})
}

func (eb eventBus) HandleEvent(ctx context.Context, e Event) error {
	handlers := eb[e.Type()]
	if len(handlers) == 0 {
		return nil
	}
	var errs []error
	for _, f := range handlers {
		if err := f(ctx, e); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

type app struct {
	cb commandBusPort
	qb queryBusPort
	eb eventBusPort
	es eventSourcePort
}

func (app *app) Close() error {
	var errs []error
	if err := app.cb.Close(); err != nil {
		errs = append(errs, err)
	}
	if err := app.qb.Close(); err != nil {
		errs = append(errs, err)
	}
	if err := app.eb.Close(); err != nil {
		errs = append(errs, err)
	}
	if err := app.es.Close(); err != nil {
		errs = append(errs, err)
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

// New returns new CQRS application facade instance.
func New() App {
	return &app{
		cb: make(commandBus),
		qb: make(queryBus),
		eb: make(eventBus),
		es: &eventSource{
			closed:  false,
			streams: make(map[chan Event]context.CancelFunc),
			wg:      sync.WaitGroup{},
			mu:      sync.RWMutex{},
		},
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
		var errs []error
		for _, event := range events {
			// Event sourcing
			app.es.HandleEvent(ctx, event)
			// Event handling
			if err := app.eb.HandleEvent(ctx, event); err != nil {
				errs = append(errs, err)
			}
		}
		if len(errs) > 0 {
			return events, errors.Join(errs...)
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

func (app *app) HandleEvent(ctx context.Context, event Event) error {
	return app.eb.HandleEvent(ctx, event)
}

func (app *app) EventStream(ctx context.Context) (<-chan Event, error) {
	return app.es.EventStream(ctx)
}

type eventSource struct {
	closed  bool
	streams map[chan Event]context.CancelFunc
	wg      sync.WaitGroup
	mu      sync.RWMutex
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
	for _, cancel := range es.streams {
		cancel()
	}
	es.mu.Unlock()
	es.wg.Wait()
	return nil
}

func (es *eventSource) HandleEvent(ctx context.Context, event Event) error {
	es.mu.RLock()
	if es.closed {
		es.mu.RUnlock()
		return io.ErrClosedPipe
	}
	es.mu.RUnlock()
	es.wg.Go(func() {
		defer func() {
			if r := recover(); r != nil {
				// Prevent goroutine panic crash
			}
		}()
		es.mu.RLock()
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
	})
	return nil
}
