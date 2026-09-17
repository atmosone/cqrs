package cqrs

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
)

type CommandBus struct {
	logger *slog.Logger
	bus    map[CommandType]CommandHandlerFunc
	mu     sync.RWMutex
}

func NewCommandBus() *CommandBus {
	return &CommandBus{
		logger: slog.Default(),
		bus:    make(map[CommandType]CommandHandlerFunc),
		mu:     sync.RWMutex{},
	}
}

func (cb *CommandBus) WithLogger(logger *slog.Logger) *CommandBus {
	if logger != nil {
		cb.logger = logger
	}
	return cb
}

// Handle implements [CommandHandler].
func (cb *CommandBus) Handle(ctx context.Context, c Command) ([]Event, error) {
	cb.mu.RLock()
	f, ok := cb.bus[c.Type()]
	cb.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("%w (%T)", ErrUnimplementedHandler, c)
	}
	return f(ctx, c)
}

// HandleFunc implements [CommandBusPort].
func (cb *CommandBus) HandleFunc(t CommandType, f CommandHandlerFunc) {
	if f == nil {
		cb.logger.
			With(slog.String("command_type", t.String())).
			Debug("unimplemented CommandHandlerFunc")
		return
	}
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.bus[t] = func(ctx context.Context, c Command) (events []Event, err error) {
		defer func() {
			if r := recover(); r != nil {
				cb.logger.With(
					slog.String("trace_id", Trace(ctx).String()),
					slog.String("command_type", c.Type().String()),
					slog.Any("panic", r),
				).Error("panic in command handler func")
				err = fmt.Errorf("%w (%v)", ErrPanicRecovered, r)
			}
		}()
		return f(ctx, c)
	}
}

type QueryBus struct {
	logger *slog.Logger
	bus    map[QueryType]QueryHandlerFunc
	mu     sync.RWMutex
}

func NewQueryBus() *QueryBus {
	return &QueryBus{
		logger: slog.Default(),
		bus:    make(map[QueryType]QueryHandlerFunc),
		mu:     sync.RWMutex{},
	}
}

func (qb *QueryBus) WithLogger(logger *slog.Logger) *QueryBus {
	if logger != nil {
		qb.logger = logger
	}
	return qb
}

// HandleFunc implements [QueryBusPort].
func (qb *QueryBus) HandleFunc(t QueryType, f QueryHandlerFunc) {
	if f == nil {
		qb.logger.
			With(slog.String("query_type", t.String())).
			Debug("unimplemented QueryHandlerFunc")
		return
	}
	qb.mu.Lock()
	defer qb.mu.Unlock()
	qb.bus[t] = func(ctx context.Context, q Query) (result Result, err error) {
		defer func() {
			if r := recover(); r != nil {
				qb.logger.With(
					slog.String("trace_id", Trace(ctx).String()),
					slog.String("query_type", q.Type().String()),
					slog.Any("panic", r),
				).Error("panic in query handler func")
				err = fmt.Errorf("%w (%v)", ErrPanicRecovered, r)
			}
		}()
		return f(ctx, q)
	}
}

// Handle implements [QueryHandler].
func (qb *QueryBus) Handle(ctx context.Context, q Query) (Result, error) {
	qb.mu.RLock()
	f, ok := qb.bus[q.Type()]
	qb.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("%w (%T)", ErrUnimplementedHandler, q)
	}
	return f(ctx, q)
}

type EventBus struct {
	logger *slog.Logger
	bus    map[EventType][]EventHandlerFunc
	mu     sync.RWMutex
}

func NewEventBus() *EventBus {
	return &EventBus{
		logger: slog.Default(),
		bus:    make(map[EventType][]EventHandlerFunc),
		mu:     sync.RWMutex{},
	}
}

func (eb *EventBus) WithLogger(logger *slog.Logger) *EventBus {
	if logger != nil {
		eb.logger = logger
	}
	return eb
}

// Types implements [EventHandlerPort].
func (eb *EventBus) Types() []EventType {
	eb.mu.RLock()
	defer eb.mu.RUnlock()
	types := make([]EventType, 0, len(eb.bus))
	for t := range eb.bus {
		types = append(types, t)
	}
	return types
}

// HandleFunc implements [EventBusPort].
func (eb *EventBus) HandleFunc(t EventType, f EventHandlerFunc) {
	if f == nil {
		eb.logger.
			With(slog.String("event_type", t.String())).
			Debug("unimplemented EventHandlerFunc")
		return
	}
	eb.mu.Lock()
	defer eb.mu.Unlock()
	eb.bus[t] = append(eb.bus[t], func(ctx context.Context, e Event) (err error) {
		defer func() {
			if r := recover(); r != nil {
				eb.logger.With(
					slog.String("trace_id", Trace(ctx).String()),
					slog.String("event_type", e.Type().String()),
					slog.Any("panic", r),
				).Error("panic in event handler func")
				err = fmt.Errorf("%w (%v)", ErrPanicRecovered, r)
			}
		}()
		return f(ctx, e)
	})
}

// Handle implements [EventHandler].
func (eb *EventBus) Handle(ctx context.Context, e Event) error {
	eb.mu.RLock()
	handlers := eb.bus[e.Type()]
	eb.mu.RUnlock()
	if len(handlers) == 0 {
		eb.logger.With(
			slog.String("trace_id", Trace(ctx).String()),
			slog.String("event_type", e.Type().String()),
		).Debug("no handlers registered for event")
		return nil
	}
	var errs []error
	for _, f := range handlers {
		if err := f(ctx, e); err != nil {
			eb.logger.With(
				slog.String("trace_id", Trace(ctx).String()),
				slog.String("event_type", e.Type().String()),
				slog.Any("error", err),
			).Error("failed to handle event")
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}
