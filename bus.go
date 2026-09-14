package cqrs

import (
	"context"
	"errors"
	"fmt"
)

var ErrUnimplementedHandler = errors.New("cqrs: unimplemented handler")

type commandBus struct {
	bus map[CommandType]CommandHandlerFunc
}

func NewCommandBus() CommandBus {
	return &commandBus{bus: make(map[CommandType]CommandHandlerFunc)}
}

// Types implements [CommandHandler].
func (cb *commandBus) Types() []CommandType {
	types := make([]CommandType, 0, len(cb.bus))
	for t := range cb.bus {
		types = append(types, t)
	}
	return types
}

// Handle implements [CommandHandler].
func (cb *commandBus) Handle(ctx context.Context, c Command) ([]Event, error) {
	f, ok := cb.bus[c.Type()]
	if !ok {
		return nil, fmt.Errorf("%w (%T)", ErrUnimplementedHandler, c)
	}
	return f(ctx, c)
}

// HandleFunc implements [CommandBus].
func (cb *commandBus) HandleFunc(t CommandType, f CommandHandlerFunc) {
	if f == nil {
		return
	}
	cb.bus[t] = func(ctx context.Context, c Command) ([]Event, error) {
		defer func() {
			if r := recover(); r != nil {
				fmt.Printf("panic in %T handler func: %v\n", c, r)
			}
		}()
		return f(ctx, c)
	}
}

type queryBus struct {
	bus map[QueryType]QueryHandlerFunc
}

func NewQueryBus() QueryBus {
	return &queryBus{bus: make(map[QueryType]QueryHandlerFunc)}
}

// Types implements [QueryHandler].
func (qb *queryBus) Types() []QueryType {
	types := make([]QueryType, 0, len(qb.bus))
	for t := range qb.bus {
		types = append(types, t)
	}
	return types
}

// HandleFunc implements [QueryBus].
func (qb *queryBus) HandleFunc(t QueryType, f QueryHandlerFunc) {
	if f == nil {
		return
	}
	qb.bus[t] = func(ctx context.Context, q Query) (Result, error) {
		defer func() {
			if r := recover(); r != nil {
				fmt.Printf("panic in %T handler func: %v\n", q, r)
			}
		}()
		return f(ctx, q)
	}
}

// Handle implements [QueryHandler].
func (qb *queryBus) Handle(ctx context.Context, q Query) (Result, error) {
	f, ok := qb.bus[q.Type()]
	if !ok {
		return Result{}, fmt.Errorf("%w (%T)", ErrUnimplementedHandler, q)
	}
	return f(ctx, q)
}

type eventBus struct {
	bus map[EventType][]EventHandlerFunc
}

func NewEventBus() EventBus {
	return &eventBus{bus: make(map[EventType][]EventHandlerFunc)}
}

// Types implements [EventHandler].
func (eb *eventBus) Types() []EventType {
	types := make([]EventType, 0, len(eb.bus))
	for t := range eb.bus {
		types = append(types, t)
	}
	return types
}

// HandleFunc implements [EventBus].
func (eb *eventBus) HandleFunc(t EventType, f EventHandlerFunc) {
	if f == nil {
		return
	}
	eb.bus[t] = append(eb.bus[t], func(ctx context.Context, e Event) error {
		defer func() {
			if r := recover(); r != nil {
				fmt.Printf("panic in %T handler func: %v\n", e, r)
			}
		}()
		return f(ctx, e)
	})
}

// Handle implements [EventHandler].
func (eb *eventBus) Handle(ctx context.Context, e Event) error {
	handlers := eb.bus[e.Type()]
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
