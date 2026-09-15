package cqrs

import "context"

// CommandType
type CommandType string

func (ct CommandType) String() string { return string(ct) }

// Command
type Command interface{ Type() CommandType }

// CommandHandlerFunc
type CommandHandlerFunc func(context.Context, Command) ([]Event, error)

// CommandHandler
type CommandHandler interface {
	Types() []CommandType
	Handle(context.Context, Command) ([]Event, error)
}

// CommandBus
type CommandBus interface {
	CommandHandler
	HandleFunc(CommandType, CommandHandlerFunc)
}

// QueryType
type QueryType string

func (qt QueryType) String() string { return string(qt) }

// Query
type Query interface{ Type() QueryType }

type Result interface {
	Data() any
	Error() error
}

// QueryHandlerFunc
type QueryHandlerFunc func(context.Context, Query) (Result, error)

// QueryHandler
type QueryHandler interface {
	Types() []QueryType
	Handle(context.Context, Query) (Result, error)
}

// QueryBus
type QueryBus interface {
	QueryHandler
	HandleFunc(QueryType, QueryHandlerFunc)
}

// EventType
type EventType string

func (et EventType) String() string { return string(et) }

// Event
type Event interface {
	Type() EventType
}

// EventHandlerFunc
type EventHandlerFunc func(context.Context, Event) error

// EventHandler
type EventHandler interface {
	Types() []EventType
	Handle(context.Context, Event) error
}

// EventBus
type EventBus interface {
	EventHandler
	HandleFunc(EventType, EventHandlerFunc)
}
