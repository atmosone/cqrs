package cqrs

import "context"

// CommandType
type CommandType string

// String
func (ct CommandType) String() string { return string(ct) }

// Command
type Command interface{ Type() CommandType }

// CommandHandlerFunc
type CommandHandlerFunc func(context.Context, Command) ([]Event, error)

// CommandHandler
type CommandHandler interface {
	Handle(context.Context, Command) ([]Event, error)
}

// CommandBus
type CommandBusPort interface {
	CommandHandler
	HandleFunc(CommandType, CommandHandlerFunc)
}

// QueryType
type QueryType string

// String
func (qt QueryType) String() string { return string(qt) }

// Query
type Query interface{ Type() QueryType }

// Result
type Result any

// QueryHandlerFunc
type QueryHandlerFunc func(context.Context, Query) (Result, error)

// QueryHandler
type QueryHandler interface {
	Handle(context.Context, Query) (Result, error)
}

// QueryBus
type QueryBusPort interface {
	QueryHandler
	HandleFunc(QueryType, QueryHandlerFunc)
}

// EventType
type EventType string

// String
func (et EventType) String() string { return string(et) }

// Event
type Event interface {
	Type() EventType
}

// EventHandlerFunc
type EventHandlerFunc func(context.Context, Event) error

// EventHandler
type EventHandler interface {
	Handle(context.Context, Event) error
}

// EventBus
type EventBusPort interface {
	EventHandler
	HandleFunc(EventType, EventHandlerFunc)
}
