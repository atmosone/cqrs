package cqrs

import (
	"context"
	"io"
)

// Command is the interface implemented by types that contains instructions
// for changing the state of the system (creating, updating, or deleting data).
type Command interface {
	Type() CommandType // Type returns [CommandType] value.
}

// CommandHandler is the interface implemented by types that can handle [Command].
type CommandHandler interface {
	// HandleCommand implements [CommandHandlerFunc] but without [Event] collection returning.
	HandleCommand(context.Context, Command) error
}

// CommandRegistry is the interface implemented by types that can register
// [CommandHandlerFunc] for [CommandType] value.
type CommandRegistry interface {
	OnCommand(CommandType, CommandHandlerFunc) // OnCommand registers [CommandHandlerFunc] for [CommandType] value.
}

// commandBusPort is the interface implemented by types that can route [Command] types
// to it's [CommandHandlerFunc] by it's [CommandType] value
// and implementing the [CommandHandler] interface to provide a single entry point for all commands.
type commandBusPort interface {
	io.Closer
	CommandHandler
	CommandRegistry
}

// Query is the interface implemented by types that contains instructions
// for read-only operations without changes in system state.
type Query interface {
	Type() QueryType // Type returns [QueryType] value.
}

// QueryHandler is the interface implemented by types that can handle [Query].
type QueryHandler interface {
	HandleQuery(context.Context, Query) (Result, error) // HandleQuery implements [QueryHandlerFunc].
}

// QueryRegistry is the interface implemented by types that can register
// [QueryHandlerFunc] for [QueryType] value.
type QueryRegistry interface {
	OnQuery(QueryType, QueryHandlerFunc) // OnQuery registers [QueryHandlerFunc] for [QueryType] value.
}

// queryBusPort is the interface implemented by types that can route [Query] types
// to it's [QueryHandlerFunc] by it's [QueryType] value
// and implementing the [QueryHandler] interface to provide a single entry point for all queries.
type queryBusPort interface {
	io.Closer
	QueryHandler
	QueryRegistry
}

// Event is the interface implemented by types that contains data
// describes system state changes.
type Event interface {
	Type() EventType // Type returns [EventType] value.
}

// EventHandler is the interface implemented by types that can handle [Event].
type EventHandler interface {
	HandleEvent(context.Context, Event) error // HandleEvent implements [EventHandlerFunc].
}

// EventRegistry is the interface implemented by types that can register
// [EventHandlerFunc] for [EventType] value.
type EventRegistry interface {
	OnEvent(EventType, EventHandlerFunc) // OnEvent registers [EventHandlerFunc] for [EventType] value.
}

// eventBusPort is the interface implemented by types that can route [Event] types
// to it's [EventHandlerFunc] by it's [EventType] value
// and implementing the [EventHandler] interface to provide a single entry point for all events.
type eventBusPort interface {
	io.Closer
	EventHandler
	EventRegistry
}

// Projection is the interface implemented by types that can collect and transform system [Event]
// or operational data into convenient, denormalized models optimized for fast reading.
// [Projection] is the polymorphic [EventHandler] with own [Event] routing.
type Projection interface {
	EventHandler
	Types() []EventType // Types returns collection of [EventType] which [Projection] can handle.
}

// ProjectionBuilder is the interface implemented by types that can register [Projection] for it's building.
type ProjectionBuilder interface {
	// Build registers [Projection.HandleEvent] method for each in [Projection.Types] [EventType] value.
	Build(Projection)
}

// App is the CQRS application facade interface.
type App interface {
	CommandRegistry
	CommandHandler
	QueryRegistry
	QueryHandler
	EventRegistry
	EventHandler
	EventStreamer
	ProjectionBuilder
	io.Closer
}

// EventStreamer is the interface implemented by types that can stream [Event] via channel.
type EventStreamer interface {
	EventStream(context.Context) (<-chan Event, error)
}

// eventSourcePort is the interface implemented by types that can accept [Event] as [EventHandler] and
// broadcast it as [EventStreamer].
type eventSourcePort interface {
	io.Closer
	EventHandler
	EventStreamer
}
