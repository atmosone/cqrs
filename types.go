package cqrs

import (
	"context"
)

// CommandType is the string alias for [Command] type declaration.
type CommandType string

// String returns base [CommandType] string value.
func (ct CommandType) String() string { return string(ct) }

// CommandHandlerFunc is the type for functions that can handle [Command] and return [Event] collection.
// Returns non-nil [error] if [Command] cannot be handled.
type CommandHandlerFunc func(context.Context, Command) ([]Event, error)

// QueryType is the string alias for [Query] type declaration.
type QueryType string

// String returns base [QueryType] string value.
func (qt QueryType) String() string { return string(qt) }

// Result is the interface implemented by types that contains read-only data
// describes current system state.
type Result any

// QueryHandlerFunc is the type for functions that can handle [Query]
// and return [Result].Returns non-nil [error] if [Query] cannot be handled.
type QueryHandlerFunc func(context.Context, Query) (Result, error)

// EventType is the string alias for [Event] type declaration.
type EventType string

// String returns base [EventType] string value.
func (et EventType) String() string { return string(et) }

// EventHandlerFunc is the type for "silent" functions that can handle [Event].
type EventHandlerFunc func(context.Context, Event)
