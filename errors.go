package cqrs

import "errors"

var (
	ErrEventHandler         = errors.New("cqrs: failed to handle event")
	ErrUnknownCommandType   = errors.New("cqrs: unknown command type")
	ErrUnknownQueryType     = errors.New("cqrs: unknown query type")
	ErrUnimplementedHandler = errors.New("cqrs: unimplemented handler")
	ErrPanicRecovered       = errors.New("cqrs: panic recovered")
)
