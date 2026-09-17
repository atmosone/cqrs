package cqrs

import (
	"context"
)

type TraceID string

func (tid TraceID) String() string { return string(tid) }

type tracingKey string

const tracingKeyValue tracingKey = "cqrs_trace_id"

func Trace(ctx context.Context) TraceID {
	value := ctx.Value(tracingKeyValue)
	traceID, ok := value.(TraceID)
	if !ok {
		return TraceID("")
	}
	return traceID
}

type CommandDecoder interface {
	DecodeCommand([]byte) (Command, error)
	CommandContentType() string
}

type QueryCodec interface {
	Decode([]byte) (Query, error)
	Encode(Result) ([]byte, error)
	ResultContentType() string
}

type EventEncoder interface {
	Encode(Event) ([]byte, error)
	EventContentType() string
}

type ResultEncoder interface {
	Encode(Result) ([]byte, error)
}

type envelope struct {
	id    string
	event string
	data  string
}
