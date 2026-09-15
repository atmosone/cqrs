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
	DecodeCommand(CommandType, []byte) (Command, error)
}

type QueryDecoder interface {
	DecodeQuery(QueryType, []byte) (Query, error)
}

type EventEncoder interface {
	EncodeEvent(Event) ([]byte, error)
}

type ResultEncoder interface {
	EncodeResult(Result) ([]byte, error)
}

type Decoder interface {
	CommandDecoder
	QueryDecoder
}

type Encoder interface {
	EventEncoder
	ResultEncoder
}

type Codec interface {
	Decoder
	Encoder
}

type envelope struct {
	id    string
	event string
	data  string
}
