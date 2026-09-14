package cqrs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"

	"github.com/google/uuid"
)

type TraceID string

func (tid TraceID) String() string { return string(tid) }

var (
	ErrUnknownCommandType = errors.New("cqrs: unknown command type")
	ErrUnknownQueryType   = errors.New("cqrs: unknown query type")
)

type Decoder interface {
	DecodeCommand(CommandType, *http.Request) (Command, error)
	DecodeQuery(QueryType, *http.Request) (Query, error)
}

type envelope struct {
	id    string
	event string
	data  string
}

type Controller struct {
	logger  *slog.Logger
	app     *App
	mux     *http.ServeMux
	decoder Decoder
	streams map[chan envelope]struct{}
	mu      sync.RWMutex
}

func (c *Controller) broadcast(ctx context.Context, event Event) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for stream := range c.streams {
		select {
		case stream <- envelope{
			id:    Trace(ctx).String(),
			event: event.Type().String(),
			data:  event.String(),
		}:
		default:
			continue
		}
	}
}

func (c *Controller) Commands(w http.ResponseWriter, r *http.Request) {
	w.Header().Add("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(c.app.CommandHandler().Types())
}

func (c *Controller) Queries(w http.ResponseWriter, r *http.Request) {
	w.Header().Add("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(c.app.QueryHandler().Types())
}

func (c *Controller) Events(w http.ResponseWriter, r *http.Request) {
	w.Header().Add("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(c.app.EventHandler().Types())
}

func (c *Controller) ServeCommands(w http.ResponseWriter, r *http.Request) {
	command, err := c.decoder.DecodeCommand(CommandType(r.PathValue("command")), r)
	if err != nil {
		if errors.Is(err, ErrUnknownCommandType) {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusAccepted)
	go func(ctx context.Context) {
		events, err := c.app.CommandHandler().Handle(ctx, command)
		if err != nil {
			c.logger.
				With(slog.String("trace_id", Trace(ctx).String())).
				Error("unexpected", slog.Any("error", err))
		}
		for _, event := range events {
			c.broadcast(ctx, event)
		}
	}(context.WithoutCancel(r.Context()))
}

func (c *Controller) ServeQueries(w http.ResponseWriter, r *http.Request) {
	query, err := c.decoder.DecodeQuery(QueryType(r.PathValue("query")), r)
	if err != nil {
		if errors.Is(err, ErrUnknownQueryType) {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	result, err := c.app.QueryHandler().Handle(r.Context(), query)
	if err != nil {
		if errors.Is(err, ErrUnimplementedHandler) {
			http.Error(w, err.Error(), http.StatusNotImplemented)
			return
		}
		c.logger.
			With(slog.String("trace_id", Trace(r.Context()).String())).
			Error("unexpected", slog.Any("error", err))
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header().Add("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(result)
}

func (c *Controller) ServeEvents(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	stream := make(chan envelope, 10)

	c.mu.Lock()
	c.streams[stream] = struct{}{}
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		delete(c.streams, stream)
		c.mu.Unlock()
	}()

	for {
		select {
		case <-r.Context().Done():
			return
		case envelope, ok := <-stream:
			if !ok {
				return
			}
			fmt.Fprintf(w, "id: %s\n", envelope.id)
			fmt.Fprintf(w, "event: %s\n", envelope.event)
			fmt.Fprintf(w, "data: %s\n\n", envelope.data)
			flusher.Flush()
		}
	}
}

func withServerHeader(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("Server", "atmosone/cqrs")
		next.ServeHTTP(w, r)
	})
}

type tracingKey string

const tracingKeyValue tracingKey = "cqrs_trace_id"

func withTracing(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		traceID := TraceID(uuid.NewString())
		w.Header().Add("X-CQRS-Request-ID", traceID.String())
		ctx := context.WithValue(r.Context(), tracingKeyValue, traceID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func Trace(ctx context.Context) TraceID {
	value := ctx.Value(tracingKeyValue)
	traceID, ok := value.(TraceID)
	if !ok {
		return TraceID("")
	}
	return traceID
}

func (c *Controller) Handler() http.Handler {
	return withServerHeader(withTracing(c.mux))
}

func NewController(logger *slog.Logger, app *App, decoder Decoder) *Controller {
	c := &Controller{
		logger:  logger,
		app:     app,
		decoder: decoder,
		streams: make(map[chan envelope]struct{}),
		mu:      sync.RWMutex{},
	}
	mux := http.NewServeMux()
	mux.HandleFunc("OPTIONS /events", c.Events)
	mux.HandleFunc("OPTIONS /queries", c.Queries)
	mux.HandleFunc("OPTIONS /commands", c.Commands)
	mux.HandleFunc("GET /events", c.ServeEvents)
	mux.HandleFunc("GET /queries/{query}", c.ServeQueries)
	mux.HandleFunc("POST /commands/{command}", c.ServeCommands)
	c.mux = mux
	return c
}

func (c *Controller) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for stream := range c.streams {
		close(stream)
	}
	c.streams = make(map[chan envelope]struct{})
}
