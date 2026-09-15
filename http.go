package cqrs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"

	"github.com/google/uuid"
)

type ControllerConfig struct {
	MaxEventBuffer     int
	MaxConcurrentTasks int
}

func DefaultControllerConfig() ControllerConfig {
	return ControllerConfig{
		MaxEventBuffer:     10,
		MaxConcurrentTasks: 50,
	}
}

type Controller struct {
	logger  *slog.Logger
	app     *App
	mux     *http.ServeMux
	codec   Codec
	streams map[chan envelope]struct{}
	wg      sync.WaitGroup
	queue   int
	closed  bool
	config  ControllerConfig
	mu      sync.RWMutex
}

func (c *Controller) WithConfig(config ControllerConfig) *Controller {
	if config.MaxConcurrentTasks > 0 {
		c.config.MaxConcurrentTasks = config.MaxConcurrentTasks
	}
	if config.MaxEventBuffer > 0 {
		c.config.MaxEventBuffer = config.MaxEventBuffer
	}
	return c
}

func NewController(logger *slog.Logger, app *App, codec Codec) *Controller {
	if logger == nil {
		logger = slog.Default()
	}
	if app == nil {
		panic("unimplemented app")
	}
	if codec == nil {
		panic("unimplemented codec")
	}
	c := &Controller{
		logger:  logger,
		app:     app,
		codec:   codec,
		streams: make(map[chan envelope]struct{}),
		wg:      sync.WaitGroup{},
		queue:   0,
		closed:  false,
		config:  DefaultControllerConfig(),
		mu:      sync.RWMutex{},
	}
	mux := http.NewServeMux()
	mux.HandleFunc("OPTIONS /events", c.Events)
	mux.HandleFunc("OPTIONS /queries", c.Queries)
	mux.HandleFunc("OPTIONS /commands", c.Commands)
	mux.HandleFunc("GET /events", c.openedOnly(c.ServeEvents))
	mux.HandleFunc("GET /queries/{query}", c.ServeQueries)
	mux.HandleFunc("POST /commands/{command}", c.taskLimiter(c.ServeCommands))
	c.mux = mux
	return c
}

func (c *Controller) Streams() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.streams)
}

func (c *Controller) Queue() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.queue
}

func (c *Controller) broadcast(ctx context.Context, event Event) {
	data, err := c.codec.EncodeEvent(event)
	if err != nil {
		c.logger.With(
			slog.String("trace_id", Trace(ctx).String()),
			slog.String("event_type", event.Type().String()),
			slog.Any("error", err),
		).Error("failed to encode event")
		return
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	for stream := range c.streams {
		select {
		case stream <- envelope{
			id:    Trace(ctx).String(),
			event: event.Type().String(),
			data:  string(data),
		}:
		default:
			c.logger.With(
				slog.String("trace_id", Trace(ctx).String()),
				slog.String("event_type", event.Type().String()),
			).Warn("event stream buffer overflowed")
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
	body, err := io.ReadAll(r.Body)
	if err != nil {
		c.logger.With(
			slog.String("trace_id", Trace(r.Context()).String()),
			slog.Any("error", err),
		).Warn("failed to read request body")
		c.decrementQueue()
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	defer r.Body.Close()
	command, err := c.codec.DecodeCommand(CommandType(r.PathValue("command")), body)
	if err != nil {
		c.logger.With(
			slog.String("trace_id", Trace(r.Context()).String()),
			slog.Any("error", err),
		).Warn("failed to decode command")
		c.decrementQueue()
		if errors.Is(err, ErrUnknownCommandType) {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusAccepted)
	c.wg.Add(1)

	go func(ctx context.Context) {
		defer func() {
			c.decrementQueue()
			c.wg.Done()
		}()
		events, err := c.app.CommandHandler().Handle(ctx, command)
		if err != nil {
			c.logger.
				With(slog.String("trace_id", Trace(ctx).String()),
					slog.Any("command_type", command.Type().String()),
					slog.Any("error", err),
				).Error("failed to handle command")
		}
		for _, event := range events {
			c.broadcast(ctx, event)
		}
	}(context.WithoutCancel(r.Context()))
}

func (c *Controller) ServeQueries(w http.ResponseWriter, r *http.Request) {
	query, err := c.codec.DecodeQuery(QueryType(r.PathValue("query")), []byte(r.URL.RawQuery))
	if err != nil {
		c.logger.With(
			slog.String("trace_id", Trace(r.Context()).String()),
			slog.Any("error", err),
		).Warn("failed to decode query")
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
		c.logger.With(
			slog.String("trace_id", Trace(r.Context()).String()),
			slog.String("query_type", query.Type().String()),
			slog.Any("error", err),
		).Error("failed to handle query")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	data, err := c.codec.EncodeResult(result)
	if err != nil {
		c.logger.With(
			slog.String("trace_id", Trace(r.Context()).String()),
			slog.String("query_type", query.Type().String()),
			slog.Any("error", err),
		).Error("failed to encode result")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write(data)
}

func (c *Controller) decrementQueue() {
	c.mu.Lock()
	c.queue--
	c.mu.Unlock()
}

func (c *Controller) ServeEvents(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		c.logger.With(
			slog.String("trace_id", Trace(r.Context()).String()),
		).Warn("event streaming unsupported")
		http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	stream := make(chan envelope, c.config.MaxEventBuffer)

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

func (c *Controller) openedOnly(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		c.mu.RLock()
		closed := c.closed
		c.mu.RUnlock()
		if closed {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		next(w, r)
	}
}

func (c *Controller) taskLimiter(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		c.mu.Lock()
		if c.closed {
			c.mu.Unlock()
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		if c.queue >= c.config.MaxConcurrentTasks {
			c.logger.With(
				slog.Int("queue", c.queue),
				slog.Int("limit", c.config.MaxConcurrentTasks),
			).Warn("request was prevented by task limiter")
			c.mu.Unlock()
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		c.queue++
		c.mu.Unlock()
		next(w, r)
	}
}

func withServerHeader(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("Server", "atmosone/cqrs")
		next.ServeHTTP(w, r)
	})
}

func withTracing(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		traceID := TraceID(uuid.NewString())
		w.Header().Add("X-CQRS-Request-ID", traceID.String())
		ctx := context.WithValue(r.Context(), tracingKeyValue, traceID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (c *Controller) Handler() http.Handler {
	return withServerHeader(withTracing((c.mux)))
}

func (c *Controller) Close() {
	c.mu.Lock()
	c.closed = true
	for stream := range c.streams {
		close(stream)
	}
	c.streams = make(map[chan envelope]struct{})
	c.mu.Unlock()

	c.wg.Wait()
}
