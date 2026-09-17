package cqrs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

type EventControllerConfig struct {
	MaxEventBufferSize int
}

const closeEvent = "close"

type EventController struct {
	logger     *slog.Logger
	config     EventControllerConfig
	encoders   map[EventType]EventEncoder
	streams    map[chan envelope]TraceID
	closed     bool
	mux        *http.ServeMux
	clientWG   sync.WaitGroup
	serverWG   sync.WaitGroup
	mu         sync.RWMutex
	goroutines int
}

func NewEventController(bus EventBusPort, encoders map[EventType]EventEncoder) *EventController {
	if encoders == nil {
		encoders = make(map[EventType]EventEncoder)
	}
	controller := &EventController{
		logger: slog.Default(),
		config: EventControllerConfig{
			MaxEventBufferSize: 15,
		},
		encoders: encoders,
		streams:  make(map[chan envelope]TraceID),
		closed:   false,
		mux:      http.NewServeMux(),
		clientWG: sync.WaitGroup{},
		serverWG: sync.WaitGroup{},
		mu:       sync.RWMutex{},
	}
	controller.mux.HandleFunc("OPTIONS /", controller.Types)
	controller.mux.HandleFunc("GET /", controller.Events)
	controller.mux.HandleFunc("GET /metrics", controller.Metrics)
	for eventType := range encoders {
		bus.HandleFunc(eventType, controller.broadcast)
	}
	return controller
}

func (c *EventController) WithLogger(logger *slog.Logger) *EventController {
	if logger != nil {
		c.logger = logger
	}
	return c
}

func (c *EventController) WithConfig(config EventControllerConfig) *EventController {
	if config.MaxEventBufferSize > 0 {
		c.config.MaxEventBufferSize = config.MaxEventBufferSize
	}
	return c
}

func (c *EventController) isClosed() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.closed
}

func (c *EventController) Metrics(w http.ResponseWriter, r *http.Request) {
	c.mu.RLock()
	metrics := map[string]any{
		"online":            !c.closed,
		"total_goroutines":  c.goroutines,
		"client_goroutines": len(c.streams),
		"server_goroutines": c.goroutines - len(c.streams),
		"timestamp":         time.Now(),
	}
	c.mu.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(metrics)
}

func (c *EventController) Types(w http.ResponseWriter, r *http.Request) {
	c.mu.RLock()
	types := make(map[EventType]string, len(c.encoders))
	for eventType, eventEncoder := range c.encoders {
		types[eventType] = eventEncoder.EventContentType()
	}
	c.mu.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(types)
}

func (c *EventController) Events(w http.ResponseWriter, r *http.Request) {
	if c.isClosed() {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	log := c.logger.With(
		slog.String("method", r.Method),
		slog.String("path", r.URL.Path),
		slog.String("trace_id", Trace(r.Context()).String()),
	)
	flusher, ok := w.(http.Flusher)
	if !ok {
		log.Warn("event streaming unsupported")
		http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()
	log.Info("successfully connected")

	stream := make(chan envelope, c.config.MaxEventBufferSize)

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.streams[stream] = Trace(r.Context())
	c.goroutines++
	c.clientWG.Add(1)
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		delete(c.streams, stream)
		c.goroutines--
		c.mu.Unlock()
		c.clientWG.Done()
		close(stream)
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
			if envelope.event == closeEvent {
				return
			}
		}
	}
}

func (e *EventController) Close() error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil
	}
	e.closed = true
	e.mu.Unlock()

	e.serverWG.Wait()

	var errs []error
	e.mu.Lock()
	for stream, traceID := range e.streams {
		select {
		case stream <- envelope{
			id:    traceID.String(),
			event: closeEvent,
			data:  "shutdown",
		}:
		default:
			select {
			case <-stream:
			default:
			}
			select {
			case stream <- envelope{
				id:    traceID.String(),
				event: closeEvent,
				data:  "shutdown",
			}:
			default:
				errs = append(errs, fmt.Errorf("force close stream %s", traceID.String()))
			}
		}
	}
	e.mu.Unlock()
	e.clientWG.Wait()
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

func (c *EventController) broadcast(ctx context.Context, event Event) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.goroutines++
	c.serverWG.Add(1)
	c.mu.Unlock()
	go func(ctx context.Context, event Event) {
		defer func() {
			c.mu.Lock()
			c.goroutines--
			c.mu.Unlock()
			c.serverWG.Done()
		}()

		log := c.logger.With(
			slog.String("trace_id", Trace(ctx).String()),
			slog.String("event_type", event.Type().String()),
		)

		c.mu.RLock()
		encoder, ok := c.encoders[event.Type()]
		c.mu.RUnlock()

		if !ok {
			log.Debug("encoder not found, event dropped")
			return
		}

		data, err := encoder.Encode(event)
		if err != nil {
			log.With(slog.Any("error", err)).Error("failed to encode event")
			return
		}

		env := envelope{
			id:    Trace(ctx).String(),
			event: event.Type().String(),
			data:  string(data),
		}

		c.mu.RLock()
		for stream, traceID := range c.streams {
			select {
			case stream <- env:
			default:
				log.With(
					slog.String("stream_trace_id", traceID.String()),
				).Warn("stream buffer overflowed, message dropped")
			}
		}
		c.mu.RUnlock()

		log.Info("successfully broadcasted")
	}(context.WithoutCancel(ctx), event)

	return nil
}

// ServeHTTP implements [EventController].
func (c *EventController) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	withServerHeader(withTracing(c.mux)).ServeHTTP(w, r)
}
