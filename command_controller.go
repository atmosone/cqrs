package cqrs

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

type CommandController struct {
	logger     *slog.Logger
	decoders   map[CommandType]CommandDecoder
	bus        CommandBusPort
	wg         sync.WaitGroup
	mux        *http.ServeMux
	goroutines int
	closed     bool
	mu         sync.RWMutex
}

func NewCommandController(bus CommandBusPort, decoders map[CommandType]CommandDecoder) *CommandController {
	if decoders == nil {
		decoders = make(map[CommandType]CommandDecoder)
	}
	controller := &CommandController{
		logger:   slog.Default(),
		decoders: decoders,
		bus:      bus,
		wg:       sync.WaitGroup{},
		mux:      http.NewServeMux(),
	}
	controller.mux.HandleFunc("OPTIONS /", controller.Types)
	controller.mux.HandleFunc("POST /{command}", controller.Commands)
	controller.mux.HandleFunc("GET /metrics", controller.Metrics)
	return controller
}

func (c *CommandController) WithLogger(logger *slog.Logger) *CommandController {
	if logger != nil {
		c.logger = logger
	}
	return c
}

func (c *CommandController) isClosed() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.closed
}

func (c *CommandController) Metrics(w http.ResponseWriter, r *http.Request) {
	c.mu.RLock()
	metrics := map[string]any{
		"online":           !c.closed,
		"total_goroutines": c.goroutines,
		"timestamp":        time.Now(),
	}
	c.mu.RUnlock()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(metrics)
}

func (c *CommandController) Types(w http.ResponseWriter, r *http.Request) {
	types := make(map[CommandType]string, len(c.decoders))
	c.mu.RLock()
	for commandType, commandDecoder := range c.decoders {
		types[commandType] = commandDecoder.CommandContentType()
	}
	c.mu.RUnlock()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(types)
}

func (c *CommandController) Commands(w http.ResponseWriter, r *http.Request) {
	if c.isClosed() {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	log := c.logger.With(
		slog.String("method", r.Method),
		slog.String("path", r.URL.Path),
		slog.String("trace_id", Trace(r.Context()).String()),
		slog.String("command_type", r.PathValue("command")),
	)
	decoder, ok := c.decoders[CommandType(r.PathValue("command"))]
	if !ok {
		log.Debug("unknown command type")
		http.Error(w, "unknown command type", http.StatusNotImplemented)
		return
	}
	if contentType := r.Header.Get("Content-Type"); contentType != decoder.CommandContentType() {
		log.With(slog.String("client_content_type", contentType),
			slog.String("server_content_type", decoder.CommandContentType()),
		).Warn("unsupported media type")
		http.Error(w,
			fmt.Sprintf("unsupported media type, expected %s",
				decoder.CommandContentType(),
			),
			http.StatusUnsupportedMediaType,
		)
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		log.With(slog.Any("error", err)).Warn("failed to read body")
		http.Error(w, "failed to read body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()
	command, err := decoder.DecodeCommand(body)
	if err != nil {
		log.With(slog.Any("error", err)).Warn("failed to decode command")
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	c.mu.Lock()
	c.goroutines++
	c.mu.Unlock()
	c.wg.Add(1)
	go func(ctx context.Context) {
		defer func() {
			c.mu.Lock()
			c.goroutines--
			c.mu.Unlock()
			c.wg.Done()
		}()
		if _, err := c.bus.Handle(ctx, command); err != nil {
			c.logger.With(
				slog.String("trace_id", Trace(ctx).String()),
				slog.String("command_type", command.Type().String()),
				slog.Any("error", err),
			).Error("failed to handle command")
			return
		}
		log.Info("successfully handled")
	}(context.WithoutCancel(r.Context()))
	w.WriteHeader(http.StatusAccepted)
	log.Info("accepted")
}

// Close implements [CommandController].
func (c *CommandController) Close() error {
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()
	c.wg.Wait()
	return nil
}

// ServeHTTP implements [CommandController].
func (c *CommandController) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	withServerHeader(withTracing(c.mux)).ServeHTTP(w, r)
}
