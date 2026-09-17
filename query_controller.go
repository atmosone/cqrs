package cqrs

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

type QueryController struct {
	logger  *slog.Logger
	codecs  map[QueryType]QueryCodec
	bus     QueryBusPort
	mux     *http.ServeMux
	closed  bool
	traffic int64
	mu      sync.RWMutex
}

func NewQueryController(bus QueryBusPort, codecs map[QueryType]QueryCodec) *QueryController {
	if codecs == nil {
		codecs = make(map[QueryType]QueryCodec)
	}
	controller := &QueryController{
		logger: slog.Default(),
		codecs: codecs,
		bus:    bus,
		mux:    http.NewServeMux(),
		closed: false,
		mu:     sync.RWMutex{},
	}
	controller.mux.HandleFunc("OPTIONS /", controller.Types)
	controller.mux.HandleFunc("GET /{query}", controller.Queries)
	controller.mux.HandleFunc("GET /metrics", controller.Metrics)
	return controller
}

func (c *QueryController) WithLogger(logger *slog.Logger) *QueryController {
	if logger != nil {
		c.logger = logger
	}
	return c
}

func (c *QueryController) isClosed() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.closed
}

// Close implements [QueryController].
func (c *QueryController) Close() error {
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()
	return nil
}

// ServeHTTP implements [QueryController].
func (c *QueryController) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	withServerHeader(withTracing(c.mux)).ServeHTTP(w, r)
}

func (c *QueryController) Metrics(w http.ResponseWriter, r *http.Request) {
	c.mu.RLock()
	metrics := map[string]any{
		"total_bytes_received": c.traffic,
		"timestamp":            time.Now(),
	}
	c.mu.RUnlock()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(metrics)
}

func (c *QueryController) Types(w http.ResponseWriter, r *http.Request) {
	c.mu.RLock()
	types := make(map[QueryType]string, len(c.codecs))
	for queryType, queryCodec := range c.codecs {
		types[queryType] = queryCodec.ResultContentType()
	}
	c.mu.RUnlock()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(types)
}

func (c *QueryController) Queries(w http.ResponseWriter, r *http.Request) {
	if c.isClosed() {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	log := c.logger.With(
		slog.String("method", r.Method),
		slog.String("path", r.URL.Path),
		slog.String("trace_id", Trace(r.Context()).String()),
		slog.String("query_type", r.PathValue("query")),
	)
	codec, ok := c.codecs[QueryType(r.PathValue("query"))]
	if !ok {
		log.Debug("unknown query type")
		http.Error(w, "unknown query type", http.StatusNotImplemented)
		return
	}
	query, err := codec.Decode([]byte(r.URL.RawQuery))
	if err != nil {
		log.With(slog.Any("error", err)).Warn("failed to decode query")
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	result, err := c.bus.Handle(r.Context(), query)
	if err != nil {
		log.With(
			slog.Any("error", err),
		).Error("failed to handle query")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	data, err := codec.Encode(result)
	if err != nil {
		log.With(slog.Any("error", err)).Warn("failed to encode result")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", codec.ResultContentType())
	w.WriteHeader(http.StatusOK)
	traffic, err := w.Write(data)
	if err != nil {
		log.With(slog.Any("error", err)).Error("failed to write data")
		return
	}
	c.mu.Lock()
	c.traffic += int64(traffic)
	c.mu.Unlock()
}
