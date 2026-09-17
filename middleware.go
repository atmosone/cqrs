package cqrs

import (
	"context"
	"net/http"

	"github.com/google/uuid"
)

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
