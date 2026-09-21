# cqrs

[![Go Reference](https://pkg.go.dev/badge/github.com/atmosone/cqrs.svg)](https://pkg.go.dev/github.com/atmosone/cqrs)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A lightweight, high-performance, **zero-allocation** in-memory CQRS and Event Streaming toolkit for Go.

Designed with Clean Architecture and DDD principles in mind. It serves as a non-intrusive glue code layer for your domain models without forcing heavy framework dependencies or leaky abstractions into your core logic.

## Highlights

- **Zero Allocations & Zero Copy:** Operates directly on standard Go types and interfaces without internal byte-buffering or JSON wrapping overhead.
- **Zero Dependencies:** Relies purely on the Go Standard Library.
- **Native Event Streaming:** Built-in `EventStreamer` channel abstraction out-of-the-box for Server-Sent Events (SSE), WebSockets, or background workers.
- **Non-Intrusive (Low Lock-In):** Your domain entities, commands, and events remain 100% framework-agnostic. Migrating away or refactoring requires zero changes to core business logic.
- **Thread-Safe & Reliable:** Safe for concurrent use with built-in panic recovery for command/event handlers and slow-consumer handling for event streams.

## Installation

```bash
go get github.com/atmosone/cqrs
```

## Quick Start
1. Define Domain Types
Command, Query, and Event objects are simple Go structs implementing a Type() method.

```go
package main

import (
	"context"
	"time"

	"github.com/atmosone/cqrs"
)

// Command
const CommandTypeCreateNote = cqrs.CommandType("create_note")

type CreateNoteCommand struct {
	Name    string
	Content string
}

func (c CreateNoteCommand) Type() cqrs.CommandType { return CommandTypeCreateNote }

// Event
const EventTypeNoteCreated = cqrs.EventType("note_created")

type NoteCreatedEvent struct {
	ID        string
	Name      string
	CreatedAt time.Time
}

func (e NoteCreatedEvent) Type() cqrs.EventType { return EventTypeNoteCreated }
```

2. Implement Handlers
Handlers are clean, idiomatic Go functions:

```go
func CreateNoteHandler(ctx context.Context, cmd cqrs.Command) ([]cqrs.Event, error) {
	c := cmd.(CreateNoteCommand)

	// ... execute domain logic / persist to repository ...

	event := NoteCreatedEvent{
		ID:        "note-123",
		Name:      c.Name,
		CreatedAt: time.Now(),
	}

	return []cqrs.Event{event}, nil
}
```

3. Bootstrap Application & Serve SSE

```go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/atmosone/cqrs"
)

func main() {
	app := cqrs.New()
	defer app.Close()

	// Register Command Handler
	app.OnCommand(CommandTypeCreateNote, CreateNoteHandler)

	// HTTP Endpoint: Execute Command
	http.HandleFunc("POST /notes", func(w http.ResponseWriter, r *http.Request) {
		var cmd CreateNoteCommand
		if err := json.NewDecoder(r.Body).Decode(&cmd); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if err := app.HandleCommand(r.Context(), cmd); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	// HTTP Endpoint: Stream Real-Time Events via SSE
	http.HandleFunc("GET /events", func(w http.ResponseWriter, r *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "Streaming unsupported", http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")

		stream, err := app.EventStream(r.Context())
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		for event := range stream {
			fmt.Fprintf(w, "event: %s\ndata: ", event.Type().String())
			_ = json.NewEncoder(w).Encode(event)
			fmt.Fprint(w, "\n")
			flusher.Flush()
		}
	})

	http.ListenAndServe(":8080", nil)
}
```

## Projections
Register projections to build read models asynchronously or synchronize state:

```go
type AuditProjection struct{}

func (p *AuditProjection) Types() []cqrs.EventType {
	return []cqrs.EventType{EventTypeNoteCreated}
}

func (p *AuditProjection) HandleEvent(ctx context.Context, event cqrs.Event) error {
	// Update read storage, elasticsearch, cache, etc.
	return nil
}

// Register projection
app.Build(&AuditProjection{})
```

## Benchmarks
Because the library executes direct map lookups and standard Go channel dispatches without intermediate byte allocations:

Command Routing Delay: ~10-15 ns/op
Memory Allocations: 0 B/op (framework core)

License
MIT