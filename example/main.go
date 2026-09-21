package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/atmosone/cqrs"
)

func main() {
	ctx, cancel := signal.NotifyContext(
		context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	repository := NewRepository()
	app := cqrs.New()
	app.OnCommand(CommandTypeCreateNote,
		TraceCommand(CreateNote(repository)))
	app.OnQuery(QueryTypeGetNote,
		TraceQuery(GetNote(repository)))
	http.HandleFunc("GET /api/events", HttpEvents(app))
	http.HandleFunc("GET /api/queries/get_note", HttpGetNote(app))
	http.HandleFunc("POST /api/commands/create_note", HttpCreateNote(app))
	go func() {
		if err := http.ListenAndServe(":8091", nil); err != nil &&
			!errors.Is(err, http.ErrServerClosed) {
			panic(err)
		}
	}()
	<-ctx.Done()
	app.Close()
}

func TraceQuery(next cqrs.QueryHandlerFunc) cqrs.QueryHandlerFunc {
	return func(ctx context.Context, q cqrs.Query) (cqrs.Result, error) {
		return next(context.WithValue(ctx, "trace_id", "trace_query"), q)
	}
}

func TraceCommand(next cqrs.CommandHandlerFunc) cqrs.CommandHandlerFunc {
	return func(ctx context.Context, c cqrs.Command) ([]cqrs.Event, error) {
		return next(context.WithValue(ctx, "trace_id", "trace_command"), c)
	}
}

const CommandTypeCreateNote = cqrs.CommandType("create_note")

type CreateNoteCommand struct {
	NoteName    string `json:"note_name"`
	NoteContent string `json:"note_content"`
}

func (c CreateNoteCommand) Type() cqrs.CommandType { return CommandTypeCreateNote }

type CreateNoteRepositoryPort interface {
	SaveNote(context.Context, Note) error
}

func CreateNote(repository CreateNoteRepositoryPort) cqrs.CommandHandlerFunc {
	return func(ctx context.Context, c cqrs.Command) ([]cqrs.Event, error) {
		cmd := c.(CreateNoteCommand)
		note := Note{
			ID:        "new note",
			Name:      cmd.NoteName,
			Content:   cmd.NoteContent,
			CreatedAt: time.Now(),
		}
		if err := repository.SaveNote(ctx, note); err != nil {
			return nil, err
		}
		return []cqrs.Event{
			NoteCreatedEvent{
				NoteID:        note.ID,
				NoteName:      note.Name,
				NoteContent:   note.Content,
				NoteCreatedAt: note.CreatedAt,
			},
		}, nil
	}
}

// MODEL LAYER (DOMAIN)

type Note struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Content   string    `json:"content"`
	CreatedAt time.Time `json:"created_at"`
}

// APPLICATION LAYER

const QueryTypeGetNote = cqrs.QueryType("get_note")

type GetNoteQuery struct {
	NoteID string `json:"note_id"`
}

func (q GetNoteQuery) Type() cqrs.QueryType { return QueryTypeGetNote }

type GetNoteResult struct {
	Data Note `json:"data"`
}

type GetNoteRepositoryPort interface {
	Note(context.Context, string) (Note, error)
}

func GetNote(repository GetNoteRepositoryPort) cqrs.QueryHandlerFunc {
	return func(ctx context.Context, q cqrs.Query) (cqrs.Result, error) {
		query := q.(GetNoteQuery)
		note, err := repository.Note(ctx, query.NoteID)
		if err != nil {
			return nil, err
		}
		return GetNoteResult{Data: note}, nil
	}
}

const EventTypeNoteCreated = cqrs.EventType("note_created")

type NoteCreatedEvent struct {
	NoteID        string    `json:"note_id"`
	NoteName      string    `json:"note_name"`
	NoteContent   string    `json:"note_content"`
	NoteCreatedAt time.Time `json:"note_created_at"`
}

func (e NoteCreatedEvent) Type() cqrs.EventType { return EventTypeNoteCreated }

// REPOSITORY LAYER

type Repository struct {
	store map[string]Note
	mu    sync.RWMutex
}

// Note implements [GetNoteRepositoryPort].
func (r *Repository) Note(ctx context.Context, noteID string) (Note, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	note, ok := r.store[noteID]
	if !ok {
		return Note{}, errors.New("note not found")
	}
	return note, nil
}

// SaveNote implements [CreateNoteRepositoryPort].
func (r *Repository) SaveNote(ctx context.Context, note Note) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.store[note.ID] = note
	fmt.Println("note saved", note.ID)
	return nil
}

func NewRepository() *Repository {
	return &Repository{
		store: make(map[string]Note),
		mu:    sync.RWMutex{},
	}
}

// API LAYER

func HttpCreateNote(handler cqrs.CommandHandler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var cmd CreateNoteCommand
		if err := json.NewDecoder(r.Body).Decode(&cmd); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if err := handler.HandleCommand(r.Context(), cmd); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}
}

func HttpGetNote(handler cqrs.QueryHandler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		res, err := handler.HandleQuery(r.Context(), GetNoteQuery{
			NoteID: r.URL.Query().Get("note_id"),
		})
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(res)
	}
}

func HttpEvents(handler cqrs.EventStreamer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "Streaming unsupported!", http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.WriteHeader(http.StatusOK)
		flusher.Flush()

		stream, err := handler.EventStream(r.Context())
		if err != nil {
			if errors.Is(err, io.ErrClosedPipe) {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		for event := range stream {
			fmt.Fprintf(w, "event: %s\n", event.Type().String())
			fmt.Fprint(w, "data: ")
			if err := json.NewEncoder(w).Encode(event); err != nil {
				return
			}
			fmt.Fprint(w, "\n")
			flusher.Flush()
		}
	}
}
