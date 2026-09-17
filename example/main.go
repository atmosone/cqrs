package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/atmosone/cqrs"
	"github.com/google/uuid"
)

func main() {
	ctx, cancel := signal.NotifyContext(
		context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	logger := slog.New(slog.NewTextHandler(
		os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug},
	)).With(slog.String("app", "test-to-do-app"))
	repository := NewRepository()
	// application setup
	app := cqrs.New(
		cqrs.NewCommandBus().WithLogger(logger),
		cqrs.NewQueryBus().WithLogger(logger),
		cqrs.NewEventBus().WithLogger(logger)).WithLogger(logger)
	app.HandleCommand(CommandTypeCreateNote,
		CreateNote(repository))
	app.HandleQuery(QueryTypeGetNote,
		GetNote(repository))
	// controller setup
	controller := cqrs.NewController(
		cqrs.NewCommandController(app.CommandBus(), commandDecoders()).WithLogger(logger),
		cqrs.NewEventController(app.EventBus(), eventEncoders()).WithLogger(logger),
		cqrs.NewQueryController(app.QueryBus(), queryCodecs()).WithLogger(logger))
	// start server
	server := &http.Server{
		Addr:    ":44044",
		Handler: controller,
	}
	go func() {
		if err := server.ListenAndServe(); err != nil &&
			!errors.Is(err, http.ErrServerClosed) {
			panic(err)
		}
	}()
	<-ctx.Done()
	if err := controller.Close(); err != nil {
		logger.Error("failed to close controller",
			slog.Any("error", err))
	}
	if err := server.Shutdown(context.TODO()); err != nil {
		logger.Error("failed to shutdown server",
			slog.Any("error", err))
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
			ID:        uuid.NewString(),
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
	fmt.Println("note saved", cqrs.Trace(ctx).String())
	return nil
}

func NewRepository() *Repository {
	return &Repository{
		store: make(map[string]Note),
		mu:    sync.RWMutex{},
	}
}

// API LAYER

type CreateNoteCommandDecoder struct{}

func NewCreateNoteCommandDecoder() cqrs.CommandDecoder {
	return &CreateNoteCommandDecoder{}
}

// CommandContentType implements [cqrs.CommandDecoder].
func (c *CreateNoteCommandDecoder) CommandContentType() string { return "application/json" }

// DecodeCommand implements [cqrs.CommandDecoder].
func (c *CreateNoteCommandDecoder) DecodeCommand(data []byte) (cqrs.Command, error) {
	var cmd CreateNoteCommand
	if err := json.Unmarshal(data, &cmd); err != nil {
		return nil, err
	}
	return cmd, nil
}

func commandDecoders() map[cqrs.CommandType]cqrs.CommandDecoder {
	return map[cqrs.CommandType]cqrs.CommandDecoder{
		CommandTypeCreateNote: NewCreateNoteCommandDecoder(),
	}
}

type GetNoteQueryCodec struct{}

// Decode implements [cqrs.QueryCodec].
func (c *GetNoteQueryCodec) Decode(data []byte) (cqrs.Query, error) {
	query, err := url.ParseQuery(string(data))
	if err != nil {
		return nil, err
	}
	return GetNoteQuery{NoteID: query.Get("id")}, nil
}

// Encode implements [cqrs.QueryCodec].
func (c *GetNoteQueryCodec) Encode(result cqrs.Result) ([]byte, error) {
	return json.Marshal(result)
}

// ResultContentType implements [cqrs.QueryCodec].
func (c *GetNoteQueryCodec) ResultContentType() string { return "application/json" }

func NewGetNoteQueryCodec() cqrs.QueryCodec {
	return &GetNoteQueryCodec{}
}

func queryCodecs() map[cqrs.QueryType]cqrs.QueryCodec {
	return map[cqrs.QueryType]cqrs.QueryCodec{
		QueryTypeGetNote: NewGetNoteQueryCodec(),
	}
}

type NoteCreatedEventEncoder struct{}

// Encode implements [cqrs.EventEncoder].
func (n *NoteCreatedEventEncoder) Encode(event cqrs.Event) ([]byte, error) {
	return json.Marshal(event)
}

// EventContentType implements [cqrs.EventEncoder].
func (n *NoteCreatedEventEncoder) EventContentType() string {
	return "application/json"
}

func NewNoteCreatedEventEncoder() cqrs.EventEncoder {
	return &NoteCreatedEventEncoder{}
}

func eventEncoders() map[cqrs.EventType]cqrs.EventEncoder {
	return map[cqrs.EventType]cqrs.EventEncoder{
		EventTypeNoteCreated: NewNoteCreatedEventEncoder(),
	}
}
