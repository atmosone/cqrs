package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/atmosone/cqrs"
)

type Codec struct{}

// DecodeCommand implements [cqrs.Codec].
func (c *Codec) DecodeCommand(cqrs.CommandType, []byte) (cqrs.Command, error) {
	panic("unimplemented")
}

// DecodeQuery implements [cqrs.Codec].
func (c *Codec) DecodeQuery(cqrs.QueryType, []byte) (cqrs.Query, error) {
	panic("unimplemented")
}

// EncodeEvent implements [cqrs.Codec].
func (c *Codec) EncodeEvent(cqrs.Event) ([]byte, error) {
	panic("unimplemented")
}

// EncodeResult implements [cqrs.Codec].
func (c *Codec) EncodeResult(cqrs.Result) ([]byte, error) {
	panic("unimplemented")
}

func NewCodec() cqrs.Codec {
	return &Codec{}
}

const CommandTypeCreateNote cqrs.CommandType = "create_note"

type CreateNoteCommand struct{}

func (c CreateNoteCommand) Type() cqrs.CommandType { return CommandTypeCreateNote }

const EventTypeNoteCreated cqrs.EventType = "note_created"

type NoteCreatedEvent struct{}

func (e NoteCreatedEvent) Type() cqrs.EventType { return EventTypeNoteCreated }
func (e NoteCreatedEvent) String() string       { return "" }

const QueryTypeNotes cqrs.QueryType = "notes"

type NotesQuery struct{}

func (q NotesQuery) Type() cqrs.QueryType { return QueryTypeNotes }

func CreateNote() cqrs.CommandHandlerFunc {
	return func(ctx context.Context, c cqrs.Command) ([]cqrs.Event, error) {
		cmd := c.(CreateNoteCommand)
		fmt.Println(cmd.Type())
		return []cqrs.Event{
			NoteCreatedEvent{},
		}, nil
	}
}

func OnNoteCreated() cqrs.EventHandlerFunc {
	return func(ctx context.Context, e cqrs.Event) error {
		event := e.(NoteCreatedEvent)
		fmt.Println(event.Type())
		return nil
	}
}

func Notes() cqrs.QueryHandlerFunc {
	return func(ctx context.Context, q cqrs.Query) (cqrs.Result, error) {
		query := q.(NotesQuery)
		fmt.Println(query.Type())
		return nil, nil
	}
}

func main() {
	app := cqrs.NewApp(slog.Default())
	app.HandleCommand(CommandTypeCreateNote, CreateNote())
	app.HandleEvent(EventTypeNoteCreated, OnNoteCreated())
	app.HandleQuery(QueryTypeNotes, Notes())
	controller := cqrs.NewController(slog.Default(), app, NewCodec())
	defer controller.Close()
	server := &http.Server{
		Addr:    ":8090",
		Handler: controller.Handler(),
	}
	if err := server.ListenAndServe(); err != nil &&
		errors.Is(err, http.ErrServerClosed) {
		panic(err)
	}
}
