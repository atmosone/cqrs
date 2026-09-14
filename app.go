package cqrs

import (
	"context"
	"errors"
	"fmt"
)

var (
	ErrEventHandler = errors.New("cqrs: event handling failed")
)

type App struct {
	cb CommandBus
	eb EventBus
	qb QueryBus
}

func NewApp() *App {
	return &App{cb: NewCommandBus(), eb: NewEventBus(), qb: NewQueryBus()}
}

func (app *App) HandleCommand(t CommandType, f CommandHandlerFunc) {
	app.cb.HandleFunc(t, func(ctx context.Context, c Command) ([]Event, error) {
		events, err := f(ctx, c)
		if err != nil {
			return nil, err
		}
		if len(events) == 0 {
			return nil, nil
		}
		var errs []error
		for _, event := range events {
			if err := app.eb.Handle(ctx, event); err != nil {
				errs = append(errs, fmt.Errorf("%w (%s)", ErrEventHandler, err.Error()))
			}
		}
		if len(errs) > 0 {
			return events, errors.Join(errs...)
		}
		return events, nil
	})
}

func (app *App) HandleQuery(t QueryType, f QueryHandlerFunc) { app.qb.HandleFunc(t, f) }

func (app *App) HandleEvent(t EventType, f EventHandlerFunc) { app.eb.HandleFunc(t, f) }

func (app *App) CommandHandler() CommandHandler { return app.cb }
func (app *App) QueryHandler() QueryHandler     { return app.qb }
func (app *App) EventHandler() EventHandler     { return app.eb }
