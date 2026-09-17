package cqrs

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
)

type App struct {
	logger *slog.Logger
	cb     CommandBusPort
	eb     EventBusPort
	qb     QueryBusPort
}

func New(cb CommandBusPort, qb QueryBusPort, eb EventBusPort) *App {
	return &App{
		logger: slog.Default(),
		cb:     cb,
		eb:     eb,
		qb:     qb,
	}
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
				app.logger.With(
					slog.String("trace_id", Trace(ctx).String()),
					slog.String("command_type", c.Type().String()),
					slog.String("event_type", event.Type().String()),
					slog.Any("error", err),
				).Error("failed to handle event")
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

func (app *App) CommandBus() CommandBusPort { return app.cb }
func (app *App) QueryBus() QueryBusPort     { return app.qb }
func (app *App) EventBus() EventBusPort     { return app.eb }

func (app *App) WithLogger(logger *slog.Logger) *App {
	if logger != nil {
		app.logger = logger
	}
	return app
}
