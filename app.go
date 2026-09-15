package cqrs

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
)

type App struct {
	logger *slog.Logger
	cb     CommandBus
	eb     EventBus
	qb     QueryBus
}

func NewApp(logger *slog.Logger) *App {
	if logger == nil {
		logger = slog.Default()
	}
	return &App{
		logger: logger,
		cb:     NewCommandBus(logger.With(slog.String("component", "CommandBus"))),
		eb:     NewEventBus(logger.With(slog.String("component", "EventBus"))),
		qb:     NewQueryBus(logger.With(slog.String("component", "QueryBus"))),
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
func (app *App) CommandHandler() CommandHandler              { return app.cb }
func (app *App) QueryHandler() QueryHandler                  { return app.qb }
func (app *App) EventHandler() EventHandler                  { return app.eb }

func (app *App) WithEventBus(eb EventBus) *App {
	if eb == nil {
		app.logger.Debug("unimplemented EventBus")
		return app
	}
	app.eb = eb
	return app
}

func (app *App) WithCommandBus(cb CommandBus) *App {
	if cb == nil {
		app.logger.Debug("unimplemented CommandBus")
		return app
	}
	app.cb = cb
	return app
}

func (app *App) WithQueryBus(qb QueryBus) *App {
	if qb == nil {
		app.logger.Debug("unimplemented QueryBus")
		return app
	}
	app.qb = qb
	return app
}
