package cqrs

import (
	"errors"
	"net/http"
)

type Controller struct {
	mux *http.ServeMux
	cc  *CommandController
	ec  *EventController
	qc  *QueryController
}

func (c *Controller) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	c.mux.ServeHTTP(w, r)
}

func (c *Controller) Close() error {
	var errs []error
	if err := c.cc.Close(); err != nil {
		errs = append(errs, err)
	}
	if err := c.ec.Close(); err != nil {
		errs = append(errs, err)
	}
	if err := c.qc.Close(); err != nil {
		errs = append(errs, err)
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

func NewController(cc *CommandController, ec *EventController, qc *QueryController) *Controller {
	mux := http.NewServeMux()
	mux.Handle("/commands/", http.StripPrefix("/commands", cc))
	mux.Handle("/queries/", http.StripPrefix("/queries", qc))
	mux.Handle("/events/", http.StripPrefix("/events", ec))
	return &Controller{mux: mux, cc: cc, ec: ec, qc: qc}
}
