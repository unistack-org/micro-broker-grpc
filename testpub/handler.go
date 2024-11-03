package main

import (
	"fmt"

	"go.unistack.org/micro/v3/broker"
	"go.unistack.org/micro/v3/logger"
)

type Handler struct {
	Log logger.Logger
}

func (h *Handler) Handle(e broker.Event) error {
	h.Log.Info(e.Context(), fmt.Sprintf("Message from %s with value: %v | error: %s",
		e.Topic(),
		e.Message(),
		e.Error(),
	))
	return nil
}
