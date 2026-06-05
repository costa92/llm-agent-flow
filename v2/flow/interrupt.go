package flow

import (
	"encoding/json"
	"errors"
)

// InterruptRequest is the human-interaction payload a node attaches when
// it suspends the run for human input.
type InterruptRequest struct {
	Kind   string          `json:"kind,omitempty"`   // "approval"|"input"|"confirm_dangerous"
	Prompt string          `json:"prompt,omitempty"`
	Schema json.RawMessage `json:"schema,omitempty"` // advisory at MVP
}

// InterruptError is the sentinel a node's Run returns to request a human
// interrupt. The engine detects it via errors.As on the checkpoint path.
type InterruptError struct{ Request InterruptRequest }

func (e *InterruptError) Error() string { return "flow: node requested human interrupt" }

// Interrupt constructs an InterruptError for a node to return from Run.
func Interrupt(req InterruptRequest) error { return &InterruptError{Request: req} }

// InterruptCapable is the optional capability a NodeKind declares to be
// allowed to interrupt. A node that returns an InterruptError without
// implementing this (and returning true) fails the run with
// ErrUninstrumentedInterrupt — this keeps the compile-time
// "one interrupt per layer" static check honest.
type InterruptCapable interface {
	NodeKind
	CanInterrupt() bool
}

var (
	ErrUninstrumentedInterrupt = errors.New("flow: node returned InterruptError but is not InterruptCapable")
)
