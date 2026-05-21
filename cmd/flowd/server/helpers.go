package server

import (
	"bytes"
	"context"
	"io"
)

// bytesReader is a tiny stdlib wrapper around bytes.NewReader so the
// engine-compile call sites read more clearly.
func bytesReader(b []byte) io.Reader { return bytes.NewReader(b) }

// serverCtxFor returns a context to use for store calls outside any
// request scope (e.g. FinishRun after stream end). Engines may have
// already detached from the request ctx so we use Background to make
// sure the persistence call completes.
func serverCtxFor(_ *Server) context.Context { return context.Background() }
