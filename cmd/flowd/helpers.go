package main

import (
	"bytes"
	"io"
)

// bytesReader wraps a []byte as an io.Reader. Defined here so main.go
// reads more clearly at the seedFlow call site.
func bytesReader(b []byte) io.Reader { return bytes.NewReader(b) }
