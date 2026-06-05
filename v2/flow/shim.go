package flow

import (
	"context"
	"fmt"
)

// RunString is the v0.1-compatible string convenience entry point. It
// boxes the caller's string inputs into the any carrier, runs the v2
// engine, then unboxes the any outputs back to string — with zero
// behavior difference from the any path. This is the migration entry
// for v0.1 (map[string]string) callers.
//
// An output value that is not a string (because a node produced a
// structured Go value) is an error: the string shim cannot represent it.
func (e *Engine) RunString(ctx context.Context, inputs map[string]string) (map[string]string, error) {
	out, err := e.Run(ctx, boxStrMap(inputs))
	if err != nil {
		return nil, err
	}
	return unboxStrMap(out)
}

// RunStreamString is the string-keyed streaming shim. Inputs are boxed
// to any; the emitted Events carry any-typed maps unchanged (the engine
// event shape is any-native), so this differs from RunStream only in the
// input boxing.
func (e *Engine) RunStreamString(ctx context.Context, inputs map[string]string) (<-chan Event, error) {
	return e.RunStream(ctx, boxStrMap(inputs))
}

func boxStrMap(in map[string]string) map[string]any {
	if in == nil {
		return nil
	}
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func unboxStrMap(in map[string]any) (map[string]string, error) {
	out := make(map[string]string, len(in))
	for k, v := range in {
		s, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("flow: run: output %q is %T, not string — use the any-typed Run for structured outputs", k, v)
		}
		out[k] = s
	}
	return out, nil
}
