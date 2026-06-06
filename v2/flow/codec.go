package flow

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
)

var (
	codecMu     sync.RWMutex
	codecByName = map[string]reflect.Type{}
	codecByType = map[reflect.Type]string{}
)

// RegisterCodec registers a Go type T under name for checkpoint port-value
// serialization. Idempotent re-registration of the same (name,type) is OK;
// a conflicting re-registration is ignored (last-wins is avoided to keep
// global state stable across test packages).
func RegisterCodec[T any](name string) {
	t := reflect.TypeFor[T]()
	codecMu.Lock()
	defer codecMu.Unlock()
	if existing, ok := codecByName[name]; ok && existing != t {
		return // conflicting re-registration: keep the first
	}
	if existing, ok := codecByType[t]; ok && existing != name {
		return
	}
	codecByName[name] = t
	codecByType[t] = name
}

type taggedValue struct {
	T string          `json:"t"`
	V json.RawMessage `json:"v"`
}

// encodePortValue tags v with its registered codec name and marshals it.
// A value whose dynamic type has no registered codec yields an error
// wrapping ErrNotCheckpointable (this is the "non-serializable port at
// suspend" case).
func encodePortValue(v any) (json.RawMessage, error) {
	if v == nil {
		return nil, fmt.Errorf("%w: nil port value", ErrNotCheckpointable)
	}
	t := reflect.TypeOf(v)
	codecMu.RLock()
	name, ok := codecByType[t]
	codecMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("%w: no codec registered for port type %s (call flow.RegisterCodec)", ErrNotCheckpointable, t)
	}
	inner, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("flow: encode port value %s: %w", t, err)
	}
	return json.Marshal(taggedValue{T: name, V: inner})
}

// decodePortValue reverses encodePortValue.
func decodePortValue(raw json.RawMessage) (any, error) {
	var tv taggedValue
	if err := json.Unmarshal(raw, &tv); err != nil {
		return nil, fmt.Errorf("flow: decode port value: %w", err)
	}
	codecMu.RLock()
	t, ok := codecByName[tv.T]
	codecMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("flow: decode port value: no codec for tag %q", tv.T)
	}
	ptr := reflect.New(t)
	if err := json.Unmarshal(tv.V, ptr.Interface()); err != nil {
		return nil, fmt.Errorf("flow: decode port value %q: %w", tv.T, err)
	}
	return ptr.Elem().Interface(), nil
}

func init() { RegisterCodec[string]("string") } // built-in for the common string-port HITL route
