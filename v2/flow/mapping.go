package flow

import (
	"fmt"
	"reflect"
)

func (e *Engine) applyInputMappings(inputs map[string]any, setPort func(string, string, any), getPorts func(string) map[string]any, activated map[string]bool) error {
	for i, m := range e.flow.Mappings {
		if m.Source.Input == "" {
			continue
		}
		raw, ok := inputs[m.Source.Input]
		if !ok {
			return fmt.Errorf("flow: run: mapping[%d]: missing required input %q", i, m.Source.Input)
		}
		value, err := selectPath(raw, m.Source.Path)
		if err != nil {
			return fmt.Errorf("flow: run: mapping[%d] input %q: %w", i, m.Source.Input, err)
		}
		if err := setMappedPort(m.Target.Node, m.Target.Port, m.Target.Path, value, setPort, getPorts); err != nil {
			return fmt.Errorf("flow: run: mapping[%d] target %s.%s: %w", i, m.Target.Node, m.Target.Port, err)
		}
		activated[m.Target.Node] = true
	}
	return nil
}

func setMappedPort(nodeID, port string, path []string, value any, setPort func(string, string, any), getPorts func(string) map[string]any) error {
	if len(path) == 0 {
		setPort(nodeID, port, value)
		return nil
	}

	var root map[string]any
	if getPorts != nil {
		if existing, ok := getPorts(nodeID)[port]; ok {
			m, ok := existing.(map[string]any)
			if !ok {
				return fmt.Errorf("cannot write path into existing %T", existing)
			}
			root = cloneAnyMap(m)
		}
	}
	if root == nil {
		root = make(map[string]any)
	}

	cur := root
	for _, key := range path[:len(path)-1] {
		if key == "" {
			return fmt.Errorf("empty target path segment")
		}
		nextRaw, ok := cur[key]
		if !ok {
			next := make(map[string]any)
			cur[key] = next
			cur = next
			continue
		}
		next, ok := nextRaw.(map[string]any)
		if !ok {
			return fmt.Errorf("target path segment %q is %T, not map[string]any", key, nextRaw)
		}
		cur = next
	}
	last := path[len(path)-1]
	if last == "" {
		return fmt.Errorf("empty target path segment")
	}
	cur[last] = value
	setPort(nodeID, port, root)
	return nil
}

func selectPath(value any, path []string) (any, error) {
	cur := value
	for _, key := range path {
		if key == "" {
			return nil, fmt.Errorf("empty source path segment")
		}
		var err error
		cur, err = selectOne(cur, key)
		if err != nil {
			return nil, err
		}
	}
	return cur, nil
}

func selectOne(value any, key string) (any, error) {
	switch v := value.(type) {
	case map[string]any:
		out, ok := v[key]
		if !ok {
			return nil, fmt.Errorf("path %q not found", key)
		}
		return out, nil
	case map[string]string:
		out, ok := v[key]
		if !ok {
			return nil, fmt.Errorf("path %q not found", key)
		}
		return out, nil
	}

	rv := reflect.ValueOf(value)
	for rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return nil, fmt.Errorf("path %q on nil pointer", key)
		}
		rv = rv.Elem()
	}
	if rv.Kind() != reflect.Struct {
		return nil, fmt.Errorf("path %q on %T", key, value)
	}
	fv := rv.FieldByName(key)
	if !fv.IsValid() {
		return nil, fmt.Errorf("path %q not found", key)
	}
	if !fv.CanInterface() {
		return nil, fmt.Errorf("path %q is not exported", key)
	}
	return fv.Interface(), nil
}
