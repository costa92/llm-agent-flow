package flow

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"reflect"
	"sort"
	"strings"
)

// stateCodecTag is the guard string stored in Checkpoint.StateCodec. It
// combines the registered codec tag for S with a structural fingerprint of
// S's shape, all under a "state:" namespace so it can never alias a port
// codec tag (e.g. the built-in "string" port codec):
//
//	state:<codecTag>:<fingerprintHex>
//
// The bare codec tag alone is NOT a sufficient resume guard (MAJOR-4): the
// codec registry is first-write-wins, so a different-shape S registered under
// the same tag in a later process would pass a name-only check and then
// json.Unmarshal silently into a mismatched shape — corruption. The
// fingerprint catches the same-tag/different-shape case; the tag catches the
// different-tag case.
func stateCodecTag(t reflect.Type) (string, error) {
	codecMu.RLock()
	name, ok := codecByType[t]
	codecMu.RUnlock()
	if !ok {
		return "", fmt.Errorf("%w: no codec registered for state type %s (call flow.RegisterCodec)", ErrNotCheckpointable, t)
	}
	return "state:" + name + ":" + stateFingerprint(t), nil
}

// stateFingerprint computes a deterministic sha256 hex digest of t's
// structural shape. Two types with the same descriptor hash identically;
// any change to a struct's field names or field types changes the digest.
// This is the MAJOR-4(a) shape guard folded into Checkpoint.StateCodec.
func stateFingerprint(t reflect.Type) string {
	sum := sha256.Sum256([]byte(typeDescriptor(t, map[reflect.Type]bool{})))
	return hex.EncodeToString(sum[:])
}

// typeDescriptor renders a canonical, deterministic string describing t's
// shape. Struct fields are sorted by name so field-declaration order does
// not affect the fingerprint (JSON round-trips are field-order-independent).
// seen guards against recursive types (a self-referential field renders as a
// back-reference rather than recursing forever).
func typeDescriptor(t reflect.Type, seen map[reflect.Type]bool) string {
	if t == nil {
		return "nil"
	}
	if seen[t] {
		return "@" + t.String() // cyclic back-reference
	}
	switch t.Kind() {
	case reflect.Ptr:
		return "*" + typeDescriptor(t.Elem(), seen)
	case reflect.Slice:
		return "[]" + typeDescriptor(t.Elem(), seen)
	case reflect.Array:
		return fmt.Sprintf("[%d]%s", t.Len(), typeDescriptor(t.Elem(), seen))
	case reflect.Map:
		return "map[" + typeDescriptor(t.Key(), seen) + "]" + typeDescriptor(t.Elem(), seen)
	case reflect.Struct:
		seen[t] = true
		defer delete(seen, t)
		fields := make([]string, 0, t.NumField())
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			// Include the JSON tag (or field name) + the field's shape. The
			// JSON tag is load-bearing because it governs the on-disk shape.
			key := f.Name
			if tag, ok := f.Tag.Lookup("json"); ok {
				if name := strings.Split(tag, ",")[0]; name != "" {
					key = name
				}
			}
			fields = append(fields, key+":"+typeDescriptor(f.Type, seen))
		}
		sort.Strings(fields)
		return "struct{" + strings.Join(fields, ";") + "}"
	default:
		// Scalars + everything else identify by their kind+name (e.g.
		// "string", "int", a named type's underlying kind via Kind()).
		return t.Kind().String()
	}
}
