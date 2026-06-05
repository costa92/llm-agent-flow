package graph

import (
	"reflect"
	"testing"
)

// Test types live at package scope: registering a folder writes to the
// global concatRegistry, so each uses a unique concrete/interface type to
// avoid cross-test interference. Marker methods keep the interfaces narrow
// so the assignability scan can't accidentally match a built-in folder.

type concatIface interface{ concatMarker() string }
type concatImpl struct{ v string }

func (c concatImpl) concatMarker() string { return c.v }

type ambIface interface{ ambMarker() }
type ambA struct{}
type ambB struct{}

func (ambA) ambMarker() {}
func (ambB) ambMarker() {}

type exactT struct{ v string }

// TestLookupConcat_ExactStillWins: an exact registration is found and
// returned by lookupConcat (the fast path), unchanged by the new fallback.
func TestLookupConcat_ExactStillWins(t *testing.T) {
	RegisterConcatenator[exactT, exactT](func(sr StreamReader[exactT]) (any, error) {
		defer sr.Close()
		return exactT{v: "exact"}, nil
	})
	fn, ok := lookupConcat(reflect.TypeFor[exactT]())
	if !ok {
		t.Fatal("exact lookup miss")
	}
	got, err := fn(box[any](exactT{}))
	if err != nil {
		t.Fatalf("fold: %v", err)
	}
	if got.(exactT).v != "exact" {
		t.Fatalf("folded %v, want exact", got)
	}
}

// TestLookupConcat_InterfaceTargetViaAssignability: with no exact folder
// for an interface target, a folder whose produced concrete type implements
// the interface is selected — Stream is no longer more restrictive than
// Invoke for interface-typed edges.
func TestLookupConcat_InterfaceTargetViaAssignability(t *testing.T) {
	RegisterConcatenator[concatImpl, concatImpl](func(sr StreamReader[concatImpl]) (any, error) {
		defer sr.Close()
		return concatImpl{v: "via-iface"}, nil
	})
	// No folder is registered for concatIface itself; the scan must find
	// the concatImpl folder (concatImpl implements concatIface).
	if _, ok := lookupConcat(reflect.TypeFor[concatIface]()); !ok {
		t.Fatal("interface-target lookup miss; assignability fallback did not fire")
	}
	fn, _ := lookupConcat(reflect.TypeFor[concatIface]())
	got, err := fn(box[any](concatImpl{}))
	if err != nil {
		t.Fatalf("fold: %v", err)
	}
	if got.(concatImpl).v != "via-iface" {
		t.Fatalf("folded %v, want via-iface", got)
	}
}

// TestLookupConcat_AmbiguousAssignable_Errors: two registered folders both
// satisfy an interface target and neither is exact — lookup fails loud
// rather than silently picking one.
func TestLookupConcat_AmbiguousAssignable_Errors(t *testing.T) {
	RegisterConcatenator[ambA, ambA](func(sr StreamReader[ambA]) (any, error) {
		defer sr.Close()
		return ambA{}, nil
	})
	RegisterConcatenator[ambB, ambB](func(sr StreamReader[ambB]) (any, error) {
		defer sr.Close()
		return ambB{}, nil
	})
	if _, ok := lookupConcat(reflect.TypeFor[ambIface]()); ok {
		t.Fatal("ambiguous interface target resolved to a folder; want fail-loud (false)")
	}
}

// TestLookupConcat_ConcreteUnregistered_StillMisses: a concrete target with
// no registration finds nothing (the assignability scan only matches
// interface targets), so the existing "no Concatenator" behaviour for plain
// missing folders is preserved.
func TestLookupConcat_ConcreteUnregistered_StillMisses(t *testing.T) {
	type unregistered struct{}
	if _, ok := lookupConcat(reflect.TypeFor[unregistered]()); ok {
		t.Fatal("unregistered concrete type resolved a folder")
	}
}
