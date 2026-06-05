package graph

import (
	"fmt"
	"reflect"
)

// anyType is reflect.Type of the empty interface (any).
var anyType = reflect.TypeFor[any]()

// assignable reports whether a value of type from can feed a port of
// type to, per the three §1.B rules:
//
//	① identical types (from == to).
//	② to is an interface and from implements it (pointer-receiver
//	   methods are honoured by also trying *from).
//	③ to is any — accepts anything.
//
// A fourth case is signalled separately via deferToRuntime: when from is
// any, static checking can't decide, so the edge is accepted now and the
// downstream adapter re-checks at run time.
func assignable(from, to reflect.Type) (ok bool, deferToRuntime bool) {
	switch {
	case from == to:
		return true, false
	case to == anyType:
		// ③ any target accepts everything.
		return true, false
	case from == anyType:
		// any source: undecidable statically — defer to runtime assert.
		return true, true
	case to.Kind() == reflect.Interface:
		// ② interface target: from must implement it. Try the pointer
		// type too so pointer-receiver method sets count.
		if from.Implements(to) {
			return true, false
		}
		if from.Kind() != reflect.Pointer && reflect.PointerTo(from).Implements(to) {
			return true, false
		}
		return false, false
	default:
		return false, false
	}
}

// AddEdge connects from.out to to.in. It checks assignability at build
// time; a mismatch is both returned and latched so a later Compile still
// fails even if the caller ignored this return value.
func (g *Graph[I, O]) AddEdge(from, to NodeRef) error {
	ok, _ := assignable(from.outT, to.inT)
	if !ok {
		return g.latch(fmt.Errorf("graph: edge %q.%s -> %q.%s: %s not assignable to %s",
			from.id, portOut, to.id, portIn, from.outT, to.inT))
	}
	g.edges = append(g.edges, graphEdge{from: from.id, to: to.id, fromPort: portOut, toPort: portIn})
	return nil
}

// AddEdgeTo connects from.out to a NAMED input port of to. It is the
// multi-input-port counterpart of AddEdge: for a fan-in combiner (to.inPorts
// non-nil) it resolves the target port's declared type from to.inPorts;
// for a single-port node it requires toPort == portIn and uses to.inT. The
// from.outT -> port-type assignability is checked at build time, latched on
// mismatch like AddEdge.
func (g *Graph[I, O]) AddEdgeTo(from, to NodeRef, toPort string) error {
	var portType reflect.Type
	if to.inPorts != nil {
		pt, ok := to.inPorts[toPort]
		if !ok {
			return g.latch(fmt.Errorf("graph: edge %q.%s -> %q.%s: %q is not a declared input port",
				from.id, portOut, to.id, toPort, toPort))
		}
		portType = pt
	} else {
		if toPort != portIn {
			return g.latch(fmt.Errorf("graph: edge %q.%s -> %q.%s: single-port node only accepts port %q",
				from.id, portOut, to.id, toPort, portIn))
		}
		portType = to.inT
	}
	if ok, _ := assignable(from.outT, portType); !ok {
		return g.latch(fmt.Errorf("graph: edge %q.%s -> %q.%s: %s not assignable to %s",
			from.id, portOut, to.id, toPort, from.outT, portType))
	}
	g.edges = append(g.edges, graphEdge{from: from.id, to: to.id, fromPort: portOut, toPort: toPort})
	return nil
}

// Entry marks to as the graph's input node: the graph input I is boxed
// into to's in port at Invoke. Checks I is assignable to to.inT.
func (g *Graph[I, O]) Entry(to NodeRef) error {
	ok, _ := assignable(g.inT, to.inT)
	if !ok {
		return g.latch(fmt.Errorf("graph: entry %q: graph input %s not assignable to %s",
			to.id, g.inT, to.inT))
	}
	ref := to
	g.entry = &ref
	return nil
}

// Exit marks from as the graph's output node: from's out port is
// unboxed to the graph output O at Invoke. Checks from.outT is
// assignable to O.
func (g *Graph[I, O]) Exit(from NodeRef) error {
	ok, _ := assignable(from.outT, g.outT)
	if !ok {
		return g.latch(fmt.Errorf("graph: exit %q: node output %s not assignable to graph output %s",
			from.id, from.outT, g.outT))
	}
	ref := from
	g.exit = &ref
	return nil
}
