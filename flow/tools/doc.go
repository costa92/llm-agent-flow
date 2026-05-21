// Package tools provides a JSON manifest format that describes the
// Tools a flow can reach, plus two built-in kinds (http and exec) and
// a pluggable KindRegistry for adding more.
//
// The manifest decouples a flow's IR — which references tools by name
// only — from the actual Tool implementations. With a manifest a user
// can run any flow.json against any set of tools without recompiling
// the CLI / flowd binary.
//
// Manifest shape:
//
//	{
//	  "tools": [
//	    {
//	      "name": "translate",
//	      "kind": "http",
//	      "url":  "http://localhost:8080/translate",
//	      "method": "POST"
//	    },
//	    {
//	      "name": "wc",
//	      "kind": "exec",
//	      "command": ["wc", "-w"]
//	    }
//	  ]
//	}
//
// Built-in kinds (no extra deps required):
//
//   - "http" — POST {"input":"..."} to a URL; response shaped as
//     {"output":"..."} is decoded, otherwise the raw body becomes
//     the output string.
//   - "exec" — runs a command, sends the JSON args on stdin, captures
//     stdout as the output string.
//
// Custom kinds plug in via RegisterKind. They share the same Tool
// interface flow's ToolNode already consumes, so a registered tool
// drops straight into Engine.Deps.Tools.
package tools
