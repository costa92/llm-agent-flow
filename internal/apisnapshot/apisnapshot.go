// Package apisnapshot is the v1.0 exported-API-surface snapshot gate for
// llm-agent-rag.
//
// It contains a pure-stdlib generator (built on go/parser, go/ast,
// go/token and go/printer — no module dependency) that walks the module's
// own source, collects every exported declaration, and renders a
// deterministic, sorted text representation of the whole exported surface.
// The companion test compares that rendering against the committed
// baseline api/v1.snapshot.txt and fails any change that renames, removes,
// or re-signs an exported symbol; a -update flag rewrites the baseline for
// deliberate, v1-additive changes.
//
// The snapshot gate complements the contract package. contract is the
// narrow cross-repo compile-pin: it pins, at compile time, the subset of
// symbols current llm-agent integrations consume, and must be coordinated
// with the llm-agent repo. This snapshot is the whole-surface intra-repo
// diff: it answers "did this PR break the v1 promise for any exported
// symbol in this module?". Both are stdlib and both run at `go test` time.
//
// The generator skips every _test.go file and anything under an internal/
// path segment — internal/ is non-importable externally, so the gate
// machinery is correctly not part of the frozen public surface. Because
// go/parser ignores build constraints, the build-tagged adapter/llmagent
// package is parsed directly and is therefore covered by the snapshot.
package apisnapshot

import (
	"bytes"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

const modulePath = "github.com/costa92/llm-agent-flow"

// header is the fixed first line of every generated snapshot.
const header = "# llm-agent-flow v0.1 exported API snapshot — generated, do not hand-edit."

// symbol is one rendered exported declaration within a package.
type symbol struct {
	kind string // "const", "var", "func", "type"
	name string // sort key
	text string // fully rendered line(s)
}

// pkg accumulates the exported declarations of a single import path.
type pkg struct {
	importPath string
	symbols    []symbol
}

// Generate walks moduleRoot, parses every non-test .go file outside any
// internal/ path segment, collects every exported declaration, and renders
// a deterministic sorted text snapshot of the module's exported API.
func Generate(moduleRoot string) (string, error) {
	fset := token.NewFileSet()
	// pkgs is keyed by import path; populated in WalkDir order, sorted at render.
	pkgs := map[string]*pkg{}

	err := filepath.WalkDir(moduleRoot, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			// Skip hidden dirs and any directory named internal.
			name := d.Name()
			if path != moduleRoot && (strings.HasPrefix(name, ".") || name == "internal") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		rel, rerr := filepath.Rel(moduleRoot, path)
		if rerr != nil {
			return rerr
		}
		// Defensive: skip anything under an internal/ path segment.
		for _, seg := range strings.Split(filepath.ToSlash(rel), "/") {
			if seg == "internal" {
				return nil
			}
		}

		file, perr := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
		if perr != nil {
			return perr
		}

		importPath := importPathFor(rel)
		p := pkgs[importPath]
		if p == nil {
			p = &pkg{importPath: importPath}
			pkgs[importPath] = p
		}
		collect(fset, file, p)
		return nil
	})
	if err != nil {
		return "", err
	}

	// Sort packages by import path.
	paths := make([]string, 0, len(pkgs))
	for ip := range pkgs {
		paths = append(paths, ip)
	}
	sort.Strings(paths)

	var b strings.Builder
	b.WriteString(header)
	b.WriteString("\n")
	for _, ip := range paths {
		p := pkgs[ip]
		// Sort symbols by kind then name; both are deterministic.
		sort.Slice(p.symbols, func(i, j int) bool {
			if p.symbols[i].kind != p.symbols[j].kind {
				return p.symbols[i].kind < p.symbols[j].kind
			}
			return p.symbols[i].name < p.symbols[j].name
		})
		b.WriteString("package ")
		b.WriteString(ip)
		b.WriteString("\n")
		for _, s := range p.symbols {
			b.WriteString(s.text)
			b.WriteString("\n")
		}
	}
	return b.String(), nil
}

// importPathFor maps a file path relative to the module root to its
// package import path. A file in the root dir maps to the module path.
func importPathFor(rel string) string {
	dir := filepath.ToSlash(filepath.Dir(rel))
	if dir == "." || dir == "" {
		return modulePath
	}
	return modulePath + "/" + dir
}

// collect appends every exported declaration in file to p.
func collect(fset *token.FileSet, file *ast.File, p *pkg) {
	for _, decl := range file.Decls {
		switch d := decl.(type) {
		case *ast.FuncDecl:
			collectFunc(fset, d, p)
		case *ast.GenDecl:
			collectGen(fset, d, p)
		}
	}
}

// collectFunc records exported funcs and exported methods on exported types.
func collectFunc(fset *token.FileSet, d *ast.FuncDecl, p *pkg) {
	if !d.Name.IsExported() {
		return
	}
	if d.Recv == nil || len(d.Recv.List) == 0 {
		// Plain func.
		p.symbols = append(p.symbols, symbol{
			kind: "func",
			name: d.Name.Name,
			text: "func " + d.Name.Name + sigText(fset, d.Type),
		})
		return
	}
	// Method: only record if the receiver's base type is exported.
	recvType := receiverBaseName(d.Recv.List[0].Type)
	if recvType == "" || !ast.IsExported(recvType) {
		return
	}
	recvText := render(fset, d.Recv.List[0].Type)
	p.symbols = append(p.symbols, symbol{
		kind: "func",
		name: recvType + "." + d.Name.Name,
		text: "method (" + recvText + ") " + d.Name.Name + sigText(fset, d.Type),
	})
}

// receiverBaseName returns the bare type name of a receiver expression,
// unwrapping pointers and generic instantiations.
func receiverBaseName(expr ast.Expr) string {
	switch e := expr.(type) {
	case *ast.Ident:
		return e.Name
	case *ast.StarExpr:
		return receiverBaseName(e.X)
	case *ast.IndexExpr: // generic receiver: T[P]
		return receiverBaseName(e.X)
	case *ast.IndexListExpr: // generic receiver: T[P, Q]
		return receiverBaseName(e.X)
	}
	return ""
}

// collectGen records exported types, vars and consts.
func collectGen(fset *token.FileSet, d *ast.GenDecl, p *pkg) {
	for _, spec := range d.Specs {
		switch s := spec.(type) {
		case *ast.TypeSpec:
			if !s.Name.IsExported() {
				continue
			}
			collectType(fset, s, p)
		case *ast.ValueSpec:
			kind := "var"
			if d.Tok == token.CONST {
				kind = "const"
			}
			for i, name := range s.Names {
				if !name.IsExported() {
					continue
				}
				text := kind + " " + name.Name
				if s.Type != nil {
					text += " " + render(fset, s.Type)
				}
				if i < len(s.Values) && s.Values[i] != nil {
					text += " = " + render(fset, s.Values[i])
				}
				p.symbols = append(p.symbols, symbol{kind: kind, name: name.Name, text: text})
			}
		}
	}
}

// collectType records an exported type plus its exported struct fields or
// interface methods.
func collectType(fset *token.FileSet, s *ast.TypeSpec, p *pkg) {
	tparams := ""
	if s.TypeParams != nil {
		tparams = render(fset, s.TypeParams)
	}
	var lines []string

	switch t := s.Type.(type) {
	case *ast.StructType:
		lines = append(lines, "type "+s.Name.Name+tparams+" struct")
		var fields []string
		for _, f := range t.Fields.List {
			ft := render(fset, f.Type)
			if len(f.Names) == 0 {
				// Embedded field — exported if the base name is exported.
				base := embeddedName(f.Type)
				if base != "" && ast.IsExported(base) {
					fields = append(fields, "\tfield "+ft)
				}
				continue
			}
			for _, n := range f.Names {
				if n.IsExported() {
					fields = append(fields, "\tfield "+n.Name+" "+ft)
				}
			}
		}
		sort.Strings(fields)
		lines = append(lines, fields...)
	case *ast.InterfaceType:
		lines = append(lines, "type "+s.Name.Name+tparams+" interface")
		var methods []string
		for _, m := range t.Methods.List {
			if len(m.Names) == 0 {
				// Embedded interface — render the type.
				methods = append(methods, "\tembed "+render(fset, m.Type))
				continue
			}
			for _, n := range m.Names {
				if n.IsExported() {
					sig := render(fset, m.Type)
					if ft, ok := m.Type.(*ast.FuncType); ok {
						sig = sigText(fset, ft)
					}
					methods = append(methods, "\tmethod "+n.Name+sig)
				}
			}
		}
		sort.Strings(methods)
		lines = append(lines, methods...)
	default:
		// A defined type ("type T U") or an alias ("type T = U").
		assign := ""
		if s.Assign.IsValid() {
			assign = " ="
		}
		lines = append(lines, "type "+s.Name.Name+tparams+assign+" "+render(fset, s.Type))
	}

	p.symbols = append(p.symbols, symbol{
		kind: "type",
		name: s.Name.Name,
		text: strings.Join(lines, "\n"),
	})
}

// embeddedName returns the bare name of an embedded struct field type.
func embeddedName(expr ast.Expr) string {
	switch e := expr.(type) {
	case *ast.Ident:
		return e.Name
	case *ast.StarExpr:
		return embeddedName(e.X)
	case *ast.SelectorExpr:
		return e.Sel.Name
	case *ast.IndexExpr:
		return embeddedName(e.X)
	case *ast.IndexListExpr:
		return embeddedName(e.X)
	}
	return ""
}

// sigText renders a *ast.FuncType as "(params) results", stripping the
// leading "func" keyword that go/printer emits for a bare FuncType so the
// caller controls the "func Name"/"method Name" prefix.
func sigText(fset *token.FileSet, ft *ast.FuncType) string {
	s := render(fset, ft)
	s = strings.TrimPrefix(s, "func")
	return s
}

// render prints an AST node deterministically via go/printer.
func render(fset *token.FileSet, node ast.Node) string {
	var buf bytes.Buffer
	cfg := &printer.Config{Mode: printer.RawFormat, Tabwidth: 8}
	if err := cfg.Fprint(&buf, fset, node); err != nil {
		return "<unprintable>"
	}
	// Collapse internal newlines so each symbol stays on a stable shape.
	s := buf.String()
	s = strings.ReplaceAll(s, "\n", " ")
	for strings.Contains(s, "  ") {
		s = strings.ReplaceAll(s, "  ", " ")
	}
	return strings.TrimSpace(s)
}
