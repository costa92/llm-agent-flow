package apisnapshot

import (
	"flag"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// update, when set, rewrites the committed api/v0.1.snapshot.txt baseline
// instead of comparing against it. Use for deliberate v0.1-additive changes:
//
//	go test ./internal/apisnapshot/ -run TestAPISnapshot -update
var update = flag.Bool("update", false, "rewrite api/v0.1.snapshot.txt")

// moduleRoot resolves the module root from the test's location
// (internal/apisnapshot/ → ../..) as an absolute path.
func moduleRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("resolve module root: %v", err)
	}
	return root
}

// snapshotPath is the location of the committed baseline.
func snapshotPath(root string) string {
	return filepath.Join(root, "api", "v0.1.snapshot.txt")
}

// TestAPISnapshot regenerates the exported-API surface and compares it to
// the committed baseline. It fails any change that renames, removes, or
// re-signs an exported symbol. With -update it rewrites the baseline.
func TestAPISnapshot(t *testing.T) {
	root := moduleRoot(t)
	got, err := Generate(root)
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}

	path := snapshotPath(root)

	if *update {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("mkdir api/: %v", err)
		}
		if err := os.WriteFile(path, []byte(got), 0o644); err != nil {
			t.Fatalf("write baseline: %v", err)
		}
		t.Logf("updated baseline: %s", path)
		return
	}

	wantBytes, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read baseline %s: %v\nregenerate with: go test ./internal/apisnapshot/ -run TestAPISnapshot -update", path, err)
	}
	want := string(wantBytes)

	if got == want {
		return
	}

	// Report the first differing lines for a readable diff.
	gotLines := strings.Split(got, "\n")
	wantLines := strings.Split(want, "\n")
	var diff strings.Builder
	n := len(gotLines)
	if len(wantLines) > n {
		n = len(wantLines)
	}
	shown := 0
	for i := 0; i < n && shown < 20; i++ {
		var g, w string
		if i < len(gotLines) {
			g = gotLines[i]
		}
		if i < len(wantLines) {
			w = wantLines[i]
		}
		if g != w {
			diff.WriteString("  line ")
			diff.WriteString(strconv.Itoa(i + 1))
			diff.WriteString(":\n    baseline: ")
			diff.WriteString(w)
			diff.WriteString("\n    current:  ")
			diff.WriteString(g)
			diff.WriteString("\n")
			shown++
		}
	}

	t.Fatalf("exported API surface changed — the v0.1 snapshot gate fired.\n%s\n"+
		"If this change is a deliberate v0.1-additive change, regenerate the baseline:\n"+
		"  go test ./internal/apisnapshot/ -run TestAPISnapshot -update",
		diff.String())
}

// TestGenerateIsDeterministic asserts two consecutive Generate calls on
// the same source produce byte-identical output — determinism is the
// gate's correctness property.
func TestGenerateIsDeterministic(t *testing.T) {
	root := moduleRoot(t)
	a, err := Generate(root)
	if err != nil {
		t.Fatalf("Generate (1): %v", err)
	}
	b, err := Generate(root)
	if err != nil {
		t.Fatalf("Generate (2): %v", err)
	}
	if a != b {
		t.Fatalf("Generate is not deterministic — two runs differ (len %d vs %d)", len(a), len(b))
	}
}
