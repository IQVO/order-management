package http

import (
	"go/ast"
	"go/parser"
	"go/token"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/claudioed/order-management/internal/domain/order"
)

// (a) Every problem type declares exactly one replay class on its catalogue entry. There is no
// runtime default: a new problem type is classified by the PR that adds it, or this test is red.
func TestEveryProblemTypeDeclaresAReplayClass(t *testing.T) {
	types := allProblemTypes()
	if len(types) == 0 {
		t.Fatal("the catalogue accessor returned no problem types: the test would pass on nothing")
	}
	classOf := map[string]replayClass{}
	for _, p := range types {
		t.Run(p.Slug, func(t *testing.T) {
			if !validReplayClass(p.Class) {
				t.Errorf("problem type %q (%d) declares no replay class (%q): set FINAL, OUTCOME or RETRYABLE on its catalogue entry (.claude/rules/fleet/idempotency-and-outbox.md)", p.Slug, p.Status, p.Class)
			}
			if p.Status >= http.StatusInternalServerError && p.Class != classRetryable {
				t.Errorf("problem type %q answers %d: a 5xx is never stored, so its class is RETRYABLE, got %q", p.Slug, p.Status, p.Class)
			}
			if prev, ok := classOf[p.Slug]; ok && prev != p.Class {
				t.Errorf("slug %q is declared %s and %s: one problem type has exactly one class", p.Slug, prev, p.Class)
			}
			classOf[p.Slug] = p.Class
		})
	}
}

// The check above must bite: an unclassified entry and a misspelt class are refused.
func TestTheUnclassifiedEntryCheckBites(t *testing.T) {
	for _, c := range []replayClass{"", "final", "STORED", "OUTCOME "} {
		if validReplayClass(c) {
			t.Errorf("class %q is accepted; only FINAL, OUTCOME and RETRYABLE are classes", c)
		}
	}
	for _, c := range []replayClass{classFinal, classOutcome, classRetryable} {
		if !validReplayClass(c) {
			t.Errorf("class %q is refused", c)
		}
	}
}

// problemInfo literals are the catalogue: they may only be written where the class is declared
// (errors.go's catalogue and problems.go's register calls), and each one carries its class. A
// literal built at a call site would escape the unclassified-entry check above.
func TestEveryProblemInfoLiteralIsInTheCatalogueAndClassified(t *testing.T) {
	literals := problemInfoLiterals(t)
	if len(literals) == 0 {
		t.Fatal("no problemInfo literal found: the scan would pass on nothing")
	}
	for _, l := range literals {
		if l.file != "errors.go" && l.file != "problems.go" {
			t.Errorf("%s: a problemInfo literal outside errors.go and problems.go: declare it with register(...) in problems.go so it carries a class", l.pos)
		}
		if !literalDeclaresClass(l.lit) {
			t.Errorf("%s: this problemInfo literal declares no class", l.pos)
		}
	}
}

type problemInfoLiteral struct {
	file string
	pos  token.Position
	lit  *ast.CompositeLit
}

// problemInfoLiterals finds every problemInfo composite literal of the package's non-test files.
func problemInfoLiterals(t *testing.T) []problemInfoLiteral {
	t.Helper()
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	var found []problemInfoLiteral
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(f, func(n ast.Node) bool {
			if lit, ok := n.(*ast.CompositeLit); ok && isProblemInfo(lit) {
				found = append(found, problemInfoLiteral{file: name, pos: fset.Position(lit.Pos()), lit: lit})
			}
			return true
		})
	}
	return found
}

func isProblemInfo(lit *ast.CompositeLit) bool {
	id, ok := lit.Type.(*ast.Ident)
	return ok && id.Name == "problemInfo"
}

func literalDeclaresClass(lit *ast.CompositeLit) bool {
	for i, e := range lit.Elts {
		if kv, ok := e.(*ast.KeyValueExpr); ok {
			if k, ok := kv.Key.(*ast.Ident); ok && k.Name == "class" {
				return true
			}
		} else if i == 2 {
			return true
		}
	}
	return false
}

// The ADR's class table is the human copy of the catalogue: one row per slug, same status and class.
func TestTheADRClassTableMatchesTheCatalogue(t *testing.T) {
	raw, err := os.ReadFile("../../../../docs/docs/adr/0054-idempotency-replay-classes.md")
	if err != nil {
		t.Fatalf("the ADR with the class table: %v", err)
	}
	row := regexp.MustCompile("(?m)^\\| `([a-z0-9-]+)` \\| (\\d{3}) \\| (FINAL|OUTCOME|RETRYABLE) \\|")
	inADR := map[string]string{}
	for _, m := range row.FindAllStringSubmatch(string(raw), -1) {
		if _, dup := inADR[m[1]]; dup {
			t.Errorf("slug %q is listed twice in the ADR table", m[1])
		}
		inADR[m[1]] = m[3]
	}
	inCode := map[string]string{}
	for _, p := range allProblemTypes() {
		inCode[p.Slug] = string(p.Class)
	}
	for slug, class := range inCode {
		if got, ok := inADR[slug]; !ok {
			t.Errorf("problem type %q (%s) is missing from the ADR table", slug, class)
		} else if got != class {
			t.Errorf("problem type %q is %s in the catalogue and %s in the ADR", slug, class, got)
		}
	}
	for slug := range inADR {
		if _, ok := inCode[slug]; !ok {
			t.Errorf("the ADR lists %q, which is not a problem type of the catalogue", slug)
		}
	}
}

// Every catalogue entry's status is the one statusFor answers: the accessor cannot drift.
func TestCatalogueStatusIsWhatStatusForAnswers(t *testing.T) {
	for _, e := range problemCatalog {
		var got int
		for _, p := range allProblemTypes() {
			if p.Slug == e.info.slug {
				got = p.Status
			}
		}
		if want := statusFor(e.err); got != want {
			t.Errorf("%s: statusFor = %d, accessor = %d", e.info.slug, want, got)
		}
	}
}

// The decision the middleware takes, for every status and class: 2xx stored, 5xx never, the rest
// by the declared class, and a refusal without a class (or with a misspelt one) is released.
func TestStorableDecisionByStatusAndClass(t *testing.T) {
	cases := []struct {
		code  int
		class replayClass
		want  bool
	}{
		{http.StatusCreated, "", true},
		{http.StatusNoContent, "", true},
		{http.StatusOK, classRetryable, true},
		{http.StatusInternalServerError, classFinal, false},
		{http.StatusInternalServerError, classOutcome, false},
		{http.StatusServiceUnavailable, "", false},
		{http.StatusBadRequest, classFinal, true},
		{http.StatusConflict, classOutcome, true},
		{http.StatusUnprocessableEntity, classFinal, true},
		{http.StatusConflict, classRetryable, false},
		{http.StatusNotFound, classRetryable, false},
		{http.StatusConflict, "", false},
		{http.StatusBadRequest, "final", false},
		{http.StatusMovedPermanently, "", false},
	}
	for _, c := range cases {
		rec := newCapture()
		rec.markReplayClass(c.class)
		rec.WriteHeader(c.code)
		if got := rec.storable(); got != c.want {
			t.Errorf("status %d class %q: storable = %v, want %v", c.code, c.class, got, c.want)
		}
	}
}

// writeProblem marks the capturing writer with the class of the problem it answers.
func TestWriteProblemMarksTheCaptureWithTheCatalogueClass(t *testing.T) {
	for _, p := range allProblemTypes() {
		rec := newCapture()
		writeProblem(rec, p.Status, problemInfo{slug: p.Slug, title: "t", class: p.Class}, "d", "/x")
		if rec.class != p.Class {
			t.Errorf("%s: capture class = %q, want %q", p.Slug, rec.class, p.Class)
		}
	}
	// A plain writer (no idempotency middleware wired) is not a capture: nothing to mark.
	writeProblem(httptest.NewRecorder(), http.StatusConflict, problemInternalError, "d", "/x")
}

func TestWriteErrorMarksTheClassOfTheMappedProblem(t *testing.T) {
	for _, e := range problemCatalog {
		rec := newCapture()
		writeError(rec, httptest.NewRequest(http.MethodPost, "/x", nil), e.err)
		if rec.class != e.info.class || !validReplayClass(rec.class) {
			t.Errorf("%s: writeError marked %q, the entry declares %q", e.info.slug, rec.class, e.info.class)
		}
	}
}

// An error the catalogue does not map answers 500 internal-error, a 5xx: RETRYABLE, never stored.
func TestAnUnmappedErrorIsAnInternalErrorThatIsNeverStored(t *testing.T) {
	rec := newCapture()
	writeError(rec, httptest.NewRequest(http.MethodPost, "/x", nil), order.ErrShipCompleteBlocked)
	if rec.Code != http.StatusInternalServerError || rec.class != classRetryable || rec.storable() {
		t.Fatalf("unmapped error = %d class %q storable=%v, want 500 RETRYABLE not storable", rec.Code, rec.class, rec.storable())
	}
}
