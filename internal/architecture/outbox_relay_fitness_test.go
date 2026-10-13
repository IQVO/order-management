package architecture

// Outbox relay attempt-cap sensor (fleet gap fix D12, ADR 0040).
//
//   - TestOutboxRelayCallsSetMaxAttempts: every non-test file under cmd/
//     that builds an outbox relay (a call to NewOutboxRelay) must pass a
//     WithMaxAttempts option. Without it the relay retries a poison row
//     forever and that one row stops every later event from publishing.
//   - TestOutboxRelayCapSensorBites proves the sensor flags a relay built
//     without the option and accepts one built with it.

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
)

// callName returns the called function's name: f(...) or pkg.F(...).
func callName(call *ast.CallExpr) string {
	switch fn := call.Fun.(type) {
	case *ast.Ident:
		return fn.Name
	case *ast.SelectorExpr:
		return fn.Sel.Name
	}
	return ""
}

// relayCallsWithoutCap returns the positions of NewOutboxRelay calls in f
// that pass no WithMaxAttempts(...) argument.
func relayCallsWithoutCap(fset *token.FileSet, f *ast.File) []string {
	var out []string
	ast.Inspect(f, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok || callName(call) != "NewOutboxRelay" {
			return true
		}
		for _, arg := range call.Args {
			if opt, ok := arg.(*ast.CallExpr); ok && callName(opt) == "WithMaxAttempts" {
				return true
			}
		}
		out = append(out, fset.Position(call.Pos()).String())
		return true
	})
	return out
}

func TestOutboxRelayCallsSetMaxAttempts(t *testing.T) {
	for _, p := range goFiles(t, "cmd") {
		fset, f := parseFile(t, p)
		for _, pos := range relayCallsWithoutCap(fset, f) {
			t.Errorf("%s", archViolation("contents", "outbox relays cap attempts",
				pos+": NewOutboxRelay without WithMaxAttempts: a poison row would block the relay forever "+
					"(pass postgres.WithMaxAttempts(OUTBOX_MAX_ATTEMPTS), ADR 0040)"))
		}
	}
}

func TestOutboxRelayCapSensorBites(t *testing.T) {
	cases := []struct {
		name string
		src  string
		want int
	}{
		{"relay without the cap is flagged", `package main
func f() { _ = postgres.NewOutboxRelay(pool, sink, logger, postgres.WithInterval(d)) }`, 1},
		{"relay with the cap passes", `package main
func f() { _ = postgres.NewOutboxRelay(pool, sink, logger, postgres.WithMaxAttempts(n)) }`, 0},
		{"unrelated calls are ignored", `package main
func f() { _ = postgres.NewOutboxPublisher(pool) }`, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fset := token.NewFileSet()
			f, err := parser.ParseFile(fset, "src.go", tc.src, 0)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			if got := len(relayCallsWithoutCap(fset, f)); got != tc.want {
				t.Fatalf("flagged %d calls, want %d", got, tc.want)
			}
		})
	}
}
