package architecture

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strconv"
	"strings"
	"testing"

	archgo "github.com/arch-go/arch-go/api"
	"github.com/arch-go/arch-go/api/configuration"
)

// a2aPackages is the A2A inbound adapter (ADR-0046).
const a2aPackages = "**.internal.adapters.inbound.a2a.**"

// TestA2AAdapterDependencyRule encodes ADR-0046, in the same shape as ADR-0008
// does for MCP: the A2A inbound adapter is additive. It may depend only on the
// application and domain layers (never on an outbound adapter, never on cmd,
// never on the MCP or HTTP adapters), and nothing else in this codebase may
// depend on it -- cmd/a2a wires it up and nothing else touches it.
func TestA2AAdapterDependencyRule(t *testing.T) {
	moduleInfo := configuration.Load(modulePath)

	t.Run("a2a adapter depends only on application and domain", func(t *testing.T) {
		result := archgo.CheckArchitecture(moduleInfo, configuration.Config{
			DependenciesRules: []*configuration.DependenciesRule{
				{
					Package: a2aPackages,
					ShouldOnlyDependsOn: &configuration.Dependencies{
						Internal: []string{a2aPackages, applicationPackages, domainPackages},
					},
				},
			},
		})

		assertArchGoPasses(t, result)
	})

	t.Run("nothing else depends on the a2a adapter", func(t *testing.T) {
		result := archgo.CheckArchitecture(moduleInfo, configuration.Config{
			DependenciesRules: []*configuration.DependenciesRule{
				{
					Package: allInternalPackages,
					ShouldNotDependsOn: &configuration.Dependencies{
						Internal: []string{a2aPackages},
					},
				},
			},
		})

		assertArchGoPasses(t, result)
	})
}

// a2aClientImport is the SDK's OUTBOUND client package.
const a2aClientImport = "github.com/a2aproject/a2a-go/v2/a2aclient"

// importsOf returns the unquoted import paths of one Go source file.
func importsOf(t *testing.T, path string, src any) []string {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), path, src, parser.ImportsOnly)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	var out []string
	for _, imp := range file.Imports {
		p, err := strconv.Unquote(imp.Path.Value)
		if err != nil {
			t.Fatalf("%s: bad import %s", path, imp.Path.Value)
		}
		out = append(out, p)
	}
	return out
}

// a2aClientViolations returns the files (among the given sources) that import
// the A2A SDK's outbound client.
func a2aClientViolations(t *testing.T, sources map[string]string) []string {
	t.Helper()
	var out []string
	for path, src := range sources {
		for _, p := range importsOf(t, path, src) {
			if p == a2aClientImport {
				out = append(out, path)
			}
		}
	}
	return out
}

// TestNoOutboundA2AClient: this context only ever RECEIVES delegations (ADR-0046).
// A2A is not a sibling-context call channel for it (the Customer/Supplier
// boundary of bounded-context-boundary.md stays HTTP + Kafka contracts), so no
// production source may import the SDK's outbound client.
func TestNoOutboundA2AClient(t *testing.T) {
	sources := map[string]string{}
	for _, root := range []string{"..", "../../cmd"} {
		for _, path := range goFilesUnder(t, root, false) {
			src, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read %s: %v", path, err)
			}
			sources[path] = string(src)
		}
	}
	if len(sources) == 0 {
		t.Fatal("found no sources: the sensor is looking in the wrong place")
	}
	if v := a2aClientViolations(t, sources); len(v) > 0 {
		t.Errorf("%s", archViolation("contents", "no production code imports the A2A SDK outbound client", v))
	}
}

// TestA2AClientSensorFailsOnBadFixtures proves the sensor bites.
func TestA2AClientSensorFailsOnBadFixtures(t *testing.T) {
	bad := map[string]string{"x.go": "package x\n\nimport \"" + a2aClientImport + "\"\n"}
	if got := a2aClientViolations(t, bad); len(got) != 1 {
		t.Fatalf("an a2aclient import was not flagged: %v", got)
	}
	ok := map[string]string{"y.go": "package y\n\nimport \"github.com/a2aproject/a2a-go/v2/a2asrv\"\n"}
	if got := a2aClientViolations(t, ok); len(got) != 0 {
		t.Fatalf("the server package was falsely flagged: %v", got)
	}
}

// allowedA2ADepsFields are the ONLY fields of the A2A adapter's Deps, with the
// type each must have. order-management's MCP surface offers no write
// (ADR-0010), so by the A2A write gate (ADR-0046) the agent is read-only: Deps
// may carry the GetOrder read use case and the analytics read port, and no
// write use case (ReceiveOrder, CancelOrder, RetryAllocation, ReleaseHeldOrder,
// ...). Adding a field here is a conscious widening of what this context
// exposes and needs a new ADR, not a drive-by edit.
var allowedA2ADepsFields = map[string]string{
	"GetOrder":      "*usecases.GetOrder",
	"PromiseHealth": "PromiseHealthStore",
}

// a2aDepsFields returns name -> rendered type of the Deps struct's fields.
func a2aDepsFields(t *testing.T, path string, src any) map[string]string {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), path, src, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	fields := map[string]string{}
	for _, decl := range file.Decls {
		collectDepsFields(decl, fields)
	}
	return fields
}

// collectDepsFields adds the fields of a `type Deps struct` declaration.
func collectDepsFields(decl ast.Decl, into map[string]string) {
	gen, ok := decl.(*ast.GenDecl)
	if !ok {
		return
	}
	for _, spec := range gen.Specs {
		ts, ok := spec.(*ast.TypeSpec)
		if !ok || ts.Name.Name != "Deps" {
			continue
		}
		st, ok := ts.Type.(*ast.StructType)
		if !ok {
			continue
		}
		for _, f := range st.Fields.List {
			for _, name := range f.Names {
				into[name.Name] = typeString(f.Type)
			}
		}
	}
}

// typeString renders the small set of type expressions Deps fields use.
func typeString(e ast.Expr) string {
	switch v := e.(type) {
	case *ast.StarExpr:
		return "*" + typeString(v.X)
	case *ast.SelectorExpr:
		return typeString(v.X) + "." + v.Sel.Name
	case *ast.Ident:
		return v.Name
	default:
		return "?"
	}
}

// a2aDepsViolations lists every Deps field outside the read-only allowlist.
func a2aDepsViolations(fields map[string]string) []string {
	var out []string
	for name, typ := range fields {
		want, ok := allowedA2ADepsFields[name]
		if !ok || want != typ {
			out = append(out, name+" "+typ)
		}
	}
	for name := range allowedA2ADepsFields {
		if _, ok := fields[name]; !ok {
			out = append(out, "missing "+name)
		}
	}
	return out
}

// TestA2AAgentIsReadOnly: the write gate. Deps carries no write use case.
func TestA2AAgentIsReadOnly(t *testing.T) {
	path := "../adapters/inbound/a2a/server.go"
	src, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	fields := a2aDepsFields(t, path, string(src))
	if len(fields) == 0 {
		t.Fatal("found no Deps struct: the sensor is looking in the wrong place")
	}
	if v := a2aDepsViolations(fields); len(v) > 0 {
		t.Errorf("%s", archViolation("contents", "the A2A agent is read-only: Deps carries only the GetOrder read use case and the analytics read port (MCP offers no write, ADR-0010/0046)", v))
	}

	// No file of the adapter may reference a write use case at all.
	for _, p := range goFilesUnder(t, "../adapters/inbound/a2a", false) {
		body, err := os.ReadFile(p)
		if err != nil {
			t.Fatalf("read %s: %v", p, err)
		}
		for _, write := range []string{"usecases.ReceiveOrder", "usecases.CancelOrder", "usecases.RetryAllocation", "usecases.ReleaseHeldOrder"} {
			if strings.Contains(string(body), write) {
				t.Errorf("%s references %s: a write use case behind a read-only agent", p, write)
			}
		}
	}
}

// TestA2AReadOnlySensorFailsOnBadFixtures proves the sensor bites on the exact
// violation it exists for: a write use case added to Deps.
func TestA2AReadOnlySensorFailsOnBadFixtures(t *testing.T) {
	const widened = `package a2a

type Deps struct {
	GetOrder      *usecases.GetOrder
	PromiseHealth PromiseHealthStore
	ReceiveOrder  *usecases.ReceiveOrder
}
`
	if v := a2aDepsViolations(a2aDepsFields(t, "x.go", widened)); len(v) != 1 || !strings.Contains(v[0], "ReceiveOrder") {
		t.Fatalf("a write use case in Deps was not flagged: %v", v)
	}
	const retyped = `package a2a

type Deps struct {
	GetOrder      *usecases.CancelOrder
	PromiseHealth PromiseHealthStore
}
`
	if v := a2aDepsViolations(a2aDepsFields(t, "y.go", retyped)); len(v) != 1 {
		t.Fatalf("a retyped GetOrder was not flagged: %v", v)
	}
	const exact = `package a2a

type Deps struct {
	GetOrder      *usecases.GetOrder
	PromiseHealth PromiseHealthStore
}
`
	if v := a2aDepsViolations(a2aDepsFields(t, "z.go", exact)); len(v) != 0 {
		t.Fatalf("the legitimate Deps was falsely flagged: %v", v)
	}
}
