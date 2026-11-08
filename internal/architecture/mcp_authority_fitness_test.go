package architecture

// Agent authority + MCP surface fitness tests (Agentic Standard v2, AS-1 and
// AS-3). Portable and STATIC: go/ast over the adapter sources, no running
// server and no database, so any repo can copy this file.
//
// The fleet rule is agent-authority (warehouse-docs agents/fleet/agent-authority.md):
// every MCP tool and every A2A skill declares exactly one authority level L1..L4,
// a read tool is L1/L2, a write tool is L3 unless an ADR in the owning context
// grants L4, and every MCP server offers tools plus at least one resource and
// at least one prompt.
//
// The ONE accepted declaration shape (pinned in contracts.md G2; it is the shape
// workforce-management ships):
//
//	MCP: Meta: authorityMeta(authorityL3)  // a package helper + string constants
//	     func authorityMeta(level string) mcp.Meta { return mcp.Meta{authorityMetaKey: level} }
//	     const authorityMetaKey = "com.warehouse/authority"; const authorityL3 = "L3"
//	     (a string literal argument, authorityMeta("L3"), is the same thing; an inline
//	     mcp.Meta{"com.warehouse/authority": "L3"} literal is accepted as well because
//	     it IS the wire contract. Anything computed at run time is rejected: the
//	     sensor cannot read it.)
//	A2A: Tags: []string{"staffing", "write", "authority:L3"}
//
// L4 additionally needs a comment in the SAME FILE naming the ADR that grants it:
//
//	//authority:L4 ADR-0042 grants a bounded auto-reallocation envelope
//
// Rollout is phased, so the test that asks for resources and prompts (AS-3) can
// be parked with a marker file internal/adapters/inbound/mcp/.agent-surface-pending
// holding ONE line of reason; the test then SKIPS and prints the reason, so the
// repo is not red but is visibly pending. See HARNESS.md "Agent authority".
//
// The tests skip when the service has no MCP adapter (or its go.mod does not
// depend on modelcontextprotocol/go-sdk); a service WITH the adapter must have
// sources there, and the scanners fail when they find nothing to check.

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
)

const (
	agentAuthorityKey         = "com.warehouse/authority"
	agentAuthorityHelper      = "authorityMeta"
	a2aAdapterDir             = "../adapters/inbound/a2a"
	moduleGoMod               = "../../go.mod"
	agentSurfacePendingMarker = ".agent-surface-pending"
	mcpSDKImportPrefix        = "github.com/modelcontextprotocol/go-sdk/mcp"
	mcpSDKModule              = "modelcontextprotocol/go-sdk"

	// mcpAdapterDir is defined in mcp_optional_inputs_test.go in the
	// template; this repo's equivalent file predates that const, so it is
	// declared here (the only line this unmanaged copy adds).
	mcpAdapterDir = "../adapters/inbound/mcp"
)

var (
	authorityLevelRE  = regexp.MustCompile(`^L[1-4]$`)
	a2aAuthorityTagRE = regexp.MustCompile(`^authority:(L[1-4])$`)
	l4CommentRE       = regexp.MustCompile(`^//\s*authority:L4\s+(\S.*)$`)
	adrRefRE          = regexp.MustCompile(`(?i)(\badr[-_ ]?\d+|/adr/\d+)`)
)

// agentViolation renders a failure for an LLM (or human) reader: WHAT broke, WHY
// the rule exists, the exact FIX.
func agentViolation(what, fix string) string {
	return "AGENT AUTHORITY VIOLATION\n  WHAT: " + what +
		"\n  WHY:  fleet rule agent-authority (warehouse-docs agents/fleet/agent-authority.md): every MCP tool and A2A skill " +
		"declares exactly one authority level L1..L4 so the caller knows what gating to apply (read = L1/L2, write = L3 unless " +
		"an ADR grants L4), and every MCP server offers tools, at least one resource and at least one prompt." +
		"\n  FIX:  " + fix + "\n"
}

const (
	fixDeclareMCP = "register the tool with `Meta: authorityMeta(authorityL1)` (L1 reads facts, L2 recommends, L3 writes on approval) and " +
		"define once in the adapter package: `const authorityMetaKey = \"com.warehouse/authority\"`, `const authorityL1 = \"L1\"` (and L2, L3), " +
		"`func authorityMeta(level string) mcp.Meta { return mcp.Meta{authorityMetaKey: level} }`. " +
		"Do not loosen this test or add an exception."
	fixDeclareA2A = "add the tag to the skill: `Tags: []string{\"<context>\", \"read\", \"authority:L1\"}` " +
		"(a write skill keeps its `write` tag and carries `authority:L3`)."
)

// ---------------------------------------------------------------------
// loading
// ---------------------------------------------------------------------

type agentFile struct {
	path     string
	file     *ast.File
	mcpAlias string // local name of go-sdk/mcp in this file, "" when not imported
	l4Found  bool   // an //authority:L4 comment exists
	l4ADR    bool   // ... and it names an ADR
}

type agentPkg struct {
	dir     string
	fset    *token.FileSet
	files   []*agentFile
	pkgBind map[string][]ast.Expr
}

// loadAgentPackages parses every non-test .go file under root (testdata
// skipped), grouped by directory (= Go package).
func loadAgentPackages(root string) ([]*agentPkg, error) {
	fset := token.NewFileSet()
	byDir := map[string]*agentPkg{}
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if d.Name() == "testdata" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		return addAgentFile(fset, byDir, path)
	})
	if err != nil {
		return nil, err
	}
	dirs := make([]string, 0, len(byDir))
	for d := range byDir {
		dirs = append(dirs, d)
	}
	sort.Strings(dirs)
	var out []*agentPkg
	for _, d := range dirs {
		byDir[d].bindPackageLevel()
		out = append(out, byDir[d])
	}
	return out, nil
}

// bindPackageLevel records every package-level const/var binding of the package.
func (p *agentPkg) bindPackageLevel() {
	for _, f := range p.files {
		for _, decl := range f.file.Decls {
			if gd, ok := decl.(*ast.GenDecl); ok {
				addBindings(p.pkgBind, gd)
			}
		}
	}
}

func addAgentFile(fset *token.FileSet, byDir map[string]*agentPkg, path string) error {
	f, err := parser.ParseFile(fset, path, nil, parser.ParseComments|parser.SkipObjectResolution)
	if err != nil {
		return err
	}
	dir := filepath.Dir(path)
	p := byDir[dir]
	if p == nil {
		p = &agentPkg{dir: dir, fset: fset, pkgBind: map[string][]ast.Expr{}}
		byDir[dir] = p
	}
	p.files = append(p.files, newAgentFile(path, f))
	return nil
}

// l4Documented reports whether the file carries an //authority:L4 comment that names an ADR.
func (f *agentFile) l4Documented() bool { return f.l4Found && f.l4ADR }

func newAgentFile(path string, f *ast.File) *agentFile {
	af := &agentFile{path: path, file: f}
	for _, imp := range f.Imports {
		ip, _ := strconv.Unquote(imp.Path.Value)
		if ip != mcpSDKImportPrefix {
			continue
		}
		af.mcpAlias = "mcp"
		if imp.Name != nil {
			af.mcpAlias = imp.Name.Name
		}
	}
	for _, cg := range f.Comments {
		for _, c := range cg.List {
			if m := l4CommentRE.FindStringSubmatch(c.Text); m != nil {
				af.l4Found = true
				if adrRefRE.MatchString(m[1]) {
					af.l4ADR = true
				}
			}
		}
	}
	return af
}

// addBindings records `name = expr` pairs from const/var specs and := / = statements.
func addBindings(into map[string][]ast.Expr, n ast.Node) {
	ast.Inspect(n, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.ValueSpec:
			if len(x.Values) == len(x.Names) {
				for i, id := range x.Names {
					into[id.Name] = append(into[id.Name], x.Values[i])
				}
			}
		case *ast.AssignStmt:
			addAssignBindings(into, x)
		}
		return true
	})
}

func addAssignBindings(into map[string][]ast.Expr, x *ast.AssignStmt) {
	if (x.Tok != token.DEFINE && x.Tok != token.ASSIGN) || len(x.Lhs) != len(x.Rhs) {
		return
	}
	for i, l := range x.Lhs {
		if id, ok := l.(*ast.Ident); ok {
			into[id.Name] = append(into[id.Name], x.Rhs[i])
		}
	}
}

// scope resolves identifiers: bindings of the enclosing function win over the package's.
type scope struct{ pkg, local map[string][]ast.Expr }

func (s scope) lookup(name string) []ast.Expr {
	if v, ok := s.local[name]; ok {
		return v
	}
	return s.pkg[name]
}

// str resolves a string literal, or an identifier bound to one (all bindings must agree).
func (s scope) str(e ast.Expr, depth int) (string, bool) {
	if depth > 6 || e == nil {
		return "", false
	}
	switch x := e.(type) {
	case *ast.BasicLit:
		if x.Kind != token.STRING {
			return "", false
		}
		v, err := strconv.Unquote(x.Value)
		return v, err == nil
	case *ast.ParenExpr:
		return s.str(x.X, depth+1)
	case *ast.Ident:
		return agreeing(s.lookup(x.Name), func(e ast.Expr) (string, bool) { return s.str(e, depth+1) })
	}
	return "", false
}

// boolean resolves true/false, or an identifier bound to one.
func (s scope) boolean(e ast.Expr, depth int) (bool, bool) {
	if depth > 6 || e == nil {
		return false, false
	}
	switch x := e.(type) {
	case *ast.ParenExpr:
		return s.boolean(x.X, depth+1)
	case *ast.Ident:
		if x.Name == "true" || x.Name == "false" {
			if len(s.lookup(x.Name)) == 0 {
				return x.Name == "true", true
			}
		}
		return agreeing(s.lookup(x.Name), func(e ast.Expr) (bool, bool) { return s.boolean(e, depth+1) })
	}
	return false, false
}

func agreeing[T comparable](cands []ast.Expr, resolve func(ast.Expr) (T, bool)) (T, bool) {
	var zero, first T
	if len(cands) == 0 {
		return zero, false
	}
	for i, c := range cands {
		v, ok := resolve(c)
		if !ok {
			return zero, false
		}
		if i == 0 {
			first = v
		} else if v != first {
			return zero, false
		}
	}
	return first, true
}

// selParts splits `x.Sel` (through pointers) into its parts.
func selParts(t ast.Expr) (x, sel string, ok bool) {
	if st, isStar := t.(*ast.StarExpr); isStar {
		t = st.X
	}
	se, isSel := t.(*ast.SelectorExpr)
	if !isSel {
		return "", "", false
	}
	id, isID := se.X.(*ast.Ident)
	if !isID {
		return "", "", false
	}
	return id.Name, se.Sel.Name, true
}

// eachLiteral calls visit for every composite literal whose (possibly elided) type matches.
func (p *agentPkg) eachLiteral(match func(f *agentFile, x, sel string) bool, visit func(f *agentFile, lit *ast.CompositeLit, sc scope)) {
	for _, f := range p.files {
		for _, decl := range f.file.Decls {
			sc := scope{pkg: p.pkgBind}
			if fd, ok := decl.(*ast.FuncDecl); ok && fd.Body != nil {
				sc.local = map[string][]ast.Expr{}
				addBindings(sc.local, fd.Body)
			}
			ast.Inspect(decl, func(n ast.Node) bool {
				if lit, ok := n.(*ast.CompositeLit); ok {
					for _, m := range matchedLiterals(f, lit, match) {
						visit(f, m, sc)
					}
				}
				return true
			})
		}
	}
}

// matchedLiterals returns lit itself when its type matches, plus the elided
// elements of a slice literal such as []*mcp.Tool{{...}, {...}}.
func matchedLiterals(f *agentFile, lit *ast.CompositeLit, match func(f *agentFile, x, sel string) bool) []*ast.CompositeLit {
	if lit.Type == nil {
		return nil
	}
	var out []*ast.CompositeLit
	if x, sel, ok := selParts(lit.Type); ok && match(f, x, sel) {
		out = append(out, lit)
	}
	at, ok := lit.Type.(*ast.ArrayType)
	if !ok {
		return out
	}
	if x, sel, ok := selParts(at.Elt); ok && match(f, x, sel) {
		for _, e := range lit.Elts {
			if cl, ok := e.(*ast.CompositeLit); ok && cl.Type == nil {
				out = append(out, cl)
			}
		}
	}
	return out
}

func fieldExpr(lit *ast.CompositeLit, name string) ast.Expr {
	for _, e := range lit.Elts {
		if kv, ok := e.(*ast.KeyValueExpr); ok {
			if id, ok := kv.Key.(*ast.Ident); ok && id.Name == name {
				return kv.Value
			}
		}
	}
	return nil
}

func isLevel12(l string) bool { return l == "L1" || l == "L2" }

// ---------------------------------------------------------------------
// MCP tools
// ---------------------------------------------------------------------

type agentTool struct {
	pos, name    string
	f            *agentFile
	level        string // "" when not resolvable to L1..L4
	levelProblem string
	readOnly     bool // ReadOnlyHint provably true
	writeNote    string
}

type mcpScan struct{ tools []agentTool }

func scanMCPAdapter(root string) (mcpScan, error) {
	pkgs, err := loadAgentPackages(root)
	if err != nil {
		return mcpScan{}, err
	}
	var s mcpScan
	for _, p := range pkgs {
		p.eachLiteral(
			func(f *agentFile, x, sel string) bool { return f.mcpAlias != "" && x == f.mcpAlias && sel == "Tool" },
			func(f *agentFile, lit *ast.CompositeLit, sc scope) {
				s.tools = append(s.tools, analyzeTool(p, f, lit, sc))
			})
	}
	return s, nil
}

func analyzeTool(p *agentPkg, f *agentFile, lit *ast.CompositeLit, sc scope) agentTool {
	t := agentTool{pos: p.fset.Position(lit.Pos()).String(), name: "<unnamed>", f: f}
	if v, ok := sc.str(fieldExpr(lit, "Name"), 0); ok {
		t.name = v
	}
	t.level, t.levelProblem = authorityFromMeta(p, sc, fieldExpr(lit, "Meta"))
	t.readOnly, t.writeNote = readOnlyFromAnnotations(sc, fieldExpr(lit, "Annotations"))
	return t
}

// authorityFromMeta resolves a tool's declared level from its Meta expression.
func authorityFromMeta(p *agentPkg, sc scope, meta ast.Expr) (level, problem string) {
	if meta == nil {
		return "", "has no Meta field"
	}
	var raw ast.Expr
	switch m := meta.(type) {
	case *ast.CallExpr:
		if problem = p.checkHelperCall(m); problem != "" {
			return "", problem
		}
		raw = m.Args[0]
	case *ast.CompositeLit:
		if raw = metaLiteralLevel(sc, m); raw == nil {
			return "", "has a Meta literal without the " + agentAuthorityKey + " key"
		}
	default:
		return "", "sets Meta to an expression the sensor cannot read statically"
	}
	v, ok := sc.str(raw, 0)
	if !ok {
		return "", "declares a level that is not a string literal or a constant resolvable in this package"
	}
	if !authorityLevelRE.MatchString(v) {
		return "", fmt.Sprintf("declares level %q, which is not one of L1, L2, L3, L4 (there is no L5)", v)
	}
	return v, ""
}

// checkHelperCall returns a problem unless m is authorityMeta(x) with a helper that sets the key.
func (p *agentPkg) checkHelperCall(m *ast.CallExpr) string {
	id, ok := m.Fun.(*ast.Ident)
	if !ok || id.Name != agentAuthorityHelper || len(m.Args) != 1 {
		return "builds Meta with something other than " + agentAuthorityHelper + "(level) or a Meta literal"
	}
	if !p.helperSetsKey() {
		return "uses " + agentAuthorityHelper + "() but this package does not define it, or it does not set the " + agentAuthorityKey + " key"
	}
	return ""
}

// metaLiteralLevel returns the value expression of the authority key in a Meta literal, or nil.
func metaLiteralLevel(sc scope, m *ast.CompositeLit) ast.Expr {
	for _, e := range m.Elts {
		if kv, ok := e.(*ast.KeyValueExpr); ok {
			if k, ok := sc.str(kv.Key, 0); ok && k == agentAuthorityKey {
				return kv.Value
			}
		}
	}
	return nil
}

// helperSetsKey reports whether this package defines authorityMeta and the body sets the authority key.
func (p *agentPkg) helperSetsKey() bool {
	sc := scope{pkg: p.pkgBind}
	for _, f := range p.files {
		for _, decl := range f.file.Decls {
			fd, ok := decl.(*ast.FuncDecl)
			if !ok || fd.Recv != nil || fd.Name.Name != agentAuthorityHelper || fd.Body == nil {
				continue
			}
			found := false
			ast.Inspect(fd.Body, func(n ast.Node) bool {
				if kv, ok := n.(*ast.KeyValueExpr); ok {
					if k, ok := sc.str(kv.Key, 0); ok && k == agentAuthorityKey {
						found = true
					}
				}
				return true
			})
			if found {
				return true
			}
		}
	}
	return false
}

// readOnlyFromAnnotations reports whether ReadOnlyHint is provably true, else why not.
func readOnlyFromAnnotations(sc scope, ann ast.Expr) (bool, string) {
	if ann == nil {
		return false, "it has no Annotations, so ReadOnlyHint is not true"
	}
	if u, ok := ann.(*ast.UnaryExpr); ok {
		ann = u.X
	}
	lit, ok := ann.(*ast.CompositeLit)
	if !ok {
		return false, "its Annotations are not a literal the sensor can read, so ReadOnlyHint is not provably true"
	}
	h := fieldExpr(lit, "ReadOnlyHint")
	if h == nil {
		return false, "its Annotations do not set ReadOnlyHint (the default is false)"
	}
	v, ok := sc.boolean(h, 0)
	switch {
	case !ok:
		return false, "its ReadOnlyHint is neither the literal true nor a variable assigned the literal true"
	case !v:
		return false, "its ReadOnlyHint is false"
	}
	return true, ""
}

func mcpDeclarationViolations(s mcpScan) []string {
	var out []string
	for _, t := range s.tools {
		if t.levelProblem == "" {
			continue
		}
		out = append(out, agentViolation(
			fmt.Sprintf("%s: MCP tool %q %s", t.pos, t.name, t.levelProblem), fixDeclareMCP))
	}
	return out
}

func mcpLevelViolations(s mcpScan) []string {
	var out []string
	for _, t := range s.tools {
		if t.level == "" {
			continue // reported by the declaration test
		}
		if !t.readOnly && isLevel12(t.level) {
			out = append(out, agentViolation(
				fmt.Sprintf("%s: MCP tool %q is declared %s but is not a read tool (%s); a write tool is L3 unless an ADR grants L4", t.pos, t.name, t.level, t.writeNote),
				"if the tool really only reads, set `Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true}`; if it writes, declare `Meta: authorityMeta(authorityL3)`."))
		}
		if t.level == "L4" && !t.f.l4Documented() {
			out = append(out, agentViolation(
				fmt.Sprintf("%s: MCP tool %q is declared L4 but %s has no `//authority:L4 <adr>` comment naming the ADR that grants it", t.pos, t.name, t.f.path),
				"write the ADR in the owning context first, then add `//authority:L4 ADR-0042 <one-line envelope>` to this file; otherwise declare L3."))
		}
	}
	return out
}

// ---------------------------------------------------------------------
// A2A skills
// ---------------------------------------------------------------------

type agentSkill struct {
	pos, id string
	f       *agentFile
	level   string
	write   bool
	problem string
}

func scanA2AAdapter(root string) ([]agentSkill, error) {
	pkgs, err := loadAgentPackages(root)
	if err != nil {
		return nil, err
	}
	var out []agentSkill
	for _, p := range pkgs {
		p.eachLiteral(
			func(_ *agentFile, _, sel string) bool { return sel == "AgentSkill" },
			func(f *agentFile, lit *ast.CompositeLit, sc scope) {
				out = append(out, analyzeSkill(p, f, lit, sc))
			})
	}
	return out, nil
}

func analyzeSkill(p *agentPkg, f *agentFile, lit *ast.CompositeLit, sc scope) agentSkill {
	s := agentSkill{pos: p.fset.Position(lit.Pos()).String(), id: "<unnamed>", f: f}
	if v, ok := sc.str(fieldExpr(lit, "ID"), 0); ok {
		s.id = v
	}
	tags, ok := fieldExpr(lit, "Tags").(*ast.CompositeLit)
	if !ok {
		s.problem = "has no Tags []string literal"
		return s
	}
	levels := map[string]bool{}
	for _, e := range tags.Elts {
		tag, ok := sc.str(e, 0)
		if !ok {
			continue
		}
		if tag == "write" {
			s.write = true
		}
		if strings.HasPrefix(tag, "authority:") {
			if m := a2aAuthorityTagRE.FindStringSubmatch(tag); m != nil {
				levels[m[1]] = true
			} else {
				s.problem = fmt.Sprintf("has the malformed tag %q (use authority:L1 .. authority:L4)", tag)
			}
		}
	}
	switch {
	case s.problem != "":
	case len(levels) == 0:
		s.problem = "has no authority:L1..authority:L4 tag"
	case len(levels) > 1:
		s.problem = "declares more than one authority tag"
	default:
		for l := range levels {
			s.level = l
		}
	}
	return s
}

func a2aSkillViolations(skills []agentSkill) []string {
	var out []string
	for _, s := range skills {
		if s.problem != "" {
			out = append(out, agentViolation(fmt.Sprintf("%s: A2A skill %q %s", s.pos, s.id, s.problem), fixDeclareA2A))
			continue
		}
		if s.write && isLevel12(s.level) {
			out = append(out, agentViolation(
				fmt.Sprintf("%s: A2A skill %q has the `write` tag but is declared authority:%s; a write skill is L3 unless an ADR grants L4", s.pos, s.id, s.level),
				"change the tag to `authority:L3` (keep `write`); if the skill only reads, drop the `write` tag."))
		}
		if s.level == "L4" && !s.f.l4Documented() {
			out = append(out, agentViolation(
				fmt.Sprintf("%s: A2A skill %q is declared authority:L4 but %s has no `//authority:L4 <adr>` comment naming the ADR that grants it", s.pos, s.id, s.f.path),
				"write the ADR in the owning context first, then add `//authority:L4 ADR-0042 <one-line envelope>` to this file; otherwise declare authority:L3."))
		}
	}
	return out
}

// ---------------------------------------------------------------------
// resources + prompts (AS-3)
// ---------------------------------------------------------------------

// mcpSurfaceVerdict returns a skip reason (pending marker) or violations.
func mcpSurfaceVerdict(root string) (skip string, violations []string, err error) {
	pkgs, err := loadAgentPackages(root)
	if err != nil {
		return "", nil, err
	}
	resources, prompts := countSurfaceCalls(pkgs)
	if reason, found := readPendingMarker(root); found {
		if reason == "" {
			return "", []string{agentViolation(
				filepath.Join(root, agentSurfacePendingMarker)+" exists but is empty",
				"put ONE line in it saying why the resources/prompts are not there yet (and who adds them), or delete the file and add them.")}, nil
		}
		note := ""
		if resources > 0 && prompts > 0 {
			note = " (the surface is already complete: delete the marker)"
		}
		return "agent surface pending: " + reason + note, nil, nil
	}
	if resources == 0 {
		violations = append(violations, agentViolation(
			"the MCP adapter offers tools but never calls AddResource or AddResourceTemplate",
			"register at least one resource (read-only context data an agent can load without a tool call), e.g. `server.AddResourceTemplate(&mcp.ResourceTemplate{...}, handler)`; "+
				"if the repo is not rolled yet, park this test with a one-line reason in internal/adapters/inbound/mcp/"+agentSurfacePendingMarker+"."))
	}
	if prompts == 0 {
		violations = append(violations, agentViolation(
			"the MCP adapter offers tools but never calls AddPrompt",
			"register at least one workflow prompt, e.g. `server.AddPrompt(&mcp.Prompt{Name: \"cover_staffing_gaps\", ...}, handler)`; "+
				"if the repo is not rolled yet, park this test with a one-line reason in internal/adapters/inbound/mcp/"+agentSurfacePendingMarker+"."))
	}
	return "", violations, nil
}

// countSurfaceCalls counts AddResource/AddResourceTemplate and AddPrompt calls in non-test sources.
func countSurfaceCalls(pkgs []*agentPkg) (resources, prompts int) {
	for _, p := range pkgs {
		for _, f := range p.files {
			ast.Inspect(f.file, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				se, ok := call.Fun.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				switch se.Sel.Name {
				case "AddResource", "AddResourceTemplate":
					resources++
				case "AddPrompt":
					prompts++
				}
				return true
			})
		}
	}
	return resources, prompts
}

// readPendingMarker returns the marker's first non-empty line and whether the marker exists.
func readPendingMarker(root string) (reason string, found bool) {
	raw, err := os.ReadFile(filepath.Join(root, agentSurfacePendingMarker))
	if err != nil {
		return "", false
	}
	for _, line := range strings.Split(string(raw), "\n") {
		if l := strings.TrimSpace(line); l != "" {
			return l, true
		}
	}
	return "", true
}

// ---------------------------------------------------------------------
// skip rules
// ---------------------------------------------------------------------

// agentAdapterSkip returns the skip reason, or "" when the adapter must be checked.
// goMod == "" means no dependency condition (A2A).
func agentAdapterSkip(dir, goMod string) string {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		return "no " + strings.TrimPrefix(dir, "../") + " in this service: nothing to check"
	}
	if goMod == "" {
		return ""
	}
	raw, err := os.ReadFile(goMod)
	if err != nil || !strings.Contains(string(raw), mcpSDKModule) {
		return "go.mod does not depend on " + mcpSDKModule + ": no MCP server to check"
	}
	return ""
}

func requireMCPAdapter(t *testing.T) {
	t.Helper()
	if reason := agentAdapterSkip(mcpAdapterDir, moduleGoMod); reason != "" {
		t.Skip(reason)
	}
}

func mustScanMCP(t *testing.T, root string) mcpScan {
	t.Helper()
	s, err := scanMCPAdapter(root)
	if err != nil {
		t.Fatalf("scan %s: %v", root, err)
	}
	if len(s.tools) == 0 {
		t.Fatalf("%s exists but no mcp.Tool{...} literal was found; the sensor is looking in the wrong place or the tools are built in a way it cannot read (vacuous pass refused)", root)
	}
	return s
}

// ---------------------------------------------------------------------
// the tests that run against the real adapter
// ---------------------------------------------------------------------

func logTools(t *testing.T, s mcpScan) {
	t.Helper()
	for _, tool := range s.tools {
		t.Logf("checked tool %q: authority %q readOnly=%v", tool.name, tool.level, tool.readOnly)
	}
}

// TestEveryMCPToolDeclaresAuthority: every mcp.Tool literal declares its level.
func TestEveryMCPToolDeclaresAuthority(t *testing.T) {
	requireMCPAdapter(t)
	scan := mustScanMCP(t, mcpAdapterDir)
	logTools(t, scan)
	for _, v := range mcpDeclarationViolations(scan) {
		t.Error(v)
	}
}

// TestMCPWriteToolsAreAtLeastL3: a tool that is not provably read-only is L3, or L4 with an ADR comment.
func TestMCPWriteToolsAreAtLeastL3(t *testing.T) {
	requireMCPAdapter(t)
	scan := mustScanMCP(t, mcpAdapterDir)
	logTools(t, scan)
	for _, v := range mcpLevelViolations(scan) {
		t.Error(v)
	}
}

// TestA2ASkillsDeclareAuthority: every a2a.AgentSkill literal carries authority:Ln; write skills are L3/L4.
func TestA2ASkillsDeclareAuthority(t *testing.T) {
	if reason := agentAdapterSkip(a2aAdapterDir, ""); reason != "" {
		t.Skip(reason)
	}
	skills, err := scanA2AAdapter(a2aAdapterDir)
	if err != nil {
		t.Fatalf("scan %s: %v", a2aAdapterDir, err)
	}
	if len(skills) == 0 {
		t.Fatalf("%s exists but no a2a.AgentSkill{...} literal was found; the sensor is looking in the wrong place (vacuous pass refused)", a2aAdapterDir)
	}
	for _, s := range skills {
		t.Logf("checked skill %q: authority %q write=%v", s.id, s.level, s.write)
	}
	for _, v := range a2aSkillViolations(skills) {
		t.Error(v)
	}
}

// TestMCPServerOffersResourceAndPrompt (AS-3): tools alone are not an agent surface.
// A repo not yet rolled parks it with internal/adapters/inbound/mcp/.agent-surface-pending.
func TestMCPServerOffersResourceAndPrompt(t *testing.T) {
	requireMCPAdapter(t)
	mustScanMCP(t, mcpAdapterDir)
	skip, violations, err := mcpSurfaceVerdict(mcpAdapterDir)
	if err != nil {
		t.Fatalf("scan %s: %v", mcpAdapterDir, err)
	}
	if skip != "" {
		t.Skip(skip)
	}
	for _, v := range violations {
		t.Error(v)
	}
}

// ---------------------------------------------------------------------
// the sensors' own tests: throwaway fixture trees, red then green
// ---------------------------------------------------------------------

const (
	fxMCPHead = "package mcp\n\nimport \"github.com/modelcontextprotocol/go-sdk/mcp\"\n\n"
	fxHelper  = "const (\n\tauthorityMetaKey = \"com.warehouse/authority\"\n\tauthorityL1 = \"L1\"\n\tauthorityL2 = \"L2\"\n\tauthorityL3 = \"L3\"\n\tauthorityL4 = \"L4\"\n)\n\n" +
		"func authorityMeta(level string) mcp.Meta { return mcp.Meta{authorityMetaKey: level} }\n\n"
	fxA2AHead = "package a2a\n\nimport \"github.com/a2aproject/a2a-go/a2a\"\n\n"
)

// fxRegister wraps tool literals in a registration function.
func fxRegister(body string) string {
	return "func register(s *mcp.Server) {\n\treadOnly := true\n\twritable := false\n\t_, _ = readOnly, writable\n" + body + "}\n"
}

func fxTool(name, annotations, meta string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "\tadd(s, &mcp.Tool{Name: %q", name)
	if annotations != "" {
		b.WriteString(", Annotations: " + annotations)
	}
	if meta != "" {
		b.WriteString(", Meta: " + meta)
	}
	b.WriteString("})\n")
	return b.String()
}

func fxSkills(skills ...string) string {
	return "func card() []a2a.AgentSkill {\n\treturn []a2a.AgentSkill{\n" + strings.Join(skills, "") + "\t}\n}\n"
}

func fxSkill(id, tags string) string {
	return fmt.Sprintf("\t\t{ID: %q, Tags: %s},\n", id, tags)
}

func writeAgentFixture(t *testing.T, files map[string]string) string {
	t.Helper()
	root := t.TempDir()
	for rel, src := range files {
		p := filepath.Join(root, rel)
		if err := os.MkdirAll(filepath.Dir(p), 0o750); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		if err := os.WriteFile(p, []byte(src), 0o600); err != nil {
			t.Fatalf("write fixture: %v", err)
		}
	}
	return root
}

// wantAgentViolations asserts green (want empty) or red with every needle present
// and the WHAT / WHY / FIX anatomy and the rule name in the message.
func wantAgentViolations(t *testing.T, got []string, want []string) {
	t.Helper()
	joined := strings.Join(got, "\n")
	if len(want) == 0 {
		if len(got) != 0 {
			t.Fatalf("compliant fixture reported violations:\n%s", joined)
		}
		return
	}
	if len(got) == 0 {
		t.Fatalf("sensor did not flag the bad fixture (wanted %q)", want)
	}
	for _, needle := range append([]string{"WHAT:", "WHY:", "FIX:", "agent-authority"}, want...) {
		if !strings.Contains(joined, needle) {
			t.Fatalf("violation does not contain %q:\n%s", needle, joined)
		}
	}
}

func TestMCPAuthoritySensorBites(t *testing.T) {
	ro := "&mcp.ToolAnnotations{ReadOnlyHint: true}"
	cases := []struct {
		name      string
		src       string
		wantTools int
		want      []string
	}{
		{"helper with named constant (the pinned shape)", fxRegister(fxTool("get_x", ro, "authorityMeta(authorityL1)")), 1, nil},
		{"helper with a string literal", fxRegister(fxTool("get_x", ro, `authorityMeta("L2")`)), 1, nil},
		{"inline Meta literal with the key constant", fxRegister(fxTool("get_x", ro, "mcp.Meta{authorityMetaKey: authorityL1}")), 1, nil},
		{"inline Meta literal with the key literal", fxRegister(fxTool("get_x", ro, `mcp.Meta{"com.warehouse/authority": "L3"}`)), 1, nil},
		{"elided slice literal is found", "var tools = []*mcp.Tool{{Name: \"a\", Meta: authorityMeta(authorityL1)}, {Name: \"b\"}}\n", 2, []string{`"b" has no Meta field`}},
		{"no Meta", fxRegister(fxTool("get_x", ro, "")), 1, []string{`"get_x" has no Meta field`, "authorityMeta(authorityL1)"}},
		{"Meta without the key", fxRegister(fxTool("get_x", ro, `mcp.Meta{"other": "L1"}`)), 1, []string{"without the com.warehouse/authority key"}},
		{"level L5", fxRegister(fxTool("get_x", ro, `authorityMeta("L5")`)), 1, []string{`"L5"`, "no L5"}},
		{"level lowercase", fxRegister(fxTool("get_x", ro, `authorityMeta("l1")`)), 1, []string{`"l1"`}},
		{"level computed at run time", fxRegister(fxTool("get_x", ro, "authorityMeta(os.Getenv(\"LEVEL\"))")), 1, []string{"not a string literal or a constant"}},
		{"unsupported builder", fxRegister(fxTool("get_x", ro, "buildMeta()")), 1, []string{"something other than authorityMeta"}},
		{"helper that does not set the key", "func authorityMeta(level string) mcp.Meta { return nil }\n" + fxRegister(fxTool("get_x", ro, `authorityMeta("L1")`)), 1, []string{"does not set the com.warehouse/authority key"}},
		{"helper not defined", fxRegister(fxTool("get_x", ro, `authorityMeta("L1")`)), 1, []string{"does not define it"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			src := fxMCPHead + tc.src
			if !strings.Contains(tc.name, "helper that does not") && !strings.Contains(tc.name, "helper not defined") {
				src = fxMCPHead + fxHelper + tc.src
			}
			root := writeAgentFixture(t, map[string]string{"tools.go": src})
			s, err := scanMCPAdapter(root)
			if err != nil {
				t.Fatalf("scan: %v", err)
			}
			if len(s.tools) != tc.wantTools {
				t.Fatalf("found %d tools, want %d", len(s.tools), tc.wantTools)
			}
			wantAgentViolations(t, mcpDeclarationViolations(s), tc.want)
		})
	}

	t.Run("an adapter with no tool literal finds zero tools (the real test fails on that)", func(t *testing.T) {
		root := writeAgentFixture(t, map[string]string{"server.go": fxMCPHead + "func New() *mcp.Server { return nil }\n"})
		s, err := scanMCPAdapter(root)
		if err != nil || len(s.tools) != 0 {
			t.Fatalf("want zero tools and no error, got %d, %v", len(s.tools), err)
		}
	})

	t.Run("a file that does not import the go-sdk is not scanned", func(t *testing.T) {
		root := writeAgentFixture(t, map[string]string{"x.go": "package mcp\n\ntype Tool struct{ Name string }\n\nvar _ = mcp.Tool{Name: \"x\"}\n"})
		s, err := scanMCPAdapter(root)
		if err != nil || len(s.tools) != 0 {
			t.Fatalf("want zero tools and no error, got %d, %v", len(s.tools), err)
		}
	})

	t.Run("declarations resolve across files of one package", func(t *testing.T) {
		root := writeAgentFixture(t, map[string]string{
			"consts.go": fxMCPHead + fxHelper,
			"tools.go":  fxMCPHead + fxRegister(fxTool("get_x", ro, "authorityMeta(authorityL1)")),
		})
		s, err := scanMCPAdapter(root)
		if err != nil {
			t.Fatalf("scan: %v", err)
		}
		wantAgentViolations(t, mcpDeclarationViolations(s), nil)
	})
}

func TestMCPWriteLevelSensorBites(t *testing.T) {
	ro := "&mcp.ToolAnnotations{ReadOnlyHint: true}"
	cases := []struct {
		name string
		src  string
		want []string
	}{
		{"read tool L1", fxTool("get_x", ro, "authorityMeta(authorityL1)"), nil},
		{"read tool L2", fxTool("get_x", ro, "authorityMeta(authorityL2)"), nil},
		{"read via a local variable assigned true", fxTool("get_x", "&mcp.ToolAnnotations{ReadOnlyHint: readOnly}", "authorityMeta(authorityL1)"), nil},
		{"write tool L3", fxTool("do_x", "&mcp.ToolAnnotations{ReadOnlyHint: false}", "authorityMeta(authorityL3)"), nil},
		{"explicit ReadOnlyHint false and L2", fxTool("do_x", "&mcp.ToolAnnotations{ReadOnlyHint: false}", "authorityMeta(authorityL2)"), []string{`"do_x" is declared L2`, "ReadOnlyHint is false"}},
		{"explicit ReadOnlyHint false and L1", fxTool("do_x", "&mcp.ToolAnnotations{ReadOnlyHint: false}", "authorityMeta(authorityL1)"), []string{`"do_x" is declared L1`}},
		{"ReadOnlyHint via a variable assigned false", fxTool("do_x", "&mcp.ToolAnnotations{ReadOnlyHint: writable}", "authorityMeta(authorityL1)"), []string{"ReadOnlyHint is false"}},
		{"no Annotations at all", fxTool("do_x", "", "authorityMeta(authorityL1)"), []string{"no Annotations"}},
		{"Annotations without ReadOnlyHint", fxTool("do_x", "&mcp.ToolAnnotations{Title: \"x\"}", "authorityMeta(authorityL2)"), []string{"do not set ReadOnlyHint"}},
		{"ReadOnlyHint computed", fxTool("do_x", "&mcp.ToolAnnotations{ReadOnlyHint: isRO()}", "authorityMeta(authorityL1)"), []string{"neither the literal true"}},
		{"L4 without any comment", fxTool("do_x", "&mcp.ToolAnnotations{ReadOnlyHint: false}", "authorityMeta(authorityL4)"), []string{"declared L4", "//authority:L4"}},
		{"L4 with a comment that names no ADR", "//authority:L4 because we said so\n" + fxTool("do_x", "&mcp.ToolAnnotations{ReadOnlyHint: false}", "authorityMeta(authorityL4)"), []string{"declared L4"}},
		{"L4 with an ADR comment", "//authority:L4 ADR-0042 bounded auto-reallocation envelope\n" + fxTool("do_x", "&mcp.ToolAnnotations{ReadOnlyHint: false}", "authorityMeta(authorityL4)"), nil},
		{"L4 with an ADR path comment", "//authority:L4 docs/adr/0042-envelope.md\n" + fxTool("do_x", "&mcp.ToolAnnotations{ReadOnlyHint: false}", "authorityMeta(authorityL4)"), nil},
		{"undeclared tools are left to the declaration test", fxTool("do_x", "&mcp.ToolAnnotations{ReadOnlyHint: false}", ""), nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// A leading //authority:L4 comment is file-level; the tool literal goes inside register().
			comment, tool, _ := strings.Cut(tc.src, "	add(")
			src := fxMCPHead + fxHelper + comment + fxRegister("	add("+tool)
			root := writeAgentFixture(t, map[string]string{"tools.go": src})
			s, err := scanMCPAdapter(root)
			if err != nil || len(s.tools) != 1 {
				t.Fatalf("want one tool, got %d, %v", len(s.tools), err)
			}
			wantAgentViolations(t, mcpLevelViolations(s), tc.want)
		})
	}
}

func TestA2AAuthoritySensorBites(t *testing.T) {
	cases := []struct {
		name       string
		src        string
		wantSkills int
		want       []string
	}{
		{"read skill L1", fxSkills(fxSkill("status", `[]string{"workforce", "read", "authority:L1"}`)), 1, nil},
		{"write skill L3", fxSkills(fxSkill("assign", `[]string{"write", "authority:L3"}`)), 1, nil},
		{"tag through a constant", "const tagL1 = \"authority:L1\"\n" + fxSkills(fxSkill("status", `[]string{tagL1}`)), 1, nil},
		{"skill without an authority tag", fxSkills(fxSkill("status", `[]string{"workforce", "read"}`)), 1, []string{`"status" has no authority:L1..authority:L4 tag`, "authority:L1"}},
		{"skill with no Tags", "func card() []a2a.AgentSkill { return []a2a.AgentSkill{{ID: \"x\"}} }\n", 1, []string{"no Tags"}},
		{"malformed tag", fxSkills(fxSkill("status", `[]string{"authority:L5"}`)), 1, []string{`malformed tag "authority:L5"`}},
		{"two authority tags", fxSkills(fxSkill("status", `[]string{"authority:L1", "authority:L3"}`)), 1, []string{"more than one"}},
		{"write skill declared L1", fxSkills(fxSkill("assign", `[]string{"write", "authority:L1"}`)), 1, []string{"has the `write` tag but is declared authority:L1"}},
		{"write skill declared L2", fxSkills(fxSkill("assign", `[]string{"write", "authority:L2"}`)), 1, []string{"authority:L2"}},
		{"L4 without ADR comment", fxSkills(fxSkill("assign", `[]string{"write", "authority:L4"}`)), 1, []string{"declared authority:L4", "//authority:L4"}},
		{"L4 with ADR comment", "//authority:L4 ADR-0042 envelope\n" + fxSkills(fxSkill("assign", `[]string{"write", "authority:L4"}`)), 1, nil},
		{"two skills, one bad", fxSkills(fxSkill("a", `[]string{"authority:L1"}`), fxSkill("b", `[]string{"read"}`)), 2, []string{`"b"`}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			root := writeAgentFixture(t, map[string]string{"server.go": fxA2AHead + tc.src})
			skills, err := scanA2AAdapter(root)
			if err != nil {
				t.Fatalf("scan: %v", err)
			}
			if len(skills) != tc.wantSkills {
				t.Fatalf("found %d skills, want %d", len(skills), tc.wantSkills)
			}
			wantAgentViolations(t, a2aSkillViolations(skills), tc.want)
		})
	}

	t.Run("an adapter with no skill literal finds zero skills (the real test fails on that)", func(t *testing.T) {
		root := writeAgentFixture(t, map[string]string{"server.go": fxA2AHead + "func New() {}\n"})
		skills, err := scanA2AAdapter(root)
		if err != nil || len(skills) != 0 {
			t.Fatalf("want zero skills and no error, got %d, %v", len(skills), err)
		}
	})
}

func TestMCPSurfaceSensorBites(t *testing.T) {
	const (
		tools     = "func register(s *mcp.Server) {}\n"
		resource  = "func res(s *mcp.Server) { s.AddResourceTemplate(&mcp.ResourceTemplate{}, nil) }\n"
		resource2 = "func res2(s *mcp.Server) { s.AddResource(&mcp.Resource{}, nil) }\n"
		prompt    = "func prm(s *mcp.Server) { s.AddPrompt(&mcp.Prompt{}, nil) }\n"
	)
	cases := []struct {
		name     string
		files    map[string]string
		wantSkip string
		want     []string
	}{
		{"tools only", map[string]string{"s.go": fxMCPHead + tools}, "", []string{"never calls AddResource", "never calls AddPrompt", ".agent-surface-pending"}},
		{"resource but no prompt", map[string]string{"s.go": fxMCPHead + tools + resource}, "", []string{"never calls AddPrompt"}},
		{"prompt but no resource", map[string]string{"s.go": fxMCPHead + tools + prompt}, "", []string{"never calls AddResource"}},
		{"resource template and prompt", map[string]string{"s.go": fxMCPHead + tools + resource + prompt}, "", nil},
		{"AddResource and prompt in other files", map[string]string{"s.go": fxMCPHead + tools, "r.go": fxMCPHead + resource2, "p.go": fxMCPHead + prompt}, "", nil},
		{"pending marker parks the test", map[string]string{"s.go": fxMCPHead + tools, agentSurfacePendingMarker: "\nrollout wave 2: resources and prompts follow with ADR-0007\n"}, "agent surface pending: rollout wave 2", nil},
		{"stale marker on a complete surface says so", map[string]string{"s.go": fxMCPHead + tools + resource + prompt, agentSurfacePendingMarker: "wave 2\n"}, "delete the marker", nil},
		{"empty marker is refused", map[string]string{"s.go": fxMCPHead + tools, agentSurfacePendingMarker: "  \n"}, "", []string{"exists but is empty"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			skip, got, err := mcpSurfaceVerdict(writeAgentFixture(t, tc.files))
			if err != nil {
				t.Fatalf("verdict: %v", err)
			}
			if tc.wantSkip != "" {
				if !strings.Contains(skip, tc.wantSkip) || len(got) != 0 {
					t.Fatalf("want skip containing %q and no violations, got skip %q, violations %v", tc.wantSkip, skip, got)
				}
				return
			}
			if skip != "" {
				t.Fatalf("unexpected skip %q", skip)
			}
			wantAgentViolations(t, got, tc.want)
		})
	}
}

func TestAgentAdapterSkipRules(t *testing.T) {
	root := t.TempDir()
	adapter := filepath.Join(root, "mcp")
	if err := os.MkdirAll(adapter, 0o750); err != nil {
		t.Fatal(err)
	}
	withSDK := filepath.Join(root, "with.mod")
	withoutSDK := filepath.Join(root, "without.mod")
	if err := os.WriteFile(withSDK, []byte("module x\n\nrequire github.com/modelcontextprotocol/go-sdk v1.8.0\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(withoutSDK, []byte("module x\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	for name, tc := range map[string]struct {
		dir, mod string
		skip     bool
	}{
		"no adapter directory":           {filepath.Join(root, "absent"), withSDK, true},
		"adapter but no go-sdk in mod":   {adapter, withoutSDK, true},
		"adapter but no go.mod at all":   {adapter, filepath.Join(root, "missing.mod"), true},
		"adapter and go-sdk":             {adapter, withSDK, false},
		"a2a: adapter, no mod condition": {adapter, "", false},
		"a2a: no adapter":                {filepath.Join(root, "absent"), "", true},
	} {
		if got := agentAdapterSkip(tc.dir, tc.mod) != ""; got != tc.skip {
			t.Errorf("%s: skip=%v, want %v", name, got, tc.skip)
		}
	}
}
