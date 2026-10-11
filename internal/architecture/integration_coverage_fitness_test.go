package architecture

// Sensor for a real CI escape (2026-10-11): the `integration` job ran a
// hand-kept list of `go test -tags=integration ./pkg` lines, and the list
// silently omitted internal/adapters/inbound/mcp and
// internal/application/usecases, so the MCP Streamable-HTTP and use-case
// wiring suites (#169) never ran in CI while every check stayed green. The
// job now runs ./...; this test keeps it that way by failing whenever a
// package holding a //go:build integration test is not reached by any
// integration `go test` command in ci.yml.

import (
	"bufio"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

var integrationTestCmdRx = regexp.MustCompile(`go test\b[^\n]*-tags[= ]"?[^\s"]*\bintegration\b`)

// integrationTestCommands returns every `go test ... -tags=integration ...`
// line in the ci.yml text.
func integrationTestCommands(ci string) []string {
	var cmds []string
	for _, line := range strings.Split(ci, "\n") {
		if integrationTestCmdRx.MatchString(line) {
			cmds = append(cmds, line)
		}
	}
	return cmds
}

// covers reports whether a `go test` command line reaches package dir
// (slash-separated, relative to the module root): via ./..., a parent
// ./x/..., or the package itself.
func covers(cmd, dir string) bool {
	for _, f := range strings.Fields(cmd) {
		if !strings.HasPrefix(f, "./") {
			continue
		}
		p := strings.TrimPrefix(f, "./")
		if p == "..." {
			return true
		}
		if strings.HasSuffix(p, "/...") {
			if prefix := strings.TrimSuffix(p, "/..."); dir == prefix || strings.HasPrefix(dir, prefix+"/") {
				return true
			}
			continue
		}
		if p == dir {
			return true
		}
	}
	return false
}

// uncoveredIntegrationPackages returns the packages no integration command
// reaches.
func uncoveredIntegrationPackages(ci string, pkgs []string) []string {
	cmds := integrationTestCommands(ci)
	var out []string
	for _, pkg := range pkgs {
		reached := false
		for _, c := range cmds {
			if covers(c, pkg) {
				reached = true
				break
			}
		}
		if !reached {
			out = append(out, pkg)
		}
	}
	sort.Strings(out)
	return out
}

// integrationPackages walks the module for *_test.go files whose build
// constraint names the integration tag and returns their package dirs.
func integrationPackages(t *testing.T, root string) []string {
	t.Helper()
	seen := map[string]bool{}
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", "node_modules", "vendor", "docs", "web":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, "_test.go") || !hasIntegrationTag(t, path) {
			return nil
		}
		rel, err := filepath.Rel(root, filepath.Dir(path))
		if err != nil {
			return err
		}
		seen[filepath.ToSlash(rel)] = true
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", root, err)
	}
	pkgs := make([]string, 0, len(seen))
	for p := range seen {
		pkgs = append(pkgs, p)
	}
	sort.Strings(pkgs)
	return pkgs
}

func hasIntegrationTag(t *testing.T, path string) bool {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer func() { _ = f.Close() }()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if strings.HasPrefix(line, "package ") {
			return false // build constraints must precede the package clause
		}
		if strings.HasPrefix(line, "//go:build") && regexp.MustCompile(`\bintegration\b`).MatchString(line) {
			return true
		}
	}
	return false
}

func TestEveryIntegrationPackageRunsInCI(t *testing.T) {
	root := filepath.Join("..", "..")
	ci, err := os.ReadFile(filepath.Join(root, ".github", "workflows", "ci.yml"))
	if err != nil {
		t.Fatalf("read ci.yml: %v", err)
	}
	pkgs := integrationPackages(t, root)
	if len(pkgs) == 0 {
		t.Fatal("found no //go:build integration test packages; the walker is broken")
	}
	if missing := uncoveredIntegrationPackages(string(ci), pkgs); len(missing) > 0 {
		t.Fatalf("integration test packages no `go test -tags=integration` command in ci.yml reaches "+
			"(they never run in CI): %v -- run `go test -tags=integration ./...` instead of a package list", missing)
	}
}

// TestIntegrationCoverageSensorFailsOnBadFixtures proves the sensor bites:
// the exact pre-fix shape (a package list missing two suites) is flagged,
// and every legitimate form of coverage is accepted.
func TestIntegrationCoverageSensorFailsOnBadFixtures(t *testing.T) {
	pkgs := []string{"cmd/order", "internal/adapters/inbound/mcp", "internal/application/usecases"}

	list := "run: |\n  go test -tags=integration ./cmd/order\n  go test -tags=integration -v ./internal/adapters/outbound/postgres\n"
	got := uncoveredIntegrationPackages(list, pkgs)
	want := []string{"internal/adapters/inbound/mcp", "internal/application/usecases"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("pre-fix package list: uncovered = %v, want %v", got, want)
	}

	for name, ci := range map[string]string{
		"./... wildcard":        "run: go test -tags=integration ./... -race -count=1",
		"parent wildcards":      "go test -tags=integration ./cmd/... ./internal/...",
		"space-separated tags":  "go test -tags integration ./...",
		"quoted multi-tag list": `go test -tags="integration e2e" ./...`,
	} {
		if missing := uncoveredIntegrationPackages(ci, pkgs); len(missing) > 0 {
			t.Errorf("%s: falsely flagged %v", name, missing)
		}
	}

	if missing := uncoveredIntegrationPackages("go test ./... -race", pkgs); len(missing) != len(pkgs) {
		t.Errorf("a go test without the integration tag must not count as coverage, got missing=%v", missing)
	}
	if covers("go test -tags=integration ./internal/app", "internal/application/usecases") {
		t.Error("a sibling prefix (./internal/app) must not cover internal/application/...")
	}
}
