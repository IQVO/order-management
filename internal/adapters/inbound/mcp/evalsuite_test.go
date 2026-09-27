// E3 — behavioral eval suite: Gherkin scenarios in
// testdata/features/mcp_tools.feature driven through godog, the same
// Cucumber-for-Go engine the repo-root REST acceptance suite uses. The
// suite lives inside the mcp package so the evals ship with the adapter
// they evaluate and run in the existing CI test job (`go test ./...`) with
// zero new infrastructure.
package mcp_test

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/cucumber/godog"
)

// currentEvalT carries the running *testing.T into the scenario world;
// godog's ScenarioInitializer API does not hand it to step contexts, and
// the shared harness needs it for t.Cleanup. Safe here because the suite
// runs scenarios sequentially inside one test function.
var currentEvalT *testing.T

// TestMCPEvalSuite runs every Gherkin scenario under testdata/features
// against a freshly wired MCP server + client session.
func TestMCPEvalSuite(t *testing.T) {
	currentEvalT = t
	suite := godog.TestSuite{
		ScenarioInitializer: initializeMCPEvalScenario,
		Options: &godog.Options{
			Format:   "pretty",
			Paths:    []string{"testdata/features"},
			Strict:   true,
			TestingT: t,
		},
	}
	if suite.Run() != 0 {
		t.Fatal("non-zero status returned, failed to run MCP eval scenarios")
	}
}

// mcpEvalWorld is the per-scenario state: one harness (server + session +
// seeded repos) per scenario, rebuilt by the Background step.
type mcpEvalWorld struct {
	h *evalHarness
}

func initializeMCPEvalScenario(sc *godog.ScenarioContext) {
	w := &mcpEvalWorld{}

	sc.Before(func(ctx context.Context, _ *godog.Scenario) (context.Context, error) {
		return ctx, nil
	})

	// Given
	sc.Step(`^the MCP server is running with the canonical eval state \(order ORD-1 with one allocated and one backordered line, one hour of promise-health analytics\)$`, w.serverRunning)

	// When
	sc.Step(`^I call the tool "([^"]*)" with argument "([^"]*)" = "([^"]*)"$`, w.callWithStringArg)
	sc.Step(`^I call the tool "([^"]*)" with argument "([^"]*)" = (\d+)$`, w.callWithNumberArg)
	sc.Step(`^I call the tool "([^"]*)" with arguments$`, w.callWithTableArgs)
	sc.Step(`^I call the tool "([^"]*)" with no arguments$`, w.callWithNoArgs)

	// Then
	sc.Step(`^the tool call succeeds$`, w.callSucceeded)
	sc.Step(`^the tool call does not succeed silently$`, w.callDidNotSucceedSilently)
	sc.Step(`^the tool call reports a problem mentioning "([^"]*)"$`, w.callErroredMentioning)
	sc.Step(`^the structured result field "([^"]*)" is "([^"]*)"$`, w.fieldIsString)
	sc.Step(`^the structured result field "([^"]*)" is (\d+)$`, w.fieldIsNumber)
	sc.Step(`^the structured result field "([^"]*)" is (\d+\.\d+)$`, w.fieldIsFloat)
	sc.Step(`^the structured result field "([^"]*)" is true$`, w.fieldIsTrue)
	sc.Step(`^the structured result field "([^"]*)" is false$`, w.fieldIsFalse)
	sc.Step(`^the structured result line (\d+) field "([^"]*)" is "([^"]*)"$`, w.lineFieldIsString)
	sc.Step(`^the structured result line (\d+) field "([^"]*)" is (\d+)$`, w.lineFieldIsNumber)
	sc.Step(`^the structured result line (\d+) field "([^"]*)" is true$`, w.lineFieldIsTrue)
	sc.Step(`^the structured result line (\d+) field "([^"]*)" is false$`, w.lineFieldIsFalse)
	sc.Step(`^the structured result line (\d+) omits "([^"]*)"$`, w.lineOmits)
}

func (w *mcpEvalWorld) serverRunning() error {
	// The harness binds its own lifecycle to the running *testing.T via
	// newEvalHarness; the world only carries the pointer.
	w.h = newEvalHarness(currentEvalT)
	return nil
}

func (w *mcpEvalWorld) callWithStringArg(tool, arg, value string) error {
	return w.h.callTool(context.Background(), tool, map[string]any{arg: value})
}

func (w *mcpEvalWorld) callWithNumberArg(tool, arg string, value int64) error {
	return w.h.callTool(context.Background(), tool, map[string]any{arg: value})
}

func (w *mcpEvalWorld) callWithTableArgs(tool string, table *godog.Table) error {
	args := map[string]any{}
	for _, row := range table.Rows {
		cells := row.Cells
		if len(cells) != 2 {
			return fmt.Errorf("arguments table needs exactly two columns, got %d", len(cells))
		}
		key, raw := cells[0].Value, cells[1].Value
		if n, err := strconv.Atoi(raw); err == nil {
			args[key] = n
			continue
		}
		args[key] = raw
	}
	return w.h.callTool(context.Background(), tool, args)
}

func (w *mcpEvalWorld) callWithNoArgs(tool string) error {
	return w.h.callTool(context.Background(), tool, map[string]any{})
}

func (w *mcpEvalWorld) callSucceeded() error {
	if w.h.lastCallErr != nil {
		return fmt.Errorf("tool call failed: %w", w.h.lastCallErr)
	}
	if w.h.lastCallResult == nil || w.h.lastCallResult.IsError {
		return fmt.Errorf("tool call returned an error result: %s", w.h.lastCallContent)
	}
	return nil
}

func (w *mcpEvalWorld) callDidNotSucceedSilently() error {
	if w.h.lastCallErr != nil {
		return nil // protocol-level rejection
	}
	if w.h.lastCallResult != nil && w.h.lastCallResult.IsError {
		return nil // tool-level rejection
	}
	return fmt.Errorf("the call succeeded silently — wrong-typed arguments must not be coerced")
}

func (w *mcpEvalWorld) callErroredMentioning(fragment string) error {
	if w.h.lastCallErr == nil && (w.h.lastCallResult == nil || !w.h.lastCallResult.IsError) {
		return fmt.Errorf("expected a tool error, got success: %s", w.h.lastCallContent)
	}
	if !containsFold(w.h.lastCallContent, fragment) && w.h.lastCallErr != nil && !containsFold(w.h.lastCallErr.Error(), fragment) {
		return fmt.Errorf("expected the tool error to mention %q, got %q / %v", fragment, w.h.lastCallContent, w.h.lastCallErr)
	}
	return nil
}

func (w *mcpEvalWorld) fieldIsString(field, want string) error {
	got, err := w.structuredField(field)
	if err != nil {
		return err
	}
	if s, ok := got.(string); ok && s == want {
		return nil
	}
	return fmt.Errorf("structured result field %q = %v, want %q", field, got, want)
}

func (w *mcpEvalWorld) fieldIsNumber(field string, want int64) error {
	got, err := w.structuredField(field)
	if err != nil {
		return err
	}
	switch v := got.(type) {
	case float64:
		if int64(v) == want {
			return nil
		}
	case int:
		if int64(v) == want {
			return nil
		}
	case int64:
		if v == want {
			return nil
		}
	}
	return fmt.Errorf("structured result field %q = %v, want %d", field, got, want)
}

func (w *mcpEvalWorld) fieldIsFloat(field string, want float64) error {
	got, err := w.structuredField(field)
	if err != nil {
		return err
	}
	if v, ok := got.(float64); ok && v == want {
		return nil
	}
	return fmt.Errorf("structured result field %q = %v, want %v", field, got, want)
}

func (w *mcpEvalWorld) fieldIsTrue(field string) error {
	got, err := w.structuredField(field)
	if err != nil {
		return err
	}
	if b, ok := got.(bool); ok && b {
		return nil
	}
	return fmt.Errorf("structured result field %q = %v, want true", field, got)
}

func (w *mcpEvalWorld) fieldIsFalse(field string) error {
	got, err := w.structuredField(field)
	if err != nil {
		return err
	}
	if b, ok := got.(bool); ok && !b {
		return nil
	}
	return fmt.Errorf("structured result field %q = %v, want false", field, got)
}

// lineField returns a field of the nth (1-based) entry of the structured
// result's "lines" array — get_order's per-line projection.
func (w *mcpEvalWorld) lineField(n int, field string) (any, error) {
	if w.h.lastCallResult == nil {
		return nil, fmt.Errorf("no tool result recorded")
	}
	obj, ok := w.h.lastCallResult.StructuredContent.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("structured result is not an object: %+v", w.h.lastCallResult.StructuredContent)
	}
	lines, ok := obj["lines"].([]any)
	if !ok {
		return nil, fmt.Errorf("structured result has no lines array: %+v", obj)
	}
	if n < 1 || n > len(lines) {
		return nil, fmt.Errorf("lines has %d entries, want entry %d", len(lines), n)
	}
	line, ok := lines[n-1].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("lines entry %d is not an object: %+v", n, lines[n-1])
	}
	got, ok := line[field]
	if !ok {
		return nil, fmt.Errorf("lines entry %d has no field %q: %+v", n, field, line)
	}
	return got, nil
}

func (w *mcpEvalWorld) lineFieldIsString(n int, field, want string) error {
	got, err := w.lineField(n, field)
	if err != nil {
		return err
	}
	if s, ok := got.(string); ok && s == want {
		return nil
	}
	return fmt.Errorf("structured result line %d field %q = %v, want %q", n, field, got, want)
}

func (w *mcpEvalWorld) lineFieldIsNumber(n int, field string, want int64) error {
	got, err := w.lineField(n, field)
	if err != nil {
		return err
	}
	if v, ok := got.(float64); ok && int64(v) == want {
		return nil
	}
	return fmt.Errorf("structured result line %d field %q = %v, want %d", n, field, got, want)
}

func (w *mcpEvalWorld) lineFieldIsTrue(n int, field string) error {
	got, err := w.lineField(n, field)
	if err != nil {
		return err
	}
	if b, ok := got.(bool); ok && b {
		return nil
	}
	return fmt.Errorf("structured result line %d field %q = %v, want true", n, field, got)
}

func (w *mcpEvalWorld) lineFieldIsFalse(n int, field string) error {
	got, err := w.lineField(n, field)
	if err != nil {
		return err
	}
	if b, ok := got.(bool); ok && !b {
		return nil
	}
	return fmt.Errorf("structured result line %d field %q = %v, want false", n, field, got)
}

// lineOmits pins that a line projection OMITS a field entirely — the
// reservationId-is-absent-means-not-allocated contract in mapping.go.
func (w *mcpEvalWorld) lineOmits(n int, field string) error {
	if w.h.lastCallResult == nil {
		return fmt.Errorf("no tool result recorded")
	}
	obj, ok := w.h.lastCallResult.StructuredContent.(map[string]any)
	if !ok {
		return fmt.Errorf("structured result is not an object: %+v", w.h.lastCallResult.StructuredContent)
	}
	lines, ok := obj["lines"].([]any)
	if !ok {
		return fmt.Errorf("structured result has no lines array: %+v", obj)
	}
	if n < 1 || n > len(lines) {
		return fmt.Errorf("lines has %d entries, want entry %d", len(lines), n)
	}
	line, ok := lines[n-1].(map[string]any)
	if !ok {
		return fmt.Errorf("lines entry %d is not an object: %+v", n, lines[n-1])
	}
	if _, present := line[field]; present {
		return fmt.Errorf("structured result line %d must omit %q, got %v", n, field, line[field])
	}
	return nil
}

func (w *mcpEvalWorld) structuredField(field string) (any, error) {
	if w.h.lastCallResult == nil {
		return nil, fmt.Errorf("no tool result recorded")
	}
	obj, ok := w.h.lastCallResult.StructuredContent.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("structured result is not an object: %+v", w.h.lastCallResult.StructuredContent)
	}
	got, ok := obj[field]
	if !ok {
		return nil, fmt.Errorf("structured result has no field %q: %+v", field, obj)
	}
	return got, nil
}

func containsFold(s, substr string) bool {
	return strings.Contains(strings.ToLower(s), strings.ToLower(substr))
}
