package architecture

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// optionalWordRE finds "optional" as a word in a jsonschema description.
var optionalWordRE = regexp.MustCompile(`(?i)\boptional\b`)

// optionalFieldViolations returns one message per struct field whose
// `jsonschema` description says the field is optional while its `json` tag
// lacks `omitempty`. The MCP Go SDK infers an input schema's `required` list
// from the json tags: a field without omitempty is REQUIRED, so a tool call
// that leaves the "optional" filter out is rejected before the handler runs
// (fleet gap-fix D16).
func optionalFieldViolations(path string, src []byte) ([]string, error) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, src, parser.SkipObjectResolution)
	if err != nil {
		return nil, err
	}
	var out []string
	ast.Inspect(file, func(n ast.Node) bool {
		field, ok := n.(*ast.Field)
		if !ok || field.Tag == nil {
			return true
		}
		if msg := optionalFieldViolation(field.Tag.Value); msg != "" {
			out = append(out, fmt.Sprintf("%s: %s", fset.Position(field.Pos()), msg))
		}
		return true
	})
	return out, nil
}

// optionalFieldViolation checks one raw struct tag literal.
func optionalFieldViolation(rawTag string) string {
	unquoted, err := strconv.Unquote(rawTag)
	if err != nil {
		return ""
	}
	tag := reflect.StructTag(unquoted)
	desc, ok := tag.Lookup("jsonschema")
	if !ok || !optionalWordRE.MatchString(desc) {
		return ""
	}
	jsonTag := tag.Get("json")
	if jsonTag == "-" {
		return ""
	}
	for _, opt := range strings.Split(jsonTag, ",")[1:] {
		if opt == "omitempty" || opt == "omitzero" {
			return ""
		}
	}
	return fmt.Sprintf("field described as optional (%q) has json tag %q without omitempty", desc, jsonTag)
}

// TestOptionalMCPInputsCarryOmitempty: every struct field whose jsonschema
// description calls it optional carries omitempty, so the generated MCP
// input schema does not list it as required.
func TestOptionalMCPInputsCarryOmitempty(t *testing.T) {
	for _, path := range goFilesUnder(t, "..", false) {
		src, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		violations, err := optionalFieldViolations(path, src)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		for _, v := range violations {
			t.Errorf("%s", archViolation("contents", "optional MCP tool inputs carry omitempty", v+
				" FIX: add ,omitempty to the json tag (or stop calling the field optional)."))
		}
	}
}

func TestOptionalMCPInputSensorFailsOnBadFixtures(t *testing.T) {
	const bad = "package x\n\ntype In struct {\n\tPathId string `json:\"pathId\" jsonschema:\"optional exact-match process path filter\"`\n}\n"
	const badCase = "package x\n\ntype In struct {\n\tSite string `json:\"site\" jsonschema:\"Optional site filter\"`\n}\n"
	const good = "package x\n\ntype In struct {\n\tPathId string `json:\"pathId,omitempty\" jsonschema:\"optional exact-match process path filter\"`\n}\n"
	const required = "package x\n\ntype In struct {\n\tFrom string `json:\"from\" jsonschema:\"inclusive start of the window\"`\n}\n"
	const noSchema = "package x\n\ntype In struct {\n\tA string `json:\"a\"`\n}\n"

	cases := []struct {
		name    string
		content string
		want    int
	}{
		{"optional without omitempty is flagged", bad, 1},
		{"capitalised Optional is flagged", badCase, 1},
		{"optional with omitempty is clean", good, 0},
		{"required field is clean", required, 0},
		{"field without jsonschema is clean", noSchema, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := optionalFieldViolations("fixture.go", []byte(tc.content))
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			if len(got) != tc.want {
				t.Fatalf("want %d violation(s), got %d: %v", tc.want, len(got), got)
			}
		})
	}
}
