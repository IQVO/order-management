// Agentic Standard v2 (AS-4): the Streamable HTTP handler is stateless, so it
// answers the 2026-07-28 per-request handshake (server/discover) while legacy
// session-based clients keep working. Raw POSTs, no SDK client: these pin the
// wire facts (contracts S1-S3), not the SDK's negotiation.
package mcp_test

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	inboundmcp "github.com/claudioed/order-management/internal/adapters/inbound/mcp"
)

const newProtocol = "2026-07-28"

// newProtocolParams is the per-request _meta every 2026-07-28 call carries.
func newProtocolParams(extra map[string]any) map[string]any {
	p := map[string]any{"_meta": map[string]any{
		"io.modelcontextprotocol/protocolVersion":    newProtocol,
		"io.modelcontextprotocol/clientCapabilities": map[string]any{},
	}}
	for k, v := range extra {
		p[k] = v
	}
	return p
}

// rawPost sends one JSON-RPC request and returns the status, the response
// headers and the decoded JSON-RPC result/error. A text/event-stream body is
// unwrapped to its first data: line.
func rawPost(t *testing.T, url string, headers map[string]string, method string, params any) (int, http.Header, map[string]any) {
	t.Helper()
	body, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	req, err := http.NewRequest(http.MethodPost, url, strings.NewReader(string(body)))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST %s: %v", method, err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	payload := string(raw)
	if strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream") {
		for _, line := range strings.Split(payload, "\n") {
			if data, ok := strings.CutPrefix(line, "data:"); ok {
				payload = strings.TrimSpace(data)
				break
			}
		}
	}
	var out map[string]any
	if err := json.Unmarshal([]byte(payload), &out); err != nil {
		t.Fatalf("%s: response is not JSON (%v): %s", method, err, raw)
	}
	return resp.StatusCode, resp.Header, out
}

func newStatelessServer(t *testing.T) string {
	t.Helper()
	srv := httptest.NewServer(inboundmcp.Handler(inboundmcp.NewServer(newEvalDeps())))
	t.Cleanup(srv.Close)
	return srv.URL
}

func resultOf(t *testing.T, resp map[string]any) map[string]any {
	t.Helper()
	if e, ok := resp["error"]; ok {
		t.Fatalf("JSON-RPC error: %v", e)
	}
	res, ok := resp["result"].(map[string]any)
	if !ok {
		t.Fatalf("response has no result object: %v", resp)
	}
	return res
}

// (a) server/discover with no initialize and no session advertises 2026-07-28.
func TestStateless_ServerDiscoverAdvertisesNewRevision(t *testing.T) {
	url := newStatelessServer(t)
	status, hdr, resp := rawPost(t, url, map[string]string{
		"MCP-Protocol-Version": newProtocol,
		"Mcp-Method":           "server/discover",
	}, "server/discover", newProtocolParams(nil))
	if status != http.StatusOK {
		t.Fatalf("server/discover status = %d, want 200 (body %v)", status, resp)
	}
	if id := hdr.Get("Mcp-Session-Id"); id != "" {
		t.Errorf("stateless server issued Mcp-Session-Id %q", id)
	}
	versions, _ := resultOf(t, resp)["supportedVersions"].([]any)
	found := false
	for _, v := range versions {
		if v == newProtocol {
			found = true
		}
	}
	if !found {
		t.Fatalf("supportedVersions = %v, want it to contain %s", versions, newProtocol)
	}
}

// (b) tools/list the same way returns every tool with its authority in _meta.
func TestStateless_ToolsListCarriesAuthorityMeta(t *testing.T) {
	url := newStatelessServer(t)
	status, _, resp := rawPost(t, url, map[string]string{
		"MCP-Protocol-Version": newProtocol,
		"Mcp-Method":           "tools/list",
	}, "tools/list", newProtocolParams(nil))
	if status != http.StatusOK {
		t.Fatalf("tools/list status = %d, want 200 (body %v)", status, resp)
	}
	tools, _ := resultOf(t, resp)["tools"].([]any)
	want := map[string]string{
		"get_order":          "L1",
		"get_promise_health": "L1",
	}
	got := map[string]string{}
	for _, raw := range tools {
		tool, _ := raw.(map[string]any)
		meta, _ := tool["_meta"].(map[string]any)
		level, _ := meta[authorityKey].(string)
		name, _ := tool["name"].(string)
		got[name] = level
	}
	if len(got) != len(want) {
		t.Fatalf("tools = %v, want %v", got, want)
	}
	for name, level := range want {
		if got[name] != level {
			t.Errorf("tool %s authority = %q, want %q (all: %v)", name, got[name], level, got)
		}
	}
}

// (c) a legacy initialize (2025-06-18) still answers 200 and negotiates the
// legacy revision.
func TestStateless_LegacyInitializeStillAnswers(t *testing.T) {
	url := newStatelessServer(t)
	status, _, resp := rawPost(t, url, map[string]string{
		"MCP-Protocol-Version": "2025-06-18",
	}, "initialize", map[string]any{
		"protocolVersion": "2025-06-18",
		"capabilities":    map[string]any{},
		"clientInfo":      map[string]any{"name": "legacy-client", "version": "0.0.1"},
	})
	if status != http.StatusOK {
		t.Fatalf("legacy initialize status = %d, want 200 (body %v)", status, resp)
	}
	res := resultOf(t, resp)
	if res["protocolVersion"] != "2025-06-18" {
		t.Errorf("protocolVersion = %v, want 2025-06-18", res["protocolVersion"])
	}
}
