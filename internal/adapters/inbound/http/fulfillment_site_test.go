package http_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
)

const problemSiteSlug = "https://errors.order-management.warehouse-systems.dev/invalid-fulfillment-site-id"

func rawBody(t *testing.T, body []byte) map[string]json.RawMessage {
	t.Helper()
	var m map[string]json.RawMessage
	if err := json.Unmarshal(body, &m); err != nil {
		t.Fatalf("decode: %v (%s)", err, body)
	}
	return m
}

func siteOf(t *testing.T, body []byte) string {
	t.Helper()
	raw, ok := rawBody(t, body)["fulfillmentSiteId"]
	if !ok {
		return ""
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		t.Fatalf("fulfillmentSiteId: %v", err)
	}
	return s
}

func TestPostOrders_FulfillmentSite(t *testing.T) {
	t.Run("a valid site is stored and echoed on create and on every read", testSiteEchoedEverywhere)
	t.Run("a network-originated hold carries its site", testSiteOnHeldOrder)
	t.Run("an order without a site omits the field entirely", testNoSiteOmitsField)
	t.Run("a malformed site is a 400 with the documented slug", testMalformedSiteIs400)
	t.Run("an explicit null site is a 400, not an absent site", testNullSiteIs400)
	t.Run("cancelling an order never changes its site", testSiteSurvivesCancel)
}

func testSiteEchoedEverywhere(t *testing.T) {
	e := newTestEnv(t)
	rec := e.do(t, http.MethodPost, "/orders", `{"fulfillmentSiteId":"WH2","lines":[{"sku":"SKU-1","quantity":1}]}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d (%s)", rec.Code, rec.Body.String())
	}
	if got := siteOf(t, rec.Body.Bytes()); got != "WH2" {
		t.Fatalf("POST fulfillmentSiteId = %q, want WH2", got)
	}
	id := decodeOrder(t, rec).ID
	get := e.do(t, http.MethodGet, "/orders/"+id, "")
	if got := siteOf(t, get.Body.Bytes()); got != "WH2" {
		t.Fatalf("GET fulfillmentSiteId = %q, want WH2", got)
	}
}

func testSiteOnHeldOrder(t *testing.T) {
	e := newTestEnv(t)
	rec := e.do(t, http.MethodPost, "/orders", `{"fulfillmentSiteId":"DC7","releaseOnAllocation":false,"requiredShipBy":"2026-09-25T08:00:00Z","lines":[{"sku":"SKU-1","quantity":1}]}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d (%s)", rec.Code, rec.Body.String())
	}
	if got := siteOf(t, rec.Body.Bytes()); got != "DC7" {
		t.Fatalf("held order fulfillmentSiteId = %q, want DC7", got)
	}
	id := decodeOrder(t, rec).ID
	released := e.do(t, http.MethodPost, "/orders/"+id+"/release", "")
	if released.Code != http.StatusOK {
		t.Fatalf("release status = %d (%s)", released.Code, released.Body.String())
	}
	if got := siteOf(t, released.Body.Bytes()); got != "DC7" {
		t.Fatalf("site after release = %q, want DC7", got)
	}
}

func testNoSiteOmitsField(t *testing.T) {
	e := newTestEnv(t)
	rec := e.do(t, http.MethodPost, "/orders", `{"lines":[{"sku":"SKU-1","quantity":1}]}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d", rec.Code)
	}
	if _, present := rawBody(t, rec.Body.Bytes())["fulfillmentSiteId"]; present {
		t.Fatalf("response carries fulfillmentSiteId for an order without one: %s", rec.Body.String())
	}
}

func testMalformedSiteIs400(t *testing.T) {
	for _, site := range []string{"", "wh1", "Wh1", "WH-1", "WH 1", "WH1\\n", "WH_1"} {
		e := newTestEnv(t)
		body := `{"fulfillmentSiteId":"` + site + `","lines":[{"sku":"SKU-1","quantity":1}]}`
		rec := e.do(t, http.MethodPost, "/orders", body)
		p := assertProblem(t, rec, http.StatusBadRequest)
		if p.Type != problemSiteSlug {
			t.Fatalf("site %q: problem.type = %q, want %q", site, p.Type, problemSiteSlug)
		}
		if p.Instance != "/orders" {
			t.Fatalf("site %q: instance = %q", site, p.Instance)
		}
	}
}

func testNullSiteIs400(t *testing.T) {
	e := newTestEnv(t)
	rec := e.do(t, http.MethodPost, "/orders", `{"fulfillmentSiteId":null,"lines":[{"sku":"SKU-1","quantity":1}]}`)
	p := assertProblem(t, rec, http.StatusBadRequest)
	if !strings.HasSuffix(p.Type, "/malformed-request-body") || !strings.Contains(p.Detail, "fulfillmentSiteId") {
		t.Fatalf("problem = %+v", p)
	}
}

func testSiteSurvivesCancel(t *testing.T) {
	e := newTestEnv(t)
	rec := e.do(t, http.MethodPost, "/orders", `{"fulfillmentSiteId":"WH2","releaseOnAllocation":false,"lines":[{"sku":"SKU-1","quantity":1}]}`)
	id := decodeOrder(t, rec).ID
	del := e.do(t, http.MethodDelete, "/orders/"+id, "")
	if del.Code != http.StatusNoContent {
		t.Fatalf("cancel status = %d (%s)", del.Code, del.Body.String())
	}
	get := e.do(t, http.MethodGet, "/orders/"+id, "")
	if got := siteOf(t, get.Body.Bytes()); got != "WH2" {
		t.Fatalf("site after cancel = %q, want WH2 (%s)", got, get.Body.String())
	}
}
