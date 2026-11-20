// Consumer contract tests (R1-R4) for the inventory-storage REST edge.
// testdata/inventory-storage.openapi.yaml is a BYTE COPY of
// IQVO/inventory-storage develop's apis/openapi.yaml (registered in
// contracts/pinned.json; the pinned-contracts CI job fails when the copy
// drifts from the producer). R1: every (method, path) the client calls is
// declared by the pinned spec. R2: the local fixtures satisfy the pinned
// response schemas. R3: those fixtures served over httptest flow through
// the REAL client into the domain, the problem+json fixtures map onto the
// client's sentinel errors, and the request the client sends satisfies
// the pinned request schema. R4: every wire field the client's DTOs read
// or write is declared by the pinned schema with a compatible type, and
// the schema's absence rules are honored.
//
// Fixture provenance: problem-insufficient-usable.json and
// problem-idempotency-key-reused.json are verbatim copies of the pinned
// spec's own 409 response examples; reservation.example.json is derived
// from the Reservation schema's own property examples (the spec's 201
// response carries no example — producer example backfill pending).
package inventorystorage

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"

	"github.com/claudioed/order-management/internal/application/ports"
)

// pinnedInventoryStorageSpec is the pinned copy next to this adapter.
const pinnedInventoryStorageSpec = "testdata/inventory-storage.openapi.yaml"

// inventoryCallSites is the (method, path) list the client's code dials,
// taken from Client.Reserve (POST /reservations) and Client.RevokeReservation
// (DELETE /reservations/{id}).
var inventoryCallSites = []struct{ method, path string }{
	{http.MethodPost, "/reservations"},
	{http.MethodDelete, "/reservations/{id}"},
}

func loadPinnedOpenAPI(t *testing.T) *openapi3.T {
	t.Helper()
	loader := openapi3.NewLoader()
	doc, err := loader.LoadFromFile(pinnedInventoryStorageSpec)
	if err != nil {
		t.Fatalf("load pinned inventory-storage spec (copy it from the producer, never hand-write it): %v", err)
	}
	return doc
}

func readInventoryFixture(t *testing.T, name string) []byte {
	t.Helper()
	raw, err := os.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatalf("read fixture %s: %v", name, err)
	}
	return raw
}

func pinnedInventorySchema(t *testing.T, doc *openapi3.T, name string) *openapi3.Schema {
	t.Helper()
	ref := doc.Components.Schemas[name]
	if ref == nil || ref.Value == nil {
		t.Fatalf("schema %s is not in the pinned spec copy", name)
	}
	return ref.Value
}

// R1: the pinned spec declares every (method, path) the client calls.
func TestPinnedSpecCoversEveryCallSite(t *testing.T) {
	doc := loadPinnedOpenAPI(t)
	for _, c := range inventoryCallSites {
		item := doc.Paths.Find(c.path)
		if item == nil {
			t.Errorf("pinned inventory-storage spec has no path %s, the client calls it", c.path)
			continue
		}
		if item.Operations()[c.method] == nil {
			t.Errorf("pinned spec path %s has no %s operation", c.path, c.method)
		}
	}
}

// R2: every local fixture satisfies the pinned schema of the response it
// stands in for: the reservation fixture against Reservation, and each
// problem+json fixture against the Problem schema the spec pins on 409s.
func TestExamplesSatisfyPinnedSpec(t *testing.T) {
	doc := loadPinnedOpenAPI(t)
	for _, c := range []struct{ fixture, schema string }{
		{"reservation.example.json", "Reservation"},
		{"problem-insufficient-usable.json", "Problem"},
		{"problem-idempotency-key-reused.json", "Problem"},
	} {
		var v any
		if err := json.Unmarshal(readInventoryFixture(t, c.fixture), &v); err != nil {
			t.Fatalf("%s: %v", c.fixture, err)
		}
		if err := pinnedInventorySchema(t, doc, c.schema).VisitJSON(v); err != nil {
			t.Errorf("%s does not satisfy the pinned %s schema: %v", c.fixture, c.schema, err)
		}
	}
}

// contractReserveRequest is one real line reservation, the client's input.
func contractReserveRequest() ports.ReservationRequest {
	return ports.ReservationRequest{
		SKU: "SKU-1", Quantity: 3, DemandRef: "ord-7", LineNo: 2, Attempt: 1,
	}
}

// reserveTestServer answers POST /reservations with the given status,
// content type and fixture bytes, capturing the request body.
func reserveTestServer(t *testing.T, status int, contentType string, fixture []byte) (*httptest.Server, *[]byte) {
	t.Helper()
	var body []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/reservations" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read request body: %v", err)
		}
		body = raw
		w.Header().Set("Content-Type", contentType)
		w.WriteHeader(status)
		if fixture != nil {
			_, _ = w.Write(fixture)
		}
	}))
	t.Cleanup(srv.Close)
	return srv, &body
}

// R3 (happy path): the producer's reservation document served over
// httptest flows through the REAL Client.Reserve into the domain, and the
// request body the client writes satisfies the pinned ReserveStockRequest
// schema (the producer would accept it).
func TestGatewayDecodesSpecExamples(t *testing.T) {
	doc := loadPinnedOpenAPI(t)
	srv, body := reserveTestServer(t, http.StatusCreated, "application/json", readInventoryFixture(t, "reservation.example.json"))

	res, err := NewClient(srv.URL, srv.Client()).Reserve(t.Context(), contractReserveRequest())
	if err != nil {
		t.Fatalf("Reserve against the producer's document: %v", err)
	}
	if want := "res-a1b2c3d4-0000-0000-0000-000000000001"; res.ReservationID != want {
		t.Errorf("ReservationID = %q, the fixture's reservation id is %q", res.ReservationID, want)
	}

	var sent any
	if err := json.Unmarshal(*body, &sent); err != nil {
		t.Fatalf("the request the client sends is not JSON: %v", err)
	}
	if err := pinnedInventorySchema(t, doc, "ReserveStockRequest").VisitJSON(sent); err != nil {
		t.Errorf("the request the client sends does not satisfy the pinned ReserveStockRequest schema: %v", err)
	}
}

// R3 (problem+json): the pinned spec's own 409 documents map onto the
// client's sentinel errors — insufficient-usable is the business fact
// (ports.ErrInsufficientStock), while idempotency-key-reused is a client
// bug and must NOT read as "no stock" (it is a hard downstream error).
func TestGatewayMapsProblemJSONSentinels(t *testing.T) {
	for _, c := range []struct {
		fixture        string
		wantInsuffient bool
	}{
		{fixture: "problem-insufficient-usable.json", wantInsuffient: true},
		{fixture: "problem-idempotency-key-reused.json"},
	} {
		t.Run(c.fixture, func(t *testing.T) {
			srv, _ := reserveTestServer(t, http.StatusConflict, "application/problem+json", readInventoryFixture(t, c.fixture))
			_, err := NewClient(srv.URL, srv.Client()).Reserve(t.Context(), contractReserveRequest())
			if err == nil {
				t.Fatal("a 409 must be an error")
			}
			if got := errors.Is(err, ports.ErrInsufficientStock); got != c.wantInsuffient {
				t.Errorf("ErrInsufficientStock = %v, want %v (err: %v)", got, c.wantInsuffient, err)
			}
			if !c.wantInsuffient && !errors.Is(err, ports.ErrDownstreamUnavailable) {
				t.Errorf("a reused idempotency key must be a hard downstream error, got %v", err)
			}
		})
	}
}

// R3 (revoke): the pinned DELETE route's documented outcomes map onto the
// client's idempotent success — 204 (revoked) and 404 (nothing to delete)
// are both success.
func TestGatewayRevokeMapsNoContentAndNotFound(t *testing.T) {
	for _, status := range []int{http.StatusNoContent, http.StatusNotFound} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			if status == http.StatusNotFound {
				w.Header().Set("Content-Type", "application/problem+json")
				_, _ = w.Write([]byte(`{"type":"https://errors.inventory-storage.warehouse-systems.dev/reservation-not-found"}`))
			}
			w.WriteHeader(status)
		}))
		if err := NewClient(srv.URL, srv.Client()).RevokeReservation(t.Context(), "res-1"); err != nil {
			t.Errorf("RevokeReservation over %d: %v", status, err)
		}
		srv.Close()
	}
}

// R4 (absence): the pinned Reservation schema declares NO required
// properties, so a reservation document may legitimately omit `id`. The
// DTO reads `id` as a plain string — the documented absence story is the
// client's explicit empty-id check: an absent id is a HARD downstream
// error, never a silent zero reservation id.
func TestGatewayRejectsAbsentReservationID(t *testing.T) {
	srv, _ := reserveTestServer(t, http.StatusCreated, "application/json", []byte(`{}`))
	_, err := NewClient(srv.URL, srv.Client()).Reserve(t.Context(), contractReserveRequest())
	if !errors.Is(err, ports.ErrDownstreamUnavailable) || !errors.Is(err, ErrUnexpectedStatus) {
		t.Fatalf("an absent reservation id must be a hard error, got %v", err)
	}
}

// R4: every wire field the response DTO reads is declared by the pinned
// Reservation schema with a compatible type; every field the request DTO
// writes is declared by ReserveStockRequest with a compatible type.
func TestFieldsWeReadAreSpecified(t *testing.T) {
	doc := loadPinnedOpenAPI(t)
	checkDTOFieldsAgainstInventorySpec(t, reflect.TypeOf(reservationResponse{}),
		pinnedInventorySchema(t, doc, "Reservation"), "Reservation")
	checkDTOFieldsAgainstInventorySpec(t, reflect.TypeOf(reserveRequest{}),
		pinnedInventorySchema(t, doc, "ReserveStockRequest"), "ReserveStockRequest")
}

// checkDTOFieldsAgainstInventorySpec walks the DTO's json-tagged fields
// against the pinned schema's properties, asserting a compatible wire
// type per Go kind (the client's DTOs are flat scalars plus *int).
func checkDTOFieldsAgainstInventorySpec(t *testing.T, dt reflect.Type, sch *openapi3.Schema, path string) {
	t.Helper()
	for i := range dt.NumField() {
		f := dt.Field(i)
		name, _, _ := strings.Cut(f.Tag.Get("json"), ",")
		if name == "" || name == "-" {
			continue // not a wire field
		}
		p := sch.Properties[name]
		if p == nil || p.Value == nil {
			t.Errorf("%s.%s: json %q is not declared by the pinned schema", path, f.Name, name)
			continue
		}
		if !wireTypeCompatible(f.Type, p.Value) {
			t.Errorf("%s.%s: Go %s, pinned schema says %v", path, f.Name, f.Type, p.Value.Type.Slice())
		}
	}
}

func wireTypeCompatible(goType reflect.Type, sch *openapi3.Schema) bool {
	switch goType.Kind() {
	case reflect.String:
		return sch.Type.Includes("string")
	case reflect.Int, reflect.Int64:
		return sch.Type.Includes("integer")
	case reflect.Pointer:
		return wireTypeCompatible(goType.Elem(), sch)
	default:
		return false
	}
}
