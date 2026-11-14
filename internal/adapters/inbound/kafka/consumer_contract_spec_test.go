package kafka

// Shared support for the event consumer contract tests (E1-E4): the
// loaders and assertions every per-producer contract file in this package
// (and, by relative path, the outbound kafkacatalog / kafkacptschedule /
// kafkapathcapacity contract files) read the PINNED producer AsyncAPI
// copies through. The pinned copies in testdata/ are byte-for-byte
// producer files registered in contracts/pinned.json; the
// pinned-contracts CI job fails when a copy drifts from the producer's
// develop. Never edit a pin — refresh = copy the producer's file over it.
//
// Fleet convention caveat, verified against every pinned file this repo
// reads: MOST fleet asyncapi files are CHANNEL-PERSPECTIVE (a producer's
// OWN topics sit under 'subscribe:'), but they are NOT uniform —
// process-path-management's file is app-perspective (its own topic under
// 'publish:'). Every E1 below names the operation key it reads and why.

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// The pinned producer spec copies next to this package's consumers.
const (
	pinnedFulfillmentSpec   = "testdata/fulfillment-execution.asyncapi.yaml"
	pinnedOutboundShipping  = "testdata/outbound-shipping.asyncapi.yaml"
	pinnedProcessPathSpec   = "testdata/process-path-management.asyncapi.yaml"
	pinnedProductMasterSpec = "testdata/product-master.asyncapi.yaml"
	pinnedWarehousePlanSpec = "testdata/warehouse-planning.asyncapi.yaml"
	pinnedWorkPlanningSpec  = "testdata/wes-work-planning.asyncapi.yaml"
	pinnedSelfAnalyticsSpec = "testdata/order-management.asyncapi.yaml"
)

// pinnedAsyncAPI is the minimal slice of AsyncAPI 2.6 the contract tests
// read: channels (with their operations), and the components/messages +
// components/schemas the channel message $refs point at.
type pinnedAsyncAPI struct {
	AsyncAPI           string                   `yaml:"asyncapi"`
	DefaultContentType string                   `yaml:"defaultContentType"`
	Channels           map[string]pinnedChannel `yaml:"channels"`
	Components         pinnedComponents         `yaml:"components"`
}

type pinnedChannel struct {
	Subscribe *pinnedOperation `yaml:"subscribe"`
	Publish   *pinnedOperation `yaml:"publish"`
}

type pinnedOperation struct {
	OperationID string            `yaml:"operationId"`
	Message     *pinnedMessageRef `yaml:"message"`
}

// pinnedMessageRef is either a single $ref or a oneOf list of $refs.
type pinnedMessageRef struct {
	Ref   string             `yaml:"$ref"`
	OneOf []pinnedMessageRef `yaml:"oneOf"`
}

type pinnedComponents struct {
	Messages map[string]pinnedMessage `yaml:"messages"`
	Schemas  map[string]any           `yaml:"schemas"`
}

type pinnedMessage struct {
	Name        string          `yaml:"name"`
	ContentType string          `yaml:"contentType"`
	Payload     any             `yaml:"payload"`
	Examples    []pinnedExample `yaml:"examples"`
}

// pinnedExample is a message example: a full wire payload (a structured
// CloudEvents JSON document, as YAML) plus its Kafka headers.
type pinnedExample struct {
	Name    string            `yaml:"name"`
	Headers map[string]string `yaml:"headers"`
	Payload map[string]any    `yaml:"payload"`
}

// loadPinnedAsyncAPI reads and parses one pinned spec copy.
func loadPinnedAsyncAPI(t *testing.T, path string) *pinnedAsyncAPI {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read pinned spec %s (copy it from the producer, never hand-write it): %v", path, err)
	}
	var doc pinnedAsyncAPI
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("parse pinned spec %s: %v", path, err)
	}
	return &doc
}

// channelMessageRefs returns the components/messages names a topic's
// operation documents. op is 'subscribe' for a channel-perspective
// producer file (its own topics under 'subscribe:') and 'publish' for an
// app-perspective one (process-path-management, verified against its
// file: its own topic sits under 'publish:').
func (d *pinnedAsyncAPI) channelMessageRefs(t *testing.T, topic, op string) []string {
	t.Helper()
	ch, ok := d.Channels[topic]
	if !ok {
		t.Fatalf("pinned spec has no channel for topic %q", topic)
	}
	operation := ch.Subscribe
	if op == "publish" {
		operation = ch.Publish
	}
	if operation == nil {
		t.Fatalf("pinned spec documents topic %q without a '%s:' operation", topic, op)
	}
	if operation.Message == nil {
		t.Fatalf("pinned spec's %s operation on %q carries no message", op, topic)
	}
	var refs []pinnedMessageRef
	if operation.Message.Ref != "" {
		refs = append(refs, *operation.Message)
	}
	refs = append(refs, operation.Message.OneOf...)
	if len(refs) == 0 {
		t.Fatalf("pinned spec's %s operation on %q lists no messages", op, topic)
	}
	names := make([]string, 0, len(refs))
	for _, r := range refs {
		names = append(names, refLastName(t, r.Ref))
	}
	return names
}

// message resolves one components/messages entry, failing on a dangling
// $ref (a spec whose channel points at a message it does not define).
func (d *pinnedAsyncAPI) message(t *testing.T, name string) pinnedMessage {
	t.Helper()
	msg, ok := d.Components.Messages[name]
	if !ok {
		t.Fatalf("pinned spec references undefined components/messages/%s", name)
	}
	return msg
}

// documentedType returns the CloudEvents `type` string a message's
// payload schema pins, wherever the spec keeps it — directly under
// properties, in an allOf overlay on the shared envelope schema, or
// behind a payload-level $ref into components/schemas. When the schema
// pins no const (product-master's ProductClassifiedEvent documents its
// type only in the message description and examples), the message's own
// example payload's `type` is the documented value.
func (d *pinnedAsyncAPI) documentedType(t *testing.T, msg pinnedMessage) string {
	t.Helper()
	consts := map[string]string{}
	d.collectPayloadConsts(t, msg.Payload, consts, map[string]bool{})
	if c := consts["type"]; c != "" {
		return c
	}
	for _, ex := range msg.Examples {
		if c, _ := ex.Payload["type"].(string); c != "" {
			return c
		}
	}
	t.Fatalf("pinned spec's %s neither pins a type const nor carries a typed example", msg.Name)
	return ""
}

func (d *pinnedAsyncAPI) collectPayloadConsts(t *testing.T, node any, consts map[string]string, resolving map[string]bool) {
	v, ok := node.(map[string]any)
	if !ok {
		return
	}
	if schema, ok := d.resolveSchemaRef(t, v, resolving); ok {
		d.collectPayloadConsts(t, schema, consts, resolving)
		return
	}
	for attr, c := range propertyConstsOf(v) {
		if _, seen := consts[attr]; !seen {
			consts[attr] = c
		}
	}
	for _, item := range allOfItemsOf(v) {
		d.collectPayloadConsts(t, item, consts, resolving)
	}
}

// resolveSchemaRef resolves node's top-level $ref into components/schemas
// (cycle-guarded). ok is false when node carries no $ref, or the ref is
// already being resolved.
func (d *pinnedAsyncAPI) resolveSchemaRef(t *testing.T, node map[string]any, resolving map[string]bool) (any, bool) {
	ref, ok := node["$ref"].(string)
	if !ok {
		return nil, false
	}
	name := refLastName(t, ref)
	if resolving[name] {
		return nil, false
	}
	resolving[name] = true
	schema, ok := d.Components.Schemas[name]
	if !ok {
		t.Fatalf("pinned spec: unresolved schema $ref %q", ref)
	}
	return schema, true
}

// propertyConstsOf returns the const string each directly-declared
// property of node pins (empty when node pins none).
func propertyConstsOf(node map[string]any) map[string]string {
	consts := map[string]string{}
	props, ok := node["properties"].(map[string]any)
	if !ok {
		return consts
	}
	for attr, p := range props {
		if pm, ok := p.(map[string]any); ok {
			if c, ok := pm["const"].(string); ok {
				consts[attr] = c
			}
		}
	}
	return consts
}

// allOfItemsOf returns node's allOf entries (nil when none).
func allOfItemsOf(node map[string]any) []any {
	items, _ := node["allOf"].([]any)
	return items
}

// envelopeRequired returns the required attribute names declared by a
// message payload's envelope schema (resolving top-level $refs into
// components/schemas and walking allOf). The subset naming CloudEvents
// attributes is what E3 asserts on.
func (d *pinnedAsyncAPI) envelopeRequired(t *testing.T, payload any) map[string]bool {
	t.Helper()
	required := map[string]bool{}
	d.collectRequired(t, payload, required, map[string]bool{})
	return required
}

func (d *pinnedAsyncAPI) collectRequired(t *testing.T, node any, required map[string]bool, resolving map[string]bool) {
	v, ok := node.(map[string]any)
	if !ok {
		return
	}
	for _, r := range requiredListOf(v) {
		required[r] = true
	}
	if schema, ok := d.resolveSchemaRef(t, v, resolving); ok {
		d.collectRequired(t, schema, required, resolving)
		return
	}
	for _, item := range allOfItemsOf(v) {
		d.collectRequired(t, item, required, resolving)
	}
}

// requiredListOf returns node's required attribute names (nil when none).
func requiredListOf(node map[string]any) []string {
	raw, ok := node["required"].([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(raw))
	for _, r := range raw {
		if s, ok := r.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

// refLastName extracts the last path segment of a local $ref
// ("#/components/messages/TaskCompleted" -> "TaskCompleted").
func refLastName(t *testing.T, ref string) string {
	t.Helper()
	i := strings.LastIndex(ref, "/")
	if i < 0 || i == len(ref)-1 {
		t.Fatalf("malformed local $ref %q", ref)
	}
	return ref[i+1:]
}

// goldenJSON marshals a pinned example's YAML-decoded payload into the
// JSON bytes a Kafka consumer actually receives (structured-mode
// CloudEvents: the message value IS the JSON envelope).
func goldenJSON(t *testing.T, payload map[string]any) []byte {
	t.Helper()
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal golden payload: %v", err)
	}
	return raw
}

// goldenMessage returns the pinned spec's examples for one message on
// the subscribed channel, failing when the producer documents none (a
// spec without examples is not fit for E2 golden fixtures).
func (d *pinnedAsyncAPI) goldenMessage(t *testing.T, name string) pinnedMessage {
	t.Helper()
	msg := d.message(t, name)
	if len(msg.Examples) == 0 {
		t.Fatalf("pinned spec documents %s without examples", name)
	}
	return msg
}

// clonePayload deep-copies a golden example payload so a test can mutate
// it (adapt a business key to seeded state, inject unknown attributes)
// without touching the shared fixture.
func clonePayload(p map[string]any) map[string]any {
	return cloneMap(p).(map[string]any)
}

func cloneMap(v any) any {
	switch t := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, val := range t {
			out[k] = cloneMap(val)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, val := range t {
			out[i] = cloneMap(val)
		}
		return out
	default:
		return v
	}
}
