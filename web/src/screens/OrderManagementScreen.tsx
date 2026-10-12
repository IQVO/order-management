import { useRef, useState, type FormEvent } from "react";
import { Card, StatusPill } from "@warehouse/ui-kit";
import { apiDelete, apiGet, apiPost, ApiError, newIdempotencyKey } from "../api";
import type { Order, ReceiveOrderLineRequest } from "../types";
import {
  CheckboxField,
  FormRow,
  InlineError,
  InlineSuccess,
  SubmitButton,
  TextField,
} from "../components/formkit";

/** A single intake line in the "place order" form's local (string-typed)
 *  editing state -- converted to ReceiveOrderLineRequest on submit. */
interface DraftLine {
  sku: string;
  quantity: string;
  giftWrap: boolean;
}

function emptyLine(): DraftLine {
  return { sku: "", quantity: "", giftWrap: false };
}

function formatDate(iso: string | undefined): string {
  if (!iso) return "—";
  return new Date(iso).toLocaleString();
}

/**
 * The single screen for this remote. order-management's REST API has
 * exactly three endpoints in this v1 scope -- POST /orders, GET
 * /orders/{id}, DELETE /orders/{id} -- with no list/search endpoint, so
 * this stays two panels on one screen rather than a list+detail route,
 * mirroring labor-performance's own single-screen precedent for a
 * similarly-thin REST surface:
 *
 *  - "Place order": an intake form (line items + allowPartialShipment)
 *    that POSTs and renders the created order's id/status/lines on
 *    success.
 *  - "Look up / cancel order": enter an order id, GET its current state,
 *    and (when BR6 still permits it) DELETE to cancel. A 409 from
 *    cancelling a released order surfaces as a normal inline error, not
 *    a crash.
 */
export function OrderManagementScreen() {
  // --- Place order ---
  const [lines, setLines] = useState<DraftLine[]>([emptyLine()]);
  const [allowPartialShipment, setAllowPartialShipment] = useState(false);
  const [placing, setPlacing] = useState(false);
  const [placeError, setPlaceError] = useState<string | null>(null);
  const [placedOrder, setPlacedOrder] = useState<Order | null>(null);
  // Idempotency-Key of the CURRENT submission (ADR 0038, server side ADR
  // 0023). Minted on the first click and kept while the outcome is unknown
  // (no response arrived), so a retry cannot create a second order. Dropped
  // once the service answers, or when the form is edited -- an edited form
  // is a different request, and reusing the key with a different body is
  // answered 422 idempotency-key-reused.
  const submissionKey = useRef<string | null>(null);

  function startNewSubmission() {
    submissionKey.current = null;
  }

  function updateLine(index: number, patch: Partial<DraftLine>) {
    startNewSubmission();
    setLines((prev) => prev.map((l, i) => (i === index ? { ...l, ...patch } : l)));
  }

  function addLine() {
    startNewSubmission();
    setLines((prev) => [...prev, emptyLine()]);
  }

  function removeLine(index: number) {
    startNewSubmission();
    setLines((prev) => (prev.length > 1 ? prev.filter((_, i) => i !== index) : prev));
  }

  function onAllowPartialShipmentChange(value: boolean) {
    startNewSubmission();
    setAllowPartialShipment(value);
  }

  async function onPlaceOrder(e: FormEvent) {
    e.preventDefault();
    setPlaceError(null);
    setPlacedOrder(null);
    setPlacing(true);
    const idempotencyKey = (submissionKey.current ??= newIdempotencyKey());
    try {
      const requestLines: ReceiveOrderLineRequest[] = lines.map((l) => ({
        sku: l.sku.trim(),
        quantity: Number(l.quantity),
        giftWrap: l.giftWrap,
      }));
      const order = await apiPost<Order>(
        "/orders",
        { lines: requestLines, allowPartialShipment },
        { idempotencyKey },
      );
      startNewSubmission();
      setPlacedOrder(order);
      setLines([emptyLine()]);
      setAllowPartialShipment(false);
    } catch (err) {
      // The service answered: that outcome is final AND cached under this
      // key (ADR 0023 caches every normal response, 5xx included), so
      // resending the key would only replay it. A retry becomes a new
      // submission. When no answer arrived (fetch threw), the order may or
      // may not exist, so the key is kept for the retry.
      if (err instanceof ApiError) startNewSubmission();
      setPlaceError(err instanceof ApiError ? err.message : "Failed to place order.");
    } finally {
      setPlacing(false);
    }
  }

  const canSubmitPlace =
    !placing && lines.every((l) => l.sku.trim() !== "" && Number(l.quantity) > 0);

  // --- Look up / cancel order ---
  const [orderIdInput, setOrderIdInput] = useState("");
  const [lookupOrder, setLookupOrder] = useState<Order | null>(null);
  const [lookupLoading, setLookupLoading] = useState(false);
  const [lookupError, setLookupError] = useState<string | null>(null);
  const [cancelling, setCancelling] = useState(false);
  const [cancelError, setCancelError] = useState<string | null>(null);
  const [cancelSuccess, setCancelSuccess] = useState<string | null>(null);

  async function onLookupOrder(e: FormEvent) {
    e.preventDefault();
    const id = orderIdInput.trim();
    if (!id) return;
    setLookupLoading(true);
    setLookupError(null);
    setCancelError(null);
    setCancelSuccess(null);
    try {
      const order = await apiGet<Order>(`/orders/${encodeURIComponent(id)}`);
      setLookupOrder(order);
    } catch (err) {
      setLookupOrder(null);
      setLookupError(
        err instanceof ApiError && err.status === 404
          ? `No order found for id ${id}.`
          : err instanceof ApiError
            ? err.message
            : "Failed to look up order.",
      );
    } finally {
      setLookupLoading(false);
    }
  }

  async function onCancelOrder() {
    if (!lookupOrder) return;
    setCancelling(true);
    setCancelError(null);
    setCancelSuccess(null);
    try {
      await apiDelete(`/orders/${encodeURIComponent(lookupOrder.id)}`);
      setCancelSuccess(`Order ${lookupOrder.id} cancelled.`);
      setLookupOrder({ ...lookupOrder, status: "Cancelled" });
    } catch (err) {
      // A 409 here is a normal, expected business outcome (BR6: at least
      // one line has already been released) -- surface it as a plain
      // inline error, never a crash.
      setCancelError(err instanceof ApiError ? err.message : "Failed to cancel order.");
    } finally {
      setCancelling(false);
    }
  }

  const isCancellable = lookupOrder
    ? lookupOrder.lines.every((l) => l.status !== "Released") &&
      lookupOrder.status !== "Cancelled"
    : false;

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "var(--wh-space-5)" }}>
      <div>
        <h1 style={{ fontSize: "var(--wh-font-size-2xl)", margin: 0 }}>Order management</h1>
        <p style={{ color: "var(--wh-color-text-muted)", marginTop: 4 }}>
          order-management · order intake, allocation/release status, and cancellation
        </p>
      </div>

      <Card title="Place order">
        <form
          onSubmit={onPlaceOrder}
          style={{ display: "flex", flexDirection: "column", gap: "var(--wh-space-3)" }}
        >
          {lines.map((line, index) => (
            <FormRow key={index}>
              <TextField
                label={`Line ${index + 1} SKU`}
                value={line.sku}
                onChange={(v) => updateLine(index, { sku: v })}
                placeholder="SKU-1"
                required
              />
              <TextField
                label="Quantity"
                type="number"
                value={line.quantity}
                onChange={(v) => updateLine(index, { quantity: v })}
                placeholder="2"
                required
              />
              <CheckboxField
                label="Gift wrap"
                checked={line.giftWrap}
                onChange={(v) => updateLine(index, { giftWrap: v })}
              />
              <SubmitButton
                type="button"
                tone="danger"
                disabled={lines.length === 1}
                onClick={() => removeLine(index)}
              >
                Remove
              </SubmitButton>
            </FormRow>
          ))}
          <div>
            <SubmitButton type="button" onClick={addLine}>
              Add line
            </SubmitButton>
          </div>
          <CheckboxField
            label="Allow partial shipment"
            checked={allowPartialShipment}
            onChange={onAllowPartialShipmentChange}
          />
          <div>
            <SubmitButton disabled={!canSubmitPlace}>
              {placing ? "Placing…" : "Place order"}
            </SubmitButton>
          </div>
          <InlineError message={placeError} />
          {placedOrder && (
            <InlineSuccess
              message={`Order ${placedOrder.id} placed — status ${placedOrder.status}.`}
            />
          )}
        </form>

        {placedOrder && (
          <div style={{ marginTop: "var(--wh-space-4)" }}>
            <OrderDetail order={placedOrder} />
          </div>
        )}
      </Card>

      <Card title="Look up / cancel order">
        <form
          onSubmit={onLookupOrder}
          style={{ display: "flex", flexDirection: "column", gap: "var(--wh-space-3)" }}
        >
          <FormRow>
            <TextField
              label="Order ID"
              value={orderIdInput}
              onChange={setOrderIdInput}
              placeholder="ord-a1b2c3d4-0000-0000-0000-000000000001"
              required
            />
            <SubmitButton disabled={!orderIdInput.trim() || lookupLoading}>
              {lookupLoading ? "Looking up…" : "Look up"}
            </SubmitButton>
          </FormRow>
        </form>

        {lookupLoading && <p style={{ color: "var(--wh-color-text-muted)" }}>Loading…</p>}
        <InlineError message={lookupError} />

        {lookupOrder && !lookupLoading && (
          <div style={{ marginTop: "var(--wh-space-3)", display: "flex", flexDirection: "column", gap: "var(--wh-space-3)" }}>
            <OrderDetail order={lookupOrder} />
            <div>
              <SubmitButton
                type="button"
                tone="danger"
                disabled={!isCancellable || cancelling}
                onClick={onCancelOrder}
              >
                {cancelling ? "Cancelling…" : "Cancel order"}
              </SubmitButton>
            </div>
            <InlineError message={cancelError} />
            <InlineSuccess message={cancelSuccess} />
          </div>
        )}
      </Card>
    </div>
  );
}

/** Shared read-only rendering of an Order, used by both panels. Renders
 *  Order.Status / OrderLine.status through StatusPill per ADR-0007's own
 *  requirement, rather than hand-rolling a local color mapping. */
function OrderDetail({ order }: { order: Order }) {
  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "var(--wh-space-3)" }}>
      <div style={{ display: "flex", gap: "var(--wh-space-5)", flexWrap: "wrap", alignItems: "center" }}>
        <div>
          <div style={{ fontSize: "var(--wh-font-size-xs)", color: "var(--wh-color-text-muted)" }}>
            Order ID
          </div>
          <div style={{ fontSize: "var(--wh-font-size-sm)", fontFamily: "var(--wh-font-mono)" }}>
            {order.id}
          </div>
        </div>
        <div>
          <div style={{ fontSize: "var(--wh-font-size-xs)", color: "var(--wh-color-text-muted)" }}>
            Status
          </div>
          <StatusPill status={order.status} size="sm" />
        </div>
        <div>
          <div style={{ fontSize: "var(--wh-font-size-xs)", color: "var(--wh-color-text-muted)" }}>
            Promise date
          </div>
          <div style={{ fontSize: "var(--wh-font-size-sm)" }}>{formatDate(order.promiseDate)}</div>
        </div>
        <div>
          <div style={{ fontSize: "var(--wh-font-size-xs)", color: "var(--wh-color-text-muted)" }}>
            Ship-complete
          </div>
          <div style={{ fontSize: "var(--wh-font-size-sm)" }}>
            {order.allowPartialShipment ? "No (partial allowed)" : "Yes (BR3)"}
          </div>
        </div>
      </div>
      <table style={{ width: "100%", borderCollapse: "collapse" }}>
        <thead>
          <tr>
            {["Line", "SKU", "Qty", "Path", "Gift wrap", "Status", "Reservation"].map((h) => (
              <th
                key={h}
                style={{
                  textAlign: "left",
                  fontSize: "var(--wh-font-size-xs)",
                  color: "var(--wh-color-text-faint)",
                  padding: "var(--wh-space-2) var(--wh-space-2) var(--wh-space-2) 0",
                }}
              >
                {h}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {order.lines.map((line) => (
            <tr key={line.lineNo}>
              <td style={{ padding: "var(--wh-space-2) var(--wh-space-2) var(--wh-space-2) 0" }}>
                {line.lineNo}
              </td>
              <td style={{ padding: "var(--wh-space-2) var(--wh-space-2) var(--wh-space-2) 0" }}>
                {line.sku}
              </td>
              <td style={{ padding: "var(--wh-space-2) var(--wh-space-2) var(--wh-space-2) 0" }}>
                {line.quantity}
              </td>
              <td style={{ padding: "var(--wh-space-2) var(--wh-space-2) var(--wh-space-2) 0" }}>
                {line.pathId}
              </td>
              <td style={{ padding: "var(--wh-space-2) var(--wh-space-2) var(--wh-space-2) 0" }}>
                {line.giftWrap ? "Yes" : "No"}
              </td>
              <td style={{ padding: "var(--wh-space-2) var(--wh-space-2) var(--wh-space-2) 0" }}>
                <StatusPill status={line.status} size="sm" />
              </td>
              <td style={{ padding: "var(--wh-space-2) var(--wh-space-2) var(--wh-space-2) 0", fontFamily: "var(--wh-font-mono)" }}>
                {line.reservationId ?? "—"}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
