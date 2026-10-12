import { describe, expect, it } from "vitest";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { http, HttpResponse, server } from "../test/mocks/server";
import { ORDER_API_BASE } from "../config";
import { OrderManagementScreen } from "./OrderManagementScreen";

const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;

const sampleOrder = {
  id: "ord-a1b2c3d4-0000-0000-0000-000000000001",
  status: "Released",
  allowPartialShipment: false,
  promiseDate: "2026-08-27T09:00:00Z",
  lines: [
    {
      lineNo: 1,
      sku: "SKU-1",
      quantity: 2,
      pathId: "pick",
      giftWrap: false,
      status: "Released",
      reservationId: "res-a1b2c3d4-0000-0000-0000-000000000009",
    },
  ],
};

describe("OrderManagementScreen", () => {
  it("places an order and shows the created order's id/status/lines", async () => {
    server.use(
      http.post(`${ORDER_API_BASE}/orders`, () => HttpResponse.json(sampleOrder, { status: 201 })),
    );

    render(<OrderManagementScreen />);
    await userEvent.type(screen.getByLabelText("Line 1 SKU *"), "SKU-1");
    await userEvent.type(screen.getByLabelText("Quantity *"), "2");
    await userEvent.click(screen.getByRole("button", { name: "Place order" }));

    await waitFor(() =>
      expect(
        screen.getByText("Order ord-a1b2c3d4-0000-0000-0000-000000000001 placed — status Released."),
      ).toBeInTheDocument(),
    );
    expect(screen.getByText("SKU-1")).toBeInTheDocument();
  });

  it("sends a UUID Idempotency-Key on POST /orders (ADR 0023 requires it)", async () => {
    const keys: (string | null)[] = [];
    server.use(
      http.post(`${ORDER_API_BASE}/orders`, ({ request }) => {
        keys.push(request.headers.get("Idempotency-Key"));
        return HttpResponse.json(sampleOrder, { status: 201 });
      }),
    );

    render(<OrderManagementScreen />);
    await userEvent.type(screen.getByLabelText("Line 1 SKU *"), "SKU-1");
    await userEvent.type(screen.getByLabelText("Quantity *"), "2");
    await userEvent.click(screen.getByRole("button", { name: "Place order" }));

    await screen.findByText(`Order ${sampleOrder.id} placed — status Released.`);
    expect(keys).toHaveLength(1);
    expect(keys[0]).toMatch(UUID_PATTERN);
  });

  it("reuses the Idempotency-Key when the same submission is retried, and mints a new one for the next order", async () => {
    const keys: (string | null)[] = [];
    let calls = 0;
    server.use(
      http.post(`${ORDER_API_BASE}/orders`, ({ request }) => {
        keys.push(request.headers.get("Idempotency-Key"));
        calls += 1;
        if (calls === 1) return HttpResponse.error(); // network failure: the user retries
        return HttpResponse.json(sampleOrder, { status: 201 });
      }),
    );

    render(<OrderManagementScreen />);
    await userEvent.type(screen.getByLabelText("Line 1 SKU *"), "SKU-1");
    await userEvent.type(screen.getByLabelText("Quantity *"), "2");
    await userEvent.click(screen.getByRole("button", { name: "Place order" }));
    await screen.findByText("Failed to place order.");

    // Retry of the SAME submission (form untouched): same key, so the
    // service replays the first outcome instead of creating a second order.
    await userEvent.click(screen.getByRole("button", { name: "Place order" }));
    await screen.findByText(`Order ${sampleOrder.id} placed — status Released.`);

    // A NEW submission after a success gets a fresh key.
    await userEvent.type(screen.getByLabelText("Line 1 SKU *"), "SKU-2");
    await userEvent.type(screen.getByLabelText("Quantity *"), "1");
    await userEvent.click(screen.getByRole("button", { name: "Place order" }));
    await waitFor(() => expect(keys).toHaveLength(3));

    expect(keys[0]).toMatch(UUID_PATTERN);
    expect(keys[1]).toBe(keys[0]);
    expect(keys[2]).toMatch(UUID_PATTERN);
    expect(keys[2]).not.toBe(keys[0]);
  });

  it("mints a new Idempotency-Key after the service answered with an error (that answer is cached)", async () => {
    const keys: (string | null)[] = [];
    server.use(
      http.post(`${ORDER_API_BASE}/orders`, ({ request }) => {
        keys.push(request.headers.get("Idempotency-Key"));
        return HttpResponse.json(
          {
            type: "https://errors.order-management.warehouse-systems.dev/downstream-unavailable",
            title: "Downstream unavailable",
            status: 503,
            detail: "inventory-storage unavailable",
            instance: "/orders",
          },
          { status: 503 },
        );
      }),
    );

    render(<OrderManagementScreen />);
    await userEvent.type(screen.getByLabelText("Line 1 SKU *"), "SKU-1");
    await userEvent.type(screen.getByLabelText("Quantity *"), "2");
    await userEvent.click(screen.getByRole("button", { name: "Place order" }));
    await screen.findByText("inventory-storage unavailable");

    await userEvent.click(screen.getByRole("button", { name: "Place order" }));
    await waitFor(() => expect(keys).toHaveLength(2));

    expect(keys[1]).toMatch(UUID_PATTERN);
    expect(keys[1]).not.toBe(keys[0]);
  });

  it("mints a new Idempotency-Key when the form is edited after a failed submission", async () => {
    const keys: (string | null)[] = [];
    server.use(
      http.post(`${ORDER_API_BASE}/orders`, ({ request }) => {
        keys.push(request.headers.get("Idempotency-Key"));
        return HttpResponse.error();
      }),
    );

    render(<OrderManagementScreen />);
    await userEvent.type(screen.getByLabelText("Line 1 SKU *"), "SKU-1");
    await userEvent.type(screen.getByLabelText("Quantity *"), "2");
    await userEvent.click(screen.getByRole("button", { name: "Place order" }));
    await screen.findByText("Failed to place order.");

    // Editing the order makes it a different submission: reusing the key
    // would get 422 idempotency-key-reused (different body hash).
    await userEvent.type(screen.getByLabelText("Quantity *"), "0");
    await userEvent.click(screen.getByRole("button", { name: "Place order" }));
    await waitFor(() => expect(keys).toHaveLength(2));

    expect(keys[1]).toMatch(UUID_PATTERN);
    expect(keys[1]).not.toBe(keys[0]);
  });

  it("shows the RFC 7807 problem detail when placing an order fails", async () => {
    server.use(
      http.post(`${ORDER_API_BASE}/orders`, () =>
        HttpResponse.json(
          {
            type: "https://errors.order-management.warehouse-systems.dev/non-positive-quantity",
            title: "Quantity must be greater than zero",
            status: 422,
            detail: "quantity must be greater than zero",
            instance: "/orders",
          },
          { status: 422 },
        ),
      ),
    );

    render(<OrderManagementScreen />);
    await userEvent.type(screen.getByLabelText("Line 1 SKU *"), "SKU-1");
    await userEvent.type(screen.getByLabelText("Quantity *"), "1");
    await userEvent.click(screen.getByRole("button", { name: "Place order" }));

    expect(
      await screen.findByText("quantity must be greater than zero"),
    ).toBeInTheDocument();
  });

  it("looks up an order by id and renders its status/lines/promiseDate", async () => {
    server.use(
      http.get(`${ORDER_API_BASE}/orders/${sampleOrder.id}`, () => HttpResponse.json(sampleOrder)),
    );

    render(<OrderManagementScreen />);
    await userEvent.type(screen.getByLabelText("Order ID *"), sampleOrder.id);
    await userEvent.click(screen.getByRole("button", { name: "Look up" }));

    expect(await screen.findByText(sampleOrder.id)).toBeInTheDocument();
    expect(screen.getByText("res-a1b2c3d4-0000-0000-0000-000000000009")).toBeInTheDocument();
  });

  it("shows a not-found message for a 404 lookup", async () => {
    server.use(
      http.get(`${ORDER_API_BASE}/orders/ord-does-not-exist`, () =>
        HttpResponse.json(
          {
            type: "https://errors.order-management.warehouse-systems.dev/order-not-found",
            title: "Order not found",
            status: 404,
            detail: "order not found",
            instance: "/orders/ord-does-not-exist",
          },
          { status: 404 },
        ),
      ),
    );

    render(<OrderManagementScreen />);
    await userEvent.type(screen.getByLabelText("Order ID *"), "ord-does-not-exist");
    await userEvent.click(screen.getByRole("button", { name: "Look up" }));

    expect(
      await screen.findByText("No order found for id ord-does-not-exist."),
    ).toBeInTheDocument();
  });

  it("cancels a cancellable order and shows a success message", async () => {
    const pendingOrder = {
      ...sampleOrder,
      status: "Received",
      lines: [{ ...sampleOrder.lines[0], status: "Pending", reservationId: undefined }],
    };
    server.use(
      http.get(`${ORDER_API_BASE}/orders/${sampleOrder.id}`, () => HttpResponse.json(pendingOrder)),
      http.delete(`${ORDER_API_BASE}/orders/${sampleOrder.id}`, () => new HttpResponse(null, { status: 204 })),
    );

    render(<OrderManagementScreen />);
    await userEvent.type(screen.getByLabelText("Order ID *"), sampleOrder.id);
    await userEvent.click(screen.getByRole("button", { name: "Look up" }));
    await screen.findByText(sampleOrder.id);

    await userEvent.click(screen.getByRole("button", { name: "Cancel order" }));

    await waitFor(() =>
      expect(screen.getByText(`Order ${sampleOrder.id} cancelled.`)).toBeInTheDocument(),
    );
  });

  it("disables Cancel order client-side once a line has reached Released (BR6)", async () => {
    server.use(
      http.get(`${ORDER_API_BASE}/orders/${sampleOrder.id}`, () => HttpResponse.json(sampleOrder)),
    );

    render(<OrderManagementScreen />);
    await userEvent.type(screen.getByLabelText("Order ID *"), sampleOrder.id);
    await userEvent.click(screen.getByRole("button", { name: "Look up" }));
    await screen.findByText(sampleOrder.id);

    // sampleOrder's only line is already Released, so this mirrors BR6
    // client-side: Cancel order starts disabled without needing a round
    // trip to find out.
    expect(screen.getByRole("button", { name: "Cancel order" })).toBeDisabled();
  });

  it("surfaces a BR6 409 cancellation conflict from a race as an inline error, not a crash", async () => {
    const pendingOrder = {
      ...sampleOrder,
      status: "Received",
      lines: [{ ...sampleOrder.lines[0], status: "Pending", reservationId: undefined }],
    };
    server.use(
      http.get(`${ORDER_API_BASE}/orders/${sampleOrder.id}`, () => HttpResponse.json(pendingOrder)),
      http.delete(`${ORDER_API_BASE}/orders/${sampleOrder.id}`, () =>
        HttpResponse.json(
          {
            type: "https://errors.order-management.warehouse-systems.dev/order-already-released",
            title: "Order already has released lines and can no longer be cancelled",
            status: 409,
            detail: "order already has released lines and can no longer be cancelled",
            instance: `/orders/${sampleOrder.id}`,
          },
          { status: 409 },
        ),
      ),
    );

    render(<OrderManagementScreen />);
    await userEvent.type(screen.getByLabelText("Order ID *"), sampleOrder.id);
    await userEvent.click(screen.getByRole("button", { name: "Look up" }));
    await screen.findByText(sampleOrder.id);

    await userEvent.click(screen.getByRole("button", { name: "Cancel order" }));

    expect(
      await screen.findByText("order already has released lines and can no longer be cancelled"),
    ).toBeInTheDocument();
  });
});
