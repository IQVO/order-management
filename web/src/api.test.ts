import { describe, expect, it } from "vitest";
import { http, HttpResponse, server } from "./test/mocks/server";
import { ORDER_API_BASE } from "./config";
import { apiPost, newIdempotencyKey } from "./api";

describe("apiPost", () => {
  it("sends the Idempotency-Key header when one is given", async () => {
    let seen: string | null = null;
    server.use(
      http.post(`${ORDER_API_BASE}/orders`, ({ request }) => {
        seen = request.headers.get("Idempotency-Key");
        return HttpResponse.json({ id: "ord-1" }, { status: 201 });
      }),
    );

    await apiPost("/orders", { lines: [] }, { idempotencyKey: "key-123" });

    expect(seen).toBe("key-123");
  });

  it("sends no Idempotency-Key header when none is given", async () => {
    let present = true;
    server.use(
      http.post(`${ORDER_API_BASE}/orders/ord-1/release`, ({ request }) => {
        present = request.headers.has("Idempotency-Key");
        return new HttpResponse(null, { status: 204 });
      }),
    );

    await apiPost("/orders/ord-1/release", {});

    expect(present).toBe(false);
  });
});

describe("newIdempotencyKey", () => {
  it("returns a distinct RFC 4122 v4 UUID on every call", () => {
    const a = newIdempotencyKey();
    const b = newIdempotencyKey();
    expect(a).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
    expect(b).not.toBe(a);
  });
});
