---
paths:
  - "web/**"
---

# Frontend micro-frontend remote (`web/`)

This repo also owns `web/`: **`order_mgmt_mfe`**, a Vite + React Module
Federation **remote** consumed by the separate `warehouse-console` shell
repo (ADR-0007). `warehouse-console`'s own `vite.config.ts`/`App.tsx`
federate it by exactly this name at `http://localhost:5181/remoteEntry.js`,
lazy-imported as `order_mgmt_mfe/App` — the remote's `name` in its own
`vite.config.ts` and its dev/preview port must stay byte-identical to those
two values. It is a plain browser client of this service's own REST API —
nothing in `web/` talks to any other bounded context, and nothing in
`internal/` knows `web/` exists.

- **v1 scope uses 3 of this service's REST endpoints** (there is no
  list/search endpoint; retry-allocation and hold/release are not wired
  into the UI): a "place order" intake form
  (`POST /orders`, rendering the created order's id/status/lines), and a
  "look up / cancel order" panel (`GET /orders/{id}` by id, then
  `DELETE /orders/{id}` when BR6 still permits it — a 409 there is a normal
  inline error, not a crash). One screen, two `Card`s
  (`src/screens/OrderManagementScreen.tsx`), mirroring
  `labor-performance/web`'s single-screen precedent for a similarly-thin
  REST surface. `Order.status` / `OrderLine.status` render through
  `@warehouse/ui-kit`'s `StatusPill`, never a hand-rolled color mapping.
- Own `package.json`, build, and dev server (`:5181`, `npm run dev` /
  `npm run build` / `npm run lint` / `npm run test` / `npm run preview`).
- Does **not** participate in this repo's Go quality gate (`make
  check`/`check-all`) and is **not** part of the Go module.
- See ADR-0002 in `warehouse-ops-agent`'s docs (the fleet-wide canonical
  MFE-architecture record — note this is a DIFFERENT "ADR-0002" than this
  repo's own inventory-storage/wes-work-planning boundary ADR, see
  `adrs.md`) and this repo's own adoption record, ADR-0007.
- CORS: `CORS_ALLOWED_ORIGINS` (env, default
  `http://localhost:5173,http://localhost:5181`) was added alongside the
  `go-chi/cors` middleware in `internal/adapters/inbound/http/server.go`
  (`corsMiddleware`) in an earlier PR (`feat: add CORS middleware for the
  warehouse-console browser SPA`) — this is the ONLY behavior change in
  `internal/` ADR-0007 requires, and it already shipped before this
  remote's own code did.
- Order Management is the **first hop** in the Order Lifecycle fan-out:
  `warehouse-console`'s BFF calls `GET /orders/{id}` directly — no new
  lookup-by-reference endpoint was needed here (unlike some sibling
  services), since `GET /orders/{id}` already returns everything the BFF
  needs.
