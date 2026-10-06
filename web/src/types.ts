/** Mirrors order-management's own apis/openapi.yaml schemas exactly (see
 *  apis/openapi.yaml's components.schemas) -- this is the frontend's copy
 *  of that same boundary contract. Field names/optionality match the spec
 *  verbatim. v1 of this remote only needs the three REST endpoints this
 *  service actually exposes for order intake/lookup/cancel: POST /orders,
 *  GET /orders/{id}, DELETE /orders/{id}. There is no list/search
 *  endpoint, so this remote is lookup-by-id + intake, never a list. */

export type OrderStatus =
  | "Received"
  | "Allocated"
  | "PartiallyAllocated"
  | "Backordered"
  | "Released"
  | "PartiallyReleased"
  | "Cancelled";

export type OrderLineStatus =
  | "Pending"
  | "Allocated"
  | "Backordered"
  | "Released"
  | "Cancelled";

export interface OrderLine {
  lineNo: number;
  sku: string;
  quantity: number;
  /** The process path the service chose for this line (ADR 0021; default
   *  "pick" when no catalogue is configured) -- read-only, never settable on
   *  the intake request. */
  pathId: string;
  giftWrap: boolean;
  status: OrderLineStatus;
  /** inventory-storage's reservation id. Present only once the line is
   *  allocated -- omitted (not empty) when not allocated. */
  reservationId?: string;
}

export interface Order {
  id: string;
  /** Always derived from the line statuses at read time -- never stored. */
  status: OrderStatus;
  allowPartialShipment: boolean;
  /** RFC 3339. Absent until at least one line is allocated. */
  promiseDate?: string;
  lines: OrderLine[];
}

export interface ReceiveOrderLineRequest {
  sku: string;
  quantity: number;
  /** OPTIONAL. Defaults to false server-side when omitted. */
  giftWrap?: boolean;
}

export interface ReceiveOrderRequest {
  lines: ReceiveOrderLineRequest[];
  /** OPTIONAL. Defaults to false (ship-complete, BR3) server-side when
   *  omitted. */
  allowPartialShipment?: boolean;
}
