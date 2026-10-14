package shared

// DemandSource names one source of demand that may route work onto a
// process path, as process-path-management declares it on each path's
// `intake` (its ADR 0034; ADR-0042 here). The vocabulary is owned by
// process-path-management: CUSTOMER_ORDER, STOCK_TRANSFER,
// SLOT_RELOCATION, INVENTORY_CONTROL. order-management is the
// CUSTOMER_ORDER source and only ever asks about that one value, so it
// keeps the type open (a plain string) and tolerates values it does not
// know — a tolerant reader of a published language.
type DemandSource string

// DemandSourceCustomerOrder is the demand source of this context's order
// lines: order.PathSelectionPolicy considers only active paths whose
// intake contains it.
const DemandSourceCustomerOrder DemandSource = "CUSTOMER_ORDER"
