import { ORDER_API_BASE } from "./config";

/** RFC 7807 problem+json body every error response from order-management
 *  returns (see its own server.go's problemDetails type). Mirrors
 *  labor-performance/process-path-mfe's own ApiError shape byte-for-byte. */
export interface ProblemDetails {
  type: string;
  title: string;
  status: number;
  detail: string;
  instance?: string;
}

export class ApiError extends Error {
  problem: ProblemDetails | null;
  status: number;

  constructor(status: number, problem: ProblemDetails | null, fallbackMessage: string) {
    super(problem?.detail || problem?.title || fallbackMessage);
    this.status = status;
    this.problem = problem;
  }
}

async function parseProblemOrThrow(res: Response): Promise<void> {
  let problem: ProblemDetails | null = null;
  try {
    problem = (await res.json()) as ProblemDetails;
  } catch {
    // non-JSON error body -- fall through with problem = null
  }
  throw new ApiError(res.status, problem, `${res.status} ${res.statusText}`);
}

/** Per-request options for apiPost. */
interface PostOptions {
  /** Sent as the `Idempotency-Key` header. order-management requires one on
   *  POST /orders whenever it runs with a database (ADR 0023): the caller
   *  mints one per submission and reuses it when retrying that submission. */
  idempotencyKey?: string;
}

/** A fresh Idempotency-Key: one random (v4) UUID per order submission
 *  (ADR 0038). */
export function newIdempotencyKey(): string {
  return crypto.randomUUID();
}

/**
 * POST call for placing an order (POST /orders). Allocation and release
 * happen automatically, folded into this same call (ADR-0005) -- the
 * response reflects whatever that implicit attempt actually achieved, so
 * callers inspect status/line status rather than assuming "Received".
 * Parses an RFC 7807 problem+json body on failure so the form can surface
 * the exact domain-error detail instead of a generic "request failed".
 */
export async function apiPost<TResponse>(
  path: string,
  body: unknown,
  options: PostOptions = {},
): Promise<TResponse> {
  const headers: Record<string, string> = { "Content-Type": "application/json" };
  if (options.idempotencyKey) headers["Idempotency-Key"] = options.idempotencyKey;
  const res = await fetch(`${ORDER_API_BASE}${path}`, {
    method: "POST",
    headers,
    body: JSON.stringify(body),
  });
  if (!res.ok) await parseProblemOrThrow(res);
  if (res.status === 204) return undefined as TResponse;
  return (await res.json()) as TResponse;
}

/**
 * GET call for reading one order's current state (GET /orders/{id}).
 * There is no list/search endpoint on this service -- order lookup is
 * always by id.
 */
export async function apiGet<TResponse>(path: string): Promise<TResponse> {
  const res = await fetch(`${ORDER_API_BASE}${path}`);
  if (!res.ok) await parseProblemOrThrow(res);
  return (await res.json()) as TResponse;
}

/**
 * DELETE call for cancelling an order (DELETE /orders/{id}). Legal only
 * while no line has reached Released (BR6) -- a 409 here is a normal,
 * expected business outcome, not a crash; the caller surfaces it as an
 * inline error message.
 */
export async function apiDelete(path: string): Promise<void> {
  const res = await fetch(`${ORDER_API_BASE}${path}`, {
    method: "DELETE",
  });
  if (!res.ok) await parseProblemOrThrow(res);
}
