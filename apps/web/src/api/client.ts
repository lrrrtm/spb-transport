import type { AnalyticsOverview, EtaComparison, Readiness, Route, RouteCluster, RoutePage, SegmentMetric, TransportAnomaly, VehiclePage, VehiclePosition } from "./types";

const PAGE_SIZE = 500;
const MAX_PAGES = 100;

export class ApiError extends Error {
  constructor(
    message: string,
    readonly status: number,
  ) {
    super(message);
  }
}

async function requestJson(url: string, signal?: AbortSignal): Promise<unknown> {
  const response = await fetch(url, { headers: { Accept: "application/json" }, signal });
  if (!response.ok) {
    let message = `API вернул HTTP ${response.status}`;
    try {
      const body = (await response.json()) as { detail?: unknown };
      if (typeof body.detail === "string") message = body.detail;
    } catch {
      // The status is enough when an upstream proxy returns a non-JSON error.
    }
    throw new ApiError(message, response.status);
  }
  return response.json();
}

function isVehicle(value: unknown): value is VehiclePosition {
  if (typeof value !== "object" || value === null) return false;
  const vehicle = value as Record<string, unknown>;
  const nullable = (field: unknown, type: "string" | "number") =>
    field === null || typeof field === type;
  return (
    typeof vehicle.vehicle_id === "string" &&
    nullable(vehicle.route_id, "string") &&
    nullable(vehicle.event_id, "string") &&
    typeof vehicle.source_timestamp === "string" &&
    nullable(vehicle.collected_at, "string") &&
    typeof vehicle.latitude === "number" &&
    typeof vehicle.longitude === "number" &&
    nullable(vehicle.bearing, "number") &&
    nullable(vehicle.speed, "number") &&
    nullable(vehicle.label, "string") &&
    nullable(vehicle.license_plate, "string") &&
    Array.isArray(vehicle.quality_flags) &&
    vehicle.quality_flags.every((flag) => typeof flag === "string")
  );
}

function parseVehiclePage(value: unknown): VehiclePage {
  if (typeof value !== "object" || value === null) throw new Error("Некорректный ответ API");
  const page = value as Record<string, unknown>;
  if (
    !Array.isArray(page.items) ||
    !page.items.every(isVehicle) ||
    !(typeof page.next_cursor === "string" || page.next_cursor === null)
  ) {
    throw new Error("Некорректный ответ API");
  }
  return page as unknown as VehiclePage;
}

function isRoute(value: unknown): value is Route {
  if (typeof value !== "object" || value === null) return false;
  const route = value as Record<string, unknown>;
  const nullable = (field: unknown, type: "string" | "number") =>
    field === null || typeof field === type;
  return (
    typeof route.route_id === "string" &&
    nullable(route.agency_id, "string") &&
    nullable(route.short_name, "string") &&
    nullable(route.long_name, "string") &&
    nullable(route.route_type, "number")
  );
}

function parseRoutePage(value: unknown): RoutePage {
  if (typeof value !== "object" || value === null) throw new Error("Некорректный ответ API");
  const page = value as Record<string, unknown>;
  if (
    !Array.isArray(page.items) ||
    !page.items.every(isRoute) ||
    !(typeof page.next_cursor === "string" || page.next_cursor === null) ||
    typeof page.static_feed_version !== "string"
  ) {
    throw new Error("Некорректный ответ API");
  }
  return page as unknown as RoutePage;
}

async function fetchAllPages<T>(
  path: string,
  parse: (value: unknown) => { items: T[]; next_cursor: string | null },
  signal?: AbortSignal,
): Promise<T[]> {
  const items: T[] = [];
  const seenCursors = new Set<string>();
  let cursor: string | null = null;

  for (let pageNumber = 0; pageNumber < MAX_PAGES; pageNumber += 1) {
    const query = new URLSearchParams({ limit: String(PAGE_SIZE) });
    if (cursor !== null) query.set("cursor", cursor);
    const page = parse(await requestJson(`${path}?${query}`, signal));
    items.push(...page.items);
    cursor = page.next_cursor;
    if (cursor === null) return items;
    if (seenCursors.has(cursor)) throw new Error("API вернул повторяющийся курсор пагинации");
    seenCursors.add(cursor);
  }

  throw new Error("Превышен лимит страниц API");
}

export async function fetchAllVehicles(signal?: AbortSignal): Promise<VehiclePosition[]> {
  return fetchAllPages("/api/v1/vehicles", parseVehiclePage, signal);
}

export async function fetchAllRoutes(signal?: AbortSignal): Promise<Route[]> {
  return fetchAllPages("/api/v1/routes", parseRoutePage, signal);
}

export async function fetchReadiness(signal?: AbortSignal): Promise<Readiness> {
  const value = await requestJson("/ready", signal);
  if (typeof value !== "object" || value === null) throw new Error("Некорректный ответ API");
  const readiness = value as Record<string, unknown>;
  if (
    typeof readiness.status !== "string" ||
    typeof readiness.cassandra !== "string" ||
    !(typeof readiness.static_feed_version === "string" ||
      readiness.static_feed_version === null) ||
    !(typeof readiness.detail === "string" || readiness.detail === null)
  ) {
    throw new Error("Некорректный ответ API");
  }
  return value as Readiness;
}

async function fetchTyped<T>(path: string, signal?: AbortSignal): Promise<T> {
  return (await requestJson(path, signal)) as T;
}

export const fetchAnalyticsOverview = (signal?: AbortSignal) => fetchTyped<AnalyticsOverview>("/api/v1/analytics", signal);
export const fetchSegments = (routeId: string, dayType: string, hour: number, signal?: AbortSignal) => fetchTyped<SegmentMetric[]>(`/api/v1/analytics/routes/${encodeURIComponent(routeId)}/segments?day_type=${dayType}&hour=${hour}`, signal);
export const fetchAnomalies = (routeId: string, signal?: AbortSignal) => fetchTyped<TransportAnomaly[]>(`/api/v1/analytics/routes/${encodeURIComponent(routeId)}/anomalies`, signal);
export const fetchEtaComparison = (routeId: string, signal?: AbortSignal) => fetchTyped<EtaComparison[]>(`/api/v1/analytics/routes/${encodeURIComponent(routeId)}/eta-comparison`, signal);
export const fetchClusters = (signal?: AbortSignal) => fetchTyped<RouteCluster[]>("/api/v1/analytics/clusters", signal);
