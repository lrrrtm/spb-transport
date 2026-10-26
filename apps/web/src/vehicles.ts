import type { Readiness, Route, VehiclePosition } from "./api/types";

/**
 * Routes of the vehicles currently on the map. A route without static metadata (not published
 * yet or unavailable) is still offered by its id so that filtering does not depend on it.
 */
export function routeOptions(vehicles: VehiclePosition[], routes: Route[]): Route[] {
  const activeRouteIds = new Set(
    vehicles.flatMap(({ route_id }) => (route_id === null ? [] : [route_id])),
  );
  const known = new Map(routes.map((route) => [route.route_id, route]));
  return [...activeRouteIds]
    .map(
      (routeId) =>
        known.get(routeId) ?? {
          route_id: routeId,
          agency_id: null,
          short_name: null,
          long_name: null,
          route_type: null,
        },
    )
    .sort((left, right) => routeLabel(left).localeCompare(routeLabel(right), "ru", { numeric: true }));
}

export function apiStatusLabel(
  readiness: Readiness | null,
  error: string | null,
  hasVehicles: boolean,
): string {
  if (readiness?.status === "ready") return "API доступен";
  if (error !== null) return "Нет соединения";
  if (readiness !== null || hasVehicles) return "API не готов";
  return "Проверка API";
}

export function routeLabel(route: Route): string {
  return route.short_name ?? route.long_name ?? route.route_id;
}

export function filterVehicles(
  vehicles: VehiclePosition[],
  routeId: string | null,
): VehiclePosition[] {
  return routeId === null ? vehicles : vehicles.filter(({ route_id }) => route_id === routeId);
}

export function formatUpdatedAt(value: Date | null): string {
  return value === null
    ? "—"
    : new Intl.DateTimeFormat("ru-RU", {
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit",
      }).format(value);
}
