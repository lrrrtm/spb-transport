import type { Route, VehiclePosition } from "./api/types";

export function routeOptions(vehicles: VehiclePosition[], routes: Route[]): Route[] {
  const activeRouteIds = new Set(
    vehicles.flatMap(({ route_id }) => (route_id === null ? [] : [route_id])),
  );
  return routes
    .filter(({ route_id }) => activeRouteIds.has(route_id))
    .sort((left, right) => routeLabel(left).localeCompare(routeLabel(right), "ru", { numeric: true }));
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
