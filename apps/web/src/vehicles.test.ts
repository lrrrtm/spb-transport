import { describe, expect, it } from "vitest";

import type { Route, VehiclePosition } from "./api/types";
import { filterVehicles, routeOptions } from "./vehicles";

function vehicle(vehicleId: string, routeId: string | null): VehiclePosition {
  return {
    vehicle_id: vehicleId,
    route_id: routeId,
    event_id: null,
    source_timestamp: "2026-10-08T12:00:00Z",
    collected_at: null,
    latitude: 59.93,
    longitude: 30.31,
    bearing: null,
    speed: null,
    label: null,
    license_plate: null,
    quality_flags: [],
  };
}

const vehicles = [vehicle("1", "10"), vehicle("2", "2"), vehicle("3", null)];
const routes: Route[] = [
  { route_id: "10", agency_id: null, short_name: "К-10", long_name: null, route_type: 3 },
  { route_id: "2", agency_id: null, short_name: "2", long_name: null, route_type: 3 },
  { route_id: "unused", agency_id: null, short_name: "1", long_name: null, route_type: 3 },
];

describe("vehicle filters", () => {
  it("builds naturally sorted route options without missing routes", () => {
    expect(routeOptions(vehicles, routes).map(({ route_id }) => route_id)).toEqual(["2", "10"]);
  });

  it("filters by route and keeps all vehicles without a selection", () => {
    expect(filterVehicles(vehicles, "10").map(({ vehicle_id }) => vehicle_id)).toEqual(["1"]);
    expect(filterVehicles(vehicles, null)).toBe(vehicles);
  });
});
