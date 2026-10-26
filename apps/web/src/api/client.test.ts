import { afterEach, describe, expect, it, vi } from "vitest";

import { fetchAllRoutes, fetchAllVehicles } from "./client";
import type { VehiclePosition } from "./types";

const vehicle: VehiclePosition = {
  vehicle_id: "42",
  route_id: "10",
  event_id: "event",
  source_timestamp: "2026-10-08T12:00:00Z",
  collected_at: "2026-10-08T12:00:01Z",
  latitude: 59.93,
  longitude: 30.31,
  bearing: 90,
  speed: 4,
  label: "1001",
  license_plate: null,
  quality_flags: [],
};

afterEach(() => vi.unstubAllGlobals());

describe("fetchAllVehicles", () => {
  it("loads every cursor page", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(
        new Response(JSON.stringify({ items: [vehicle], next_cursor: "next" }), { status: 200 }),
      )
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({ items: [{ ...vehicle, vehicle_id: "43" }], next_cursor: null }),
          { status: 200 },
        ),
      );
    vi.stubGlobal("fetch", fetchMock);

    await expect(fetchAllVehicles()).resolves.toHaveLength(2);
    expect(fetchMock).toHaveBeenNthCalledWith(
      2,
      "/api/v1/vehicles?limit=500&cursor=next",
      expect.any(Object),
    );
  });

  it("rejects a repeated cursor", async () => {
    vi.stubGlobal(
      "fetch",
      vi
        .fn()
        .mockResolvedValueOnce(
          new Response(JSON.stringify({ items: [], next_cursor: "next" }), { status: 200 }),
        )
        .mockResolvedValueOnce(
          new Response(JSON.stringify({ items: [], next_cursor: "next" }), { status: 200 }),
        ),
    );

    await expect(fetchAllVehicles()).rejects.toThrow("повторяющийся курсор");
  });

  it("rejects a response that does not match the API contract", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(
        new Response(JSON.stringify({ items: [{ ...vehicle, speed: "fast" }], next_cursor: null }), {
          status: 200,
        }),
      ),
    );

    await expect(fetchAllVehicles()).rejects.toThrow("Некорректный ответ API");
  });
});

describe("fetchAllRoutes", () => {
  it("loads human-readable route metadata", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(
        new Response(
          JSON.stringify({
            items: [
              {
                route_id: "7648",
                agency_id: "orgp",
                short_name: "245",
                long_name: "Красное Село — Кировский завод",
                route_type: 3,
              },
            ],
            next_cursor: null,
            static_feed_version: "version",
          }),
          { status: 200 },
        ),
      ),
    );

    await expect(fetchAllRoutes()).resolves.toMatchObject([
      { route_id: "7648", short_name: "245" },
    ]);
  });
});
