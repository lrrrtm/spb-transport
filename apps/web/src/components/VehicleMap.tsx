import { useEffect, useRef } from "react";
import Box from "@mui/material/Box";
import { useTheme } from "@mui/material/styles";
import type { FeatureCollection, Point } from "geojson";
import {
  type GeoJSONSource,
  Map as MapLibreMap,
  type MapLayerMouseEvent,
  NavigationControl,
  setWorkerUrl,
  type StyleSpecification,
} from "maplibre-gl";
import "maplibre-gl/dist/maplibre-gl.css";
import maplibreWorkerUrl from "maplibre-gl/dist/maplibre-gl-worker.mjs?url";

import type { VehiclePosition } from "../api/types";

// MapLibre 6 loads its worker from a file next to its own module. Vite bundles the module but does
// not emit that file, and GeoJSON sources are parsed in the worker, so without an explicit URL the
// base map renders while the vehicle layer stays empty.
setWorkerUrl(maplibreWorkerUrl);

const SOURCE_ID = "vehicles";
const LAYER_ID = "vehicles-points";
const SPB_CENTER: [number, number] = [30.3159, 59.9391];

const mapStyle: StyleSpecification = {
  version: 8,
  sources: {
    osm: {
      type: "raster",
      tiles: ["https://tile.openstreetmap.org/{z}/{x}/{y}.png"],
      tileSize: 256,
      attribution: "© OpenStreetMap contributors",
    },
  },
  layers: [{ id: "osm", type: "raster", source: "osm" }],
};

interface VehicleMapProps {
  vehicles: VehiclePosition[];
  focusKey: string | null;
  selectedVehicleId: string | null;
  onSelect: (vehicleId: string) => void;
}

function toGeoJson(
  vehicles: VehiclePosition[],
  selectedVehicleId: string | null,
): FeatureCollection<Point> {
  return {
    type: "FeatureCollection",
    features: vehicles.map((vehicle) => ({
      type: "Feature",
      geometry: { type: "Point", coordinates: [vehicle.longitude, vehicle.latitude] },
      properties: {
        vehicleId: vehicle.vehicle_id,
        selected: vehicle.vehicle_id === selectedVehicleId,
      },
    })),
  };
}

export function VehicleMap({ vehicles, focusKey, selectedVehicleId, onSelect }: VehicleMapProps) {
  const container = useRef<HTMLDivElement | null>(null);
  const map = useRef<MapLibreMap | null>(null);
  const data = useRef(toGeoJson(vehicles, selectedVehicleId));
  const onSelectRef = useRef(onSelect);
  const theme = useTheme();

  useEffect(() => {
    onSelectRef.current = onSelect;
  }, [onSelect]);

  useEffect(() => {
    if (container.current === null) return;
    const instance = new MapLibreMap({
      container: container.current,
      style: mapStyle,
      center: SPB_CENTER,
      zoom: 10,
      minZoom: 8,
    });
    instance.addControl(new NavigationControl({ showCompass: false }), "top-right");
    instance.on("load", () => {
      instance.addSource(SOURCE_ID, { type: "geojson", data: data.current });
      instance.addLayer({
        id: LAYER_ID,
        type: "circle",
        source: SOURCE_ID,
        paint: {
          "circle-color": [
            "case",
            ["==", ["get", "selected"], true],
            theme.palette.error.main,
            theme.palette.primary.main,
          ],
          "circle-radius": ["case", ["==", ["get", "selected"], true], 7, 4],
          "circle-stroke-color": theme.palette.background.paper,
          "circle-stroke-width": 1,
        },
      });
      instance.on("click", LAYER_ID, (event: MapLayerMouseEvent) => {
        const vehicleId = event.features?.[0]?.properties.vehicleId;
        if (typeof vehicleId === "string") onSelectRef.current(vehicleId);
      });
      instance.on("mouseenter", LAYER_ID, () => {
        instance.getCanvas().style.cursor = "pointer";
      });
      instance.on("mouseleave", LAYER_ID, () => {
        instance.getCanvas().style.cursor = "";
      });
    });
    map.current = instance;
    return () => {
      instance.remove();
      map.current = null;
    };
  }, [theme.palette.background.paper, theme.palette.error.main, theme.palette.primary.main]);

  useEffect(() => {
    data.current = toGeoJson(vehicles, selectedVehicleId);
    const source = map.current?.getSource(SOURCE_ID) as GeoJSONSource | undefined;
    source?.setData(data.current);
  }, [vehicles, selectedVehicleId]);

  useEffect(() => {
    if (focusKey === null || vehicles.length === 0) return;
    const longitudes = vehicles.map(({ longitude }) => longitude);
    const latitudes = vehicles.map(({ latitude }) => latitude);
    map.current?.fitBounds(
      [
        [Math.min(...longitudes), Math.min(...latitudes)],
        [Math.max(...longitudes), Math.max(...latitudes)],
      ],
      { padding: 64, maxZoom: 14 },
    );
  }, [focusKey, vehicles]);

  return (
    <Box
      ref={container}
      role="application"
      aria-label="Карта актуального транспорта Санкт-Петербурга"
      sx={{ flex: 1, minWidth: 0 }}
    />
  );
}
