import { useEffect, useRef } from "react";
import Box from "@mui/material/Box";
import type { FeatureCollection, LineString } from "geojson";
import { Map as MapLibreMap, NavigationControl, setWorkerUrl, type StyleSpecification } from "maplibre-gl";
import "maplibre-gl/dist/maplibre-gl.css";
import maplibreWorkerUrl from "maplibre-gl/dist/maplibre-gl-worker.mjs?url";

import type { SegmentMetric } from "../api/types";

setWorkerUrl(maplibreWorkerUrl);
const SOURCE_ID = "segments";
const SPB_CENTER: [number, number] = [30.3159, 59.9391];
const mapStyle: StyleSpecification = { version: 8, sources: { osm: { type: "raster", tiles: ["https://tile.openstreetmap.org/{z}/{x}/{y}.png"], tileSize: 256, attribution: "© OpenStreetMap contributors" } }, layers: [{ id: "osm", type: "raster", source: "osm" }] };

function geoJson(segments: SegmentMetric[]): FeatureCollection<LineString> {
  return { type: "FeatureCollection", features: segments.flatMap((segment) => segment.from_longitude === null || segment.from_latitude === null || segment.to_longitude === null || segment.to_latitude === null ? [] : [{ type: "Feature" as const, geometry: { type: "LineString" as const, coordinates: [[segment.from_longitude, segment.from_latitude], [segment.to_longitude, segment.to_latitude]] }, properties: { speed: segment.speed_p50_mps * 3.6 } }]) };
}

export function SegmentMap({ segments }: { segments: SegmentMetric[] }) {
  const container = useRef<HTMLDivElement | null>(null);
  useEffect(() => {
    if (container.current === null) return;
    const data = geoJson(segments);
    const map = new MapLibreMap({ container: container.current, style: mapStyle, center: SPB_CENTER, zoom: 10, minZoom: 8 });
    map.addControl(new NavigationControl({ showCompass: false }), "top-right");
    map.on("load", () => {
      map.addSource(SOURCE_ID, { type: "geojson", data });
      map.addLayer({ id: "segment-lines", type: "line", source: SOURCE_ID, paint: { "line-color": ["interpolate", ["linear"], ["get", "speed"], 5, "#c62828", 15, "#ef6c00", 30, "#2e7d32"], "line-width": 6, "line-opacity": 0.85 } });
      const points = data.features.flatMap((feature) => feature.geometry.coordinates);
      if (points.length > 0) map.fitBounds([[Math.min(...points.map(([lon]) => lon)), Math.min(...points.map(([, lat]) => lat))], [Math.max(...points.map(([lon]) => lon)), Math.max(...points.map(([, lat]) => lat))]], { padding: 48, maxZoom: 14 });
    });
    return () => map.remove();
  }, [segments]);
  return <Box ref={container} role="application" aria-label="Карта скорости на участках маршрута" sx={{ height: 360, border: 1, borderColor: "divider", borderRadius: 1, mb: 2 }} />;
}
