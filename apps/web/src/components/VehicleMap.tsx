import { useEffect, useMemo, useRef, useState } from "react";
import { createPortal } from "react-dom";
import Box from "@mui/material/Box";
import { useTheme } from "@mui/material/styles";
import type { FeatureCollection, MultiLineString, Point } from "geojson";
import {
  type GeoJSONSource,
  Map as MapLibreMap,
  type MapLayerMouseEvent,
  NavigationControl,
  Popup,
  setWorkerUrl,
} from "maplibre-gl";
import "maplibre-gl/dist/maplibre-gl.css";
import maplibreWorkerUrl from "maplibre-gl/dist/maplibre-gl-worker.mjs?url";

import type { Route, VehiclePosition } from "../api/types";
import { transportModes } from "../vehicles";
import { VehicleDetails } from "./VehicleDetails";

// MapLibre 6 loads its worker from a file next to its own module. Vite bundles the module but does
// not emit that file, and GeoJSON sources are parsed in the worker, so without an explicit URL the
// base map renders while the vehicle layer stays empty.
setWorkerUrl(maplibreWorkerUrl);

const SOURCE_ID = "vehicles";
const LAYER_ID = "vehicles-points";
const ROUTE_SOURCE = "selected-route";
const STOP_SOURCE = "selected-route-stops";
const pluralRules = new Intl.PluralRules("ru");
const unitNames: Record<string, string> = { one: "единица", few: "единицы" };
const EMPTY_ROUTE: FeatureCollection = { type: "FeatureCollection", features: [] };
const SPB_CENTER: [number, number] = [30.3159, 59.9391];

function headingIcon(): ImageData {
  const canvas = document.createElement("canvas");
  canvas.width = canvas.height = 48;
  const ctx = canvas.getContext("2d")!;
  // The image rotates around the vehicle at (24, 24). Its tip points north.
  ctx.beginPath();
  ctx.moveTo(24, 4);
  ctx.lineTo(32, 19);
  ctx.lineTo(16, 19);
  ctx.closePath();
  ctx.fillStyle = "#364450";
  ctx.strokeStyle = "#fff";
  ctx.lineWidth = 2;
  ctx.lineJoin = "round";
  ctx.fill();
  ctx.stroke();
  return ctx.getImageData(0, 0, 48, 48);
}

interface VehicleMapProps {
  showCount: boolean;
  routes: Route[];
  vehicles: VehiclePosition[];
  focusKey: string | null;
  selectedVehicleId: string | null;
  onSelect: (vehicleId: string | null) => void;
}

interface RouteStop {
  stop_id: string;
  stop_name: string | null;
  latitude: number;
  longitude: number;
}

function stopPoints(variants: { stops: RouteStop[] }[]): FeatureCollection<Point> {
  const stops = new Map(variants.flatMap((variant) => variant.stops.map((stop) => [stop.stop_id, stop] as const)));
  return {
    type: "FeatureCollection",
    features: Array.from(stops.values(), (stop) => ({
      type: "Feature",
      geometry: { type: "Point", coordinates: [stop.longitude, stop.latitude] },
      properties: { name: stop.stop_name ?? "" },
    })),
  };
}

function toGeoJson(
  vehicles: VehiclePosition[],
  routes: Route[],
  selectedVehicleId: string | null,
): FeatureCollection<Point> {
  const byId = new Map(routes.map((route) => [route.route_id, route]));
  return {
    type: "FeatureCollection",
    features: vehicles.map((vehicle) => {
      const route = byId.get(vehicle.route_id ?? "");
      return {
        type: "Feature",
        id: vehicle.vehicle_id,
        geometry: { type: "Point", coordinates: [vehicle.longitude, vehicle.latitude] },
        properties: {
          vehicleId: vehicle.vehicle_id,
          bearing: vehicle.speed === 0 ? null : vehicle.bearing,
          number: route?.short_name?.trim() ?? "",
          color: transportModes[route?.route_type ?? -1]?.color ?? "#687078",
          selected: vehicle.vehicle_id === selectedVehicleId,
        },
      };
    }),
  };
}

export function VehicleMap({ vehicles, routes, focusKey, selectedVehicleId, onSelect, showCount }: VehicleMapProps) {
  const container = useRef<HTMLDivElement | null>(null);
  const map = useRef<MapLibreMap | null>(null);
  const points = useMemo(() => toGeoJson(vehicles, routes, selectedVehicleId), [vehicles, routes, selectedVehicleId]);
  const data = useRef(points);
  const selectedVehicle = vehicles.find((vehicle) => vehicle.vehicle_id === selectedVehicleId);
  const routeId = selectedVehicle?.route_id;
  const popup = useRef<Popup | null>(null);
  const displayedVehicleId = useRef<string | null>(null);
  const [popupHost] = useState(() => document.createElement("div"));
  const count = routeId && !focusKey
    ? vehicles.filter((vehicle) => vehicle.route_id === routeId).length
    : vehicles.length;
  const routeGeometry = useRef<MultiLineString | FeatureCollection>(EMPTY_ROUTE);
  const routeStops = useRef<FeatureCollection>(EMPTY_ROUTE);
  const [failedStops, setFailedStops] = useState<string | null>(null);
  const [failedRoute, setFailedRoute] = useState<string | null>(null);
  const onSelectRef = useRef(onSelect);
  const theme = useTheme();

  useEffect(() => {
    onSelectRef.current = onSelect;
  }, [onSelect]);

  useEffect(() => {
    if (container.current === null) return;
    const instance = new MapLibreMap({
      container: container.current,
      style: "https://tiles.openfreemap.org/styles/positron",
      transformRequest: (url) => ({
        url: import.meta.env.DEV
          ? url.replace(/^https:\/\/tiles\.openfreemap\.org\//, `${window.location.origin}/basemap/`)
          : url,
      }),
      center: SPB_CENTER,
      locale: { "Popup.Close": "Закрыть карточку" },
      zoom: 10,
      minZoom: 8,
    });
    const cardPopup = new Popup({ offset: 12, maxWidth: "284px", closeOnClick: false,
      className: "vehicle-popup" }).setDOMContent(popupHost);
    const clearSelection = () => onSelectRef.current(null);
    cardPopup.on("close", clearSelection);
    popup.current = cardPopup;
    instance.addControl(new NavigationControl({ showCompass: false }), "top-right");
    instance.on("load", () => {
      instance.addSource(ROUTE_SOURCE, { type: "geojson", data: routeGeometry.current });
      instance.addLayer({
        id: ROUTE_SOURCE,
        type: "line",
        source: ROUTE_SOURCE,
        layout: { "line-cap": "round", "line-join": "round" },
        paint: { "line-color": "#842db1", "line-width": 4, "line-opacity": 0.85 },
      });
      instance.addSource(STOP_SOURCE, { type: "geojson", data: routeStops.current });
      instance.addLayer({
        id: STOP_SOURCE, type: "circle", source: STOP_SOURCE,
        paint: {
          "circle-color": "#fff", "circle-stroke-color": "#69208d", "circle-stroke-width": 2,
          "circle-radius": ["interpolate", ["linear"], ["zoom"], 10, 3, 15, 6],
        },
      });
      instance.addLayer({
        id: "selected-stop-names", type: "symbol", source: STOP_SOURCE, minzoom: 14,
        layout: {
          "text-field": ["get", "name"], "text-font": ["Noto Sans Regular"], "text-size": 12,
          "text-variable-anchor": ["top", "bottom", "right", "left"], "text-radial-offset": 0.8,
        },
        paint: { "text-color": "#522168", "text-halo-color": "#fff", "text-halo-width": 2 },
      });
      instance.addSource(SOURCE_ID, { type: "geojson", data: data.current });
      instance.addImage("vehicle-heading", headingIcon(), { pixelRatio: 2 });
      instance.addLayer({
        id: "vehicle-headings",
        type: "symbol",
        source: SOURCE_ID,
        filter: ["==", ["typeof", ["get", "bearing"]], "number"],
        layout: {
          "icon-image": "vehicle-heading",
          "icon-rotate": ["get", "bearing"],
          "icon-rotation-alignment": "map",
          "icon-size": ["case", ["get", "selected"], 1.35, 1],
          "icon-allow-overlap": true,
          "icon-ignore-placement": true,
        },
      });
      instance.addLayer({
        id: LAYER_ID,
        type: "circle",
        source: SOURCE_ID,
        paint: {
          "circle-color": ["get", "color"],
          "circle-radius": ["case", ["==", ["get", "selected"], true], 7, 4],
          "circle-stroke-color": theme.palette.background.paper,
          "circle-stroke-width": 1,
        },
      });
      instance.addLayer({
        id: "vehicles-route-numbers",
        type: "symbol",
        source: SOURCE_ID,
        layout: {
          "text-field": ["get", "number"],
          "text-font": ["Noto Sans Regular"],
          "text-size": 13,
          "text-variable-anchor": ["left", "right", "top", "bottom"],
          "text-radial-offset": 0.9,
        },
        paint: {
          "text-color": "#182329",
          "text-halo-color": "#ffffff",
          "text-halo-width": 2,
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
      cardPopup.off("close", clearSelection);
      cardPopup.remove();
      popup.current = null;
      instance.remove();
      map.current = null;
    };
  }, [theme.palette.background.paper, popupHost]);

  useEffect(() => {
    const card = popup.current;
    const instance = map.current;
    if (!card || !instance) return;
    const source = instance.getSource<GeoJSONSource>(SOURCE_ID);
    const previous = card.isOpen() && displayedVehicleId.current === selectedVehicle?.vehicle_id
      ? card.getLngLat() : null;
    displayedVehicleId.current = selectedVehicle?.vehicle_id ?? null;
    const animate = selectedVehicle && previous && source
      && (previous.lng !== selectedVehicle.longitude || previous.lat !== selectedVehicle.latitude)
      && !window.matchMedia("(prefers-reduced-motion: reduce)").matches;

    // Start the new snapshot at the position currently on screen, including an interrupted move.
    data.current = animate ? {
      ...points,
      features: points.features.map((feature) => feature.id === selectedVehicle.vehicle_id
        ? { ...feature, geometry: { type: "Point", coordinates: [previous.lng, previous.lat] } }
        : feature),
    } : points;
    void source?.setData(data.current);

    if (!selectedVehicle) {
      if (card.isOpen()) card.remove();
      return;
    }
    const { vehicle_id, longitude, latitude } = selectedVehicle;
    if (!animate) {
      card.setLngLat([longitude, latitude]);
      if (!card.isOpen()) card.addTo(instance);
      return;
    }

    const start = performance.now();
    let frame: number;
    const move = (now: number) => {
      const t = Math.min(1, (now - start) / 1000);
      const coordinates: [number, number] = [
        previous.lng + (longitude - previous.lng) * t,
        previous.lat + (latitude - previous.lat) * t,
      ];
      card.setLngLat(coordinates);
      void source.updateData({ update: [{ id: vehicle_id, newGeometry: { type: "Point", coordinates } }] });
      if (t < 1) frame = requestAnimationFrame(move);
    };
    frame = requestAnimationFrame(move);
    return () => cancelAnimationFrame(frame);
  }, [points, selectedVehicle, theme.palette.background.paper]);

  useEffect(() => {
    const controller = new AbortController();
    function draw(geometry: MultiLineString | FeatureCollection) {
      routeGeometry.current = geometry;
      map.current?.getSource<GeoJSONSource>(ROUTE_SOURCE)?.setData(geometry);
    }
    draw(EMPTY_ROUTE);
    function drawStops(stops: FeatureCollection) {
      routeStops.current = stops;
      map.current?.getSource<GeoJSONSource>(STOP_SOURCE)?.setData(stops);
    }
    drawStops(EMPTY_ROUTE);
    if (routeId) {
      fetch(`/api/v1/routes/${encodeURIComponent(routeId)}/stops`, { signal: controller.signal })
        .then(async (response) => {
          if (!response.ok) throw new Error("Route stops unavailable");
          const { variants } = await response.json();
          if (!controller.signal.aborted) {
            drawStops(stopPoints(variants));
            setFailedStops(null);
          }
        })
        .catch(() => {
          if (!controller.signal.aborted) setFailedStops(routeId);
        });
      fetch(`/api/v1/routes/${encodeURIComponent(routeId)}/geometry`, { signal: controller.signal })
        .then(async (response) => {
          if (!response.ok) throw new Error("Route geometry unavailable");
          const geometry: MultiLineString = await response.json();
          if (!controller.signal.aborted) {
            draw(geometry);
            setFailedRoute(null);
          }
        })
        .catch(() => {
          if (!controller.signal.aborted) setFailedRoute(routeId);
        });
    }
    return () => controller.abort();
  }, [routeId]);

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
    <Box sx={{ flex: 1, minWidth: 0, position: "relative" }}>
      <Box
        ref={container}
        role="application"
        aria-label="Карта актуального транспорта Санкт-Петербурга"
        sx={{
          height: "100%",
          "& .vehicle-popup .maplibregl-popup-content": {
            p: 0, width: 284, maxHeight: "calc(100vh - 155px)", overflowY: "auto",
            borderRadius: 3, boxShadow: 3, fontFamily: "inherit",
          },
          "& .vehicle-popup .maplibregl-popup-close-button": {
            top: 8, right: 8, width: 32, height: 32, borderRadius: "50%", fontSize: 22,
          },
        }}
      />
      {selectedVehicle && createPortal(
        <VehicleDetails vehicle={selectedVehicle} route={routes.find((route) => route.route_id === routeId)} />,
        popupHost,
      )}
      <Box component="ul" aria-label="Виды транспорта на карте" sx={{
        position: "absolute", bottom: 8, left: 8, display: "flex", gap: 1.5,
        m: 0, px: 1.5, py: 0.75, listStyle: "none", pointerEvents: "none",
        bgcolor: "background.paper", borderRadius: 1, boxShadow: 1, typography: "caption",
      }}>
        {Object.values(transportModes).map(({ name, color }) => (
          <Box component="li" key={name} sx={{ display: "flex", alignItems: "center", gap: 0.75 }}>
            <Box component="span" aria-hidden sx={{ width: 8, height: 8, borderRadius: "50%", bgcolor: color }} />
            {name}
          </Box>
        ))}
      </Box>
      <Box sx={{ position: "absolute", top: 8, left: 8, display: "grid", justifyItems: "start", gap: 1,
        pointerEvents: "none", "& > div": { px: 1.5, py: 0.75, bgcolor: "background.paper", borderRadius: 1,
          boxShadow: 1, typography: "body2" } }}>
        {showCount && (
          <Box role="status">
            На маршруте {count.toLocaleString("ru-RU")} {unitNames[pluralRules.select(count)] ?? "единиц"}
          </Box>
        )}
        {routeId && (failedRoute === routeId || failedStops === routeId) && (
          <Box role="status">
            {failedRoute === routeId ? "Не удалось загрузить линию маршрута" : "Не удалось загрузить остановки"}
          </Box>
        )}
      </Box>
    </Box>
  );
}
