import { useCallback, useEffect, useRef, useState } from "react";

import { fetchAllRoutes, fetchAllVehicles, fetchReadiness } from "../api/client";
import type { Readiness, Route, VehiclePosition } from "../api/types";

const REFRESH_INTERVAL_MS = 15_000;

interface LiveVehiclesState {
  vehicles: VehiclePosition[];
  routes: Route[];
  readiness: Readiness | null;
  updatedAt: Date | null;
  loading: boolean;
  refreshing: boolean;
  error: string | null;
  refresh: () => void;
}

export function useLiveVehicles(): LiveVehiclesState {
  const [vehicles, setVehicles] = useState<VehiclePosition[]>([]);
  const [routes, setRoutes] = useState<Route[]>([]);
  const [readiness, setReadiness] = useState<Readiness | null>(null);
  const [updatedAt, setUpdatedAt] = useState<Date | null>(null);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const controller = useRef<AbortController | null>(null);
  const hasData = useRef(false);

  const load = useCallback(async () => {
    controller.current?.abort();
    const nextController = new AbortController();
    controller.current = nextController;
    if (hasData.current) setRefreshing(true);
    else setLoading(true);

    // Vehicles are the only required data: route metadata needs the static GTFS batch job and
    // /ready answers 503 while Cassandra is degraded, neither should hide positions already served.
    const [vehiclesResult, routesResult, readinessResult] = await Promise.allSettled([
      fetchAllVehicles(nextController.signal),
      fetchAllRoutes(nextController.signal),
      fetchReadiness(nextController.signal),
    ]);
    if (controller.current !== nextController || nextController.signal.aborted) return;

    setReadiness(readinessResult.status === "fulfilled" ? readinessResult.value : null);
    if (routesResult.status === "fulfilled") setRoutes(routesResult.value);
    if (vehiclesResult.status === "fulfilled") {
      setVehicles(vehiclesResult.value);
      setUpdatedAt(new Date());
      setError(null);
      hasData.current = true;
    } else {
      const reason: unknown = vehiclesResult.reason;
      setError(reason instanceof Error ? reason.message : "Не удалось загрузить данные");
    }
    setLoading(false);
    setRefreshing(false);
  }, []);

  useEffect(() => {
    const initialLoad = window.setTimeout(() => void load(), 0);
    const interval = window.setInterval(() => void load(), REFRESH_INTERVAL_MS);
    return () => {
      window.clearTimeout(initialLoad);
      window.clearInterval(interval);
      controller.current?.abort();
    };
  }, [load]);

  return { vehicles, routes, readiness, updatedAt, loading, refreshing, error, refresh: () => void load() };
}
