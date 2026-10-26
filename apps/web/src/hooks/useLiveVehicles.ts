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

    try {
      const [nextVehicles, nextRoutes, nextReadiness] = await Promise.all([
        fetchAllVehicles(nextController.signal),
        fetchAllRoutes(nextController.signal),
        fetchReadiness(nextController.signal),
      ]);
      setVehicles(nextVehicles);
      setRoutes(nextRoutes);
      setReadiness(nextReadiness);
      setUpdatedAt(new Date());
      setError(null);
      hasData.current = true;
    } catch (caught) {
      if (caught instanceof DOMException && caught.name === "AbortError") return;
      setError(caught instanceof Error ? caught.message : "Не удалось загрузить данные");
    } finally {
      if (controller.current === nextController) {
        setLoading(false);
        setRefreshing(false);
      }
    }
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
