import { lazy, Suspense, useMemo, useState } from "react";
import RefreshIcon from "@mui/icons-material/Refresh";
import AppBar from "@mui/material/AppBar";
import Box from "@mui/material/Box";
import Chip from "@mui/material/Chip";
import IconButton from "@mui/material/IconButton";
import LinearProgress from "@mui/material/LinearProgress";
import Skeleton from "@mui/material/Skeleton";
import Stack from "@mui/material/Stack";
import Toolbar from "@mui/material/Toolbar";
import Tooltip from "@mui/material/Tooltip";
import Typography from "@mui/material/Typography";

import { VehicleSidebar } from "./components/VehicleSidebar";
import { MapErrorBoundary } from "./components/MapErrorBoundary";
import { useLiveVehicles } from "./hooks/useLiveVehicles";
import { apiStatusLabel, filterVehicles, formatUpdatedAt, routeOptions } from "./vehicles";

const VehicleMap = lazy(() =>
  import("./components/VehicleMap").then(({ VehicleMap: component }) => ({ default: component })),
);

export default function App() {
  const { vehicles, routes, readiness, updatedAt, loading, refreshing, error, refresh } = useLiveVehicles();
  const [routeId, setRouteId] = useState<string | null>(null);
  const [selectedVehicleId, setSelectedVehicleId] = useState<string | null>(null);
  const activeRoutes = useMemo(() => routeOptions(vehicles, routes), [vehicles, routes]);
  const shownVehicles = useMemo(() => filterVehicles(vehicles, routeId), [vehicles, routeId]);
  const selectedVehicle =
    shownVehicles.find(({ vehicle_id }) => vehicle_id === selectedVehicleId) ?? null;
  const apiReady = readiness?.status === "ready";
  const apiStatus = apiStatusLabel(readiness, error, updatedAt !== null);

  function changeRoute(value: string | null) {
    setRouteId(value);
    setSelectedVehicleId(null);
  }

  return (
    <Box sx={{ height: "100vh", minWidth: 1100, overflow: "hidden" }}>
      <AppBar
        color="inherit"
        elevation={0}
        position="static"
        sx={{ borderBottom: 1, borderColor: "divider", position: "relative" }}
      >
        <Toolbar>
          <Typography sx={{ flexGrow: 1 }} variant="h6">
            Транспорт Санкт-Петербурга
          </Typography>
          <Stack direction="row" spacing={2} sx={{ alignItems: "center" }}>
            <Chip
              color={apiReady ? "success" : "default"}
              label={apiStatus}
              size="small"
              variant="outlined"
            />
            <Typography color="text.secondary" variant="body2">
              Обновлено: {formatUpdatedAt(updatedAt)}
            </Typography>
            <Tooltip title="Обновить данные">
              <span>
                <IconButton aria-label="Обновить данные" disabled={loading || refreshing} onClick={refresh}>
                  <RefreshIcon />
                </IconButton>
              </span>
            </Tooltip>
          </Stack>
        </Toolbar>
        {refreshing && (
          <LinearProgress sx={{ bottom: 0, left: 0, position: "absolute", right: 0 }} />
        )}
      </AppBar>
      <Box component="main" sx={{ display: "flex", height: "calc(100vh - 65px)" }}>
        <VehicleSidebar
          routes={activeRoutes}
          routeId={routeId}
          shownCount={shownVehicles.length}
          totalCount={vehicles.length}
          selectedVehicle={selectedVehicle}
          loading={loading}
          error={error}
          onRouteChange={changeRoute}
          onRetry={refresh}
        />
        <MapErrorBoundary>
          <Suspense fallback={<Skeleton animation="wave" sx={{ flex: 1 }} variant="rectangular" />}>
            <VehicleMap
              vehicles={shownVehicles}
              focusKey={routeId}
              selectedVehicleId={selectedVehicleId}
              onSelect={setSelectedVehicleId}
            />
          </Suspense>
        </MapErrorBoundary>
      </Box>
    </Box>
  );
}
