import { lazy, Suspense, useMemo, useState } from "react";
import RefreshIcon from "@mui/icons-material/Refresh";
import AppBar from "@mui/material/AppBar";
import Autocomplete from "@mui/material/Autocomplete";
import Box from "@mui/material/Box";
import Chip from "@mui/material/Chip";
import IconButton from "@mui/material/IconButton";
import LinearProgress from "@mui/material/LinearProgress";
import Skeleton from "@mui/material/Skeleton";
import Stack from "@mui/material/Stack";
import Toolbar from "@mui/material/Toolbar";
import Tooltip from "@mui/material/Tooltip";
import Typography from "@mui/material/Typography";
import Tab from "@mui/material/Tab";
import Tabs from "@mui/material/Tabs";
import TextField from "@mui/material/TextField";

import { StatusView } from "./components/StatusView";
import { AnalyticsPage } from "./components/AnalyticsPage";
import { MapErrorBoundary } from "./components/MapErrorBoundary";
import { useLiveVehicles } from "./hooks/useLiveVehicles";
import { apiStatusLabel, filterVehicles, formatUpdatedAt, routeOptions, routeSearchLabel } from "./vehicles";

const VehicleMap = lazy(() =>
  import("./components/VehicleMap").then(({ VehicleMap: component }) => ({ default: component })),
);

export default function App() {
  const [section, setSection] = useState<"live" | "analytics">("live");
  const { vehicles, routes, readiness, updatedAt, loading, refreshing, error, refresh } = useLiveVehicles();
  const [routeId, setRouteId] = useState<string | null>(null);
  const [selectedVehicleId, setSelectedVehicleId] = useState<string | null>(null);
  const activeRoutes = useMemo(() => routeOptions(vehicles, routes), [vehicles, routes]);
  const shownVehicles = useMemo(() => filterVehicles(vehicles, routeId), [vehicles, routeId]);
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
        <Toolbar sx={{ gap: 3 }}>
          <Typography variant="h6">
            Транспорт Санкт-Петербурга
          </Typography>
          {section === "live" && (
            <Autocomplete
              autoHighlight
              size="small"
              sx={{ width: 260 }}
              disabled={loading || activeRoutes.length === 0}
              options={activeRoutes}
              getOptionLabel={routeSearchLabel}
              getOptionKey={(route) => route.route_id}
              isOptionEqualToValue={(option, value) => option.route_id === value.route_id}
              value={activeRoutes.find((route) => route.route_id === routeId) ?? null}
              onChange={(_, route) => changeRoute(route?.route_id ?? null)}
              renderInput={(params) => (
                <TextField {...params} placeholder="Выберите маршрут" slotProps={{
                  ...params.slotProps,
                  htmlInput: { ...params.slotProps.htmlInput, "aria-label": "Выберите маршрут" },
                }} />
              )}
            />
          )}
          <Stack direction="row" spacing={2} sx={{ alignItems: "center", ml: "auto" }}>
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
        <Tabs value={section} onChange={(_, value) => setSection(value)} sx={{ minHeight: 48, px: 2 }}>
          <Tab value="live" label="Транспорт" />
          <Tab value="analytics" label="Аналитика" />
        </Tabs>
        {refreshing && (
          <LinearProgress sx={{ bottom: 0, left: 0, position: "absolute", right: 0 }} />
        )}
      </AppBar>
      {section === "analytics" ? <AnalyticsPage /> : <Box component="main" sx={{ display: "flex", position: "relative", height: "calc(100vh - 113px)" }}>
        <MapErrorBoundary>
          <Suspense fallback={<Skeleton animation="wave" sx={{ flex: 1 }} variant="rectangular" />}>
            <VehicleMap
              showCount={!loading && error === null}
              vehicles={shownVehicles}
              routes={routes}
              focusKey={routeId}
              selectedVehicleId={selectedVehicleId}
              onSelect={setSelectedVehicleId}
            />
          </Suspense>
        </MapErrorBoundary>
        {(loading || error !== null || vehicles.length === 0) && (
          <Box sx={{ position: "absolute", top: 8, left: "50%", transform: "translateX(-50%)", width: 320 }}>
            <StatusView loading={loading} error={error} empty={vehicles.length === 0} onRetry={refresh} />
          </Box>
        )}
      </Box>}
    </Box>
  );
}
