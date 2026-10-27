import Autocomplete from "@mui/material/Autocomplete";
import Paper from "@mui/material/Paper";
import Stack from "@mui/material/Stack";
import TextField from "@mui/material/TextField";
import Typography from "@mui/material/Typography";

import type { Route, VehiclePosition } from "../api/types";
import { routeLabel } from "../vehicles";
import { StatusView } from "./StatusView";
import { VehicleDetails } from "./VehicleDetails";

interface VehicleSidebarProps {
  routes: Route[];
  routeId: string | null;
  shownCount: number;
  totalCount: number;
  selectedVehicle: VehiclePosition | null;
  loading: boolean;
  error: string | null;
  onRouteChange: (routeId: string | null) => void;
  onRetry: () => void;
}

export function VehicleSidebar({
  routes,
  routeId,
  shownCount,
  totalCount,
  selectedVehicle,
  loading,
  error,
  onRouteChange,
  onRetry,
}: VehicleSidebarProps) {
  const selectedRoute = routes.find(({ route_id }) => route_id === selectedVehicle?.route_id);

  return (
    <Paper
      component="aside"
      elevation={0}
      square
      sx={{ borderRight: 1, borderColor: "divider", flexShrink: 0, overflowY: "auto", p: 3, width: 320 }}
    >
      <Stack spacing={3}>
        <Typography component="h1" variant="h5">
          Транспорт на карте
        </Typography>
        <Autocomplete
          autoHighlight
          disabled={loading || totalCount === 0}
          getOptionLabel={routeLabel}
          isOptionEqualToValue={(option, value) => option.route_id === value.route_id}
          options={routes}
          value={routes.find((route) => route.route_id === routeId) ?? null}
          onChange={(_, value) => onRouteChange(value?.route_id ?? null)}
          renderInput={(params) => <TextField {...params} label="Маршрут" />}
        />
        <StatusView loading={loading} error={error} empty={!loading && totalCount === 0} onRetry={onRetry} />
        {!loading && error === null && totalCount > 0 && (
          <Typography color="text.secondary" variant="body2">
            Показано {shownCount.toLocaleString("ru-RU")} из {totalCount.toLocaleString("ru-RU")}
          </Typography>
        )}
        {selectedVehicle !== null && (
          <VehicleDetails
            vehicle={selectedVehicle}
            routeName={selectedRoute === undefined ? (selectedVehicle.route_id ?? "—") : routeLabel(selectedRoute)}
          />
        )}
      </Stack>
    </Paper>
  );
}
