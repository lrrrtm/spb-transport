import BoltRounded from "@mui/icons-material/BoltRounded";
import CommuteOutlined from "@mui/icons-material/CommuteOutlined";
import DirectionsBusOutlined from "@mui/icons-material/DirectionsBusOutlined";
import RadioButtonUnchecked from "@mui/icons-material/RadioButtonUnchecked";
import PlaceRounded from "@mui/icons-material/PlaceRounded";
import TramOutlined from "@mui/icons-material/TramOutlined";
import Box from "@mui/material/Box";
import Chip from "@mui/material/Chip";
import Paper from "@mui/material/Paper";
import Stack from "@mui/material/Stack";
import Typography from "@mui/material/Typography";

import type { Route, VehiclePosition } from "../api/types";
import { transportModes } from "../vehicles";

interface VehicleDetailsProps {
  vehicle: VehiclePosition;
  route?: Route;
}

function Field({ label, value }: { label: string; value: string }) {
  return (
    <Box sx={{ minWidth: 0 }}>
      <Typography component="dt" variant="caption" color="text.secondary">{label}</Typography>
      <Typography component="dd" variant="body2" sx={{ m: 0, fontWeight: 600, overflowWrap: "anywhere" }}>
        {value}
      </Typography>
    </Box>
  );
}

function RouteEndpoints({ name, color }: { name: string; color: string }) {
  // A quoted station or road name can contain a spaced dash of its own.
  const endpoints = name.trim().match(/^((?:[^"]|"[^"]*")*?)\s+[-–—]\s+(.+)$/u);
  if (!endpoints) return <Typography variant="body2">{name}</Typography>;

  return (
    <Box sx={{ display: "grid", gridTemplateColumns: "16px minmax(0, 1fr)", columnGap: 1, rowGap: 1,
      alignItems: "start", position: "relative", overflowWrap: "anywhere" }}>
      <Box sx={{ gridColumn: 1, gridRow: 1, alignSelf: "stretch", position: "relative",
        "&::after": { content: '""', position: "absolute", left: "7px", top: 16, bottom: -10,
          borderLeft: `2px solid ${color}55` } }}>
        <RadioButtonUnchecked titleAccess="Начальный пункт" sx={{ display: "block", fontSize: 12, m: "4px 2px", color }} />
      </Box>
      <Typography variant="body2">{endpoints[1]}</Typography>
      <PlaceRounded titleAccess="Конечный пункт" sx={{ fontSize: 16, mt: "2px", color }} />
      <Typography variant="body2">{endpoints[2]}</Typography>
    </Box>
  );
}

export function VehicleDetails({ vehicle, route }: VehicleDetailsProps) {
  const type = route?.route_type;
  const mode = transportModes[type ?? -1];
  const color = mode?.color ?? "#526477";
  const Icon = type === 0 ? TramOutlined : type === 3 || type === 11 ? DirectionsBusOutlined : CommuteOutlined;
  const timestamp = new Intl.DateTimeFormat("ru-RU", {
    hour: "2-digit", minute: "2-digit", second: "2-digit",
  }).format(new Date(vehicle.source_timestamp));

  return (
    <Paper component="section" aria-label="Выбранный транспорт" variant="outlined"
      sx={{ borderRadius: 3, overflow: "hidden", fontVariantNumeric: "tabular-nums" }}>
      <Stack spacing={1.5} sx={{ p: 2, bgcolor: `${color}0c`, borderBottom: 1, borderColor: "divider" }}>
        <Stack direction="row" spacing={1.5} sx={{ alignItems: "center" }}>
          <Box aria-hidden sx={{ position: "relative", display: "grid", placeItems: "center", flexShrink: 0,
            width: 48, height: 48, bgcolor: color, color: "#fff", borderRadius: 2 }}>
            <Icon sx={{ fontSize: 31 }} />
            {type === 11 && <BoltRounded sx={{ position: "absolute", top: 2, right: 2, fontSize: 14 }} />}
          </Box>
          <Box sx={{ minWidth: 0 }}>
            <Typography variant="body2" color="text.secondary">{mode?.name ?? "Транспорт"}</Typography>
            <Typography component="h2" sx={{ fontSize: 28, fontWeight: 700, lineHeight: 1.1, overflowWrap: "anywhere" }}>
              {route?.short_name ?? "Маршрут"}
            </Typography>
          </Box>
        </Stack>
        {route?.long_name && <RouteEndpoints name={route.long_name} color={color} />}
      </Stack>
      <Box component="dl" sx={{ display: "grid", gridTemplateColumns: "repeat(2, minmax(0, 1fr))", gap: 1.5, p: 2, m: 0 }}>
        <Field label="Номер машины" value={vehicle.label ?? vehicle.vehicle_id} />
        <Field label="Госномер" value={vehicle.license_plate ?? "—"} />
        <Field label="Скорость" value={vehicle.speed === null ? "—" : `${Math.round(vehicle.speed * 3.6)} км/ч`} />
        <Field label="Время позиции" value={timestamp} />
      </Box>
      {vehicle.quality_flags.length > 0 && (
        <Stack direction="row" sx={{ flexWrap: "wrap", gap: 1, px: 2, pb: 2 }}>
          {vehicle.quality_flags.map((flag) => (
            <Chip color="warning" key={flag} label={flag} size="small" variant="outlined" />
          ))}
        </Stack>
      )}
    </Paper>
  );
}
