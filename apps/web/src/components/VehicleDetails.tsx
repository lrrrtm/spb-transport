import Chip from "@mui/material/Chip";
import Divider from "@mui/material/Divider";
import Stack from "@mui/material/Stack";
import Typography from "@mui/material/Typography";

import type { VehiclePosition } from "../api/types";

interface VehicleDetailsProps {
  vehicle: VehiclePosition;
  routeName: string;
}

function Field({ label, value }: { label: string; value: string }) {
  return (
    <Stack direction="row" spacing={2} sx={{ justifyContent: "space-between" }}>
      <Typography color="text.secondary" variant="body2">
        {label}
      </Typography>
      <Typography variant="body2">{value}</Typography>
    </Stack>
  );
}

export function VehicleDetails({ vehicle, routeName }: VehicleDetailsProps) {
  const timestamp = new Intl.DateTimeFormat("ru-RU", {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  }).format(new Date(vehicle.source_timestamp));

  return (
    <Stack spacing={2}>
      <Divider />
      <Stack spacing={0.5}>
        <Typography variant="h6">Транспорт {vehicle.label ?? vehicle.vehicle_id}</Typography>
        {vehicle.license_plate !== null && (
          <Typography color="text.secondary" variant="body2">
            {vehicle.license_plate}
          </Typography>
        )}
      </Stack>
      <Stack spacing={1}>
        <Field label="Маршрут" value={routeName} />
        <Field
          label="Скорость"
          value={vehicle.speed === null ? "—" : `${Math.round(vehicle.speed * 3.6)} км/ч`}
        />
        <Field label="Время позиции" value={timestamp} />
      </Stack>
      {vehicle.quality_flags.length > 0 && (
        <Stack direction="row" sx={{ flexWrap: "wrap", gap: 1 }}>
          {vehicle.quality_flags.map((flag) => (
            <Chip color="warning" key={flag} label={flag} size="small" variant="outlined" />
          ))}
        </Stack>
      )}
    </Stack>
  );
}
