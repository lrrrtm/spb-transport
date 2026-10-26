import Alert from "@mui/material/Alert";
import Button from "@mui/material/Button";
import Skeleton from "@mui/material/Skeleton";
import Stack from "@mui/material/Stack";

interface StatusViewProps {
  loading: boolean;
  error: string | null;
  empty: boolean;
  onRetry: () => void;
}

export function StatusView({ loading, error, empty, onRetry }: StatusViewProps) {
  if (loading) {
    return (
      <Stack spacing={1} aria-label="Загрузка транспорта">
        <Skeleton variant="rounded" height={40} />
        <Skeleton variant="text" width="55%" />
      </Stack>
    );
  }
  if (error !== null) {
    return (
      <Alert
        severity="error"
        action={
          <Button color="inherit" size="small" onClick={onRetry}>
            Повторить
          </Button>
        }
      >
        {error}
      </Alert>
    );
  }
  if (empty) return <Alert severity="info">Актуальный транспорт не найден</Alert>;
  return null;
}
