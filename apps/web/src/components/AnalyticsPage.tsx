import { lazy, Suspense, useEffect, useMemo, useState } from "react";
import Alert from "@mui/material/Alert";
import Autocomplete from "@mui/material/Autocomplete";
import Box from "@mui/material/Box";
import Chip from "@mui/material/Chip";
import FormControl from "@mui/material/FormControl";
import InputLabel from "@mui/material/InputLabel";
import MenuItem from "@mui/material/MenuItem";
import Paper from "@mui/material/Paper";
import Select from "@mui/material/Select";
import Skeleton from "@mui/material/Skeleton";
import Stack from "@mui/material/Stack";
import Tab from "@mui/material/Tab";
import Table from "@mui/material/Table";
import TableBody from "@mui/material/TableBody";
import TableCell from "@mui/material/TableCell";
import TableContainer from "@mui/material/TableContainer";
import TableHead from "@mui/material/TableHead";
import TableRow from "@mui/material/TableRow";
import Tabs from "@mui/material/Tabs";
import TextField from "@mui/material/TextField";
import Typography from "@mui/material/Typography";

import { fetchAllRoutes, fetchAnalyticsOverview, fetchAnomalies, fetchClusters, fetchEtaComparison, fetchSegments } from "../api/client";
import type { AnalyticsOverview, EtaComparison, Route, RouteCluster, SegmentMetric, TransportAnomaly } from "../api/types";
import { routeLabel } from "../vehicles";

const SegmentMap = lazy(() => import("./SegmentMap").then(({ SegmentMap: component }) => ({ default: component })));

type View = "segments" | "eta" | "anomalies" | "clusters";
const anomalyLabels: Record<string, string> = { bunching: "Пачка", irregular_headway: "Аномальный интервал", stationary: "Длительная остановка", unusual_travel_time: "Нетипичное время" };
const seconds = (value: number | null) => value === null ? "—" : `${Math.round(value)} с`;
const percent = (value: number) => `${Math.round(value * 100)} %`;

export function AnalyticsPage() {
  const [routes, setRoutes] = useState<Route[]>([]);
  const [overview, setOverview] = useState<AnalyticsOverview | null>(null);
  const [routeId, setRouteId] = useState<string | null>(null);
  const [view, setView] = useState<View>("segments");
  const [dayType, setDayType] = useState("weekday");
  const [hour, setHour] = useState(8);
  const [rows, setRows] = useState<SegmentMetric[] | TransportAnomaly[] | EtaComparison[] | RouteCluster[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => { const controller = new AbortController(); Promise.all([fetchAllRoutes(controller.signal), fetchAnalyticsOverview(controller.signal)]).then(([nextRoutes, nextOverview]) => { setRoutes(nextRoutes); setOverview(nextOverview); setRouteId(nextRoutes[0]?.route_id ?? null); }).catch((reason: unknown) => { setError(reason instanceof Error ? reason.message : "Не удалось загрузить аналитику"); setLoading(false); }); return () => controller.abort(); }, []);
  useEffect(() => { const controller = new AbortController(); if (view !== "clusters" && routeId === null) return () => controller.abort(); const request = view === "segments" ? fetchSegments(routeId!, dayType, hour, controller.signal) : view === "eta" ? fetchEtaComparison(routeId!, controller.signal) : view === "anomalies" ? fetchAnomalies(routeId!, controller.signal) : fetchClusters(controller.signal); request.then(setRows).catch((reason: unknown) => { if (!controller.signal.aborted) setError(reason instanceof Error ? reason.message : "Не удалось загрузить аналитику"); }).finally(() => { if (!controller.signal.aborted) setLoading(false); }); return () => controller.abort(); }, [view, routeId, dayType, hour]);
  const routeNames = useMemo(() => new Map(routes.map((route) => [route.route_id, routeLabel(route)])), [routes]);
  const dataset = overview?.datasets.find(({ dataset }) => dataset === ({ segments: "segment_metrics", eta: "eta_comparison", anomalies: "anomalies", clusters: "route_clusters" }[view]));

  return <Box component="main" sx={{ height: "calc(100vh - 113px)", overflow: "auto" }}>
    <Tabs value={view} onChange={(_, value: View) => { setLoading(true); setError(null); setView(value); }} sx={{ borderBottom: 1, borderColor: "divider", px: 3 }}><Tab value="segments" label="Узкие участки" /><Tab value="eta" label="ETA" /><Tab value="anomalies" label="Аномалии" /><Tab value="clusters" label="Кластеры" /></Tabs>
    <Box sx={{ p: 3 }}>
    <Stack direction="row" spacing={2} sx={{ alignItems: "center", mb: 2 }}>
      {view !== "clusters" && <Autocomplete size="small" sx={{ width: 340 }} options={routes} value={routes.find((route) => route.route_id === routeId) ?? null} getOptionLabel={routeLabel} isOptionEqualToValue={(a, b) => a.route_id === b.route_id} onChange={(_, value) => { setLoading(true); setError(null); setRouteId(value?.route_id ?? null); }} renderInput={(params) => <TextField {...params} label="Маршрут" />} />}
      {view === "segments" && <><FormControl size="small" sx={{ width: 160 }}><InputLabel>Тип дня</InputLabel><Select label="Тип дня" value={dayType} onChange={(event) => { setLoading(true); setDayType(event.target.value); }}><MenuItem value="weekday">Будний</MenuItem><MenuItem value="weekend">Выходной</MenuItem></Select></FormControl><TextField size="small" label="Час" type="number" value={hour} onChange={(event) => { setLoading(true); setHour(Math.max(0, Math.min(23, Number(event.target.value)))); }} slotProps={{ htmlInput: { min: 0, max: 23 } }} sx={{ width: 100 }} /></>}
      {dataset && <Typography color="text.secondary" variant="body2">Период: {dataset.period_start} — {dataset.period_end}</Typography>}
    </Stack>
    {loading ? <Skeleton height={320} variant="rounded" /> : error ? <Alert severity="error">{error}</Alert> : rows.length === 0 ? <Alert severity="info">Для выбранного среза данных нет</Alert> : <AnalyticsTable view={view} rows={rows} routeNames={routeNames} />}
    </Box>
  </Box>;
}

function AnalyticsTable({ view, rows, routeNames }: { view: View; rows: SegmentMetric[] | TransportAnomaly[] | EtaComparison[] | RouteCluster[]; routeNames: Map<string, string> }) {
  if (view === "segments") { const segments = rows as SegmentMetric[]; return <><Suspense fallback={<Skeleton height={360} variant="rounded" />}><SegmentMap segments={segments} /></Suspense><DataTable headers={["Участок", "Скорость", "Медиана", "p90", "Покрытие", "Наблюдения"]}>{segments.map((row) => <TableRow key={`${row.shape_id}-${row.from_stop_index}`}><TableCell>{row.from_stop_name ?? row.from_stop_id} → {row.to_stop_name ?? row.to_stop_id}</TableCell><TableCell>{(row.speed_p50_mps * 3.6).toFixed(1)} км/ч</TableCell><TableCell>{seconds(row.travel_time_p50_s)}</TableCell><TableCell>{seconds(row.travel_time_p90_s)}</TableCell><TableCell>{percent(row.coverage)}</TableCell><TableCell>{row.qualified_samples}</TableCell></TableRow>)}</DataTable></>; }
  if (view === "eta") return <DataTable headers={["Горизонт", "Официальный MAE", "Baseline MAE", "Покрытие", "Сопоставлено"]}>{(rows as EtaComparison[]).map((row) => <TableRow key={row.horizon_bucket}><TableCell>{row.horizon_bucket}</TableCell><TableCell>{seconds(row.official_mae_s)}</TableCell><TableCell>{seconds(row.baseline_mae_s)}</TableCell><TableCell>{percent(row.baseline_coverage)}</TableCell><TableCell>{row.paired_forecasts} / {row.labeled_forecasts}</TableCell></TableRow>)}</DataTable>;
  if (view === "anomalies") return <DataTable headers={["Время", "Тип", "Транспорт", "Длительность", "Интервал"]}>{(rows as TransportAnomaly[]).map((row) => <TableRow key={row.anomaly_id}><TableCell>{new Date(row.started_at).toLocaleString("ru-RU")}</TableCell><TableCell><Chip size="small" label={anomalyLabels[row.anomaly_type] ?? row.anomaly_type} /></TableCell><TableCell>{row.vehicle_id ?? "—"}</TableCell><TableCell>{seconds(row.duration_s)}</TableCell><TableCell>{seconds(row.headway_s)}</TableCell></TableRow>)}</DataTable>;
  return <DataTable headers={["Маршрут", "Кластер", "Скорость", "Стабильность", "Покрытие", "Пачек/день"]}>{(rows as RouteCluster[]).map((row) => <TableRow key={row.route_id}><TableCell>{routeNames.get(row.route_id) ?? row.route_id}</TableCell><TableCell><Chip size="small" label={row.cluster_id} /></TableCell><TableCell>{(row.speed_p50_mps * 3.6).toFixed(1)} км/ч</TableCell><TableCell>{row.travel_variability.toFixed(2)}</TableCell><TableCell>{percent(row.coverage)}</TableCell><TableCell>{row.bunching_per_day.toFixed(1)}</TableCell></TableRow>)}</DataTable>;
}

function DataTable({ headers, children }: { headers: string[]; children: React.ReactNode }) { return <TableContainer component={Paper} elevation={0} sx={{ border: 1, borderColor: "divider" }}><Table stickyHeader size="small"><TableHead><TableRow>{headers.map((header) => <TableCell key={header}>{header}</TableCell>)}</TableRow></TableHead><TableBody>{children}</TableBody></Table></TableContainer>; }

