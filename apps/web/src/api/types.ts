export interface VehiclePosition {
  vehicle_id: string;
  route_id: string | null;
  event_id: string | null;
  source_timestamp: string;
  collected_at: string | null;
  latitude: number;
  longitude: number;
  bearing: number | null;
  speed: number | null;
  label: string | null;
  license_plate: string | null;
  quality_flags: string[];
}

export interface VehiclePage {
  items: VehiclePosition[];
  next_cursor: string | null;
}

export interface Route {
  route_id: string;
  agency_id: string | null;
  short_name: string | null;
  long_name: string | null;
  route_type: number | null;
}

export interface RoutePage {
  items: Route[];
  next_cursor: string | null;
  static_feed_version: string;
}

export interface Readiness {
  status: string;
  cassandra: string;
  static_feed_version: string | null;
  detail: string | null;
}

export interface AnalyticsDataset { dataset: string; algorithm_version: string; model_id: string | null; static_feed_version: string | null; period_start: string; period_end: string; training_start: string | null; training_end: string | null; updated_at: string }
export interface AnalyticsOverview { datasets: AnalyticsDataset[] }
export interface SegmentMetric { shape_id: string; direction_id: number; from_stop_id: string; to_stop_id: string; from_stop_index: number; segment_length_m: number; qualified_samples: number; coverage: number; travel_time_p50_s: number; travel_time_p90_s: number; speed_p50_mps: number; stop_delay_share: number; peak_offpeak_ratio: number; enough_data: boolean; from_stop_name: string | null; to_stop_name: string | null; from_latitude: number | null; from_longitude: number | null; to_latitude: number | null; to_longitude: number | null }
export interface TransportAnomaly { started_at: string; anomaly_id: string; anomaly_type: string; vehicle_id: string | null; related_vehicle_id: string | null; stop_id: string | null; ended_at: string | null; duration_s: number | null; distance_gap_m: number | null; headway_s: number | null }
export interface EtaComparison { horizon_bucket: string; labeled_forecasts: number; paired_forecasts: number; baseline_coverage: number; official_mae_s: number | null; baseline_mae_s: number | null }
export interface RouteCluster { route_id: string; cluster_id: number; distance: number; qualified_samples: number; speed_p50_mps: number; travel_variability: number; peak_offpeak_ratio: number; stop_delay_share: number; coverage: number; bunching_per_day: number; irregular_headways_per_day: number; stationary_per_day: number }
