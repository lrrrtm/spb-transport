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
