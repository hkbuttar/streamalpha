import type { Anomaly, StatusResponse } from "./types";

// Defaults to the backend's default `uvicorn backend.main:app` port (see
// streamalpha's README Setup & Usage). Override with VITE_API_BASE_URL if
// the backend is running elsewhere.
const API_BASE_URL: string =
  (import.meta.env.VITE_API_BASE_URL as string | undefined) ?? "http://localhost:8000";

export function wsTicksUrl(): string {
  return `${API_BASE_URL.replace(/^http/, "ws")}/ws/ticks`;
}

async function getJson<T>(path: string): Promise<T> {
  const response = await fetch(`${API_BASE_URL}${path}`);
  if (!response.ok) {
    throw new Error(`${path} returned ${response.status}`);
  }
  return (await response.json()) as T;
}

export function fetchAnomalies(limit = 50) {
  return getJson<Anomaly[]>(`/anomalies?limit=${limit}`);
}

export function fetchStatus() {
  return getJson<StatusResponse>(`/status`);
}
