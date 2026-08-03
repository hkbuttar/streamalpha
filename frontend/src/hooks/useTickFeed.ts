import { useEffect, useRef, useState } from "react";
import { wsTicksUrl } from "../api";
import type { Tick } from "../types";

const MAX_TICKS = 50;
const INITIAL_RECONNECT_DELAY_MS = 1000;
const MAX_RECONNECT_DELAY_MS = 15000;

export type ConnectionStatus = "connecting" | "open" | "closed";

// Reconnects with exponential backoff on drop, mirroring (in miniature)
// the same idea as ingestion/run.py's own backoff loop -- a dropped
// connection here should retry, not leave the dashboard silently stale.
export function useTickFeed(): { ticks: Tick[]; status: ConnectionStatus } {
  const [ticks, setTicks] = useState<Tick[]>([]);
  const [status, setStatus] = useState<ConnectionStatus>("connecting");
  const reconnectDelay = useRef(INITIAL_RECONNECT_DELAY_MS);

  useEffect(() => {
    let socket: WebSocket | null = null;
    let reconnectTimer: number | undefined;
    let cancelled = false;

    function connect() {
      if (cancelled) return;
      setStatus("connecting");
      socket = new WebSocket(wsTicksUrl());

      socket.onopen = () => {
        reconnectDelay.current = INITIAL_RECONNECT_DELAY_MS;
        setStatus("open");
      };

      socket.onmessage = (event: MessageEvent<string>) => {
        try {
          const tick = JSON.parse(event.data) as Tick;
          setTicks((prev) => [tick, ...prev].slice(0, MAX_TICKS));
        } catch {
          // Read-only passthrough on the backend (backend/main.py's
          // ws_ticks isn't schema-validated) -- drop anything that
          // doesn't parse rather than crash the feed.
        }
      };

      socket.onclose = () => {
        if (cancelled) return;
        setStatus("closed");
        reconnectTimer = window.setTimeout(connect, reconnectDelay.current);
        reconnectDelay.current = Math.min(reconnectDelay.current * 2, MAX_RECONNECT_DELAY_MS);
      };

      socket.onerror = () => {
        socket?.close();
      };
    }

    connect();

    return () => {
      cancelled = true;
      window.clearTimeout(reconnectTimer);
      socket?.close();
    };
  }, []);

  return { ticks, status };
}
