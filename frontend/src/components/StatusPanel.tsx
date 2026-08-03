import { fetchStatus } from "../api";
import { usePolling } from "../hooks/usePolling";

const POLL_INTERVAL_MS = 5000;

export function StatusPanel() {
  const { data: status, error } = usePolling(fetchStatus, POLL_INTERVAL_MS);

  return (
    <section className="panel">
      <h2>Pipeline Health</h2>
      {error && <p className="error">Failed to load: {error}</p>}
      {!status && !error && <p className="muted">Loading…</p>}
      {status && (
        <div className="status-grid">
          <div className="status-item">
            <span className="status-label">Streaming consumer lag</span>
            <span className="status-value">{status.consumer_lag.streaming.toLocaleString()}</span>
          </div>
          <div className="status-item">
            <span className="status-label">Storage sink lag</span>
            <span className="status-value">
              {status.consumer_lag.storage_sink.toLocaleString()}
            </span>
          </div>
          <div className="status-item">
            <span className="status-label">DLQ depth</span>
            <span className="status-value">{status.dlq_depth.toLocaleString()}</span>
          </div>
          <div className="status-item">
            <span className="status-label">Tickers tracked</span>
            <span className="status-value">
              {Object.keys(status.model_freshness).length.toLocaleString()}
            </span>
          </div>
        </div>
      )}
    </section>
  );
}
