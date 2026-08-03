import { fetchAnomalies } from "../api";
import { usePolling } from "../hooks/usePolling";

const POLL_INTERVAL_MS = 5000;

export function AnomaliesTable() {
  const { data: anomalies, error } = usePolling(fetchAnomalies, POLL_INTERVAL_MS);

  return (
    <section className="panel">
      <h2>Detected Anomalies</h2>
      {error && <p className="error">Failed to load: {error}</p>}
      {!anomalies && !error && <p className="muted">Loading…</p>}
      {anomalies && anomalies.length === 0 && (
        <p className="muted">No anomalies detected yet.</p>
      )}
      {anomalies && anomalies.length > 0 && (
        <table>
          <thead>
            <tr>
              <th>Ticker</th>
              <th>Type</th>
              <th>Window Start</th>
              <th>Detected At</th>
            </tr>
          </thead>
          <tbody>
            {anomalies.map((a) => (
              <tr key={`${a.ticker}-${a.window_start}-${a.anomaly_type}`}>
                <td>{a.ticker}</td>
                <td>
                  <span className={`badge badge-${a.anomaly_type}`}>
                    {a.anomaly_type === "volume_anomaly" ? "Volume Spike" : "Regime Change"}
                  </span>
                </td>
                <td>{new Date(a.window_start).toLocaleString()}</td>
                <td>{new Date(a.detected_at).toLocaleString()}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </section>
  );
}
