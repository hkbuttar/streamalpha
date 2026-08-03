import { useTickFeed } from "../hooks/useTickFeed";
import type { Tick } from "../types";

function isTrade(tick: Tick): tick is Extract<Tick, { type: "trade" }> {
  return tick.type === "trade";
}

export function TickFeed() {
  const { ticks, status } = useTickFeed();

  return (
    <section className="panel">
      <h2>
        Live Ticks <span className={`connection-dot connection-${status}`} title={status} />
      </h2>
      {ticks.length === 0 && <p className="muted">Waiting for ticks…</p>}
      <ul className="tick-list">
        {ticks.map((tick, i) => (
          <li key={i} className="tick-row">
            <span className="tick-symbol">{tick.symbol}</span>
            {isTrade(tick) ? (
              <span className="tick-detail">
                ${tick.price.toFixed(2)} × {tick.size}
              </span>
            ) : (
              <span className="tick-detail">
                bid ${tick.bid_price.toFixed(2)} / ask ${tick.ask_price.toFixed(2)}
              </span>
            )}
            <span className="tick-time">{new Date(tick.timestamp).toLocaleTimeString()}</span>
          </li>
        ))}
      </ul>
    </section>
  );
}
