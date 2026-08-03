export interface Anomaly {
  ticker: string;
  window_start: string;
  anomaly_type: "volume_anomaly" | "regime_change";
  details: Record<string, unknown>;
  detected_at: string;
}

export interface StatusResponse {
  consumer_lag: {
    streaming: number;
    storage_sink: number;
  };
  dlq_depth: number;
  model_freshness: Record<string, string | null>;
  checked_at: string;
}

interface TradeTick {
  type: "trade";
  symbol: string;
  timestamp: string;
  price: number;
  size: number;
  exchange: string;
  trade_id: number;
  conditions: string[];
  tape: string;
}

interface QuoteTick {
  type: "quote";
  symbol: string;
  timestamp: string;
  bid_price: number;
  bid_size: number;
  bid_exchange: string;
  ask_price: number;
  ask_size: number;
  ask_exchange: string;
  conditions: string[];
  tape: string;
}

export type Tick = TradeTick | QuoteTick;
