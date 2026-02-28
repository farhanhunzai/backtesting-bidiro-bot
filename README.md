# Backtesting Binance Replay Engine

High-speed backtesting service that exposes Binance-compatible REST and WebSocket market-data interfaces, then replays historical candles as a fast-forward stream.

This project is designed so any trading client that already talks to Binance-style endpoints can switch to this engine for deterministic historical replay.

## What It Does

- Fetches historical candles from Binance (Spot or Futures) for allowed intervals.
- Caches candle windows in memory for fast repeated access.
- Creates replay sessions per account type (`SPOT`, `FUTURES`).
- Emits candle updates over WebSocket in accelerated time.
- Serves REST `klines` and `bookTicker` data that stay synchronized with replay progress.

## Key Behavior

- Supports multiple intervals (`1m`, `3m`, `5m`, `15m`, `30m`, `1h`, `2h`, `4h`, `6h`, `8h`, `12h`, `1d`, `3d`, `1w`, `1M` by default).
- Stream data is fetched lazily per `symbol + interval` when that stream is subscribed/requested.
- Replay speed is controlled by `speedMultiplier`.
- `klines` output is replay-aware: it will not expose candles ahead of the current replay cursor.
- `bookTicker` prefers replay price while a session is running, then falls back to latest cached/live price.
- Spot and Futures sessions are isolated and can be controlled independently.

## Architecture

- `src/index.js`: boots Express + HTTP server + WebSocket gateway.
- `src/routes.js`: REST APIs (Binance-compatible routes + replay control routes).
- `src/candle-store.js`: candle fetching, caching, slicing, and response formatting.
- `src/replay-engine.js`: session lifecycle, timeline replay, and event emission.
- `src/ws-gateway.js`: Binance-style WS subscribe/unsubscribe stream handling.
- `src/binance-client.js`: upstream Binance REST calls.

## Replay Session Lifecycle

1. Client starts a session with `lookbackDays` and `speedMultiplier`.
2. Clients subscribe to `symbol@kline_interval` streams (or request interval klines through REST).
3. Engine fetches only the needed `symbol + interval` data and caches it.
4. On each replay tick:
   - Advances replay cursor.
   - Emits `kline` events for active streams that have a candle at that cursor.
   - Updates latest replay prices used by `bookTicker`.
5. Session auto-stops at timeline end or can be stopped manually.

## APIs

### Health

- `GET /health`
  - Returns service time and session status for both account types.

### Binance-Compatible REST

- `GET /api/v3/ping`
- `GET /api/v3/time`
- `GET /api/v3/exchangeInfo`
- `GET /api/v3/ticker/bookTicker`
- `GET /api/v3/klines`
- `GET /fapi/v1/time`
- `GET /fapi/v1/exchangeInfo`
- `GET /fapi/v1/ticker/bookTicker`
- `GET /fapi/v1/klines`

Notes:
- `klines` requires `symbol` and supports `startTime`, `endTime`, `limit`.
- `interval` must be one of the allowed intervals.
- `limit` max is `1500`.

### Replay Control

- `GET /backtest/session/status?accountType=SPOT|FUTURES`
- `POST /backtest/session/start`
- `POST /backtest/session/stop`

`start` request body:

```json
{
  "accountType": "FUTURES",
  "lookbackDays": 30,
  "speedMultiplier": 600
}
```

If `BACKTEST_CONTROL_TOKEN` is configured, include:

- Header: `x-backtest-token: <token>`

## WebSocket

Endpoint:

- `ws://localhost:3900/ws`

Subscription protocol (Binance-style):

- Method: `SUBSCRIBE`
- Method: `UNSUBSCRIBE`
- Stream format: `<symbol>@kline_<interval>` (example: `btcusdt@kline_5m`)

Example subscribe payload:

```json
{
  "method": "SUBSCRIBE",
  "params": ["btcusdt@kline_1m", "ethusdt@kline_15m"],
  "id": 1,
  "accountType": "FUTURES"
}
```

Kline event payload format follows Binance-style `kline` message shape (`e`, `E`, `s`, `k`...).

## Configuration

Environment variables:

- `PORT` default: `3900`
- `BACKTEST_BINANCE_SPOT_API` default: `https://api.binance.com/api/v3`
- `BACKTEST_BINANCE_FUTURES_API` default: `https://fapi.binance.com/fapi/v1`
- `BACKTEST_BINANCE_TIMEOUT_MS` default: `15000`
- `BACKTEST_ALLOWED_INTERVALS` optional comma-separated override for allowed intervals
- `BACKTEST_CONTROL_TOKEN` optional, secures `/backtest/session/*`

## Run Locally

```bash
npm install
npm run start
```

Dev mode:

```bash
npm run dev
```

Service will start on `http://localhost:3900`.

## Quick Start Example

1. Start service.
2. Start a replay session:

```bash
curl -X POST http://localhost:3900/backtest/session/start \
  -H "Content-Type: application/json" \
  -d "{\"accountType\":\"FUTURES\",\"lookbackDays\":30,\"speedMultiplier\":600}"
```

3. Subscribe to a replay stream:

```json
{
  "method": "SUBSCRIBE",
  "params": ["btcusdt@kline_5m"],
  "id": 1,
  "accountType": "FUTURES"
}
```

4. Check status:

```bash
curl "http://localhost:3900/backtest/session/status?accountType=FUTURES"
```

5. Request klines (replay-aware window):

```bash
curl "http://localhost:3900/fapi/v1/klines?symbol=BTCUSDT&interval=5m&limit=200"
```

## Troubleshooting

- Session starts but no movement:
  - Ensure at least one stream is subscribed (or call klines for a symbol/interval) so data loading starts.
  - Check `/health` and verify `running: true` and replay cursor advancing.
- `klines` returns empty during replay:
  - This is expected before that stream's replay cursor reaches the requested time window.
- WS client receives no candles:
  - Confirm stream is subscribed with exact pattern `symbol@kline_<interval>`.
  - Confirm `accountType` matches the running session.
