# backtesting.bidiro.online

Separate high-speed backtesting service that emulates Binance market data endpoints for 1m candles.

## Features

- REST endpoints compatible with core Binance data routes:
  - `GET /api/v3/ping`
  - `GET /api/v3/time`
  - `GET /api/v3/exchangeInfo`
  - `GET /api/v3/ticker/bookTicker`
  - `GET /api/v3/klines`
  - `GET /fapi/v1/time`
  - `GET /fapi/v1/exchangeInfo`
  - `GET /fapi/v1/ticker/bookTicker`
  - `GET /fapi/v1/klines`
- WebSocket replay endpoint at `/ws` with Binance-style `SUBSCRIBE` / `UNSUBSCRIBE` for streams like `btcusdt@kline_1m`.
- Control API for replay sessions:
  - `GET /backtest/session/status`
  - `POST /backtest/session/start`
  - `POST /backtest/session/stop`
- Fetches and caches the last 30 days of 1m data by default and replays at accelerated speed.

## Run

```bash
npm install
npm run start
```

Service default port: `3900`.

## Environment Variables

- `PORT` (default: `3900`)
- `BACKTEST_BINANCE_SPOT_API` (default: `https://api.binance.com/api/v3`)
- `BACKTEST_BINANCE_FUTURES_API` (default: `https://fapi.binance.com/fapi/v1`)
- `BACKTEST_BINANCE_TIMEOUT_MS` (default: `15000`)
- `BACKTEST_CONTROL_TOKEN` (optional; protects `/backtest/session/*` endpoints via `x-backtest-token` header)

## Start Session Example

```bash
curl -X POST http://localhost:3900/backtest/session/start \
  -H "Content-Type: application/json" \
  -d "{\"accountType\":\"FUTURES\",\"symbols\":[\"BTCUSDT\",\"ETHUSDT\"],\"lookbackDays\":30,\"speedMultiplier\":600}"
```
