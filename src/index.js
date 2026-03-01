"use strict";

const http = require("http");
const express = require("express");
const cors = require("cors");
const dotenv = require("dotenv");
const BinanceClient = require("./binance-client");
const CandleDb = require("./candle-db");
const CandleStore = require("./candle-store");
const ReplayEngine = require("./replay-engine");
const WsGateway = require("./ws-gateway");
const createRoutes = require("./routes");

dotenv.config();

const app = express();
app.use(cors({ origin: "*" }));
app.use(express.json({ limit: "1mb" }));

const binanceClient = new BinanceClient();
const persistenceEnabled = (process.env.BACKTEST_CANDLE_DB_DISABLED || "").toString().trim() !== "1";
const candleDb = persistenceEnabled ? new CandleDb() : null;
const candleStore = new CandleStore(binanceClient, candleDb);
const replayEngine = new ReplayEngine(candleStore);

if (candleDb) {
    // eslint-disable-next-line no-console
    console.log(`[backtesting] persistent candle DB enabled (${candleDb.filePath})`);
} else {
    // eslint-disable-next-line no-console
    console.log("[backtesting] persistent candle DB disabled");
}

function formatDuration(ms = 0) {
    const safe = Math.max(0, Math.floor(ms || 0));
    const totalSeconds = Math.floor(safe / 1000);
    const hours = Math.floor(totalSeconds / 3600);
    const minutes = Math.floor((totalSeconds % 3600) / 60);
    const seconds = totalSeconds % 60;
    return `${hours.toString().padStart(2, "0")}:${minutes.toString().padStart(2, "0")}:${seconds
        .toString()
        .padStart(2, "0")}`;
}

function formatReplayTime(ms = 0) {
    const safe = parseInt(ms, 10);
    if (!Number.isFinite(safe) || safe <= 0) {
        return "-";
    }
    return new Date(safe).toISOString();
}

function logProgress(event = {}) {
    const cursor = event?.replayCursor || {};
    const streams = event?.streams || {};
    const symbols = Array.isArray(streams?.symbols) ? streams.symbols : [];
    const intervals = Array.isArray(streams?.intervals) ? streams.intervals : [];
    const sampleSymbols = symbols.slice(0, 6).join(",");
    const symbolDisplay = symbols.length > 6 ? `${sampleSymbols},+${symbols.length - 6}` : sampleSymbols;
    const streamDisplay = streams?.count > 0 ? `${streams.count} [${symbolDisplay || "-"}]` : "0";
    // eslint-disable-next-line no-console
    console.log(
        `[backtesting][${event?.accountType || "SPOT"}][${(event?.reason || "tick").toUpperCase()}] ` +
            `${Number(event?.progressPct || 0).toFixed(2)}% ` +
            `(${cursor?.index || 0}/${cursor?.total || 0}) ` +
            `replayAt=${formatReplayTime(cursor?.openTime)} ` +
            `streams=${streamDisplay} ` +
            `intervals=${intervals.join(",") || "-"} ` +
            `speed=${event?.speedMultiplier || "-"}x ` +
            `tick=${event?.tickMs || "-"}ms ` +
            `elapsed=${formatDuration(event?.elapsedMs || 0)} ` +
            `eta=${formatDuration(event?.etaMs || 0)}`
    );
}

replayEngine.on("started", ({ accountType, status }) => {
    // eslint-disable-next-line no-console
    console.log(
        `[backtesting][${accountType}] session started: lookbackDays=${status?.lookbackDays}, speed=${status?.speedMultiplier}x, tick=${status?.tickMs}ms`
    );
});

replayEngine.on("stopped", ({ accountType, status }) => {
    // eslint-disable-next-line no-console
    console.log(
        `[backtesting][${accountType}] session stopped: cursor=${status?.replayCursor?.index || 0}/${status?.replayCursor?.total || 0}, running=${status?.running ? "yes" : "no"}`
    );
});

replayEngine.on("progress", (event) => {
    logProgress(event || {});
});

app.use(
    "/",
    createRoutes({
        binanceClient,
        candleStore,
        replayEngine,
    })
);

const server = http.createServer(app);
new WsGateway(server, replayEngine);

const port = parseInt(process.env.PORT || "3900", 10);
server.listen(port, () => {
    // eslint-disable-next-line no-console
    console.log(`[backtesting] listening on :${port}`);
});

function shutdown() {
    try {
        if (candleDb) {
            candleDb.close();
        }
    } catch (_e) {}
    process.exit(0);
}

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
