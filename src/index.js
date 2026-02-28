"use strict";

const http = require("http");
const express = require("express");
const cors = require("cors");
const dotenv = require("dotenv");
const BinanceClient = require("./binance-client");
const CandleStore = require("./candle-store");
const ReplayEngine = require("./replay-engine");
const WsGateway = require("./ws-gateway");
const createRoutes = require("./routes");

dotenv.config();

const app = express();
app.use(cors({ origin: "*" }));
app.use(express.json({ limit: "1mb" }));

const binanceClient = new BinanceClient();
const candleStore = new CandleStore(binanceClient);
const replayEngine = new ReplayEngine(candleStore);

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
