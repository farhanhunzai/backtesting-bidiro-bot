"use strict";

const WebSocket = require("ws");
const { normalizeAccountType } = require("./constants");

class WsGateway {
    constructor(server, replayEngine) {
        this.replayEngine = replayEngine;
        this.server = server;
        this.clients = new Set();
        this.wss = new WebSocket.Server({
            noServer: true,
        });
        this.bindUpgrade();
        this.bindWs();
        this.bindReplayEvents();
    }

    bindUpgrade() {
        this.server.on("upgrade", (request, socket, head) => {
            const url = request.url || "";
            if (!url.startsWith("/ws")) {
                socket.destroy();
                return;
            }
            this.wss.handleUpgrade(request, socket, head, (ws) => {
                this.wss.emit("connection", ws, request);
            });
        });
    }

    bindWs() {
        this.wss.on("connection", (ws) => {
            ws.subscriptions = new Set();
            ws.accountType = "SPOT";
            this.clients.add(ws);
            ws.send(JSON.stringify({ result: null, id: 0 }));

            ws.on("message", (raw) => {
                this.handleClientMessage(ws, raw);
            });
            ws.on("close", () => {
                this.clients.delete(ws);
            });
            ws.on("error", () => {
                this.clients.delete(ws);
            });
        });
    }

    handleClientMessage(ws, raw) {
        let parsed = null;
        try {
            parsed = JSON.parse(raw?.toString() || "{}");
        } catch (e) {
            return;
        }
        const method = (parsed?.method || "").toString().toUpperCase();
        const params = Array.isArray(parsed?.params) ? parsed.params : [];
        const requestId = parsed?.id || 1;
        const accountType = normalizeAccountType(parsed?.accountType || ws.accountType || "SPOT");
        ws.accountType = accountType;
        if (method === "SUBSCRIBE") {
            params.forEach((stream) => {
                if (stream) {
                    ws.subscriptions.add(stream.toString().toLowerCase());
                }
            });
            ws.send(JSON.stringify({ result: null, id: requestId }));
            return;
        }
        if (method === "UNSUBSCRIBE") {
            params.forEach((stream) => {
                ws.subscriptions.delete((stream || "").toString().toLowerCase());
            });
            ws.send(JSON.stringify({ result: null, id: requestId }));
            return;
        }
    }

    sendByStream(accountType = "SPOT", stream = "", payload = {}) {
        const normalized = normalizeAccountType(accountType);
        const streamName = (stream || "").toString().toLowerCase();
        this.clients.forEach((ws) => {
            if (!ws || ws.readyState !== WebSocket.OPEN) {
                return;
            }
            if (normalizeAccountType(ws.accountType || "SPOT") !== normalized) {
                return;
            }
            if (!ws.subscriptions?.has(streamName)) {
                return;
            }
            ws.send(JSON.stringify(payload));
        });
    }

    bindReplayEvents() {
        this.replayEngine.on("kline", (event) => {
            const stream = `${event.symbol.toLowerCase()}@kline_${event.interval}`;
            this.sendByStream(event.accountType, stream, event.payload || {});
        });
    }
}

module.exports = WsGateway;
