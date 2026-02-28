"use strict";

const WebSocket = require("ws");
const { normalizeAccountType, normalizeInterval } = require("./constants");

class WsGateway {
    constructor(server, replayEngine) {
        this.replayEngine = replayEngine;
        this.server = server;
        this.clients = new Set();
        this.streamRefCounts = {
            SPOT: new Map(),
            FUTURES: new Map(),
        };
        this.wss = new WebSocket.Server({
            noServer: true,
        });
        this.bindUpgrade();
        this.bindWs();
        this.bindReplayEvents();
    }

    parseKlineStream(stream = "") {
        const raw = (stream || "").toString().trim();
        const match = /^([a-z0-9]+)@kline_(\d+[mhdwM])$/i.exec(raw);
        if (!match) {
            return null;
        }
        return {
            symbol: match[1].toUpperCase(),
            interval: normalizeInterval(match[2]),
            stream: raw.toLowerCase(),
        };
    }

    normalizeStreamName(stream = "") {
        const parsed = this.parseKlineStream(stream);
        if (parsed) {
            return `${parsed.symbol.toLowerCase()}@kline_${parsed.interval}`;
        }
        return (stream || "").toString().trim().toLowerCase();
    }

    incrementStreamRef(accountType = "SPOT", stream = "") {
        const normalizedAccountType = normalizeAccountType(accountType);
        const normalizedStream = this.normalizeStreamName(stream);
        if (!normalizedStream) {
            return;
        }
        const map = this.streamRefCounts[normalizedAccountType];
        const count = map.get(normalizedStream) || 0;
        map.set(normalizedStream, count + 1);
    }

    decrementStreamRef(accountType = "SPOT", stream = "") {
        const normalizedAccountType = normalizeAccountType(accountType);
        const normalizedStream = this.normalizeStreamName(stream);
        if (!normalizedStream) {
            return;
        }
        const map = this.streamRefCounts[normalizedAccountType];
        const count = map.get(normalizedStream) || 0;
        if (count <= 1) {
            map.delete(normalizedStream);
            const parsed = this.parseKlineStream(normalizedStream);
            if (parsed) {
                this.replayEngine.removeStream({
                    accountType: normalizedAccountType,
                    symbol: parsed.symbol,
                    interval: parsed.interval,
                });
            }
            return;
        }
        map.set(normalizedStream, count - 1);
    }

    cleanupClientSubscriptions(ws) {
        if (!ws?.subscriptions?.size) {
            return;
        }
        ws.subscriptions.forEach((stream) => {
            const accountType = ws.subscriptionAccountTypes?.get(stream) || ws.accountType || "SPOT";
            this.decrementStreamRef(accountType, stream);
        });
        ws.subscriptions.clear();
        ws.subscriptionAccountTypes?.clear();
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
            ws.subscriptionAccountTypes = new Map();
            ws.accountType = "SPOT";
            this.clients.add(ws);
            ws.send(JSON.stringify({ result: null, id: 0 }));

            ws.on("message", (raw) => {
                this.handleClientMessage(ws, raw);
            });
            ws.on("close", () => {
                this.cleanupClientSubscriptions(ws);
                this.clients.delete(ws);
            });
            ws.on("error", () => {
                this.cleanupClientSubscriptions(ws);
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
                const normalizedStream = this.normalizeStreamName(stream);
                if (!normalizedStream) {
                    return;
                }
                const previousAccountType = ws.subscriptionAccountTypes.get(normalizedStream);
                if (!ws.subscriptions.has(normalizedStream)) {
                    ws.subscriptions.add(normalizedStream);
                    this.incrementStreamRef(accountType, normalizedStream);
                } else if (previousAccountType && previousAccountType !== accountType) {
                    this.decrementStreamRef(previousAccountType, normalizedStream);
                    this.incrementStreamRef(accountType, normalizedStream);
                }
                ws.subscriptionAccountTypes.set(normalizedStream, accountType);

                const parsedStream = this.parseKlineStream(stream);
                if (parsedStream) {
                    this.replayEngine
                        .ensureStream({
                            accountType,
                            symbol: parsedStream.symbol,
                            interval: parsedStream.interval,
                            loadNow: true,
                        })
                        .catch(() => {});
                }
            });
            ws.send(JSON.stringify({ result: null, id: requestId }));
            return;
        }
        if (method === "UNSUBSCRIBE") {
            params.forEach((stream) => {
                const normalizedStream = this.normalizeStreamName(stream);
                if (!normalizedStream) {
                    return;
                }
                const streamAccountType = ws.subscriptionAccountTypes.get(normalizedStream) || accountType;
                ws.subscriptions.delete(normalizedStream);
                ws.subscriptionAccountTypes.delete(normalizedStream);
                this.decrementStreamRef(streamAccountType, normalizedStream);
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
            if (!ws.subscriptions?.has(streamName)) {
                return;
            }
            const streamAccountType = normalizeAccountType(ws.subscriptionAccountTypes?.get(streamName) || ws.accountType || "SPOT");
            if (streamAccountType !== normalized) {
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
