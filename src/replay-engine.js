"use strict";

const { EventEmitter } = require("events");
const { ACCOUNT_TYPES, DEFAULTS, normalizeAccountType } = require("./constants");
const { nowMs, toNumber, uniq } = require("./utils");

class ReplayEngine extends EventEmitter {
    constructor(candleStore) {
        super();
        this.candleStore = candleStore;
        this.sessions = {
            SPOT: null,
            FUTURES: null,
        };
    }

    createEmptyStatus(accountType = ACCOUNT_TYPES.SPOT) {
        const normalized = normalizeAccountType(accountType);
        return {
            accountType: normalized,
            running: false,
            interval: DEFAULTS.interval,
            lookbackDays: DEFAULTS.lookbackDays,
            speedMultiplier: DEFAULTS.speedMultiplier,
            symbols: [],
            startedAt: null,
            endedAt: null,
            tickMs: Math.max(DEFAULTS.stepMsFloor, Math.floor(60000 / DEFAULTS.speedMultiplier)),
            replayCursor: {
                index: 0,
                total: 0,
                openTime: null,
            },
        };
    }

    getStatus(accountType = ACCOUNT_TYPES.SPOT) {
        const normalized = normalizeAccountType(accountType);
        const session = this.sessions[normalized];
        if (!session) {
            return this.createEmptyStatus(normalized);
        }
        return {
            accountType: normalized,
            running: !!session.running,
            interval: session.interval,
            lookbackDays: session.lookbackDays,
            speedMultiplier: session.speedMultiplier,
            symbols: session.symbols.slice(),
            startedAt: session.startedAt,
            endedAt: session.endedAt,
            tickMs: session.tickMs,
            replayCursor: {
                index: session.timelineIndex,
                total: session.timeline.length,
                openTime: session.timeline[session.timelineIndex] || null,
            },
        };
    }

    getAllStatuses() {
        return {
            SPOT: this.getStatus(ACCOUNT_TYPES.SPOT),
            FUTURES: this.getStatus(ACCOUNT_TYPES.FUTURES),
        };
    }

    getSession(accountType = ACCOUNT_TYPES.SPOT) {
        const normalized = normalizeAccountType(accountType);
        return this.sessions[normalized] || null;
    }

    getSessionVisibleEndTimeMs(accountType = ACCOUNT_TYPES.SPOT, symbol = "") {
        const session = this.getSession(accountType);
        if (!session) {
            return null;
        }
        const upperSymbol = (symbol || "").toString().toUpperCase();
        if (upperSymbol && !session.symbols.includes(upperSymbol)) {
            return null;
        }
        if (!Array.isArray(session.timeline) || !session.timeline.length) {
            return null;
        }
        if (!session.running) {
            return session.endTime || null;
        }
        if (session.timelineIndex <= 0) {
            return -1;
        }

        const emittedIndex = Math.max(0, Math.min(session.timelineIndex - 1, session.timeline.length - 1));
        const emittedOpenTime = session.timeline[emittedIndex];
        if (!Number.isFinite(emittedOpenTime)) {
            return -1;
        }

        if (upperSymbol) {
            const symbolCandle = session.candleMapBySymbol?.[upperSymbol]?.get(emittedOpenTime) || null;
            if (symbolCandle?.closeTime) {
                return symbolCandle.closeTime;
            }
        }
        return emittedOpenTime + 59_999;
    }

    getLatestPrice(symbol = "", accountType = ACCOUNT_TYPES.SPOT) {
        const normalized = normalizeAccountType(accountType);
        const upperSymbol = (symbol || "").toUpperCase();
        const session = this.sessions[normalized];
        if (!session?.latestBySymbol?.[upperSymbol]) {
            return null;
        }
        return session.latestBySymbol[upperSymbol];
    }

    buildWsPayload(candle = null, symbol = "") {
        if (!candle) {
            return null;
        }
        return {
            e: "kline",
            E: nowMs(),
            s: symbol,
            k: {
                t: candle.openTime,
                T: candle.closeTime,
                s: symbol,
                i: "1m",
                f: 0,
                L: 0,
                o: `${candle.open}`,
                c: `${candle.close}`,
                h: `${candle.high}`,
                l: `${candle.low}`,
                v: `${candle.volume}`,
                n: candle.trades || 1,
                x: true,
                q: `${candle.quoteAssetVolume || 0}`,
                V: `${candle.takerBuyBaseVolume || 0}`,
                Q: `${candle.takerBuyQuoteVolume || 0}`,
                B: "0",
            },
        };
    }

    async buildSession(options = {}) {
        const accountType = normalizeAccountType(options.accountType);
        const symbols = uniq(
            (Array.isArray(options.symbols) ? options.symbols : [])
                .map((symbol) => (symbol || "").toString().toUpperCase())
                .filter((symbol) => !!symbol)
        );
        if (!symbols.length) {
            throw new Error("At least one symbol is required to start replay.");
        }
        const interval = DEFAULTS.interval;
        const speedMultiplier = Math.max(1, Math.floor(toNumber(options.speedMultiplier, DEFAULTS.speedMultiplier)));
        const lookbackDays = Math.max(1, Math.floor(toNumber(options.lookbackDays, DEFAULTS.lookbackDays)));
        const endTime = toNumber(options.endTime, nowMs()) || nowMs();
        const startTime = toNumber(options.startTime, endTime - lookbackDays * 24 * 60 * 60 * 1000);
        const tickMs = Math.max(DEFAULTS.stepMsFloor, Math.floor(60000 / speedMultiplier));

        const candlesBySymbol = {};
        const candleMapBySymbol = {};
        const timelineSet = new Set();
        for (const symbol of symbols) {
            const candles = await this.candleStore.ensureRange(symbol, accountType, interval, {
                startTime,
                endTime,
                lookbackDays,
            });
            candlesBySymbol[symbol] = (candles || []).filter((candle) => candle.openTime >= startTime && candle.openTime <= endTime);
            candleMapBySymbol[symbol] = new Map();
            candlesBySymbol[symbol].forEach((candle) => {
                timelineSet.add(candle.openTime);
                candleMapBySymbol[symbol].set(candle.openTime, candle);
            });
        }
        const timeline = [...timelineSet].sort((a, b) => a - b);
        if (!timeline.length) {
            throw new Error("No candles available in selected replay window.");
        }

        return {
            running: false,
            accountType,
            interval,
            symbols,
            speedMultiplier,
            lookbackDays,
            tickMs,
            startTime,
            endTime,
            timeline,
            timelineIndex: 0,
            candlesBySymbol,
            candleMapBySymbol,
            latestBySymbol: {},
            timer: null,
            startedAt: null,
            endedAt: null,
        };
    }

    stop(accountType = ACCOUNT_TYPES.SPOT) {
        const normalized = normalizeAccountType(accountType);
        const session = this.sessions[normalized];
        if (!session) {
            return this.createEmptyStatus(normalized);
        }
        if (session.timer) {
            clearInterval(session.timer);
            session.timer = null;
        }
        session.running = false;
        session.endedAt = nowMs();
        this.emit("stopped", { accountType: normalized, status: this.getStatus(normalized) });
        return this.getStatus(normalized);
    }

    async start(options = {}) {
        const normalized = normalizeAccountType(options.accountType);
        this.stop(normalized);
        const session = await this.buildSession(options);
        session.running = true;
        session.startedAt = nowMs();
        session.endedAt = null;
        this.sessions[normalized] = session;
        this.emit("started", { accountType: normalized, status: this.getStatus(normalized) });

        const emitTick = () => {
            if (!session.running) {
                return;
            }
            const timelineOpenTime = session.timeline[session.timelineIndex];
            if (timelineOpenTime === undefined) {
                this.stop(normalized);
                return;
            }
            for (const symbol of session.symbols) {
                const candle = session.candleMapBySymbol?.[symbol]?.get(timelineOpenTime) || null;
                if (!candle) {
                    continue;
                }
                session.latestBySymbol[symbol] = {
                    symbol,
                    openTime: candle.openTime,
                    closeTime: candle.closeTime,
                    price: candle.close,
                    bidPrice: candle.close,
                    askPrice: candle.close,
                };
                const payload = this.buildWsPayload(candle, symbol);
                if (payload) {
                    this.emit("kline", {
                        accountType: normalized,
                        symbol,
                        interval: session.interval,
                        payload,
                    });
                }
            }
            session.timelineIndex += 1;
            if (session.timelineIndex >= session.timeline.length) {
                this.stop(normalized);
            }
        };

        emitTick();
        session.timer = setInterval(emitTick, session.tickMs);
        return this.getStatus(normalized);
    }
}

module.exports = ReplayEngine;
