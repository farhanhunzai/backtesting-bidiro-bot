"use strict";

const { EventEmitter } = require("events");
const {
    ACCOUNT_TYPES,
    DEFAULTS,
    normalizeAccountType,
    normalizeInterval,
    isSupportedInterval,
    intervalToMs,
} = require("./constants");
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

    buildStreamKey(symbol = "", interval = DEFAULTS.interval) {
        const upperSymbol = (symbol || "").toString().trim().toUpperCase();
        const normalizedInterval = normalizeInterval(interval || DEFAULTS.interval);
        return `${upperSymbol}:${normalizedInterval}`;
    }

    getActiveStreamStates(session = null) {
        if (!session?.streams) {
            return [];
        }
        return Object.values(session.streams).filter((state) => !!state?.active);
    }

    createSession(options = {}, existing = null) {
        const accountType = normalizeAccountType(options.accountType || existing?.accountType || ACCOUNT_TYPES.SPOT);
        const speedMultiplier = Math.max(1, Math.floor(toNumber(options.speedMultiplier, existing?.speedMultiplier || DEFAULTS.speedMultiplier)));
        const lookbackDays = Math.max(1, Math.floor(toNumber(options.lookbackDays, existing?.lookbackDays || DEFAULTS.lookbackDays)));
        const endTime = toNumber(options.endTime, nowMs()) || nowMs();
        const startTime = toNumber(options.startTime, endTime - lookbackDays * 24 * 60 * 60 * 1000);
        const baseInterval = DEFAULTS.interval;
        const baseIntervalMs = Math.max(60 * 1000, intervalToMs(baseInterval));
        const tickMs = Math.max(DEFAULTS.stepMsFloor, Math.floor(baseIntervalMs / speedMultiplier));
        const replayTotal = Math.max(0, Math.floor(Math.max(0, endTime - startTime) / baseIntervalMs) + 1);

        const streams = {};
        if (existing?.streams) {
            Object.values(existing.streams).forEach((state) => {
                if (!state?.symbol || !state?.interval) {
                    return;
                }
                const key = this.buildStreamKey(state.symbol, state.interval);
                streams[key] = {
                    key,
                    symbol: state.symbol,
                    interval: state.interval,
                    active: !!state.active,
                    loaded: false,
                    loadingPromise: null,
                    candles: [],
                    cursor: 0,
                    windowStart: 0,
                    windowEnd: 0,
                };
            });
        }

        return {
            running: false,
            accountType,
            baseInterval,
            baseIntervalMs,
            speedMultiplier,
            lookbackDays,
            tickMs,
            startTime,
            endTime,
            replayIndex: 0,
            replayTotal,
            replayClockMs: startTime - baseIntervalMs,
            streams,
            latestBySymbol: {},
            timer: null,
            tickBusy: false,
            startedAt: null,
            endedAt: null,
        };
    }

    createEmptyStatus(accountType = ACCOUNT_TYPES.SPOT) {
        const normalized = normalizeAccountType(accountType);
        return {
            accountType: normalized,
            running: false,
            interval: DEFAULTS.interval,
            intervals: [],
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
        const activeStreamStates = this.getActiveStreamStates(session);
        const symbols = uniq(activeStreamStates.map((stream) => stream.symbol));
        const intervals = uniq(activeStreamStates.map((stream) => stream.interval));
        return {
            accountType: normalized,
            running: !!session.running,
            interval: session.baseInterval || DEFAULTS.interval,
            intervals,
            lookbackDays: session.lookbackDays,
            speedMultiplier: session.speedMultiplier,
            symbols,
            startedAt: session.startedAt,
            endedAt: session.endedAt,
            tickMs: session.tickMs,
            replayCursor: {
                index: session.replayIndex,
                total: session.replayTotal,
                openTime: session.replayIndex > 0 ? session.replayClockMs : null,
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

    isRunning(accountType = ACCOUNT_TYPES.SPOT) {
        const session = this.getSession(accountType);
        return !!session?.running;
    }

    getSessionVisibleEndTimeMs(accountType = ACCOUNT_TYPES.SPOT, symbol = "", interval = DEFAULTS.interval) {
        const session = this.getSession(accountType);
        if (!session) {
            return null;
        }
        const upperSymbol = (symbol || "").toString().toUpperCase();
        const normalizedInterval = normalizeInterval(interval || DEFAULTS.interval);
        if (!upperSymbol || !isSupportedInterval(normalizedInterval)) {
            return null;
        }
        const key = this.buildStreamKey(upperSymbol, normalizedInterval);
        const state = session.streams?.[key];
        if (!state?.active) {
            return null;
        }
        if (state.cursor <= 0) {
            return session.running ? -1 : null;
        }
        const lastVisible = state.candles?.[state.cursor - 1] || null;
        if (!lastVisible) {
            return session.running ? -1 : null;
        }
        if (toNumber(lastVisible.closeTime, 0) > 0) {
            return lastVisible.closeTime;
        }
        return lastVisible.openTime + Math.max(60 * 1000, intervalToMs(normalizedInterval)) - 1;
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

    buildWsPayload(candle = null, symbol = "", interval = DEFAULTS.interval) {
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
                i: normalizeInterval(interval || DEFAULTS.interval),
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

    setLatestPrice(session = null, symbol = "", candle = null) {
        if (!session || !symbol || !candle) {
            return;
        }
        const existing = session.latestBySymbol[symbol];
        if (existing && toNumber(existing.openTime, 0) > toNumber(candle.openTime, 0)) {
            return;
        }
        session.latestBySymbol[symbol] = {
            symbol,
            openTime: candle.openTime,
            closeTime: candle.closeTime,
            price: candle.close,
            bidPrice: candle.close,
            askPrice: candle.close,
        };
    }

    syncStreamCursorWithClock(session = null, state = null) {
        if (!session || !state || !Array.isArray(state.candles) || !state.candles.length) {
            if (state) {
                state.cursor = 0;
            }
            return;
        }
        const target = toNumber(session.replayClockMs, 0);
        let low = 0;
        let high = state.candles.length;
        while (low < high) {
            const mid = Math.floor((low + high) / 2);
            if (toNumber(state.candles[mid]?.openTime, 0) <= target) {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        state.cursor = low;
        if (state.cursor > 0) {
            this.setLatestPrice(session, state.symbol, state.candles[state.cursor - 1]);
        }
    }

    syncStreamCursorToLastVisibleOpenTime(session = null, state = null, lastVisibleOpenTime = 0) {
        if (!session || !state || !Array.isArray(state.candles) || !state.candles.length) {
            if (state) {
                state.cursor = 0;
            }
            return;
        }
        const target = toNumber(lastVisibleOpenTime, 0);
        if (target <= 0) {
            this.syncStreamCursorWithClock(session, state);
            return;
        }
        let low = 0;
        let high = state.candles.length;
        while (low < high) {
            const mid = Math.floor((low + high) / 2);
            if (toNumber(state.candles[mid]?.openTime, 0) <= target) {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        state.cursor = low;
        if (state.cursor > 0) {
            this.setLatestPrice(session, state.symbol, state.candles[state.cursor - 1]);
        }
    }

    async ensureStreamDataLoaded(session = null, state = null) {
        if (!session || !state || !state.active) {
            return;
        }
        const intervalMs = Math.max(60 * 1000, intervalToMs(state.interval || session.baseInterval || DEFAULTS.interval));
        const chunkCandles = Math.max(500, Math.floor(toNumber(process.env.BACKTEST_STREAM_CHUNK_CANDLES, 2000)));
        const prefetchCandles = Math.max(100, Math.floor(toNumber(process.env.BACKTEST_STREAM_PREFETCH_CANDLES, 300)));
        const chunkMs = intervalMs * chunkCandles;
        const prefetchLeadMs = intervalMs * prefetchCandles;
        const desiredStart = session.startTime;

        let desiredEnd = 0;
        if (!state.loaded || !state.windowEnd || state.windowStart !== desiredStart) {
            desiredEnd = Math.min(session.endTime, desiredStart + chunkMs);
        } else {
            const shouldExtend =
                state.windowEnd < session.endTime &&
                toNumber(session.replayClockMs, 0) >= (state.windowEnd - prefetchLeadMs);
            if (!shouldExtend) {
                return;
            }
            desiredEnd = Math.min(session.endTime, state.windowEnd + chunkMs);
        }

        if (state.loaded && state.windowStart === desiredStart && state.windowEnd >= desiredEnd) {
            this.syncStreamCursorWithClock(session, state);
            return;
        }
        if (state.loadingPromise) {
            await state.loadingPromise;
            return;
        }
        state.loadingPromise = (async () => {
            const previousVisibleOpenTime =
                state.cursor > 0 && Array.isArray(state.candles) && state.candles[state.cursor - 1]
                    ? toNumber(state.candles[state.cursor - 1]?.openTime, 0)
                    : 0;
            const candles = await this.candleStore.ensureRange(state.symbol, session.accountType, state.interval, {
                startTime: desiredStart,
                endTime: desiredEnd,
                lookbackDays: session.lookbackDays,
            });
            state.candles = (candles || [])
                .filter((candle) => toNumber(candle?.openTime, 0) >= desiredStart && toNumber(candle?.openTime, 0) <= desiredEnd)
                .sort((a, b) => toNumber(a?.openTime, 0) - toNumber(b?.openTime, 0));
            state.loaded = true;
            state.windowStart = desiredStart;
            state.windowEnd = desiredEnd;
            if (previousVisibleOpenTime > 0) {
                this.syncStreamCursorToLastVisibleOpenTime(session, state, previousVisibleOpenTime);
            } else {
                // On initial stream load, align cursor to current replay clock so only forward candles are emitted.
                this.syncStreamCursorWithClock(session, state);
            }
        })();
        try {
            await state.loadingPromise;
        } finally {
            state.loadingPromise = null;
        }
    }

    async ensureStream(options = {}) {
        const normalized = normalizeAccountType(options.accountType);
        const symbol = (options.symbol || "").toString().trim().toUpperCase();
        const interval = normalizeInterval(options.interval || DEFAULTS.interval);
        if (!symbol) {
            throw new Error("symbol is required");
        }
        if (!isSupportedInterval(interval)) {
            throw new Error(`Unsupported interval: ${interval}`);
        }
        let session = this.sessions[normalized];
        if (!session) {
            session = this.createSession({ accountType: normalized });
            this.sessions[normalized] = session;
        }
        const key = this.buildStreamKey(symbol, interval);
        if (!session.streams[key]) {
            session.streams[key] = {
                key,
                symbol,
                interval,
                active: true,
                loaded: false,
                loadingPromise: null,
                candles: [],
                cursor: 0,
                windowStart: 0,
                windowEnd: 0,
            };
        } else {
            session.streams[key].active = true;
        }
        if (options.loadNow !== false) {
            await this.ensureStreamDataLoaded(session, session.streams[key]);
        }
        return session.streams[key];
    }

    removeStream(options = {}) {
        const normalized = normalizeAccountType(options.accountType);
        const symbol = (options.symbol || "").toString().trim().toUpperCase();
        const interval = normalizeInterval(options.interval || DEFAULTS.interval);
        if (!symbol || !isSupportedInterval(interval)) {
            return false;
        }
        const session = this.sessions[normalized];
        if (!session?.streams) {
            return false;
        }
        const key = this.buildStreamKey(symbol, interval);
        if (!session.streams[key]) {
            return false;
        }
        delete session.streams[key];
        const symbolStillActive = this.getActiveStreamStates(session).some((stream) => stream.symbol === symbol);
        if (!symbolStillActive) {
            delete session.latestBySymbol[symbol];
        }
        return true;
    }

    emitStreamCandlesToClock(session = null, state = null, targetOpenTime = 0) {
        if (!session || !state || !state.active || !state.loaded || !Array.isArray(state.candles)) {
            return;
        }
        while (state.cursor < state.candles.length) {
            const candle = state.candles[state.cursor];
            if (toNumber(candle?.openTime, 0) > targetOpenTime) {
                break;
            }
            state.cursor += 1;
            this.setLatestPrice(session, state.symbol, candle);
            const payload = this.buildWsPayload(candle, state.symbol, state.interval);
            if (payload) {
                this.emit("kline", {
                    accountType: session.accountType,
                    symbol: state.symbol,
                    interval: state.interval,
                    payload,
                });
            }
        }
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
        session.tickBusy = false;
        session.endedAt = nowMs();
        this.emit("stopped", { accountType: normalized, status: this.getStatus(normalized) });
        return this.getStatus(normalized);
    }

    async start(options = {}) {
        const normalized = normalizeAccountType(options.accountType);
        const existing = this.sessions[normalized] || null;
        if (existing?.timer) {
            clearInterval(existing.timer);
            existing.timer = null;
        }
        const session = this.createSession({ ...options, accountType: normalized }, existing);
        session.running = true;
        session.startedAt = nowMs();
        session.endedAt = null;
        this.sessions[normalized] = session;
        this.emit("started", { accountType: normalized, status: this.getStatus(normalized) });

        const emitTick = async () => {
            if (!session.running || session.tickBusy) {
                return;
            }
            const activeStreams = this.getActiveStreamStates(session);
            if (!activeStreams.length) {
                return;
            }
            session.tickBusy = true;
            try {
                if (session.replayIndex >= session.replayTotal) {
                    this.stop(normalized);
                    return;
                }
                session.replayIndex += 1;
                const nextClock = session.startTime + (session.replayIndex - 1) * session.baseIntervalMs;
                session.replayClockMs = Math.min(session.endTime, nextClock);

                for (const stream of activeStreams) {
                    await this.ensureStreamDataLoaded(session, stream);
                    this.emitStreamCandlesToClock(session, stream, session.replayClockMs);
                }

                if (session.replayClockMs >= session.endTime || session.replayIndex >= session.replayTotal) {
                    this.stop(normalized);
                }
            } finally {
                session.tickBusy = false;
            }
        };

        emitTick().catch(() => {});
        session.timer = setInterval(() => {
            emitTick().catch(() => {});
        }, session.tickMs);
        return this.getStatus(normalized);
    }
}

module.exports = ReplayEngine;
