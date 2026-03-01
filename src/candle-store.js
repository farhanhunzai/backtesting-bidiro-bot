"use strict";

const {
    ACCOUNT_TYPES,
    DEFAULTS,
    normalizeAccountType,
    normalizeInterval,
    intervalToMs,
    isSupportedInterval,
} = require("./constants");
const { clamp, nowMs, toNumber } = require("./utils");

class CandleStore {
    constructor(binanceClient, candleDb = null) {
        this.binanceClient = binanceClient;
        this.candleDb = candleDb || null;
        this.cache = new Map();
        this.syncLocks = new Map();
        this.bannedUntilByAccountType = {
            SPOT: 0,
            FUTURES: 0,
        };
    }

    buildKey(accountType = ACCOUNT_TYPES.SPOT, symbol = "", interval = DEFAULTS.interval) {
        const normalized = normalizeAccountType(accountType);
        const normalizedInterval = normalizeInterval(interval || DEFAULTS.interval);
        return `${normalized}:${(symbol || "").toUpperCase()}:${normalizedInterval}`;
    }

    parseRow(row = []) {
        if (!Array.isArray(row) || row.length < 7) {
            return null;
        }
        return {
            openTime: toNumber(row[0], 0),
            open: toNumber(row[1], 0),
            high: toNumber(row[2], 0),
            low: toNumber(row[3], 0),
            close: toNumber(row[4], 0),
            volume: toNumber(row[5], 0),
            closeTime: toNumber(row[6], 0),
            quoteAssetVolume: toNumber(row[7], 0),
            trades: toNumber(row[8], 0),
            takerBuyBaseVolume: toNumber(row[9], 0),
            takerBuyQuoteVolume: toNumber(row[10], 0),
            raw: row,
        };
    }

    toBinanceRows(candles = []) {
        return (candles || []).map((candle) =>
            candle.raw || [
                candle.openTime,
                `${candle.open}`,
                `${candle.high}`,
                `${candle.low}`,
                `${candle.close}`,
                `${candle.volume}`,
                candle.closeTime,
                `${candle.quoteAssetVolume || 0}`,
                `${candle.trades || 0}`,
                `${candle.takerBuyBaseVolume || 0}`,
                `${candle.takerBuyQuoteVolume || 0}`,
                "0",
            ]
        );
    }

    extractBanUntilMs(error = null) {
        const message = `${error?.message || ""} ${error?.response?.data?.msg || ""}`.trim();
        if (!message) {
            return 0;
        }
        const match = /banned until\s+(\d{10,16})/i.exec(message);
        if (!match) {
            return 0;
        }
        const raw = parseInt(match[1], 10);
        if (!Number.isFinite(raw) || raw <= 0) {
            return 0;
        }
        return raw < 1e12 ? raw * 1000 : raw;
    }

    setBanUntil(accountType = ACCOUNT_TYPES.SPOT, bannedUntil = 0) {
        const normalized = normalizeAccountType(accountType);
        const safe = Math.max(0, Math.floor(toNumber(bannedUntil, 0)));
        this.bannedUntilByAccountType[normalized] = safe;
    }

    getBanUntil(accountType = ACCOUNT_TYPES.SPOT) {
        const normalized = normalizeAccountType(accountType);
        return Math.max(0, Math.floor(toNumber(this.bannedUntilByAccountType[normalized], 0)));
    }

    isBanActive(accountType = ACCOUNT_TYPES.SPOT) {
        return this.getBanUntil(accountType) > nowMs();
    }

    withSyncLock(key = "", handler = async () => {}) {
        const previous = this.syncLocks.get(key) || Promise.resolve();
        const next = previous
            .catch(() => {})
            .then(handler)
            .finally(() => {
                if (this.syncLocks.get(key) === next) {
                    this.syncLocks.delete(key);
                }
            });
        this.syncLocks.set(key, next);
        return next;
    }

    trimRowsByRange(rows = [], startTime = 0, endTime = nowMs()) {
        const safeStart = Math.max(0, toNumber(startTime, 0));
        const safeEnd = Math.max(safeStart, toNumber(endTime, nowMs()));
        return (rows || [])
            .filter((row) => {
                const openTime = toNumber(row?.openTime, 0);
                return openTime >= safeStart && openTime <= safeEnd;
            })
            .sort((a, b) => toNumber(a?.openTime, 0) - toNumber(b?.openTime, 0));
    }

    mergeByOpenTime(baseRows = [], extraRows = [], startTime = 0, endTime = nowMs()) {
        const byOpenTime = new Map();
        [...(baseRows || []), ...(extraRows || [])].forEach((row) => {
            const openTime = toNumber(row?.openTime, 0);
            if (openTime <= 0) {
                return;
            }
            byOpenTime.set(openTime, row);
        });
        return this.trimRowsByRange(Array.from(byOpenTime.values()), startTime, endTime);
    }

    loadFromPersistence(accountType = ACCOUNT_TYPES.SPOT, symbol = "", interval = DEFAULTS.interval, startTime = 0, endTime = nowMs()) {
        if (!this.candleDb) {
            return [];
        }
        return this.candleDb.getCandles(accountType, symbol, interval, startTime, endTime) || [];
    }

    persistRows(accountType = ACCOUNT_TYPES.SPOT, symbol = "", interval = DEFAULTS.interval, rows = []) {
        if (!this.candleDb || !Array.isArray(rows) || !rows.length) {
            return 0;
        }
        return this.candleDb.upsertCandles(accountType, symbol, interval, rows);
    }

    computeMissingRanges(candles = [], startTime = 0, endTime = 0, intervalMs = 60 * 1000) {
        const safeStart = Math.max(0, Math.floor(toNumber(startTime, 0)));
        const safeEnd = Math.max(safeStart, Math.floor(toNumber(endTime, safeStart)));
        const safeIntervalMs = Math.max(60 * 1000, Math.floor(toNumber(intervalMs, 60 * 1000)));
        const sorted = this.trimRowsByRange(candles, safeStart, safeEnd);
        const ranges = [];
        const toleranceMs = safeIntervalMs;

        if (!sorted.length) {
            ranges.push({ startTime: safeStart, endTime: safeEnd });
            return ranges;
        }

        const firstOpen = toNumber(sorted[0]?.openTime, 0);
        if (firstOpen > safeStart + toleranceMs) {
            const headEnd = Math.min(safeEnd, firstOpen - safeIntervalMs);
            if (headEnd >= safeStart) {
                ranges.push({ startTime: safeStart, endTime: headEnd });
            }
        }

        for (let i = 1; i < sorted.length; i += 1) {
            const prevOpen = toNumber(sorted[i - 1]?.openTime, 0);
            const nextOpen = toNumber(sorted[i]?.openTime, 0);
            const expectedNext = prevOpen + safeIntervalMs;
            if (nextOpen > expectedNext + Math.floor(safeIntervalMs / 2)) {
                const gapStart = Math.max(safeStart, expectedNext);
                const gapEnd = Math.min(safeEnd, nextOpen - safeIntervalMs);
                if (gapEnd >= gapStart) {
                    ranges.push({ startTime: gapStart, endTime: gapEnd });
                }
            }
        }

        const lastOpen = toNumber(sorted[sorted.length - 1]?.openTime, 0);
        if (safeEnd > lastOpen + toleranceMs) {
            const tailStart = Math.max(safeStart, lastOpen + safeIntervalMs);
            if (safeEnd >= tailStart) {
                ranges.push({ startTime: tailStart, endTime: safeEnd });
            }
        }

        if (!ranges.length) {
            return [];
        }

        ranges.sort((a, b) => a.startTime - b.startTime);
        const merged = [ranges[0]];
        for (let i = 1; i < ranges.length; i += 1) {
            const current = ranges[i];
            const prev = merged[merged.length - 1];
            if (current.startTime <= prev.endTime + safeIntervalMs) {
                prev.endTime = Math.max(prev.endTime, current.endTime);
            } else {
                merged.push({ ...current });
            }
        }
        return merged.filter((range) => range.endTime >= range.startTime);
    }

    sliceFromCache(cached = [], params = {}) {
        const startTime = toNumber(params.startTime, 0);
        const endTime = toNumber(params.endTime, 0);
        const requestedLimit = clamp(params.limit || DEFAULTS.maxKlineLimit, 1, DEFAULTS.maxKlineLimit);
        let rows = cached.slice();
        if (startTime > 0) {
            rows = rows.filter((row) => row.openTime >= startTime);
        }
        if (endTime > 0) {
            rows = rows.filter((row) => row.openTime <= endTime);
        }
        if (rows.length > requestedLimit) {
            rows = rows.slice(rows.length - requestedLimit);
        }
        return rows;
    }

    async fetchRangeRows(
        symbol = "",
        accountType = ACCOUNT_TYPES.SPOT,
        interval = DEFAULTS.interval,
        startTime = 0,
        endTime = 0,
        intervalMsHint = 0
    ) {
        const upperSymbol = (symbol || "").toUpperCase();
        const normalizedAccountType = normalizeAccountType(accountType);
        const normalizedInterval = normalizeInterval(interval || DEFAULTS.interval);
        const intervalMs = Math.max(60 * 1000, intervalMsHint || intervalToMs(normalizedInterval));
        const safeStart = Math.max(0, toNumber(startTime, 0));
        const safeEnd = Math.max(safeStart, toNumber(endTime, nowMs()) || nowMs());
        const limitPerCall = 1000;
        const allRows = [];
        let cursor = safeStart;
        const hardStop = safeEnd + 1;

        if (this.isBanActive(normalizedAccountType)) {
            const bannedUntil = this.getBanUntil(normalizedAccountType);
            throw new Error(
                `Binance API is temporarily banned for ${normalizedAccountType} until ${bannedUntil}. Serving local cache only.`
            );
        }

        while (cursor < hardStop) {
            let batch = [];
            try {
                batch = await this.binanceClient.getKlines(
                    {
                        symbol: upperSymbol,
                        interval: normalizedInterval,
                        startTime: cursor,
                        endTime: safeEnd,
                        limit: limitPerCall,
                    },
                    normalizedAccountType
                );
            } catch (e) {
                const bannedUntil = this.extractBanUntilMs(e);
                if (bannedUntil > 0) {
                    this.setBanUntil(normalizedAccountType, bannedUntil);
                }
                if (this.candleDb) {
                    this.candleDb.setSyncError(
                        normalizedAccountType,
                        upperSymbol,
                        normalizedInterval,
                        e?.message || "Kline fetch failed",
                        bannedUntil
                    );
                }
                throw e;
            }

            if (!Array.isArray(batch) || !batch.length) {
                break;
            }
            allRows.push(...batch);
            const lastOpenTime = toNumber(batch[batch.length - 1]?.[0], 0);
            if (!lastOpenTime || batch.length < limitPerCall) {
                break;
            }
            const nextCursor = lastOpenTime + intervalMs;
            if (nextCursor <= cursor) {
                break;
            }
            cursor = nextCursor;
        }

        const byOpenTime = new Map();
        allRows
            .map((row) => this.parseRow(row))
            .filter((row) => row && row.openTime >= safeStart && row.openTime <= safeEnd)
            .forEach((row) => {
                byOpenTime.set(row.openTime, row);
            });
        const rows = Array.from(byOpenTime.values()).sort((a, b) => a.openTime - b.openTime);
        if (rows.length > 0 && this.candleDb) {
            this.candleDb.clearSyncError(normalizedAccountType, upperSymbol, normalizedInterval, 0);
        }
        return rows;
    }

    async fetchAndCacheRange(symbol, accountType = ACCOUNT_TYPES.SPOT, interval = DEFAULTS.interval, options = {}) {
        const upperSymbol = (symbol || "").toUpperCase();
        const normalized = normalizeAccountType(accountType);
        const normalizedInterval = normalizeInterval(interval || DEFAULTS.interval);
        if (!isSupportedInterval(normalizedInterval)) {
            throw new Error(`Unsupported interval: ${normalizedInterval}`);
        }
        const endTime = toNumber(options.endTime, nowMs()) || nowMs();
        const lookbackDays = Math.max(1, Math.floor(toNumber(options.lookbackDays, DEFAULTS.lookbackDays)));
        const startTime = toNumber(options.startTime, endTime - lookbackDays * 24 * 60 * 60 * 1000);
        return await this.ensureRange(upperSymbol, normalized, normalizedInterval, {
            startTime,
            endTime,
            lookbackDays,
        });
    }

    async ensureRange(symbol, accountType = ACCOUNT_TYPES.SPOT, interval = DEFAULTS.interval, options = {}) {
        const upperSymbol = (symbol || "").toUpperCase();
        const normalized = normalizeAccountType(accountType);
        const normalizedInterval = normalizeInterval(interval || DEFAULTS.interval);
        if (!isSupportedInterval(normalizedInterval)) {
            throw new Error(`Unsupported interval: ${normalizedInterval}`);
        }
        const key = this.buildKey(normalized, upperSymbol, normalizedInterval);
        const explicitEndTime = toNumber(options.endTime, 0);
        const endTime = explicitEndTime > 0 ? explicitEndTime : nowMs();
        const lookbackDays = Math.max(1, Math.floor(toNumber(options.lookbackDays, DEFAULTS.lookbackDays)));
        const startTime = toNumber(options.startTime, endTime - lookbackDays * 24 * 60 * 60 * 1000);
        const intervalMs = Math.max(60 * 1000, intervalToMs(normalizedInterval));
        const toleranceMs = Math.max(2 * 60 * 1000, intervalMs * 2);

        return await this.withSyncLock(key, async () => {
            if (this.candleDb) {
                const persistedSyncState = this.candleDb.getSyncState(
                    normalized,
                    upperSymbol,
                    normalizedInterval
                );
                const persistedBanUntil = toNumber(persistedSyncState?.bannedUntil, 0);
                if (persistedBanUntil > this.getBanUntil(normalized)) {
                    this.setBanUntil(normalized, persistedBanUntil);
                }
            }

            const cached = this.cache.get(key);
            const hasCoverage =
                cached &&
                Array.isArray(cached.candles) &&
                cached.candles.length > 0 &&
                toNumber(cached.startTime, 0) <= startTime &&
                toNumber(cached.endTime, 0) + toleranceMs >= endTime;
            if (hasCoverage) {
                return cached.candles;
            }

            let rows = this.loadFromPersistence(
                normalized,
                upperSymbol,
                normalizedInterval,
                startTime,
                endTime
            );
            if ((!rows || !rows.length) && cached?.candles?.length) {
                rows = this.trimRowsByRange(cached.candles, startTime, endTime);
            }
            rows = this.trimRowsByRange(rows || [], startTime, endTime);
            let lastError = null;

            const missingRanges = this.computeMissingRanges(rows, startTime, endTime, intervalMs);
            for (const missing of missingRanges) {
                if (this.isBanActive(normalized)) {
                    break;
                }
                try {
                    const fetched = await this.fetchRangeRows(
                        upperSymbol,
                        normalized,
                        normalizedInterval,
                        missing.startTime,
                        missing.endTime,
                        intervalMs
                    );
                    if (fetched.length) {
                        this.persistRows(normalized, upperSymbol, normalizedInterval, fetched);
                        rows = this.mergeByOpenTime(rows, fetched, startTime, endTime);
                    }
                } catch (e) {
                    lastError = e;
                    if (!rows.length) {
                        throw e;
                    }
                    break;
                }
            }

            rows = this.trimRowsByRange(rows, startTime, endTime);
            if (!rows.length && lastError) {
                throw lastError;
            }

            const cacheStart = rows.length ? toNumber(rows[0]?.openTime, startTime) : startTime;
            const cacheEnd = rows.length
                ? toNumber(rows[rows.length - 1]?.openTime, endTime)
                : endTime;
            this.cache.set(key, {
                fetchedAt: nowMs(),
                startTime: cacheStart,
                endTime: cacheEnd,
                candles: rows,
            });
            return rows;
        });
    }

    async getKlines(params = {}, accountType = ACCOUNT_TYPES.SPOT) {
        const symbol = (params.symbol || "").toString().toUpperCase();
        const interval = normalizeInterval(params.interval || DEFAULTS.interval);
        if (!symbol) {
            return [];
        }
        if (!isSupportedInterval(interval)) {
            throw new Error(`Unsupported interval: ${interval}`);
        }
        const startTime = toNumber(params.startTime, 0);
        const endTime = toNumber(params.endTime, 0) || nowMs();
        const inferredLookbackDays = Math.max(
            1,
            Math.ceil(
                Math.max(1, endTime - (startTime || endTime - DEFAULTS.lookbackDays * 24 * 60 * 60 * 1000)) /
                    (24 * 60 * 60 * 1000)
            )
        );
        const candles = await this.ensureRange(symbol, accountType, interval, {
            startTime,
            endTime,
            lookbackDays: inferredLookbackDays,
        });
        const sliced = this.sliceFromCache(candles, params);
        return this.toBinanceRows(sliced);
    }

    getLatestClose(symbol = "", accountType = ACCOUNT_TYPES.SPOT, interval = DEFAULTS.interval) {
        const key = this.buildKey(
            accountType,
            (symbol || "").toUpperCase(),
            normalizeInterval(interval || DEFAULTS.interval)
        );
        const cached = this.cache.get(key);
        if (cached?.candles?.length) {
            return cached.candles[cached.candles.length - 1] || null;
        }
        if (this.candleDb) {
            return this.candleDb.getLatestCandle(accountType, symbol, interval);
        }
        return null;
    }

    getLatestCloseAnyInterval(symbol = "", accountType = ACCOUNT_TYPES.SPOT) {
        const upperSymbol = (symbol || "").toUpperCase();
        const normalized = normalizeAccountType(accountType);
        const keyPrefix = `${normalized}:${upperSymbol}:`;
        let latest = null;
        for (const [key, cached] of this.cache.entries()) {
            if (!key.startsWith(keyPrefix)) {
                continue;
            }
            if (!cached?.candles?.length) {
                continue;
            }
            const candidate = cached.candles[cached.candles.length - 1];
            if (!candidate) {
                continue;
            }
            if (!latest) {
                latest = candidate;
                continue;
            }
            if (toNumber(candidate.closeTime, candidate.openTime) > toNumber(latest.closeTime, latest.openTime)) {
                latest = candidate;
            }
        }
        if (latest) {
            return latest;
        }
        if (this.candleDb) {
            return this.candleDb.getLatestCandleAnyInterval(normalized, upperSymbol);
        }
        return null;
    }

    getStorageStatus() {
        return {
            persistence: this.candleDb ? this.candleDb.getStatus() : { enabled: false, path: null },
            cacheKeys: this.cache.size,
            syncLocks: this.syncLocks.size,
            bannedUntilByAccountType: {
                SPOT: this.getBanUntil(ACCOUNT_TYPES.SPOT),
                FUTURES: this.getBanUntil(ACCOUNT_TYPES.FUTURES),
            },
        };
    }

    getSyncState(accountType = ACCOUNT_TYPES.SPOT, symbol = "", interval = DEFAULTS.interval) {
        if (!this.candleDb) {
            return null;
        }
        const upperSymbol = (symbol || "").toString().trim().toUpperCase();
        const normalizedInterval = normalizeInterval(interval || DEFAULTS.interval);
        if (!upperSymbol) {
            return null;
        }
        return this.candleDb.getSyncState(accountType, upperSymbol, normalizedInterval);
    }

    getCoverage(accountType = ACCOUNT_TYPES.SPOT, symbol = "", interval = DEFAULTS.interval) {
        if (!this.candleDb) {
            return null;
        }
        const upperSymbol = (symbol || "").toString().trim().toUpperCase();
        const normalizedInterval = normalizeInterval(interval || DEFAULTS.interval);
        if (!upperSymbol) {
            return null;
        }
        return this.candleDb.getCoverage(accountType, upperSymbol, normalizedInterval);
    }
}

module.exports = CandleStore;
