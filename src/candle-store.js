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
    constructor(binanceClient) {
        this.binanceClient = binanceClient;
        this.cache = new Map();
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
        return (candles || []).map((candle) => candle.raw || [
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
        ]);
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

    async fetchAndCacheRange(symbol, accountType = ACCOUNT_TYPES.SPOT, interval = DEFAULTS.interval, options = {}) {
        const upperSymbol = (symbol || "").toUpperCase();
        const normalized = normalizeAccountType(accountType);
        const normalizedInterval = normalizeInterval(interval || DEFAULTS.interval);
        if (!isSupportedInterval(normalizedInterval)) {
            throw new Error(`Unsupported interval: ${normalizedInterval}`);
        }
        const key = this.buildKey(normalized, upperSymbol, normalizedInterval);
        const intervalMs = Math.max(60 * 1000, intervalToMs(normalizedInterval));
        const lookbackDays = Math.max(1, Math.floor(toNumber(options.lookbackDays, DEFAULTS.lookbackDays)));
        const endTime = toNumber(options.endTime, nowMs()) || nowMs();
        const startTime = toNumber(options.startTime, endTime - lookbackDays * 24 * 60 * 60 * 1000);
        const normalizedRows = await this.fetchRangeRows(upperSymbol, normalized, normalizedInterval, startTime, endTime, intervalMs);

        this.cache.set(key, {
            fetchedAt: nowMs(),
            startTime,
            endTime,
            candles: normalizedRows,
        });
        return normalizedRows;
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

        while (cursor < hardStop) {
            const batch = await this.binanceClient.getKlines(
                {
                    symbol: upperSymbol,
                    interval: normalizedInterval,
                    startTime: cursor,
                    endTime: safeEnd,
                    limit: limitPerCall,
                },
                normalizedAccountType
            );
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
        return Array.from(byOpenTime.values()).sort((a, b) => a.openTime - b.openTime);
    }

    async ensureRange(symbol, accountType = ACCOUNT_TYPES.SPOT, interval = DEFAULTS.interval, options = {}) {
        const upperSymbol = (symbol || "").toUpperCase();
        const normalized = normalizeAccountType(accountType);
        const normalizedInterval = normalizeInterval(interval || DEFAULTS.interval);
        if (!isSupportedInterval(normalizedInterval)) {
            throw new Error(`Unsupported interval: ${normalizedInterval}`);
        }
        const key = this.buildKey(normalized, upperSymbol, normalizedInterval);
        const cached = this.cache.get(key);
        const explicitEndTime = toNumber(options.endTime, 0);
        const endTime = explicitEndTime > 0 ? explicitEndTime : toNumber(cached?.endTime, nowMs()) || nowMs();
        const lookbackDays = Math.max(1, Math.floor(toNumber(options.lookbackDays, DEFAULTS.lookbackDays)));
        const startTime = toNumber(options.startTime, endTime - lookbackDays * 24 * 60 * 60 * 1000);
        const intervalMs = Math.max(60 * 1000, intervalToMs(normalizedInterval));
        const toleranceMs = Math.max(2 * 60 * 1000, intervalMs * 2);
        const hasCoverage =
            cached &&
            Array.isArray(cached.candles) &&
            cached.candles.length > 0 &&
            toNumber(cached.startTime, 0) <= startTime &&
            toNumber(cached.endTime, 0) + toleranceMs >= endTime;
        if (hasCoverage) {
            return cached.candles;
        }

        const canExtendForward =
            cached &&
            Array.isArray(cached.candles) &&
            cached.candles.length > 0 &&
            toNumber(cached.startTime, 0) <= startTime &&
            toNumber(cached.endTime, 0) < endTime;
        if (canExtendForward) {
            const missingStart = Math.max(startTime, toNumber(cached.endTime, 0) + intervalMs);
            const existingRows = Array.isArray(cached.candles) ? cached.candles : [];
            if (missingStart > endTime) {
                return existingRows;
            }
            const additionalRows = await this.fetchRangeRows(
                upperSymbol,
                normalized,
                normalizedInterval,
                missingStart,
                endTime,
                intervalMs
            );
            const byOpenTime = new Map();
            existingRows.forEach((row) => {
                const openTime = toNumber(row?.openTime, 0);
                if (openTime > 0) {
                    byOpenTime.set(openTime, row);
                }
            });
            additionalRows.forEach((row) => {
                const openTime = toNumber(row?.openTime, 0);
                if (openTime > 0) {
                    byOpenTime.set(openTime, row);
                }
            });
            const merged = Array.from(byOpenTime.values()).sort((a, b) => a.openTime - b.openTime);
            this.cache.set(key, {
                fetchedAt: nowMs(),
                startTime: Math.min(toNumber(cached.startTime, startTime), startTime),
                endTime,
                candles: merged,
            });
            return merged;
        }

        return await this.fetchAndCacheRange(upperSymbol, normalized, normalizedInterval, {
            startTime,
            endTime,
            lookbackDays,
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
            Math.ceil(Math.max(1, endTime - (startTime || endTime - DEFAULTS.lookbackDays * 24 * 60 * 60 * 1000)) / (24 * 60 * 60 * 1000))
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
        const key = this.buildKey(accountType, (symbol || "").toUpperCase(), normalizeInterval(interval || DEFAULTS.interval));
        const cached = this.cache.get(key);
        if (!cached?.candles?.length) {
            return null;
        }
        return cached.candles[cached.candles.length - 1] || null;
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
        return latest;
    }
}

module.exports = CandleStore;
