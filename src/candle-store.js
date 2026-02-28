"use strict";

const { ACCOUNT_TYPES, DEFAULTS, normalizeAccountType } = require("./constants");
const { clamp, nowMs, toNumber } = require("./utils");

class CandleStore {
    constructor(binanceClient) {
        this.binanceClient = binanceClient;
        this.cache = new Map();
    }

    buildKey(accountType = ACCOUNT_TYPES.SPOT, symbol = "", interval = DEFAULTS.interval) {
        const normalized = normalizeAccountType(accountType);
        return `${normalized}:${(symbol || "").toUpperCase()}:${interval}`;
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
        const key = this.buildKey(normalized, upperSymbol, interval);
        const lookbackDays = Math.max(1, Math.floor(toNumber(options.lookbackDays, DEFAULTS.lookbackDays)));
        const endTime = toNumber(options.endTime, nowMs()) || nowMs();
        const startTime = toNumber(options.startTime, endTime - lookbackDays * 24 * 60 * 60 * 1000);
        const limitPerCall = 1000;
        const allRows = [];
        let cursor = startTime;
        const hardStop = endTime + 1;

        while (cursor < hardStop) {
            const batch = await this.binanceClient.getKlines(
                {
                    symbol: upperSymbol,
                    interval,
                    startTime: cursor,
                    endTime,
                    limit: limitPerCall,
                },
                normalized
            );
            if (!Array.isArray(batch) || !batch.length) {
                break;
            }
            allRows.push(...batch);
            const lastOpenTime = toNumber(batch[batch.length - 1]?.[0], 0);
            if (!lastOpenTime || batch.length < limitPerCall) {
                break;
            }
            cursor = lastOpenTime + 60 * 1000;
        }

        const normalizedRows = allRows
            .map((row) => this.parseRow(row))
            .filter((row) => row && row.openTime >= startTime && row.openTime <= endTime)
            .sort((a, b) => a.openTime - b.openTime);

        this.cache.set(key, {
            fetchedAt: nowMs(),
            startTime,
            endTime,
            candles: normalizedRows,
        });
        return normalizedRows;
    }

    async ensureRange(symbol, accountType = ACCOUNT_TYPES.SPOT, interval = DEFAULTS.interval, options = {}) {
        const upperSymbol = (symbol || "").toUpperCase();
        const normalized = normalizeAccountType(accountType);
        const key = this.buildKey(normalized, upperSymbol, interval);
        const cached = this.cache.get(key);
        const explicitEndTime = toNumber(options.endTime, 0);
        const endTime = explicitEndTime > 0 ? explicitEndTime : toNumber(cached?.endTime, nowMs()) || nowMs();
        const lookbackDays = Math.max(1, Math.floor(toNumber(options.lookbackDays, DEFAULTS.lookbackDays)));
        const startTime = toNumber(options.startTime, endTime - lookbackDays * 24 * 60 * 60 * 1000);
        const toleranceMs = 2 * 60 * 1000;
        const hasCoverage =
            cached &&
            Array.isArray(cached.candles) &&
            cached.candles.length > 0 &&
            toNumber(cached.startTime, 0) <= startTime &&
            toNumber(cached.endTime, 0) + toleranceMs >= endTime;
        if (hasCoverage) {
            return cached.candles;
        }
        return await this.fetchAndCacheRange(upperSymbol, normalized, interval, {
            startTime,
            endTime,
            lookbackDays,
        });
    }

    async getKlines(params = {}, accountType = ACCOUNT_TYPES.SPOT) {
        const symbol = (params.symbol || "").toString().toUpperCase();
        const interval = (params.interval || DEFAULTS.interval).toString();
        if (!symbol) {
            return [];
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
        const key = this.buildKey(accountType, (symbol || "").toUpperCase(), interval);
        const cached = this.cache.get(key);
        if (!cached?.candles?.length) {
            return null;
        }
        return cached.candles[cached.candles.length - 1] || null;
    }
}

module.exports = CandleStore;
