"use strict";

const fs = require("fs");
const path = require("path");
const Database = require("better-sqlite3");
const { ACCOUNT_TYPES, normalizeAccountType, normalizeInterval } = require("./constants");
const { toNumber, nowMs } = require("./utils");

class CandleDb {
    constructor(options = {}) {
        const explicitPath = (options.filePath || process.env.BACKTEST_CANDLE_DB_PATH || "").toString().trim();
        this.filePath = explicitPath || path.join(process.cwd(), "data", "candles.sqlite");
        this.ensureDirectory();
        this.db = new Database(this.filePath);
        this.configure();
        this.createSchema();
        this.prepareStatements();
    }

    ensureDirectory() {
        const dir = path.dirname(this.filePath);
        if (!fs.existsSync(dir)) {
            fs.mkdirSync(dir, { recursive: true });
        }
    }

    configure() {
        this.db.pragma("journal_mode = WAL");
        this.db.pragma("synchronous = NORMAL");
        this.db.pragma("temp_store = MEMORY");
        this.db.pragma("cache_size = -64000");
        this.db.pragma("busy_timeout = 15000");
    }

    createSchema() {
        this.db.exec(`
            CREATE TABLE IF NOT EXISTS candles (
                accountType TEXT NOT NULL,
                symbol TEXT NOT NULL,
                interval TEXT NOT NULL,
                openTime INTEGER NOT NULL,
                closeTime INTEGER NOT NULL,
                open REAL NOT NULL,
                high REAL NOT NULL,
                low REAL NOT NULL,
                close REAL NOT NULL,
                volume REAL NOT NULL,
                quoteAssetVolume REAL DEFAULT 0,
                trades INTEGER DEFAULT 0,
                takerBuyBaseVolume REAL DEFAULT 0,
                takerBuyQuoteVolume REAL DEFAULT 0,
                updatedAt INTEGER NOT NULL,
                PRIMARY KEY (accountType, symbol, interval, openTime)
            );

            CREATE INDEX IF NOT EXISTS idx_candles_lookup
                ON candles (accountType, symbol, interval, openTime);

            CREATE INDEX IF NOT EXISTS idx_candles_close_time
                ON candles (accountType, symbol, interval, closeTime);

            CREATE TABLE IF NOT EXISTS sync_state (
                accountType TEXT NOT NULL,
                symbol TEXT NOT NULL,
                interval TEXT NOT NULL,
                earliestOpenTime INTEGER NOT NULL DEFAULT 0,
                latestOpenTime INTEGER NOT NULL DEFAULT 0,
                latestCloseTime INTEGER NOT NULL DEFAULT 0,
                bannedUntil INTEGER NOT NULL DEFAULT 0,
                lastError TEXT,
                updatedAt INTEGER NOT NULL,
                PRIMARY KEY (accountType, symbol, interval)
            );
        `);
    }

    prepareStatements() {
        this.insertCandleStmt = this.db.prepare(`
            INSERT INTO candles (
                accountType, symbol, interval, openTime, closeTime,
                open, high, low, close, volume,
                quoteAssetVolume, trades, takerBuyBaseVolume, takerBuyQuoteVolume,
                updatedAt
            ) VALUES (
                @accountType, @symbol, @interval, @openTime, @closeTime,
                @open, @high, @low, @close, @volume,
                @quoteAssetVolume, @trades, @takerBuyBaseVolume, @takerBuyQuoteVolume,
                @updatedAt
            )
            ON CONFLICT(accountType, symbol, interval, openTime) DO UPDATE SET
                closeTime = excluded.closeTime,
                open = excluded.open,
                high = excluded.high,
                low = excluded.low,
                close = excluded.close,
                volume = excluded.volume,
                quoteAssetVolume = excluded.quoteAssetVolume,
                trades = excluded.trades,
                takerBuyBaseVolume = excluded.takerBuyBaseVolume,
                takerBuyQuoteVolume = excluded.takerBuyQuoteVolume,
                updatedAt = excluded.updatedAt
        `);

        this.selectCandlesStmt = this.db.prepare(`
            SELECT
                openTime, closeTime, open, high, low, close, volume,
                quoteAssetVolume, trades, takerBuyBaseVolume, takerBuyQuoteVolume
            FROM candles
            WHERE accountType = @accountType
              AND symbol = @symbol
              AND interval = @interval
              AND openTime >= @startTime
              AND openTime <= @endTime
            ORDER BY openTime ASC
        `);

        this.selectCoverageStmt = this.db.prepare(`
            SELECT
                MIN(openTime) AS earliestOpenTime,
                MAX(openTime) AS latestOpenTime,
                MAX(closeTime) AS latestCloseTime,
                COUNT(1) AS rowCount
            FROM candles
            WHERE accountType = @accountType
              AND symbol = @symbol
              AND interval = @interval
        `);

        this.selectLatestForIntervalStmt = this.db.prepare(`
            SELECT
                openTime, closeTime, open, high, low, close, volume,
                quoteAssetVolume, trades, takerBuyBaseVolume, takerBuyQuoteVolume
            FROM candles
            WHERE accountType = @accountType
              AND symbol = @symbol
              AND interval = @interval
            ORDER BY openTime DESC
            LIMIT 1
        `);

        this.selectLatestAnyIntervalStmt = this.db.prepare(`
            SELECT
                openTime, closeTime, open, high, low, close, volume,
                quoteAssetVolume, trades, takerBuyBaseVolume, takerBuyQuoteVolume,
                interval
            FROM candles
            WHERE accountType = @accountType
              AND symbol = @symbol
            ORDER BY closeTime DESC, openTime DESC
            LIMIT 1
        `);

        this.selectSyncStateStmt = this.db.prepare(`
            SELECT
                accountType, symbol, interval,
                earliestOpenTime, latestOpenTime, latestCloseTime,
                bannedUntil, lastError, updatedAt
            FROM sync_state
            WHERE accountType = @accountType
              AND symbol = @symbol
              AND interval = @interval
            LIMIT 1
        `);

        this.upsertSyncStateStmt = this.db.prepare(`
            INSERT INTO sync_state (
                accountType, symbol, interval,
                earliestOpenTime, latestOpenTime, latestCloseTime,
                bannedUntil, lastError, updatedAt
            ) VALUES (
                @accountType, @symbol, @interval,
                @earliestOpenTime, @latestOpenTime, @latestCloseTime,
                @bannedUntil, @lastError, @updatedAt
            )
            ON CONFLICT(accountType, symbol, interval) DO UPDATE SET
                earliestOpenTime = CASE
                    WHEN sync_state.earliestOpenTime = 0 THEN excluded.earliestOpenTime
                    WHEN excluded.earliestOpenTime = 0 THEN sync_state.earliestOpenTime
                    ELSE MIN(sync_state.earliestOpenTime, excluded.earliestOpenTime)
                END,
                latestOpenTime = MAX(sync_state.latestOpenTime, excluded.latestOpenTime),
                latestCloseTime = MAX(sync_state.latestCloseTime, excluded.latestCloseTime),
                bannedUntil = excluded.bannedUntil,
                lastError = excluded.lastError,
                updatedAt = excluded.updatedAt
        `);

        this.updateSyncErrorStmt = this.db.prepare(`
            INSERT INTO sync_state (
                accountType, symbol, interval,
                earliestOpenTime, latestOpenTime, latestCloseTime,
                bannedUntil, lastError, updatedAt
            ) VALUES (
                @accountType, @symbol, @interval,
                0, 0, 0,
                @bannedUntil, @lastError, @updatedAt
            )
            ON CONFLICT(accountType, symbol, interval) DO UPDATE SET
                bannedUntil = MAX(sync_state.bannedUntil, excluded.bannedUntil),
                lastError = excluded.lastError,
                updatedAt = excluded.updatedAt
        `);

        this.clearSyncErrorStmt = this.db.prepare(`
            UPDATE sync_state
            SET lastError = NULL,
                bannedUntil = @bannedUntil,
                updatedAt = @updatedAt
            WHERE accountType = @accountType
              AND symbol = @symbol
              AND interval = @interval
        `);

        this.selectTotalsStmt = this.db.prepare(`
            SELECT
                COUNT(1) AS candleRows,
                COUNT(DISTINCT accountType || ':' || symbol || ':' || interval) AS streamCount
            FROM candles
        `);

        this.selectSyncTotalsStmt = this.db.prepare(`
            SELECT COUNT(1) AS syncRows
            FROM sync_state
        `);
    }

    normalizeKey(accountType = ACCOUNT_TYPES.SPOT, symbol = "", interval = "1m") {
        return {
            accountType: normalizeAccountType(accountType),
            symbol: (symbol || "").toString().trim().toUpperCase(),
            interval: normalizeInterval(interval || "1m"),
        };
    }

    mapCandleRow(row = null) {
        if (!row) {
            return null;
        }
        return {
            openTime: toNumber(row.openTime, 0),
            closeTime: toNumber(row.closeTime, 0),
            open: toNumber(row.open, 0),
            high: toNumber(row.high, 0),
            low: toNumber(row.low, 0),
            close: toNumber(row.close, 0),
            volume: toNumber(row.volume, 0),
            quoteAssetVolume: toNumber(row.quoteAssetVolume, 0),
            trades: toNumber(row.trades, 0),
            takerBuyBaseVolume: toNumber(row.takerBuyBaseVolume, 0),
            takerBuyQuoteVolume: toNumber(row.takerBuyQuoteVolume, 0),
        };
    }

    getCandles(accountType = ACCOUNT_TYPES.SPOT, symbol = "", interval = "1m", startTime = 0, endTime = nowMs()) {
        const key = this.normalizeKey(accountType, symbol, interval);
        if (!key.symbol || !key.interval) {
            return [];
        }
        const safeStart = Math.max(0, Math.floor(toNumber(startTime, 0)));
        const safeEnd = Math.max(safeStart, Math.floor(toNumber(endTime, nowMs())));
        return this.selectCandlesStmt
            .all({
                ...key,
                startTime: safeStart,
                endTime: safeEnd,
            })
            .map((row) => this.mapCandleRow(row))
            .filter((row) => !!row);
    }

    getCoverage(accountType = ACCOUNT_TYPES.SPOT, symbol = "", interval = "1m") {
        const key = this.normalizeKey(accountType, symbol, interval);
        if (!key.symbol || !key.interval) {
            return null;
        }
        const row = this.selectCoverageStmt.get(key);
        if (!row || !toNumber(row.rowCount, 0)) {
            return null;
        }
        return {
            earliestOpenTime: toNumber(row.earliestOpenTime, 0),
            latestOpenTime: toNumber(row.latestOpenTime, 0),
            latestCloseTime: toNumber(row.latestCloseTime, 0),
            rowCount: toNumber(row.rowCount, 0),
        };
    }

    getLatestCandle(accountType = ACCOUNT_TYPES.SPOT, symbol = "", interval = "1m") {
        const key = this.normalizeKey(accountType, symbol, interval);
        if (!key.symbol || !key.interval) {
            return null;
        }
        const row = this.selectLatestForIntervalStmt.get(key);
        return this.mapCandleRow(row);
    }

    getLatestCandleAnyInterval(accountType = ACCOUNT_TYPES.SPOT, symbol = "") {
        const key = this.normalizeKey(accountType, symbol, "1m");
        if (!key.symbol) {
            return null;
        }
        const row = this.selectLatestAnyIntervalStmt.get({
            accountType: key.accountType,
            symbol: key.symbol,
        });
        if (!row) {
            return null;
        }
        const mapped = this.mapCandleRow(row);
        if (!mapped) {
            return null;
        }
        mapped.interval = normalizeInterval(row.interval || "1m");
        return mapped;
    }

    getSyncState(accountType = ACCOUNT_TYPES.SPOT, symbol = "", interval = "1m") {
        const key = this.normalizeKey(accountType, symbol, interval);
        if (!key.symbol || !key.interval) {
            return null;
        }
        const row = this.selectSyncStateStmt.get(key);
        if (!row) {
            return null;
        }
        return {
            ...key,
            earliestOpenTime: toNumber(row.earliestOpenTime, 0),
            latestOpenTime: toNumber(row.latestOpenTime, 0),
            latestCloseTime: toNumber(row.latestCloseTime, 0),
            bannedUntil: toNumber(row.bannedUntil, 0),
            lastError: row.lastError || null,
            updatedAt: toNumber(row.updatedAt, 0),
        };
    }

    setSyncError(accountType = ACCOUNT_TYPES.SPOT, symbol = "", interval = "1m", errorMessage = "", bannedUntil = 0) {
        const key = this.normalizeKey(accountType, symbol, interval);
        if (!key.symbol || !key.interval) {
            return;
        }
        this.updateSyncErrorStmt.run({
            ...key,
            bannedUntil: Math.max(0, Math.floor(toNumber(bannedUntil, 0))),
            lastError: (errorMessage || "").toString().slice(0, 2048),
            updatedAt: nowMs(),
        });
    }

    clearSyncError(accountType = ACCOUNT_TYPES.SPOT, symbol = "", interval = "1m", bannedUntil = 0) {
        const key = this.normalizeKey(accountType, symbol, interval);
        if (!key.symbol || !key.interval) {
            return;
        }
        this.clearSyncErrorStmt.run({
            ...key,
            bannedUntil: Math.max(0, Math.floor(toNumber(bannedUntil, 0))),
            updatedAt: nowMs(),
        });
    }

    upsertCandles(accountType = ACCOUNT_TYPES.SPOT, symbol = "", interval = "1m", candles = []) {
        const key = this.normalizeKey(accountType, symbol, interval);
        if (!key.symbol || !key.interval || !Array.isArray(candles) || !candles.length) {
            return 0;
        }
        let inserted = 0;
        const now = nowMs();
        let minOpen = 0;
        let maxOpen = 0;
        let maxClose = 0;

        const tx = this.db.transaction((rows) => {
            rows.forEach((row) => {
                const openTime = Math.floor(toNumber(row?.openTime, 0));
                const closeTime = Math.floor(toNumber(row?.closeTime, openTime));
                if (openTime <= 0) {
                    return;
                }
                this.insertCandleStmt.run({
                    ...key,
                    openTime,
                    closeTime,
                    open: toNumber(row?.open, 0),
                    high: toNumber(row?.high, 0),
                    low: toNumber(row?.low, 0),
                    close: toNumber(row?.close, 0),
                    volume: toNumber(row?.volume, 0),
                    quoteAssetVolume: toNumber(row?.quoteAssetVolume, 0),
                    trades: Math.floor(toNumber(row?.trades, 0)),
                    takerBuyBaseVolume: toNumber(row?.takerBuyBaseVolume, 0),
                    takerBuyQuoteVolume: toNumber(row?.takerBuyQuoteVolume, 0),
                    updatedAt: now,
                });
                inserted += 1;
                if (!minOpen || openTime < minOpen) {
                    minOpen = openTime;
                }
                if (!maxOpen || openTime > maxOpen) {
                    maxOpen = openTime;
                }
                if (!maxClose || closeTime > maxClose) {
                    maxClose = closeTime;
                }
            });
        });
        tx(candles);

        if (inserted > 0) {
            this.upsertSyncStateStmt.run({
                ...key,
                earliestOpenTime: minOpen,
                latestOpenTime: maxOpen,
                latestCloseTime: maxClose,
                bannedUntil: 0,
                lastError: null,
                updatedAt: now,
            });
            this.clearSyncError(key.accountType, key.symbol, key.interval, 0);
        }
        return inserted;
    }

    getStatus() {
        const totals = this.selectTotalsStmt.get() || {};
        const syncTotals = this.selectSyncTotalsStmt.get() || {};
        let sizeBytes = 0;
        try {
            sizeBytes = fs.statSync(this.filePath)?.size || 0;
        } catch (_e) {}
        return {
            enabled: true,
            path: this.filePath,
            sizeBytes: Math.max(0, Math.floor(toNumber(sizeBytes, 0))),
            candleRows: Math.max(0, Math.floor(toNumber(totals.candleRows, 0))),
            streamCount: Math.max(0, Math.floor(toNumber(totals.streamCount, 0))),
            syncRows: Math.max(0, Math.floor(toNumber(syncTotals.syncRows, 0))),
        };
    }

    close() {
        if (this.db && this.db.open) {
            this.db.close();
        }
    }
}

module.exports = CandleDb;
