"use strict";

const express = require("express");
const {
    ACCOUNT_TYPES,
    DEFAULTS,
    normalizeAccountType,
    normalizeInterval,
    isSupportedInterval,
    getAllowedIntervals,
} = require("./constants");
const { clamp, nowMs, toNumber, uniq } = require("./utils");

function normalizeSymbols(input = []) {
    return uniq(
        (Array.isArray(input) ? input : [])
            .map((symbol) => (symbol || "").toString().trim().toUpperCase())
            .filter((symbol) => !!symbol)
    );
}

function normalizeIntervals(input = []) {
    return uniq(
        (Array.isArray(input) ? input : [])
            .map((interval) => normalizeInterval(interval || DEFAULTS.interval))
            .filter((interval) => !!interval && isSupportedInterval(interval))
    );
}

function createRoutes({ binanceClient, candleStore, replayEngine }) {
    const router = express.Router();

    const requireControlToken = (req, res, next) => {
        const expected = (process.env.BACKTEST_CONTROL_TOKEN || "").toString().trim();
        if (!expected) {
            return next();
        }
        const received = (req.headers["x-backtest-token"] || "").toString().trim();
        if (received !== expected) {
            return res.status(401).json({
                success: false,
                message: "Invalid backtest control token.",
            });
        }
        return next();
    };

    router.get("/health", (_req, res) => {
        return res.status(200).json({
            success: true,
            now: nowMs(),
            sessions: replayEngine.getAllStatuses(),
            storage: candleStore.getStorageStatus(),
        });
    });

    router.get("/api/v3/ping", (_req, res) => {
        return res.status(200).json({});
    });

    router.get("/api/v3/time", (_req, res) => {
        return res.status(200).json({
            serverTime: nowMs(),
        });
    });

    router.get("/fapi/v1/time", (_req, res) => {
        return res.status(200).json({
            serverTime: nowMs(),
        });
    });

    router.get("/api/v3/exchangeInfo", async (_req, res) => {
        try {
            const data = await binanceClient.getExchangeInfo(ACCOUNT_TYPES.SPOT);
            return res.status(200).json(data || {});
        } catch (e) {
            return res.status(500).json({
                code: "EXCHANGE_INFO_FAILED",
                msg: e?.message || "Failed to fetch spot exchange info.",
            });
        }
    });

    router.get("/fapi/v1/exchangeInfo", async (_req, res) => {
        try {
            const data = await binanceClient.getExchangeInfo(ACCOUNT_TYPES.FUTURES);
            return res.status(200).json(data || {});
        } catch (e) {
            return res.status(500).json({
                code: "EXCHANGE_INFO_FAILED",
                msg: e?.message || "Failed to fetch futures exchange info.",
            });
        }
    });

    const handleKlines = async (req, res, accountType) => {
        try {
            const symbol = (req.query.symbol || "").toString().toUpperCase();
            const interval = normalizeInterval(req.query.interval || DEFAULTS.interval);
            if (!symbol) {
                return res.status(400).json({
                    code: "BAD_SYMBOL",
                    msg: "symbol is required",
                });
            }
            if (!isSupportedInterval(interval)) {
                return res.status(400).json({
                    code: "UNSUPPORTED_INTERVAL",
                    msg: `Unsupported interval. Allowed: ${getAllowedIntervals().join(", ")}`,
                });
            }
            if (replayEngine.isRunning(accountType)) {
                await replayEngine.ensureStream({
                    accountType,
                    symbol,
                    interval,
                    loadNow: false,
                });
            }

            const requestedStartTime = toNumber(req.query.startTime, 0) || undefined;
            let requestedEndTime = toNumber(req.query.endTime, 0) || undefined;
            const replayVisibleEndMs = replayEngine.getSessionVisibleEndTimeMs(accountType, symbol, interval);
            if (typeof replayVisibleEndMs === "number") {
                if (replayVisibleEndMs < 0) {
                    return res.status(200).json([]);
                }
                if (!requestedEndTime || requestedEndTime > replayVisibleEndMs) {
                    requestedEndTime = replayVisibleEndMs;
                }
                if (requestedStartTime && requestedStartTime > replayVisibleEndMs) {
                    return res.status(200).json([]);
                }
            }

            const rows = await candleStore.getKlines(
                {
                    symbol,
                    interval,
                    limit: clamp(toNumber(req.query.limit, 1000), 1, DEFAULTS.maxKlineLimit),
                    startTime: requestedStartTime,
                    endTime: requestedEndTime,
                },
                accountType
            );
            return res.status(200).json(rows || []);
        } catch (e) {
            return res.status(500).json({
                code: "KLINES_FAILED",
                msg: e?.message || "Failed to fetch klines",
            });
        }
    };

    router.get("/api/v3/klines", async (req, res) => {
        return await handleKlines(req, res, ACCOUNT_TYPES.SPOT);
    });

    router.get("/fapi/v1/klines", async (req, res) => {
        return await handleKlines(req, res, ACCOUNT_TYPES.FUTURES);
    });

    const handleBookTicker = async (req, res, accountType) => {
        try {
            const symbol = (req.query.symbol || "").toString().toUpperCase();
            const normalized = normalizeAccountType(accountType);
            const resolveTicker = (tickerSymbol) => {
                const latest = replayEngine.getLatestPrice(tickerSymbol, normalized);
                if (latest) {
                    return {
                        symbol: tickerSymbol,
                        bidPrice: `${latest.bidPrice}`,
                        bidQty: "100",
                        askPrice: `${latest.askPrice}`,
                        askQty: "100",
                    };
                }
                const fallback = candleStore.getLatestCloseAnyInterval(tickerSymbol, normalized) ||
                    candleStore.getLatestClose(tickerSymbol, normalized, DEFAULTS.interval);
                if (!fallback) {
                    return null;
                }
                return {
                    symbol: tickerSymbol,
                    bidPrice: `${fallback.close}`,
                    bidQty: "100",
                    askPrice: `${fallback.close}`,
                    askQty: "100",
                };
            };

            if (symbol) {
                const ticker = resolveTicker(symbol);
                if (ticker) {
                    return res.status(200).json(ticker);
                }
                const live = await binanceClient.getBookTicker(symbol, normalized);
                return res.status(200).json(live || {});
            }

            const status = replayEngine.getStatus(normalized);
            const symbols = normalizeSymbols(status.symbols || []);
            if (!symbols.length) {
                const live = await binanceClient.getBookTicker("", normalized);
                return res.status(200).json(live || []);
            }
            const rows = symbols
                .map((tickerSymbol) => resolveTicker(tickerSymbol))
                .filter((row) => !!row);
            return res.status(200).json(rows);
        } catch (e) {
            return res.status(500).json({
                code: "BOOK_TICKER_FAILED",
                msg: e?.message || "Failed to fetch bookTicker",
            });
        }
    };

    router.get("/api/v3/ticker/bookTicker", async (req, res) => {
        return await handleBookTicker(req, res, ACCOUNT_TYPES.SPOT);
    });

    router.get("/fapi/v1/ticker/bookTicker", async (req, res) => {
        return await handleBookTicker(req, res, ACCOUNT_TYPES.FUTURES);
    });

    router.get("/backtest/session/status", requireControlToken, (req, res) => {
        const accountType = normalizeAccountType(req.query.accountType || ACCOUNT_TYPES.FUTURES);
        return res.status(200).json({
            success: true,
            data: replayEngine.getStatus(accountType),
        });
    });

    router.get("/backtest/storage/status", requireControlToken, (req, res) => {
        return res.status(200).json({
            success: true,
            data: candleStore.getStorageStatus(),
        });
    });

    router.get("/backtest/storage/sync", requireControlToken, (req, res) => {
        const accountType = normalizeAccountType(req.query.accountType || ACCOUNT_TYPES.FUTURES);
        const symbol = (req.query.symbol || "").toString().trim().toUpperCase();
        const interval = normalizeInterval(req.query.interval || DEFAULTS.interval);
        if (!symbol) {
            return res.status(400).json({
                success: false,
                message: "symbol is required",
            });
        }
        return res.status(200).json({
            success: true,
            data: {
                accountType,
                symbol,
                interval,
                coverage: candleStore.getCoverage(accountType, symbol, interval),
                sync: candleStore.getSyncState(accountType, symbol, interval),
            },
        });
    });

    router.post("/backtest/session/start", requireControlToken, async (req, res) => {
        try {
            const body = req.body || {};
            const accountType = normalizeAccountType(body.accountType || ACCOUNT_TYPES.FUTURES);
            const speedMultiplier = Math.max(1, Math.floor(toNumber(body.speedMultiplier, DEFAULTS.speedMultiplier)));
            const lookbackDays = Math.max(1, Math.floor(toNumber(body.lookbackDays, DEFAULTS.lookbackDays)));
            const symbols = normalizeSymbols(body.symbols || []);
            const requestedIntervals = normalizeIntervals(body.intervals || []);
            const fallbackInterval = normalizeInterval(body.interval || DEFAULTS.interval);
            const intervals = requestedIntervals.length
                ? requestedIntervals
                : isSupportedInterval(fallbackInterval)
                    ? [fallbackInterval]
                    : [DEFAULTS.interval];
            // eslint-disable-next-line no-console
            console.log(
                `[backtesting][${accountType}] control:start requested ` +
                    `(lookbackDays=${lookbackDays}, speed=${speedMultiplier}x, symbols=${symbols.length}, intervals=${intervals.join(",")})`
            );
            const status = await replayEngine.start({
                accountType,
                speedMultiplier,
                lookbackDays,
            });
            let activatedStreams = 0;
            if (symbols.length) {
                for (const symbol of symbols) {
                    for (const interval of intervals) {
                        await replayEngine.ensureStream({
                            accountType,
                            symbol,
                            interval,
                            loadNow: false,
                        });
                        activatedStreams += 1;
                    }
                }
            }
            // eslint-disable-next-line no-console
            console.log(
                `[backtesting][${accountType}] control:start stream activation done ` +
                    `(activatedStreams=${activatedStreams}, symbols=${symbols.length}, intervals=${intervals.length})`
            );
            return res.status(200).json({
                success: true,
                message: "Backtest replay started.",
                data: {
                    ...status,
                    requestedUniverse: {
                        symbols,
                        intervals,
                        activatedStreams,
                    },
                },
            });
        } catch (e) {
            return res.status(500).json({
                success: false,
                message: e?.message || "Failed to start replay session.",
            });
        }
    });

    router.post("/backtest/session/stop", requireControlToken, (req, res) => {
        try {
            const body = req.body || {};
            const accountType = normalizeAccountType(body.accountType || ACCOUNT_TYPES.FUTURES);
            // eslint-disable-next-line no-console
            console.log(`[backtesting][${accountType}] control:stop requested`);
            const status = replayEngine.stop(accountType);
            return res.status(200).json({
                success: true,
                message: "Backtest replay stopped.",
                data: status,
            });
        } catch (e) {
            return res.status(500).json({
                success: false,
                message: e?.message || "Failed to stop replay session.",
            });
        }
    });

    return router;
}

module.exports = createRoutes;
