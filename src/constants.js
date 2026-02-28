"use strict";

const ACCOUNT_TYPES = {
    SPOT: "SPOT",
    FUTURES: "FUTURES",
};

const DEFAULT_ALLOWED_INTERVALS = [
    "1m",
    "3m",
    "5m",
    "15m",
    "30m",
    "1h",
    "2h",
    "4h",
    "6h",
    "8h",
    "12h",
    "1d",
    "3d",
    "1w",
    "1M",
];

function normalizeInterval(interval = "1m") {
    const raw = (interval || "1m").toString().trim();
    const match = /^(\d+)([mhdwM])$/.exec(raw);
    if (!match) {
        return raw;
    }
    const amount = `${Math.max(1, parseInt(match[1], 10) || 1)}`;
    const unit = match[2] === "M" ? "M" : match[2].toLowerCase();
    return `${amount}${unit}`;
}

function getAllowedIntervals() {
    const envRaw = (process.env.BACKTEST_ALLOWED_INTERVALS || "").toString().trim();
    if (!envRaw) {
        return DEFAULT_ALLOWED_INTERVALS.slice();
    }
    const normalized = envRaw
        .split(",")
        .map((value) => normalizeInterval(value))
        .filter((value) => /^(\d+)([mhdwM])$/.test(value));
    return normalized.length ? [...new Set(normalized)] : DEFAULT_ALLOWED_INTERVALS.slice();
}

function isSupportedInterval(interval = "1m") {
    const normalized = normalizeInterval(interval);
    return getAllowedIntervals().includes(normalized);
}

function intervalToMs(interval = "1m") {
    const normalized = normalizeInterval(interval);
    const match = /^(\d+)([mhdwM])$/.exec(normalized);
    if (!match) {
        return 60 * 1000;
    }
    const amount = Math.max(1, parseInt(match[1], 10) || 1);
    const unit = match[2];
    if (unit === "m") {
        return amount * 60 * 1000;
    }
    if (unit === "h") {
        return amount * 60 * 60 * 1000;
    }
    if (unit === "d") {
        return amount * 24 * 60 * 60 * 1000;
    }
    if (unit === "w") {
        return amount * 7 * 24 * 60 * 60 * 1000;
    }
    return amount * 30 * 24 * 60 * 60 * 1000;
}

const DEFAULTS = {
    interval: "1m",
    allowedIntervals: DEFAULT_ALLOWED_INTERVALS.slice(),
    lookbackDays: 30,
    speedMultiplier: 600,
    stepMsFloor: 5,
    maxKlineLimit: 1500,
    sessionAutostart: false,
};

function normalizeAccountType(accountType = ACCOUNT_TYPES.SPOT) {
    return (accountType || ACCOUNT_TYPES.SPOT).toString().toUpperCase() === ACCOUNT_TYPES.FUTURES
        ? ACCOUNT_TYPES.FUTURES
        : ACCOUNT_TYPES.SPOT;
}

module.exports = {
    ACCOUNT_TYPES,
    DEFAULTS,
    normalizeAccountType,
    normalizeInterval,
    getAllowedIntervals,
    isSupportedInterval,
    intervalToMs,
};
