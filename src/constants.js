"use strict";

const ACCOUNT_TYPES = {
    SPOT: "SPOT",
    FUTURES: "FUTURES",
};

const DEFAULTS = {
    interval: "1m",
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
};
