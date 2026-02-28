"use strict";

function toNumber(value, fallback = 0) {
    const parsed = parseFloat(value);
    return Number.isFinite(parsed) ? parsed : fallback;
}

function uniq(values = []) {
    return [...new Set((values || []).filter((value) => !!value))];
}

function chunk(values = [], size = 1000) {
    const safeSize = Math.max(1, Math.floor(toNumber(size, 1000)));
    const out = [];
    for (let i = 0; i < values.length; i += safeSize) {
        out.push(values.slice(i, i + safeSize));
    }
    return out;
}

function clamp(value, min, max) {
    const n = toNumber(value, min);
    return Math.max(min, Math.min(max, n));
}

function nowMs() {
    return Date.now();
}

module.exports = {
    toNumber,
    uniq,
    chunk,
    clamp,
    nowMs,
};
