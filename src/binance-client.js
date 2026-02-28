"use strict";

const axios = require("axios");
const { ACCOUNT_TYPES, normalizeAccountType } = require("./constants");

class BinanceClient {
    constructor() {
        this.http = axios.create({
            timeout: parseInt(process.env.BACKTEST_BINANCE_TIMEOUT_MS || "15000", 10),
        });
        this.spotApiBase = (process.env.BACKTEST_BINANCE_SPOT_API || "https://api.binance.com/api/v3").replace(/\/+$/, "");
        this.futuresApiBase = (process.env.BACKTEST_BINANCE_FUTURES_API || "https://fapi.binance.com/fapi/v1").replace(/\/+$/, "");
        this.exchangeInfoCache = {
            SPOT: null,
            FUTURES: null,
        };
    }

    getApiBase(accountType = ACCOUNT_TYPES.SPOT) {
        const normalized = normalizeAccountType(accountType);
        return normalized === ACCOUNT_TYPES.FUTURES ? this.futuresApiBase : this.spotApiBase;
    }

    async get(path, params = {}, accountType = ACCOUNT_TYPES.SPOT) {
        const base = this.getApiBase(accountType);
        const url = `${base}${path}`;
        const response = await this.http.get(url, { params });
        return response.data;
    }

    async ping() {
        return await this.get("/ping", {}, ACCOUNT_TYPES.SPOT);
    }

    async getServerTime(accountType = ACCOUNT_TYPES.SPOT) {
        return await this.get("/time", {}, accountType);
    }

    async getExchangeInfo(accountType = ACCOUNT_TYPES.SPOT) {
        const normalized = normalizeAccountType(accountType);
        if (this.exchangeInfoCache[normalized]) {
            return this.exchangeInfoCache[normalized];
        }
        const data = await this.get("/exchangeInfo", {}, normalized);
        this.exchangeInfoCache[normalized] = data;
        return data;
    }

    async getBookTicker(symbol, accountType = ACCOUNT_TYPES.SPOT) {
        const params = symbol ? { symbol: symbol.toUpperCase() } : {};
        return await this.get("/ticker/bookTicker", params, accountType);
    }

    async getKlines(params = {}, accountType = ACCOUNT_TYPES.SPOT) {
        return await this.get("/klines", params, accountType);
    }
}

module.exports = BinanceClient;
