/**
 * 台股雷達 Stock Radar — app.js
 * Version: V0.1
 * Data sources: FinMind API + TWSE Open API
 */

/* ============================================================
   CONFIGURATION
   ============================================================ */
const APP_VERSION = 'V0.1';

const CONFIG = {
  FINMIND_BASE: 'https://api.finmindtrade.com/api/v4/data',
  TWSE_BASE: 'https://openapi.twse.com.tw/v1',
  TAIEX_ID: 'Y9999',        // FinMind TAIEX index code
  CACHE_TTL_PRICE: 30 * 60 * 1000,   // 30 minutes
  CACHE_TTL_REVENUE: 6 * 60 * 60 * 1000,   // 6 hours
  CACHE_TTL_FINANCIAL: 24 * 60 * 60 * 1000, // 24 hours
  RATE_LIMIT_DELAY: 300,    // ms between API calls
};

/* ============================================================
   STATE
   ============================================================ */
let state = {
  theme: localStorage.getItem('theme') || 'dark',
  finmindToken: localStorage.getItem('finmindToken') || '',
  watchlist: JSON.parse(localStorage.getItem('watchlist') || '{"預設":{"stocks":[]}}'),
  currentPage: 'screen',
  results: [],
  isLoading: false,
  lastUpdate: null,
  apiCache: {},

  filters: {
    // Technical
    rs90: false,
    nearMonthlyHigh: false,
    shortMAAlign: false,
    longMAAlign: false,
    aboveSubPoint: false,
    // Fundamental
    revenueHighRecord: false,
    revenueYoY: false,
    revenueMoM: false,
    marginGrowth: false,
    noProfitLoss: false,
    // Chip
    chipConcentration: false,
    buyerSellerDiff: false,
    foreignBuy: false,
    trustBuy: false,
    bigHolderIncrease: false,
    institutionalRecord: false,
  },

  querySource: 'watchlist', // 'watchlist' | 'manual'
  manualStocks: '',
};

/* ============================================================
   UTILITIES
   ============================================================ */
function formatDate(d) {
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, '0');
  const dd = String(d.getDate()).padStart(2, '0');
  return `${y}-${m}-${dd}`;
}

function formatDateTime(d) {
  const yy = String(d.getFullYear()).slice(2);
  const m  = String(d.getMonth() + 1).padStart(2, '0');
  const dd = String(d.getDate()).padStart(2, '0');
  const hh = String(d.getHours()).padStart(2, '0');
  const mm = String(d.getMinutes()).padStart(2, '0');
  return `${yy}/${m}/${dd} ${hh}:${mm}`;
}

function dateMonthsAgo(n) {
  const d = new Date();
  d.setMonth(d.getMonth() - n);
  return d;
}

function average(arr) {
  if (!arr || arr.length === 0) return 0;
  return arr.reduce((a, b) => a + b, 0) / arr.length;
}

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

function pct(val) {
  if (val === null || val === undefined || isNaN(val)) return '--';
  const sign = val >= 0 ? '+' : '';
  return `${sign}${val.toFixed(2)}%`;
}

function numStr(val, dec = 2) {
  if (val === null || val === undefined || isNaN(val)) return '--';
  return Number(val).toFixed(dec);
}

function getActiveFilters() {
  return Object.entries(state.filters).filter(([, v]) => v).map(([k]) => k);
}

/* ============================================================
   CACHE MANAGER
   ============================================================ */
const Cache = {
  get(key) {
    const stored = localStorage.getItem(`cache_${key}`);
    if (!stored) return null;
    const { ts, data, ttl } = JSON.parse(stored);
    if (Date.now() - ts > ttl) { localStorage.removeItem(`cache_${key}`); return null; }
    return data;
  },
  set(key, data, ttl) {
    try {
      localStorage.setItem(`cache_${key}`, JSON.stringify({ ts: Date.now(), data, ttl }));
    } catch(e) { /* storage full — ignore */ }
  },
  clear(prefix = '') {
    Object.keys(localStorage).filter(k => k.startsWith(`cache_${prefix}`)).forEach(k => localStorage.removeItem(k));
  }
};

/* ============================================================
   API SERVICE
   ============================================================ */
const API = {
  async finmind(dataset, dataId, startDate, endDate = null) {
    if (!state.finmindToken) throw new Error('請先在【設定】頁輸入 FinMind API Token');
    const cacheKey = `fm_${dataset}_${dataId}_${startDate}`;
    const cached = Cache.get(cacheKey);
    if (cached) return cached;

    const params = new URLSearchParams({ dataset, data_id: dataId, start_date: startDate, token: state.finmindToken });
    if (endDate) params.append('end_date', endDate);

    const res = await fetch(`${CONFIG.FINMIND_BASE}?${params}`);
    if (!res.ok) throw new Error(`FinMind HTTP ${res.status}`);
    const json = await res.json();
    if (json.status !== 200) throw new Error(json.msg || `FinMind 錯誤 (${dataset})`);

    const ttl = dataset.includes('Price') ? CONFIG.CACHE_TTL_PRICE :
                dataset.includes('Revenue') ? CONFIG.CACHE_TTL_REVENUE : CONFIG.CACHE_TTL_FINANCIAL;
    Cache.set(cacheKey, json.data, ttl);
    return json.data;
  },

  async twseDayAll() {
    const cacheKey = 'twse_dayall';
    const cached = Cache.get(cacheKey);
    if (cached) return cached;
    const res = await fetch(`${CONFIG.TWSE_BASE}/exchangeReport/STOCK_DAY_ALL`);
    if (!res.ok) throw new Error(`TWSE HTTP ${res.status}`);
    const json = await res.json();
    Cache.set(cacheKey, json, CONFIG.CACHE_TTL_PRICE);
    return json;
  },

  async twseInstitutional() {
    const cacheKey = 'twse_t86';
    const cached = Cache.get(cacheKey);
    if (cached) return cached;
    const res = await fetch(`${CONFIG.TWSE_BASE}/fund/T86`);
    if (!res.ok) return [];
    const json = await res.json();
    Cache.set(cacheKey, json, CONFIG.CACHE_TTL_PRICE);
    return json;
  },

  async getStockName(stockId) {
    try {
      const all = await this.twseDayAll();
      const found = all.find(s => s.Code === stockId);
      return found ? found.Name : stockId;
    } catch { return stockId; }
  },

  async testToken(token) {
    const params = new URLSearchParams({
      dataset: 'TaiwanStockPrice',
      data_id: '2330',
      start_date: formatDate(dateMonthsAgo(1)),
      token
    });
    const res = await fetch(`${CONFIG.FINMIND_BASE}?${params}`);
    const json = await res.json();
    return json.status === 200;
  }
};

/* ============================================================
   SCREENING ENGINE
   ============================================================ */
const Screener = {

  async getPriceData(stockId) {
    const startDate = formatDate(dateMonthsAgo(14)); // ~14 months for MA240+
    const raw = await API.finmind('TaiwanStockPrice', stockId, startDate);
    return raw.sort((a, b) => b.date.localeCompare(a.date)); // newest first
  },

  async getTAIEXData() {
    const cacheKey = 'taiex_26w';
    const cached = Cache.get(cacheKey);
    if (cached) return cached;
    const startDate = formatDate(dateMonthsAgo(7));
    const raw = await API.finmind('TaiwanStockPrice', 'Y9999', startDate);
    const sorted = raw.sort((a, b) => b.date.localeCompare(a.date));
    Cache.set(cacheKey, sorted, CONFIG.CACHE_TTL_PRICE);
    return sorted;
  },

  calcRS(stockPrices, taixPrices) {
    const period = Math.min(130, stockPrices.length - 1, taixPrices.length - 1);
    if (period < 20) return null;
    const stockNow   = parseFloat(stockPrices[0].close);
    const stockPrev  = parseFloat(stockPrices[period].close);
    const taixNow    = parseFloat(taixPrices[0].close);
    const taixPrev   = parseFloat(taixPrices[period].close);
    if (!stockPrev || !taixPrev || taixPrev === 0) return null;
    const stockRet = (stockNow - stockPrev) / stockPrev;
    const taixRet  = (taixNow  - taixPrev)  / taixPrev;
    const ratio = (1 + stockRet) / (1 + Math.max(taixRet, -0.99));
    // Map ratio: 0.5→0, 1.0→50, 2.0→100
    return Math.max(0, Math.min(100, Math.round((ratio - 0.5) * 100)));
  },

  calcMAs(prices) {
    const closes = prices.map(d => parseFloat(d.close));
    return {
      ma5:   closes.length >= 5   ? average(closes.slice(0, 5))   : null,
      ma10:  closes.length >= 10  ? average(closes.slice(0, 10))  : null,
      ma20:  closes.length >= 20  ? average(closes.slice(0, 20))  : null,
      ma60:  closes.length >= 60  ? average(closes.slice(0, 60))  : null,
      ma120: closes.length >= 120 ? average(closes.slice(0, 120)) : null,
      ma240: closes.length >= 240 ? average(closes.slice(0, 240)) : null,
    };
  },

  checkShortMAAlign(ma) {
    if (!ma.ma5 || !ma.ma10 || !ma.ma20) return null;
    return ma.ma5 > ma.ma10 && ma.ma10 > ma.ma20;
  },

  checkLongMAAlign(ma) {
    if (!ma.ma20 || !ma.ma60 || !ma.ma120) return null;
    if (!ma.ma240) return ma.ma20 > ma.ma60 && ma.ma60 > ma.ma120; // partial
    return ma.ma20 > ma.ma60 && ma.ma60 > ma.ma120 && ma.ma120 > ma.ma240;
  },

  checkAboveSubPoint(prices) {
    if (prices.length < 61) return null;
    const current = parseFloat(prices[0].close);
    const p20 = parseFloat(prices[19].close);
    const p60 = parseFloat(prices[59].close);
    return current > p20 && current > p60;
  },

  distanceFromMonthlyHigh(prices) {
    if (prices.length < 1) return null;
    const lookback = Math.min(22, prices.length);
    const monthlyHigh = Math.max(...prices.slice(0, lookback).map(d => parseFloat(d.max || d.close)));
    const current = parseFloat(prices[0].close);
    return ((current / monthlyHigh) - 1) * 100;
  },

  // Revenue checks
  async getRevenueData(stockId) {
    const startDate = formatDate(dateMonthsAgo(24));
    return await API.finmind('TaiwanStockMonthRevenue', stockId, startDate);
  },

  analyzeRevenue(revenueData) {
    if (!revenueData || revenueData.length < 3) return null;
    const sorted = [...revenueData].sort((a, b) => b.date.localeCompare(a.date));

    // Latest month
    const latest = sorted[0];
    const latestRev = parseFloat(latest.revenue);

    // YoY for latest 2 months
    const yoyResults = [];
    for (let i = 0; i < Math.min(2, sorted.length); i++) {
      const cur = sorted[i];
      const curDate = new Date(cur.date);
      const prevYear = sorted.find(d => {
        const dd = new Date(d.date);
        return dd.getMonth() === curDate.getMonth() && dd.getFullYear() === curDate.getFullYear() - 1;
      });
      if (prevYear) {
        const yoy = (parseFloat(cur.revenue) - parseFloat(prevYear.revenue)) / parseFloat(prevYear.revenue);
        yoyResults.push(yoy);
      }
    }

    // MoM for latest 2 months
    const momResults = [];
    for (let i = 0; i < Math.min(2, sorted.length - 1); i++) {
      const cur = parseFloat(sorted[i].revenue);
      const prev = parseFloat(sorted[i + 1].revenue);
      if (prev > 0) momResults.push((cur - prev) / prev);
    }

    // Historical high
    const allRevenues = sorted.map(d => parseFloat(d.revenue));
    const isHistoricalHigh = latestRev >= Math.max(...allRevenues);

    // Same period last year
    const sameMonthLastYear = sorted.find(d => {
      const dd = new Date(d.date);
      const ld = new Date(latest.date);
      return dd.getMonth() === ld.getMonth() && dd.getFullYear() === ld.getFullYear() - 1;
    });
    const isSamePeriodHigh = sameMonthLastYear
      ? latestRev > parseFloat(sameMonthLastYear.revenue)
      : null;

    return {
      latest: latestRev / 1e8, // Convert to 億
      latestDate: latest.date,
      yoy2mo: yoyResults.length === 2 && yoyResults.every(v => v >= 0.20),
      mom2mo: momResults.length === 2 && momResults.every(v => v >= 0.20),
      yoyLatest: yoyResults[0] !== undefined ? yoyResults[0] * 100 : null,
      momLatest: momResults[0] !== undefined ? momResults[0] * 100 : null,
      isHistoricalHigh,
      isSamePeriodHigh,
      revenueHighRecord: isHistoricalHigh || isSamePeriodHigh,
    };
  },

  // Financial statements
  async getFinancialData(stockId) {
    const startDate = formatDate(dateMonthsAgo(18));
    try {
      return await API.finmind('TaiwanStockFinancialStatements', stockId, startDate);
    } catch { return null; }
  },

  analyzeFinancials(data) {
    if (!data || data.length < 2) return null;
    const sorted = [...data].sort((a, b) => b.date.localeCompare(a.date));

    // Latest and YoY comparison
    const latest = sorted[0];
    const lastYearSameQ = sorted.find(d => {
      const ld = new Date(latest.date);
      const dd = new Date(d.date);
      return dd.getMonth() === ld.getMonth() && dd.getFullYear() === ld.getFullYear() - 1;
    });

    const getVal = (row, type) => {
      const item = data.filter(d => d.date === row.date && d.type === type);
      return item.length ? parseFloat(item[0].value) : null;
    };

    // Try to get gross margin, operating margin from the data
    const latestGross = getVal(latest, 'GrossProfit') || getVal(latest, 'gross_profit');
    const latestRev   = getVal(latest, 'Revenue') || getVal(latest, 'revenue');
    const latestOp    = getVal(latest, 'OperatingIncome') || getVal(latest, 'operating_income');
    const latestNetIncome = getVal(latest, 'NetIncome') || getVal(latest, 'net_income');

    const grossMargin = latestRev && latestGross ? (latestGross / latestRev) * 100 : null;
    const opMargin    = latestRev && latestOp    ? (latestOp    / latestRev) * 100 : null;
    const noProfitLoss = latestNetIncome !== null ? latestNetIncome > 0 : null;

    // Compare to last year
    let grossMarginGrowth = null, opMarginGrowth = null;
    if (lastYearSameQ) {
      const lyGross = getVal(lastYearSameQ, 'GrossProfit') || getVal(lastYearSameQ, 'gross_profit');
      const lyRev   = getVal(lastYearSameQ, 'Revenue') || getVal(lastYearSameQ, 'revenue');
      const lyOp    = getVal(lastYearSameQ, 'OperatingIncome') || getVal(lastYearSameQ, 'operating_income');
      const lyGM = lyRev && lyGross ? (lyGross / lyRev) * 100 : null;
      const lyOM = lyRev && lyOp    ? (lyOp    / lyRev) * 100 : null;
      if (grossMargin !== null && lyGM !== null) grossMarginGrowth = grossMargin > lyGM;
      if (opMargin !== null && lyOM !== null) opMarginGrowth = opMargin > lyOM;
    }

    return {
      grossMargin,
      opMargin,
      noProfitLoss,
      marginGrowth: (grossMarginGrowth !== null && opMarginGrowth !== null)
        ? (grossMarginGrowth && opMarginGrowth) : null,
    };
  },

  // Institutional investors
  async getInstitutionalData(stockId) {
    const startDate = formatDate(dateMonthsAgo(1));
    try {
      return await API.finmind('TaiwanStockInstitutionalInvestors', stockId, startDate);
    } catch { return null; }
  },

  analyzeInstitutional(data) {
    if (!data || data.length === 0) return null;
    const sorted = [...data].sort((a, b) => b.date.localeCompare(a.date));

    // Sum last 5 trading days
    const recent = sorted.slice(0, 5);
    let foreignNet = 0, trustNet = 0;
    for (const row of recent) {
      if (row.name === '外陸資買賣超股數(不含外資自營商)' || row.name.includes('外資') || row.name === 'Foreign_Investor') {
        foreignNet += parseFloat(row.buy || 0) - parseFloat(row.sell || 0);
      }
      if (row.name.includes('投信') || row.name === 'Investment_Trust') {
        trustNet += parseFloat(row.buy || 0) - parseFloat(row.sell || 0);
      }
    }

    // Historical high for institutional holdings (3 months)
    const startDate3m = formatDate(dateMonthsAgo(3));
    const institutionalData3m = data.filter(d => d.date >= startDate3m);
    const dailySums = {};
    for (const row of institutionalData3m) {
      if (!dailySums[row.date]) dailySums[row.date] = 0;
      dailySums[row.date] += parseFloat(row.buy || 0) - parseFloat(row.sell || 0);
    }
    const dailyVals = Object.entries(dailySums).sort((a, b) => b[0].localeCompare(a[0]));
    const recentSum = dailyVals.slice(0, 5).reduce((a, [, v]) => a + v, 0);
    const quarterMax = dailyVals.length > 0 ? Math.max(...dailyVals.map(([, v]) => v)) : 0;
    const institutionalRecord = recentSum >= quarterMax && dailyVals.length > 10;

    return {
      foreignNet,          // shares, 5-day net
      trustNet,            // shares, 5-day net
      foreignBuy: foreignNet > 0,
      trustBuy: trustNet > 0,
      institutionalRecord,
    };
  },

  // Shareholder structure
  async getShareholderData(stockId) {
    const startDate = formatDate(dateMonthsAgo(3));
    try {
      return await API.finmind('TaiwanStockShareholderStructure', stockId, startDate);
    } catch { return null; }
  },

  analyzeShareholderStructure(data) {
    if (!data || data.length < 2) return null;
    const sorted = [...data].sort((a, b) => b.date.localeCompare(a.date));

    // Concentration = % held by top holders (>1000 shares bracket)
    const getConcentration = (row) => {
      // Sum holders with > 400 shares (大戶)
      // FinMind data has level fields for shareholder count distribution
      // Try to calculate big holder ratio
      const total = parseFloat(row.total_shares || row.TotalShares || 0);
      // If we have level data, pick the high end
      // This is simplified — actual calculation depends on data structure
      return total;
    };

    const latest = sorted[0];
    const prev   = sorted[sorted.length > 4 ? 4 : sorted.length - 1];

    // Big holders: sum shares held by accounts with >400 張
    // FinMind structure varies; we use available fields
    const bigHolderPct = parseFloat(latest.percent_above_1000 || latest.HoldingSharesRatio || 0);
    const bigHolderPrevPct = parseFloat(prev.percent_above_1000 || prev.HoldingSharesRatio || 0);

    return {
      bigHolderPct,
      bigHolderIncrease: bigHolderPct > bigHolderPrevPct,
      // Concentration increase means fewer holders hold more shares
      chipConcentration: bigHolderPct > bigHolderPrevPct,
    };
  },

  /* Main screening function for one stock */
  async screenOne(stockId, stockName, taixPrices, progress) {
    const result = {
      stockId, stockName,
      error: null,
      price: null, priceChange: null,
      rsScore: null,
      distanceFromHigh: null,
      shortMAAlign: null, longMAAlign: null, aboveSubPoint: null,
      revenue: null, revenueDate: null,
      yoyLatest: null, momLatest: null,
      revenueHighRecord: null, yoy2mo: null, mom2mo: null,
      grossMargin: null, opMargin: null,
      marginGrowth: null, noProfitLoss: null,
      foreignNet: null, trustNet: null,
      foreignBuy: null, trustBuy: null, institutionalRecord: null,
      bigHolderPct: null, bigHolderIncrease: null, chipConcentration: null,
      buyerSellerDiff: null, // requires specialized data — marked N/A in v0.1
      passCount: 0, failCount: 0,
    };

    const activeFilters = getActiveFilters();

    try {
      // Price data (needed for most technical indicators)
      const needsPrice = activeFilters.some(f => ['rs90','nearMonthlyHigh','shortMAAlign','longMAAlign','aboveSubPoint'].includes(f));
      let priceData = [];
      if (needsPrice || activeFilters.length === 0) {
        progress(`正在取得 ${stockId} 股價資料...`);
        priceData = await this.getPriceData(stockId);
        await sleep(CONFIG.RATE_LIMIT_DELAY);
      }

      if (priceData.length > 0) {
        result.price = parseFloat(priceData[0].close);
        const prevClose = priceData.length > 1 ? parseFloat(priceData[1].close) : result.price;
        result.priceChange = ((result.price - prevClose) / prevClose) * 100;
      }

      // Technical: RS
      if (priceData.length > 0) {
        if (taixPrices && taixPrices.length > 0) {
          result.rsScore = this.calcRS(priceData, taixPrices);
        }
        // MA calculations
        const ma = this.calcMAs(priceData);
        result.shortMAAlign   = this.checkShortMAAlign(ma);
        result.longMAAlign    = this.checkLongMAAlign(ma);
        result.aboveSubPoint  = this.checkAboveSubPoint(priceData);
        result.distanceFromHigh = this.distanceFromMonthlyHigh(priceData);
        result.ma = ma;
      }

      // Revenue data
      const needsRevenue = activeFilters.some(f => ['revenueHighRecord','revenueYoY','revenueMoM'].includes(f));
      if (needsRevenue || activeFilters.length === 0) {
        progress(`正在取得 ${stockId} 營收資料...`);
        try {
          const revData = await this.getRevenueData(stockId);
          const revAnalysis = this.analyzeRevenue(revData);
          if (revAnalysis) Object.assign(result, revAnalysis);
          await sleep(CONFIG.RATE_LIMIT_DELAY);
        } catch(e) { /* continue */ }
      }

      // Financial statements
      const needsFinancial = activeFilters.some(f => ['marginGrowth','noProfitLoss'].includes(f));
      if (needsFinancial || activeFilters.length === 0) {
        progress(`正在取得 ${stockId} 財報資料...`);
        try {
          const finData = await this.getFinancialData(stockId);
          if (finData) {
            const finAnalysis = this.analyzeFinancials(finData);
            if (finAnalysis) Object.assign(result, finAnalysis);
          }
          await sleep(CONFIG.RATE_LIMIT_DELAY);
        } catch(e) { /* continue */ }
      }

      // Institutional investors
      const needsInstitutional = activeFilters.some(f => ['foreignBuy','trustBuy','institutionalRecord'].includes(f));
      if (needsInstitutional || activeFilters.length === 0) {
        progress(`正在取得 ${stockId} 法人資料...`);
        try {
          const instData = await this.getInstitutionalData(stockId);
          if (instData) {
            const instAnalysis = this.analyzeInstitutional(instData);
            if (instAnalysis) Object.assign(result, instAnalysis);
          }
          await sleep(CONFIG.RATE_LIMIT_DELAY);
        } catch(e) { /* continue */ }
      }

      // Shareholder structure
      const needsShareholder = activeFilters.some(f => ['chipConcentration','bigHolderIncrease'].includes(f));
      if (needsShareholder || activeFilters.length === 0) {
        progress(`正在取得 ${stockId} 籌碼資料...`);
        try {
          const shData = await this.getShareholderData(stockId);
          if (shData) {
            const shAnalysis = this.analyzeShareholderStructure(shData);
            if (shAnalysis) Object.assign(result, shAnalysis);
          }
          await sleep(CONFIG.RATE_LIMIT_DELAY);
        } catch(e) { /* continue */ }
      }

      // Apply filter evaluation
      const filterChecks = {
        rs90:             result.rsScore !== null ? result.rsScore >= 90 : null,
        nearMonthlyHigh:  result.distanceFromHigh !== null ? result.distanceFromHigh >= -5 : null,
        shortMAAlign:     result.shortMAAlign,
        longMAAlign:      result.longMAAlign,
        aboveSubPoint:    result.aboveSubPoint,
        revenueHighRecord: result.revenueHighRecord,
        revenueYoY:       result.yoy2mo,
        revenueMoM:       result.mom2mo,
        marginGrowth:     result.marginGrowth,
        noProfitLoss:     result.noProfitLoss,
        chipConcentration: result.chipConcentration,
        buyerSellerDiff:  null, // N/A in v0.1
        foreignBuy:       result.foreignBuy,
        trustBuy:         result.trustBuy,
        bigHolderIncrease: result.bigHolderIncrease,
        institutionalRecord: result.institutionalRecord,
      };

      result.filterChecks = filterChecks;
      result.passCount = activeFilters.filter(f => filterChecks[f] === true).length;
      result.failCount = activeFilters.filter(f => filterChecks[f] === false).length;

      // Does this stock pass ALL active filters?
      result.passAll = activeFilters.length === 0 || activeFilters.every(f => filterChecks[f] !== false);

    } catch(e) {
      result.error = e.message;
    }

    return result;
  },

  /* Collect all stock IDs to screen */
  getStockIds() {
    if (state.querySource === 'manual') {
      return [...new Set(
        state.manualStocks.split(/[\n,\s]+/).map(s => s.trim()).filter(s => /^\d{4,5}$/.test(s))
      )];
    }
    // From watchlist
    const ids = [];
    for (const cat of Object.values(state.watchlist)) {
      ids.push(...(cat.stocks || []));
    }
    return [...new Set(ids)];
  }
};

/* ============================================================
   WATCHLIST MANAGER
   ============================================================ */
const WL = {
  save() {
    localStorage.setItem('watchlist', JSON.stringify(state.watchlist));
  },

  getCategories() { return Object.keys(state.watchlist); },

  addCategory(name) {
    if (!name || state.watchlist[name]) return false;
    state.watchlist[name] = { stocks: [] };
    this.save();
    return true;
  },

  removeCategory(name) {
    if (name === '預設') return false;
    delete state.watchlist[name];
    this.save();
    return true;
  },

  addStock(catName, stockId) {
    if (!state.watchlist[catName]) return false;
    if (state.watchlist[catName].stocks.includes(stockId)) return false;
    state.watchlist[catName].stocks.push(stockId);
    this.save();
    return true;
  },

  removeStock(catName, stockId) {
    if (!state.watchlist[catName]) return false;
    state.watchlist[catName].stocks = state.watchlist[catName].stocks.filter(s => s !== stockId);
    this.save();
    return true;
  },

  isWatched(stockId) {
    return Object.values(state.watchlist).some(c => c.stocks.includes(stockId));
  },

  getCategories() {
    return Object.entries(state.watchlist).map(([name, data]) => ({ name, stocks: data.stocks || [] }));
  }
};

/* ============================================================
   UI MANAGER
   ============================================================ */
const UI = {
  /* -- Navigation -- */
  navigateTo(page) {
    state.currentPage = page;
    document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
    document.querySelectorAll('.nav-btn').forEach(b => b.classList.remove('active'));
    document.getElementById(`page-${page}`)?.classList.add('active');
    document.querySelector(`.nav-btn[data-page="${page}"]`)?.classList.add('active');

    if (page === 'watchlist') this.renderWatchlist();
    if (page === 'results') this.renderResults();
  },

  /* -- Theme -- */
  applyTheme(theme) {
    state.theme = theme;
    document.documentElement.setAttribute('data-theme', theme);
    localStorage.setItem('theme', theme);
    document.getElementById('themeBtn').textContent = theme === 'dark' ? '☀️' : '🌙';
  },

  toggleTheme() {
    this.applyTheme(state.theme === 'dark' ? 'light' : 'dark');
  },

  /* -- Header -- */
  updateHeader() {
    const el = document.getElementById('updateTime');
    if (state.lastUpdate) el.textContent = '更新 ' + formatDateTime(state.lastUpdate);
    else el.textContent = '尚未查詢';
  },

  /* -- Toast -- */
  toast(msg, type = 'info', duration = 3000) {
    const container = document.getElementById('toastContainer');
    const el = document.createElement('div');
    el.className = `toast ${type}`;
    el.textContent = msg;
    container.appendChild(el);
    setTimeout(() => { el.remove(); }, duration);
  },

  /* -- Active Filter Tags -- */
  renderActiveTags() {
    const FILTER_LABELS = {
      rs90: 'RS > 90',
      nearMonthlyHigh: '距月高 < 5%',
      shortMAAlign: '短均排列↑',
      longMAAlign: '長均排列↑',
      aboveSubPoint: '站上扣抵值',
      revenueHighRecord: '營收創高',
      revenueYoY: 'YoY連2月>20%',
      revenueMoM: 'MoM連2月>20%',
      marginGrowth: '毛/營益率↑',
      noProfitLoss: '無虧損',
      chipConcentration: '籌碼集中↑',
      buyerSellerDiff: '買賣家數差<0',
      foreignBuy: '外資買超',
      trustBuy: '投信買超',
      bigHolderIncrease: '大戶比例↑',
      institutionalRecord: '法人持股季高',
    };
    const container = document.getElementById('activeTags');
    if (!container) return;
    const active = getActiveFilters();
    container.innerHTML = active.length === 0
      ? '<span style="font-size:11px;color:var(--text-dim)">未選擇篩選條件 — 將顯示所有個股基本資料</span>'
      : active.map(f => `
          <span class="filter-tag">
            ${FILTER_LABELS[f] || f}
            <span class="filter-tag-remove" onclick="UI.removeFilter('${f}')">✕</span>
          </span>`).join('');
  },

  removeFilter(key) {
    state.filters[key] = false;
    document.getElementById(`filter-${key}`)?.checked && (document.getElementById(`filter-${key}`).checked = false);
    this.renderActiveTags();
  },

  /* -- Screening Page -- */
  renderScreenPage() {
    this.renderActiveTags();
    const src = document.getElementById('querySource');
    if (src) {
      src.value = state.querySource;
      document.getElementById('manualArea').style.display = state.querySource === 'manual' ? 'block' : 'none';
    }
  },

  /* -- Results Table -- */
  renderResults() {
    const container = document.getElementById('resultsContainer');
    const results = state.results;

    if (!results || results.length === 0) {
      container.innerHTML = `
        <div class="empty-state">
          <div class="empty-icon">📊</div>
          <div class="empty-title">尚無查詢結果</div>
          <div class="empty-sub">請前往「篩選」頁<br>選擇條件並按下查詢</div>
        </div>`;
      document.getElementById('resultsCount').innerHTML = '';
      return;
    }

    const passing = results.filter(r => r.passAll && !r.error);
    document.getElementById('resultsCount').innerHTML =
      `共 <strong>${results.length}</strong> 檔個股，通過篩選 <strong>${passing.length}</strong> 檔`;

    const activeFilters = getActiveFilters();

    const FILTER_LABELS = {
      rs90: 'RS>90', nearMonthlyHigh: '距月高', shortMAAlign: '短均', longMAAlign: '長均',
      aboveSubPoint: '扣抵', revenueHighRecord: '營收高', revenueYoY: 'YoY', revenueMoM: 'MoM',
      marginGrowth: '毛/營益', noProfitLoss: '無虧損', chipConcentration: '籌碼集中',
      buyerSellerDiff: '買賣家數', foreignBuy: '外資', trustBuy: '投信',
      bigHolderIncrease: '大戶', institutionalRecord: '法人季高',
    };

    const pill = (val, passText = '✓', failText = '✗') => {
      if (val === null || val === undefined) return `<span class="ind-pill ind-na">N/A</span>`;
      if (val === true)  return `<span class="ind-pill ind-pass">${passText}</span>`;
      if (val === false) return `<span class="ind-pill ind-fail">${failText}</span>`;
      return `<span class="ind-pill ind-na">${val}</span>`;
    };

    const numPill = (val, suffix = '', positive = true) => {
      if (val === null || val === undefined || isNaN(val)) return `<span class="ind-pill ind-na">N/A</span>`;
      const cls = val > 0 ? (positive ? 'ind-pass' : 'ind-fail') : (positive ? 'ind-fail' : 'ind-pass');
      return `<span class="ind-pill ${cls}">${numStr(val, 1)}${suffix}</span>`;
    };

    const rsCell = (score) => {
      if (score === null) return '<span class="ind-pill ind-na">N/A</span>';
      const cls = score >= 90 ? 'ind-pass' : score >= 70 ? 'ind-warn' : 'ind-fail';
      return `<span class="ind-pill ${cls}">${Math.round(score)}</span>`;
    };

    const rows = results.map(r => {
      if (r.error) {
        return `<tr class="row-error">
          <td class="stock-code">${r.stockId}</td>
          <td>${r.stockName}</td>
          <td colspan="18" style="text-align:left;color:var(--negative);font-size:11px">⚠ ${r.error}</td>
        </tr>`;
      }

      const priceColor = (r.priceChange || 0) >= 0 ? 'pct-positive' : 'pct-negative';
      const pctDisp = r.priceChange !== null ? pct(r.priceChange) : '--';

      const starred = WL.isWatched(r.stockId);

      const fc = r.filterChecks || {};

      return `<tr onclick="UI.showStockDetail('${r.stockId}')">
        <td><span class="stock-code">${r.stockId}</span></td>
        <td>
          <div class="stock-name">${r.stockName}</div>
        </td>
        <td class="price-cell ${priceColor}">${r.price !== null ? numStr(r.price) : '--'}</td>
        <td class="${priceColor} num-cell">${pctDisp}</td>
        <!-- Technical -->
        <td>${rsCell(r.rsScore)}</td>
        <td>${r.distanceFromHigh !== null ? numPill(r.distanceFromHigh, '%') : '<span class="ind-pill ind-na">N/A</span>'}</td>
        <td>${pill(r.shortMAAlign)}</td>
        <td>${pill(r.longMAAlign)}</td>
        <td>${pill(r.aboveSubPoint)}</td>
        <!-- Fundamental -->
        <td class="num-cell">${r.revenue !== null ? numStr(r.revenue, 1) : '--'}</td>
        <td>${r.yoyLatest !== null ? numPill(r.yoyLatest, '%') : '<span class="ind-pill ind-na">N/A</span>'}</td>
        <td>${r.momLatest !== null ? numPill(r.momLatest, '%') : '<span class="ind-pill ind-na">N/A</span>'}</td>
        <td>${r.grossMargin !== null ? `<span class="num-cell">${numStr(r.grossMargin, 1)}%</span>` : '<span class="ind-pill ind-na">N/A</span>'}</td>
        <td>${r.opMargin !== null ? `<span class="num-cell">${numStr(r.opMargin, 1)}%</span>` : '<span class="ind-pill ind-na">N/A</span>'}</td>
        <td>${pill(r.noProfitLoss, '獲利', '虧損')}</td>
        <!-- Chip -->
        <td>${r.foreignNet !== null ? numPill(Math.round(r.foreignNet / 1000), '張') : '<span class="ind-pill ind-na">N/A</span>'}</td>
        <td>${r.trustNet !== null ? numPill(Math.round(r.trustNet / 1000), '張') : '<span class="ind-pill ind-na">N/A</span>'}</td>
        <td>${pill(r.bigHolderIncrease, '增加', '減少')}</td>
        <td>
          <span class="watch-star ${starred ? 'starred' : ''}" onclick="event.stopPropagation();UI.toggleWatchStock('${r.stockId}','${r.stockName}')">
            ${starred ? '★' : '☆'}
          </span>
        </td>
      </tr>`;
    });

    container.innerHTML = `
      <div class="table-scroll">
        <table class="stock-table">
          <thead>
            <tr>
              <th rowspan="2" onclick="UI.sortResults('stockId')" class="sorted">代號</th>
              <th rowspan="2">股名</th>
              <th rowspan="2" onclick="UI.sortResults('price')">現價</th>
              <th rowspan="2" onclick="UI.sortResults('priceChange')">漲跌%</th>
              <th colspan="5" style="color:#4FC3F7;border-bottom:1px solid #4FC3F730">📊 技術面</th>
              <th colspan="6" style="color:#81C784;border-bottom:1px solid #81C78430">📈 基本面</th>
              <th colspan="3" style="color:#FFB74D;border-bottom:1px solid #FFB74D30">🎯 籌碼面</th>
              <th rowspan="2">自選</th>
            </tr>
            <tr>
              <!-- Technical -->
              <th onclick="UI.sortResults('rsScore')" style="color:#4FC3F7">RS分數</th>
              <th onclick="UI.sortResults('distanceFromHigh')" style="color:#4FC3F7">距月高%</th>
              <th style="color:#4FC3F7">短均線<br><span style="font-size:9px;opacity:.6">MA5>10>20</span></th>
              <th style="color:#4FC3F7">中長均<br><span style="font-size:9px;opacity:.6">MA20>60>120</span></th>
              <th style="color:#4FC3F7">扣抵值<br><span style="font-size:9px;opacity:.6">站上</span></th>
              <!-- Fundamental -->
              <th style="color:#81C784">月營收<br><span style="font-size:9px;opacity:.6">億元</span></th>
              <th onclick="UI.sortResults('yoyLatest')" style="color:#81C784">年增率%<br><span style="font-size:9px;opacity:.6">最近月</span></th>
              <th onclick="UI.sortResults('momLatest')" style="color:#81C784">月增率%<br><span style="font-size:9px;opacity:.6">最近月</span></th>
              <th onclick="UI.sortResults('grossMargin')" style="color:#81C784">毛利率%</th>
              <th onclick="UI.sortResults('opMargin')" style="color:#81C784">營益率%</th>
              <th style="color:#81C784">盈虧</th>
              <!-- Chip -->
              <th onclick="UI.sortResults('foreignNet')" style="color:#FFB74D">外資<br><span style="font-size:9px;opacity:.6">近5日淨</span></th>
              <th onclick="UI.sortResults('trustNet')" style="color:#FFB74D">投信<br><span style="font-size:9px;opacity:.6">近5日淨</span></th>
              <th style="color:#FFB74D">大戶持股</th>
            </tr>
          </thead>
          <tbody>${rows.join('')}</tbody>
        </table>
      </div>`;
  },

  sortResults(field) {
    const first = state.results[0];
    const asc = first && first._sortField === field && !first._sortAsc;
    state.results.sort((a, b) => {
      const av = a[field] ?? (asc ? Infinity : -Infinity);
      const bv = b[field] ?? (asc ? Infinity : -Infinity);
      if (typeof av === 'string') return asc ? av.localeCompare(bv) : bv.localeCompare(av);
      return asc ? av - bv : bv - av;
    });
    if (state.results[0]) { state.results[0]._sortField = field; state.results[0]._sortAsc = asc; }
    this.renderResults();
  },

  toggleWatchStock(stockId, stockName) {
    if (WL.isWatched(stockId)) {
      // Remove from all lists
      for (const cat of WL.getCategories()) {
        WL.removeStock(cat.name, stockId);
      }
      this.toast(`已移除 ${stockId} 從自選股`, 'info');
    } else {
      WL.addStock('預設', stockId);
      this.toast(`已加入 ${stockId} 至自選股`, 'success');
    }
    this.renderResults();
  },

  showStockDetail(stockId) {
    const r = state.results.find(r => r.stockId === stockId);
    if (!r) return;
    // Future: show modal with full details
  },

  /* -- Watchlist Page -- */
  renderWatchlist() {
    const container = document.getElementById('watchlistContent');
    const categories = WL.getCategories();

    if (categories.every(c => c.stocks.length === 0)) {
      container.innerHTML = `
        <div class="empty-state">
          <div class="empty-icon">⭐</div>
          <div class="empty-title">自選股清單為空</div>
          <div class="empty-sub">點右上角「+ 新增分類」<br>或在查詢結果中點 ☆ 加入</div>
        </div>`;
      return;
    }

    container.innerHTML = categories.map(cat => `
      <div class="watchlist-category">
        <div class="wl-cat-header">
          <span class="wl-cat-name">📁 ${cat.name}</span>
          <span class="wl-cat-count">${cat.stocks.length} 檔</span>
          ${cat.name !== '預設' ? `<button class="btn-danger" onclick="UI.removeCategory('${cat.name}')">刪除分類</button>` : ''}
        </div>
        <div class="wl-stock-list">
          ${cat.stocks.map(sid => `
            <div class="wl-stock-item" id="wl-${cat.name}-${sid}">
              <span class="wl-stock-code">${sid}</span>
              <span class="wl-stock-name" id="wl-name-${sid}">載入中...</span>
              <span class="wl-stock-price" id="wl-price-${sid}">--</span>
              <button class="btn-danger" onclick="UI.removeFromWatchlist('${cat.name}','${sid}')">移除</button>
            </div>`).join('')}
        </div>
        <div class="wl-add-row">
          <input type="text" id="wladd-${cat.name}" placeholder="輸入股票代碼 (如 2330)" maxlength="6">
          <button class="wl-add-btn" onclick="UI.addToWatchlist('${cat.name}')">+ 加入</button>
        </div>
      </div>`).join('');

    // Async load stock names
    categories.forEach(cat => {
      cat.stocks.forEach(async sid => {
        const nameEl = document.getElementById(`wl-name-${sid}`);
        if (nameEl) {
          try {
            const name = await API.getStockName(sid);
            nameEl.textContent = name;
          } catch { nameEl.textContent = sid; }
        }
      });
    });
  },

  addToWatchlist(catName) {
    const input = document.getElementById(`wladd-${catName}`);
    const sid = input.value.trim();
    if (!/^\d{4,5}$/.test(sid)) { this.toast('請輸入 4-5 位數字股票代碼', 'error'); return; }
    if (WL.addStock(catName, sid)) {
      this.toast(`已加入 ${sid}`, 'success');
      input.value = '';
      this.renderWatchlist();
    } else {
      this.toast('股票已存在或分類不存在', 'error');
    }
  },

  removeFromWatchlist(catName, stockId) {
    WL.removeStock(catName, stockId);
    this.toast(`已移除 ${stockId}`, 'info');
    this.renderWatchlist();
  },

  removeCategory(name) {
    if (!confirm(`確定刪除分類「${name}」及其所有個股？`)) return;
    WL.removeCategory(name);
    this.toast(`已刪除分類 ${name}`, 'info');
    this.renderWatchlist();
  },

  showAddCategoryModal() {
    document.getElementById('modalAddCat').classList.add('open');
    document.getElementById('newCatName').focus();
  },

  confirmAddCategory() {
    const name = document.getElementById('newCatName').value.trim();
    if (!name) { this.toast('請輸入分類名稱', 'error'); return; }
    if (WL.addCategory(name)) {
      this.toast(`已新增分類「${name}」`, 'success');
      document.getElementById('newCatName').value = '';
      document.getElementById('modalAddCat').classList.remove('open');
      this.renderWatchlist();
    } else {
      this.toast('分類已存在', 'error');
    }
  },

  /* -- Settings Page -- */
  renderSettings() {
    const tokenEl = document.getElementById('finmindToken');
    if (tokenEl) tokenEl.value = state.finmindToken;
    this.updateTokenStatus();
  },

  updateTokenStatus() {
    const el = document.getElementById('tokenStatus');
    if (!el) return;
    if (!state.finmindToken) {
      el.className = 'token-status token-missing';
      el.textContent = '未設定';
    } else {
      el.className = 'token-status token-ok';
      el.textContent = '已設定';
    }
  },

  async saveToken() {
    const val = document.getElementById('finmindToken').value.trim();
    if (!val) { this.toast('請輸入 Token', 'error'); return; }

    const el = document.getElementById('tokenStatus');
    el.className = 'token-status token-testing';
    el.textContent = '驗證中...';

    try {
      const ok = await API.testToken(val);
      if (ok) {
        state.finmindToken = val;
        localStorage.setItem('finmindToken', val);
        this.toast('Token 驗證成功 ✓', 'success');
        el.className = 'token-status token-ok';
        el.textContent = '驗證通過';
      } else {
        this.toast('Token 驗證失敗，請確認', 'error');
        el.className = 'token-status token-missing';
        el.textContent = '驗證失敗';
      }
    } catch(e) {
      this.toast('驗證時發生錯誤: ' + e.message, 'error');
      el.className = 'token-status token-missing';
      el.textContent = '錯誤';
    }
  },

  clearCache() {
    Cache.clear();
    this.toast('已清除快取', 'success');
  }
};

/* ============================================================
   QUERY RUNNER
   ============================================================ */
async function runQuery() {
  if (!state.finmindToken) {
    UI.toast('請先在【設定】頁輸入 FinMind API Token', 'error', 4000);
    UI.navigateTo('settings');
    return;
  }

  const stockIds = Screener.getStockIds();
  if (stockIds.length === 0) {
    UI.toast(state.querySource === 'watchlist'
      ? '自選股清單為空，請先加入個股'
      : '請輸入股票代碼', 'error');
    return;
  }
  if (stockIds.length > 20) {
    UI.toast(`一次最多查詢 20 檔（目前 ${stockIds.length} 檔），請減少數量`, 'error', 4000);
    return;
  }

  const overlay = document.getElementById('loadingOverlay');
  const progressEl = document.getElementById('loadingProgress');
  const loadingText = document.getElementById('loadingText');

  overlay.classList.add('visible');
  state.isLoading = true;
  state.results = [];

  const progress = (msg) => { if (progressEl) progressEl.textContent = msg; };

  try {
    // Prefetch TAIEX for RS calculation
    let taixPrices = [];
    if (state.filters.rs90 || getActiveFilters().length === 0) {
      loadingText.textContent = '取得大盤指數資料...';
      try { taixPrices = await Screener.getTAIEXData(); } catch { /* ok */ }
    }

    for (let i = 0; i < stockIds.length; i++) {
      const sid = stockIds[i];
      loadingText.textContent = `分析 ${sid} (${i + 1}/${stockIds.length})`;
      const name = await API.getStockName(sid).catch(() => sid);
      const result = await Screener.screenOne(sid, name, taixPrices, progress);
      state.results.push(result);
    }

    state.lastUpdate = new Date();
    UI.updateHeader();
    UI.toast(`查詢完成，共 ${state.results.length} 檔`, 'success');
    UI.navigateTo('results');

  } catch (e) {
    UI.toast('查詢錯誤: ' + e.message, 'error', 5000);
  } finally {
    overlay.classList.remove('visible');
    state.isLoading = false;
  }
}

/* ============================================================
   INIT
   ============================================================ */
function init() {
  // Apply saved theme
  UI.applyTheme(state.theme);

  // Header
  document.getElementById('versionBadge').textContent = APP_VERSION;
  UI.updateHeader();

  // Service Worker
  if ('serviceWorker' in navigator) {
    navigator.serviceWorker.register('./sw.js').catch(e => console.warn('SW:', e));
  }

  // Theme button
  document.getElementById('themeBtn').addEventListener('click', () => UI.toggleTheme());

  // Navigation
  document.querySelectorAll('.nav-btn').forEach(btn => {
    btn.addEventListener('click', () => UI.navigateTo(btn.dataset.page));
  });

  // Filter checkboxes
  document.querySelectorAll('.filter-checkbox').forEach(cb => {
    cb.addEventListener('change', () => {
      state.filters[cb.dataset.filter] = cb.checked;
      UI.renderActiveTags();
    });
  });

  // Query source
  const qs = document.getElementById('querySource');
  if (qs) {
    qs.addEventListener('change', () => {
      state.querySource = qs.value;
      document.getElementById('manualArea').style.display = qs.value === 'manual' ? 'block' : 'none';
    });
  }

  // Manual stocks textarea
  const ma = document.getElementById('manualStocks');
  if (ma) ma.addEventListener('input', () => { state.manualStocks = ma.value; });

  // Query button
  document.getElementById('queryBtn')?.addEventListener('click', runQuery);

  // Settings
  document.getElementById('saveTokenBtn')?.addEventListener('click', () => UI.saveToken());
  document.getElementById('clearCacheBtn')?.addEventListener('click', () => UI.clearCache());

  // Modal
  document.getElementById('addCatBtn')?.addEventListener('click', () => UI.showAddCategoryModal());
  document.getElementById('modalAddCat')?.addEventListener('click', e => {
    if (e.target.id === 'modalAddCat') e.target.classList.remove('open');
  });
  document.getElementById('confirmCatBtn')?.addEventListener('click', () => UI.confirmAddCategory());
  document.getElementById('cancelCatBtn')?.addEventListener('click', () => {
    document.getElementById('modalAddCat').classList.remove('open');
  });
  document.getElementById('newCatName')?.addEventListener('keydown', e => {
    if (e.key === 'Enter') UI.confirmAddCategory();
  });

  // Select-all filters per category
  document.querySelectorAll('.select-all-btn').forEach(btn => {
    btn.addEventListener('click', () => {
      const cat = btn.dataset.cat;
      const checkboxes = document.querySelectorAll(`.filter-checkbox[data-cat="${cat}"]`);
      const allChecked = [...checkboxes].every(c => c.checked);
      checkboxes.forEach(c => {
        c.checked = !allChecked;
        state.filters[c.dataset.filter] = !allChecked;
      });
      UI.renderActiveTags();
    });
  });

  // Collapse filter categories
  document.querySelectorAll('.filter-cat-header').forEach(header => {
    header.addEventListener('click', () => {
      header.closest('.filter-category')?.classList.toggle('collapsed');
    });
  });

  // Init pages
  UI.renderActiveTags();
  UI.renderSettings();

  // Set initial nav
  UI.navigateTo('screen');
}

document.addEventListener('DOMContentLoaded', init);
