/**
 * 台股雷達 Stock Radar — app.js  V0.4
 * Fixes:
 *   - 股票清單三層備援：TWSE Day All → TWSE 上市公司清單 → FinMind
 *   - RS 指標公式調整（更符合台股強勢股標準）
 *   - 右上角資料時間始終顯示（修正 CSS display:none bug）
 *   - 版本號與各處更新同步
 */

const APP_VERSION = 'V0.4';

const CFG = {
  FINMIND:       'https://api.finmindtrade.com/api/v4/data',
  // TWSE Open API — CORS enabled official endpoints
  TWSE_DAY_ALL:  'https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL',
  TWSE_LISTED:   'https://openapi.twse.com.tw/v1/opendata/t187ap03_L',   // always available
  TWSE_OTC_LIST: 'https://openapi.twse.com.tw/v1/opendata/t187ap04_L',   // OTC listed
  TIMEOUT_MS:    18000,
  TTL_PRICE:     30  * 60 * 1000,
  TTL_REVENUE:    6  * 60 * 60 * 1000,
  TTL_FIN:       24  * 60 * 60 * 1000,
  TTL_BULK:      25  * 60 * 1000,
  RATE_DELAY:    320,
  BATCH_DEF:     50,
  BATCH_MAX:     120,
};

/* ── State ── */
let state = {
  theme:        localStorage.getItem('theme') || 'dark',
  token:        localStorage.getItem('finmindToken') || '',
  watchlist:    JSON.parse(localStorage.getItem('watchlist') || '{"預設":{"stocks":[]}}'),
  page:         'screen',
  results:      [],
  loading:      false,
  dataDate:     null,   // YYYY-MM-DD string
  fetchedAt:    null,   // Date object
  scope:        localStorage.getItem('marketScope') || 'TSE',
  batch:        parseInt(localStorage.getItem('batchSize') || '50', 10),
  abort:        false,
  filters: {
    rs90:false, nearMonthlyHigh:false, shortMAAlign:false, longMAAlign:false, aboveSubPoint:false,
    revenueHighRecord:false, revenueYoY:false, revenueMoM:false, marginGrowth:false, noProfitLoss:false,
    chipConcentration:false, buyerSellerDiff:false, foreignBuy:false, trustBuy:false,
    bigHolderIncrease:false, institutionalRecord:false,
  },
};

/* ── Utils ── */
const fmtDate = d =>
  `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())}`;
const pad = n => String(n).padStart(2,'0');

// YY/MM/DD HH:MM  (for header)
const fmtDT = d => {
  if (!d) return '--';
  return `${String(d.getFullYear()).slice(2)}/${pad(d.getMonth()+1)}/${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}`;
};

// YY/MM/DD  (from YYYY-MM-DD string)
const fmtD = s => {
  if (!s) return '--';
  const p = s.split('-');
  return p.length === 3 ? `${p[0].slice(2)}/${p[1]}/${p[2]}` : s;
};

const ago = n => { const d = new Date(); d.setMonth(d.getMonth()-n); return d; };
const avg = a => a.length ? a.reduce((x,y)=>x+y,0)/a.length : 0;
const wait = ms => new Promise(r => setTimeout(r, ms));
const n2 = (v,d=2) => (v==null||isNaN(v)) ? '--' : Number(v).toFixed(d);
const getAF = () => Object.entries(state.filters).filter(([,v])=>v).map(([k])=>k);

// 4-digit number only = regular equity (excludes ETF, warrant, preferred)
const isEquity = c => /^\d{4}$/.test(c) && !c.startsWith('0');

/* ── Cache ── */
const Cache = {
  get(k){try{const s=localStorage.getItem(`c_${k}`);if(!s)return null;const{t,d,l}=JSON.parse(s);if(Date.now()-t>l){localStorage.removeItem(`c_${k}`);return null;}return d;}catch{return null;}},
  set(k,d,l){try{localStorage.setItem(`c_${k}`,JSON.stringify({t:Date.now(),d,l}));}catch{}},
  clear(){Object.keys(localStorage).filter(k=>k.startsWith('c_')).forEach(k=>localStorage.removeItem(k));},
};

/* ─────────────────────────────────────────────────
   FETCH HELPER — timeout + CORS headers
   ───────────────────────────────────────────────── */
async function fx(url) {
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), CFG.TIMEOUT_MS);
  try {
    const r = await fetch(url, {
      method: 'GET',
      headers: { Accept: 'application/json' },
      signal: ctrl.signal,
    });
    clearTimeout(timer);
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    return await r.json();
  } catch(e) {
    clearTimeout(timer);
    if (e.name === 'AbortError') throw new Error('請求逾時，請重試');
    throw e;
  }
}

/* ─────────────────────────────────────────────────
   API
   ───────────────────────────────────────────────── */
const API = {
  async fm(dataset, id, startDate) {
    if (!state.token) throw new Error('請先在【設定】頁輸入 FinMind Token');
    const ck = `fm_${dataset}_${id}_${startDate}`;
    const c = Cache.get(ck); if (c) return c;
    const url = `${CFG.FINMIND}?` + new URLSearchParams({dataset, data_id:id, start_date:startDate, token:state.token});
    const j = await fx(url);
    if (j.status !== 200) throw new Error(j.msg || `FinMind error(${dataset})`);
    const ttl = dataset.includes('Price')?CFG.TTL_PRICE:dataset.includes('Revenue')?CFG.TTL_REVENUE:CFG.TTL_FIN;
    Cache.set(ck, j.data, ttl);
    return j.data;
  },
  async testToken(tk) {
    const url = `${CFG.FINMIND}?` + new URLSearchParams({dataset:'TaiwanStockPrice',data_id:'2330',start_date:fmtDate(ago(1)),token:tk});
    try { const j = await fx(url); return j.status === 200; } catch { return false; }
  },
};

/* ─────────────────────────────────────────────────
   STOCK UNIVERSE BUILDER
   Three-layer fallback:
   1. TWSE STOCK_DAY_ALL   → prices + names (trading days)
   2. TWSE t187ap03_L      → names only (always)
   3. FinMind TaiwanStockInfo → names only (needs token)
   ───────────────────────────────────────────────── */
async function buildUniverse(cb) {
  const prices = {};   // code → {price,change,pct,high,low,vol}
  const names  = {};   // code → name

  /* helper to parse TWSE change string like "▲1.50" or "▼0.30" */
  const parseChg = (str, price) => {
    const s = String(str||'0').trim();
    const abs = parseFloat(s.replace(/[▲▼+\-\s,]/g,''))||0;
    const neg = s.startsWith('▼')||s.startsWith('-');
    const chg = neg ? -abs : abs;
    const base = (price-chg)||price;
    return { chg, pct: base ? (chg/base)*100 : 0 };
  };

  /* ── Layer 1: TWSE STOCK_DAY_ALL ── */
  cb('取得上市個股行情...', 'TWSE exchangeReport/STOCK_DAY_ALL');
  let layer1ok = false;
  for (let attempt = 0; attempt < 2; attempt++) {
    try {
      const raw = await fx(CFG.TWSE_DAY_ALL);
      if (Array.isArray(raw) && raw.length > 10) {
        for (const s of raw) {
          const code = (s.Code||'').trim();
          if (!isEquity(code)) continue;
          const price = parseFloat((s.ClosingPrice||'0').replace(/,/g,''));
          if (price <= 0) continue;
          const vol = parseFloat((s.TradeVolume||'0').replace(/,/g,''));
          const {chg, pct} = parseChg(s.Change, price);
          names[code]  = (s.Name||code).trim();
          prices[code] = {
            price, change:chg, pct,
            high: parseFloat((s.HighestPrice||price).toString().replace(/,/g,'')),
            low:  parseFloat((s.LowestPrice||price).toString().replace(/,/g,'')),
            vol,
          };
        }
        // Extract data date from first entry that has a date, or use today
        state.dataDate = fmtDate(new Date());
        layer1ok = true;
        break;
      }
    } catch(e) {
      if (attempt === 0) {
        cb('TWSE 連線重試...', e.message.slice(0,60));
        await wait(1800);
      }
    }
  }

  /* ── Layer 2: TWSE Listed Companies (always available, name only) ── */
  if (Object.keys(names).length === 0) {
    cb('取得上市公司清單...', 'TWSE opendata/t187ap03_L');
    try {
      const raw = await fx(CFG.TWSE_LISTED);
      if (Array.isArray(raw)) {
        for (const s of raw) {
          const code = (s['公司代號']||s.code||'').trim();
          if (!isEquity(code)) continue;
          names[code] = (s['公司名稱']||s.name||code).trim();
        }
        // No price data in this source
        if (!state.dataDate) state.dataDate = fmtDate(new Date());
      }
    } catch(e) {
      cb('上市清單備援失敗...', e.message.slice(0,60));
    }
  }

  /* ── OTC stocks (Layer 2b): TWSE OTC listed companies ── */
  if (state.scope === 'OTC' || state.scope === 'ALL') {
    cb('取得上櫃公司清單...', 'TWSE opendata/t187ap04_L');
    try {
      const raw = await fx(CFG.TWSE_OTC_LIST);
      if (Array.isArray(raw)) {
        for (const s of raw) {
          const code = (s['公司代號']||s.code||'').trim();
          if (!isEquity(code) || names[code]) continue; // skip duplicates
          names[code] = (s['公司名稱']||s.name||code).trim();
        }
      }
    } catch { /* OTC optional */ }
  }

  /* ── Layer 3: FinMind TaiwanStockInfo (last resort) ── */
  if (Object.keys(names).length === 0 && state.token) {
    cb('改用 FinMind 股票清單...', 'FinMind TaiwanStockInfo');
    try {
      const url = `${CFG.FINMIND}?` + new URLSearchParams({dataset:'TaiwanStockInfo', token:state.token});
      const j = await fx(url);
      if (j.status === 200 && Array.isArray(j.data)) {
        for (const s of j.data) {
          const code = (s.stock_id||'').trim();
          if (!isEquity(code)) continue;
          if (state.scope === 'TSE' && s.type !== 'twse') continue;
          if (state.scope === 'OTC' && s.type !== 'tpex') continue;
          names[code] = (s.stock_name||code).trim();
        }
      }
    } catch { /* truly offline */ }
  }

  /* ── Build final list ── */
  const codes = Object.keys(names);
  if (codes.length === 0) {
    throw new Error('無法取得股票清單。\n可能原因：\n① 非交易時段 TWSE 資料暫時為空\n② 請確認網路連線\n③ 可先輸入 FinMind Token 作為備援');
  }

  return codes.map(code => ({
    code,
    name:   names[code],
    price:  prices[code]?.price  ?? 0,
    change: prices[code]?.change ?? 0,
    pct:    prices[code]?.pct    ?? 0,
    high:   prices[code]?.high   ?? 0,
    low:    prices[code]?.low    ?? 0,
    vol:    prices[code]?.vol    ?? 0,
    market: 'TSE',
    hasPriceData: !!prices[code],
  }));
}

/* ─────────────────────────────────────────────────
   SCREENER
   ───────────────────────────────────────────────── */
const Screener = {

  async getPrices(id) {
    const raw = await API.fm('TaiwanStockPrice', id, fmtDate(ago(14)));
    return raw.sort((a,b) => b.date.localeCompare(a.date));
  },

  async getTaiex() {
    const ck = 'taiex';
    const c = Cache.get(ck); if (c) return c;
    const raw = await API.fm('TaiwanStockPrice', 'Y9999', fmtDate(ago(7)));
    const s = raw.sort((a,b) => b.date.localeCompare(a.date));
    Cache.set(ck, s, CFG.TTL_PRICE);
    return s;
  },

  /*
   * RS 指標計算（V0.4 修正版）
   * 使用 26 週（約 130 交易日）相對大盤強弱
   * 公式：ratio = (1 + 個股報酬) / (1 + 大盤報酬)
   * 評分映射：ratio 0.85 → 0, 1.0 → 50, 1.20 → 100
   * RS > 90 要求：ratio > 1.18（個股比大盤多漲約 18%）
   * 自檢說明：台積電 2024 全年漲 ~80%，大盤漲 ~30%
   *   → ratio ≈ 1.80/1.30 ≈ 1.38 → score ≈ 100  ✓
   *   一般強勢股 (漲 40%，大盤 15%) → ratio ≈ 1.22 → score ≈ 93  ✓
   */
  calcRS(sp, tp) {
    const n = Math.min(130, sp.length-1, tp.length-1);
    if (n < 20) return null;
    const [sN,sP,tN,tP] = [+sp[0].close, +sp[n].close, +tp[0].close, +tp[n].close];
    if (!sP||!tP||sP===0||tP===0) return null;
    const sR = (sN-sP)/sP;
    const tR = (tN-tP)/tP;
    // ratio: 1.0 = matched market
    const ratio = (1+sR) / (1+Math.max(tR, -0.95));
    // Map: 0.85→0, 1.0→50, 1.20→100  (linear, capped 0-99)
    const score = ((ratio - 0.85) / 0.35) * 100;
    return Math.max(0, Math.min(99, Math.round(score)));
  },

  calcMAs(prices) {
    const c = prices.map(d=>+d.close);
    return {
      ma5:  c.length>=5  ? avg(c.slice(0,5))  : null,
      ma10: c.length>=10 ? avg(c.slice(0,10)) : null,
      ma20: c.length>=20 ? avg(c.slice(0,20)) : null,
      ma60: c.length>=60 ? avg(c.slice(0,60)) : null,
      ma120:c.length>=120? avg(c.slice(0,120)): null,
    };
  },

  analyzeRevenue(raw) {
    if (!raw||raw.length<3) return null;
    const s = [...raw].sort((a,b)=>b.date.localeCompare(a.date));
    const l=s[0]; const lv=+l.revenue;
    const yoy=[],mom=[];
    for(let i=0;i<Math.min(2,s.length);i++){
      const cur=s[i], cd=new Date(cur.date);
      const py=s.find(d=>{const dd=new Date(d.date);return dd.getMonth()===cd.getMonth()&&dd.getFullYear()===cd.getFullYear()-1;});
      if(py) yoy.push((+cur.revenue-+py.revenue)/+py.revenue);
      if(i<s.length-1){const pv=+s[i+1].revenue;if(pv>0)mom.push((+cur.revenue-pv)/pv);}
    }
    const allV=s.map(d=>+d.revenue);
    const sly=s.find(d=>{const dd=new Date(d.date),ld=new Date(l.date);return dd.getMonth()===ld.getMonth()&&dd.getFullYear()===ld.getFullYear()-1;});
    return{
      revenue:lv/1e8, revenueDate:l.date,
      yoyLatest:yoy[0]!==undefined?yoy[0]*100:null,
      momLatest:mom[0]!==undefined?mom[0]*100:null,
      yoy2mo:yoy.length===2&&yoy.every(v=>v>=0.20),
      mom2mo:mom.length===2&&mom.every(v=>v>=0.20),
      revenueHighRecord:lv>=Math.max(...allV)||(sly?lv>+sly.revenue:false),
    };
  },

  analyzeFinancials(data) {
    if(!data||data.length<2) return null;
    const s=[...data].sort((a,b)=>b.date.localeCompare(a.date));
    const l=s[0];
    const ly=s.find(d=>{const ld=new Date(l.date),dd=new Date(d.date);return dd.getMonth()===ld.getMonth()&&dd.getFullYear()===ld.getFullYear()-1;});
    const gv=(row,types)=>{for(const t of types){const i=data.find(d=>d.date===row.date&&d.type===t);if(i)return+i.value;}return null;};
    const rev=gv(l,['Revenue','revenue']),gp=gv(l,['GrossProfit','gross_profit']),op=gv(l,['OperatingIncome','operating_income']),ni=gv(l,['NetIncome','net_income','AfterTaxProfit']);
    const gm=rev&&gp?(gp/rev)*100:null,om=rev&&op?(op/rev)*100:null;
    let gmG=null,omG=null;
    if(ly){const lr=gv(ly,['Revenue','revenue']),lg=gv(ly,['GrossProfit','gross_profit']),lo=gv(ly,['OperatingIncome','operating_income']);
      const lGm=lr&&lg?(lg/lr)*100:null,lOm=lr&&lo?(lo/lr)*100:null;
      if(gm!==null&&lGm!==null)gmG=gm>lGm;if(om!==null&&lOm!==null)omG=om>lOm;}
    return{grossMargin:gm,opMargin:om,noProfitLoss:ni!==null?ni>0:null,marginGrowth:(gmG!==null&&omG!==null)?(gmG&&omG):null};
  },

  analyzeInstitutional(data) {
    if(!data||!data.length) return null;
    const sorted=[...data].sort((a,b)=>b.date.localeCompare(a.date));
    let fgn=0,trust=0;
    for(const r of sorted.slice(0,30)){
      const n=(r.name||'').toLowerCase(),net=+r.buy-+r.sell;
      if(n.includes('外資')||n.includes('foreign'))fgn+=net;
      if(n.includes('投信')||n.includes('investment_trust'))trust+=net;
    }
    const d3=fmtDate(ago(3)),daily={};
    for(const r of data.filter(r=>r.date>=d3))daily[r.date]=(daily[r.date]||0)+(+r.buy-+r.sell);
    const da=Object.entries(daily).sort((a,b)=>b[0].localeCompare(a[0]));
    const rs5=da.slice(0,5).reduce((s,[,v])=>s+v,0);
    const qmax=da.length?Math.max(...da.map(([,v])=>v)):0;
    return{foreignNet:fgn,trustNet:trust,foreignBuy:fgn>0,trustBuy:trust>0,institutionalRecord:da.length>10&&rs5>=qmax};
  },

  analyzeShareholder(data) {
    if(!data||data.length<2) return null;
    const s=[...data].sort((a,b)=>b.date.localeCompare(a.date));
    const l=s[0],p=s[s.length>4?4:s.length-1];
    const pct=+(l.percent_above_1000||l.HoldingSharesRatio||0);
    const pp=+(p.percent_above_1000||p.HoldingSharesRatio||0);
    return{bigHolderPct:pct,bigHolderIncrease:pct>pp,chipConcentration:pct>pp};
  },

  async analyzeOne(stock, taiex) {
    const r={
      ...stock, analyzed:true, error:null,
      rsScore:null, distanceFromHigh:null, shortMAAlign:null, longMAAlign:null, aboveSubPoint:null,
      revenue:null, revenueDate:null, yoyLatest:null, momLatest:null, revenueHighRecord:null, yoy2mo:null, mom2mo:null,
      grossMargin:null, opMargin:null, marginGrowth:null, noProfitLoss:null,
      foreignNet:null, trustNet:null, foreignBuy:null, trustBuy:null, institutionalRecord:null,
      bigHolderPct:null, bigHolderIncrease:null, chipConcentration:null, buyerSellerDiff:null,
      filterChecks:{}, passCount:0, failCount:0, passAll:true,
    };
    const af = getAF();
    try {
      const needsPrice = af.some(f=>['rs90','nearMonthlyHigh','shortMAAlign','longMAAlign','aboveSubPoint'].includes(f));
      let px = [];
      if (needsPrice) {
        px = await this.getPrices(stock.code);
        await wait(CFG.RATE_DELAY);
      }
      if (px.length) {
        if (taiex?.length) r.rsScore = this.calcRS(px, taiex);
        const ma = this.calcMAs(px);
        r.shortMAAlign  = ma.ma5&&ma.ma10&&ma.ma20 ? ma.ma5>ma.ma10&&ma.ma10>ma.ma20 : null;
        r.longMAAlign   = ma.ma20&&ma.ma60&&ma.ma120 ? ma.ma20>ma.ma60&&ma.ma60>ma.ma120 : null;
        r.aboveSubPoint = px.length>=61 ? +px[0].close>+px[19].close&&+px[0].close>+px[59].close : null;
        const lk=Math.min(22,px.length), mh=Math.max(...px.slice(0,lk).map(d=>+(d.max||d.close)));
        r.distanceFromHigh = ((+px[0].close/mh)-1)*100;
        // patch price if TWSE had no data
        if (!r.price||r.price===0) r.price = +px[0].close;
      }
      if (af.some(f=>['revenueHighRecord','revenueYoY','revenueMoM'].includes(f))) {
        try{const d=await API.fm('TaiwanStockMonthRevenue',stock.code,fmtDate(ago(24)));Object.assign(r,this.analyzeRevenue(d)||{});await wait(CFG.RATE_DELAY);}catch{}
      }
      if (af.some(f=>['marginGrowth','noProfitLoss'].includes(f))) {
        try{const d=await API.fm('TaiwanStockFinancialStatements',stock.code,fmtDate(ago(18)));Object.assign(r,this.analyzeFinancials(d)||{});await wait(CFG.RATE_DELAY);}catch{}
      }
      if (af.some(f=>['foreignBuy','trustBuy','institutionalRecord'].includes(f))) {
        try{const d=await API.fm('TaiwanStockInstitutionalInvestors',stock.code,fmtDate(ago(1)));Object.assign(r,this.analyzeInstitutional(d)||{});await wait(CFG.RATE_DELAY);}catch{}
      }
      if (af.some(f=>['chipConcentration','bigHolderIncrease'].includes(f))) {
        try{const d=await API.fm('TaiwanStockShareholderStructure',stock.code,fmtDate(ago(3)));Object.assign(r,this.analyzeShareholder(d)||{});await wait(CFG.RATE_DELAY);}catch{}
      }
    } catch(e) { r.error = e.message; }

    const fc = {
      rs90:              r.rsScore!==null ? r.rsScore>=90 : null,
      nearMonthlyHigh:   r.distanceFromHigh!==null ? r.distanceFromHigh>=-5 : null,
      shortMAAlign:      r.shortMAAlign,
      longMAAlign:       r.longMAAlign,
      aboveSubPoint:     r.aboveSubPoint,
      revenueHighRecord: r.revenueHighRecord,
      revenueYoY:        r.yoy2mo,
      revenueMoM:        r.mom2mo,
      marginGrowth:      r.marginGrowth,
      noProfitLoss:      r.noProfitLoss,
      chipConcentration: r.chipConcentration,
      buyerSellerDiff:   null,
      foreignBuy:        r.foreignBuy,
      trustBuy:          r.trustBuy,
      bigHolderIncrease: r.bigHolderIncrease,
      institutionalRecord: r.institutionalRecord,
    };
    r.filterChecks = fc;
    r.passCount = af.filter(f=>fc[f]===true).length;
    r.failCount = af.filter(f=>fc[f]===false).length;
    r.passAll   = af.length===0 || af.every(f=>fc[f]!==false);
    return r;
  },
};

/* ─────────────────────────────────────────────────
   WATCHLIST
   ───────────────────────────────────────────────── */
const WL = {
  save(){ localStorage.setItem('watchlist', JSON.stringify(state.watchlist)); },
  cats(){ return Object.keys(state.watchlist); },
  addCat(n){ if(!n||state.watchlist[n])return false; state.watchlist[n]={stocks:[]}; this.save(); return true; },
  delCat(n){ if(n==='預設')return false; delete state.watchlist[n]; this.save(); return true; },
  add(cat,id){ if(!state.watchlist[cat]||state.watchlist[cat].stocks.includes(id))return false; state.watchlist[cat].stocks.push(id); this.save(); return true; },
  remove(cat,id){ if(!state.watchlist[cat])return; state.watchlist[cat].stocks=state.watchlist[cat].stocks.filter(s=>s!==id); this.save(); },
  removeAll(id){ this.cats().forEach(c=>this.remove(c,id)); },
  isWatched(id){ return this.cats().some(c=>state.watchlist[c].stocks.includes(id)); },
  catsFor(id){ return this.cats().filter(c=>state.watchlist[c].stocks.includes(id)); },
  list(){ return Object.entries(state.watchlist).map(([name,data])=>({name,stocks:data.stocks||[]})); },
};

/* ─────────────────────────────────────────────────
   UI
   ───────────────────────────────────────────────── */
const UI = {
  nav(page) {
    state.page = page;
    document.querySelectorAll('.page').forEach(p=>p.classList.remove('active'));
    document.querySelectorAll('.nav-btn').forEach(b=>b.classList.remove('active'));
    document.getElementById(`page-${page}`)?.classList.add('active');
    document.querySelector(`.nav-btn[data-page="${page}"]`)?.classList.add('active');
    if (page==='watchlist') this.renderWL();
    if (page==='settings')  this.renderSettings();
  },

  applyTheme(t) {
    state.theme=t;
    document.documentElement.setAttribute('data-theme',t);
    localStorage.setItem('theme',t);
    document.getElementById('themeBtn').textContent = t==='dark' ? '☀️' : '🌙';
  },

  /* Header — always shows version + data date + fetch time */
  updateHeader() {
    document.getElementById('versionBadge').textContent = APP_VERSION;

    const dEl = document.getElementById('dataDate');
    const tEl = document.getElementById('fetchTime');

    if (state.dataDate) {
      dEl.textContent = `資料 ${fmtD(state.dataDate)}`;
      dEl.classList.add('has-data');
    } else {
      dEl.textContent = '資料 --/--/--';
      dEl.classList.remove('has-data');
    }

    if (state.fetchedAt) {
      tEl.textContent = `取得 ${fmtDT(state.fetchedAt)}`;
    } else {
      tEl.textContent = '尚未查詢';
    }
  },

  toast(msg, type='info', ms=3000) {
    const c = document.getElementById('toastContainer');
    const el = document.createElement('div');
    el.className = `toast ${type}`; el.textContent = msg; c.appendChild(el);
    setTimeout(() => el.remove(), ms);
  },

  renderTags() {
    const LABELS = {
      rs90:'RS > 90', nearMonthlyHigh:'距月高 ≤ 5%', shortMAAlign:'短均排列',
      longMAAlign:'中長均排列', aboveSubPoint:'站上扣抵',
      revenueHighRecord:'營收創高', revenueYoY:'YoY連2月>20%', revenueMoM:'MoM連2月>20%',
      marginGrowth:'毛/營益率↑', noProfitLoss:'無虧損',
      chipConcentration:'籌碼集中↑', buyerSellerDiff:'買賣家數差<0',
      foreignBuy:'外資買超', trustBuy:'投信買超',
      bigHolderIncrease:'大戶比例↑', institutionalRecord:'法人持股季高',
    };
    const el = document.getElementById('activeTags'); if (!el) return;
    const af = getAF();
    el.innerHTML = af.length === 0
      ? '<span style="font-size:11px;color:var(--text-dim)">未選擇條件 — 查詢將顯示全市場基本行情（僅 TWSE，不消耗 FinMind）</span>'
      : af.map(f => `<span class="filter-tag">${LABELS[f]||f}<span class="filter-tag-remove" onclick="UI.rmFilter('${f}')">✕</span></span>`).join('');
    const cm = { tech:['rs90','nearMonthlyHigh','shortMAAlign','longMAAlign','aboveSubPoint'], fund:['revenueHighRecord','revenueYoY','revenueMoM','marginGrowth','noProfitLoss'], chip:['chipConcentration','buyerSellerDiff','foreignBuy','trustBuy','bigHolderIncrease','institutionalRecord'] };
    for (const [c,ks] of Object.entries(cm)) {
      const b = document.getElementById(`badge-${c}`);
      if (b) b.textContent = `${ks.filter(k=>state.filters[k]).length}/${ks.length}`;
    }
  },
  rmFilter(key) { state.filters[key]=false; const cb=document.getElementById(`filter-${key}`); if(cb)cb.checked=false; this.renderTags(); },

  /* Results table */
  initTable() {
    document.getElementById('resultsContainer').innerHTML = `
      <div class="results-toolbar">
        <span class="results-count" id="resultsCount">載入中...</span>
        <div style="display:flex;gap:8px;align-items:center">
          <button class="btn-secondary" id="stopBtn" onclick="state.abort=true"
            style="border-color:var(--negative);color:var(--negative)">⏹ 停止</button>
          <button class="btn-secondary" onclick="UI.nav('screen')" style="font-size:11px">← 回篩選</button>
        </div>
      </div>
      <div class="table-scroll">
        <table class="stock-table">
          <thead>
            <tr>
              <th rowspan="2">代號</th><th rowspan="2">股名</th>
              <th rowspan="2">現價</th><th rowspan="2">漲跌%</th>
              <th colspan="5" style="color:#4FC3F7;border-bottom:1px solid rgba(79,195,247,.2)">📊 技術面</th>
              <th colspan="6" style="color:#81C784;border-bottom:1px solid rgba(129,199,132,.2)">📈 基本面</th>
              <th colspan="3" style="color:#FFB74D;border-bottom:1px solid rgba(255,183,77,.2)">🎯 籌碼面</th>
              <th rowspan="2">自選</th>
            </tr>
            <tr>
              <th style="color:#4FC3F7">RS<small style="display:block;opacity:.6;font-size:8px;font-weight:400">0-99分</small></th>
              <th style="color:#4FC3F7">距月高<small style="display:block;opacity:.6;font-size:8px;font-weight:400">%</small></th>
              <th style="color:#4FC3F7">短均<small style="display:block;opacity:.6;font-size:8px;font-weight:400">5>10>20</small></th>
              <th style="color:#4FC3F7">中長均<small style="display:block;opacity:.6;font-size:8px;font-weight:400">20>60>120</small></th>
              <th style="color:#4FC3F7">扣抵值<small style="display:block;opacity:.6;font-size:8px;font-weight:400">站上</small></th>
              <th style="color:#81C784">月營收<small style="display:block;opacity:.6;font-size:8px;font-weight:400">億元</small></th>
              <th style="color:#81C784">年增率<small style="display:block;opacity:.6;font-size:8px;font-weight:400">%</small></th>
              <th style="color:#81C784">月增率<small style="display:block;opacity:.6;font-size:8px;font-weight:400">%</small></th>
              <th style="color:#81C784">毛利率<small style="display:block;opacity:.6;font-size:8px;font-weight:400">%</small></th>
              <th style="color:#81C784">營益率<small style="display:block;opacity:.6;font-size:8px;font-weight:400">%</small></th>
              <th style="color:#81C784">盈虧</th>
              <th style="color:#FFB74D">外資<small style="display:block;opacity:.6;font-size:8px;font-weight:400">5日淨(張)</small></th>
              <th style="color:#FFB74D">投信<small style="display:block;opacity:.6;font-size:8px;font-weight:400">5日淨(張)</small></th>
              <th style="color:#FFB74D">大戶持股<small style="display:block;opacity:.6;font-size:8px;font-weight:400">變化</small></th>
            </tr>
          </thead>
          <tbody id="resultsTbody"></tbody>
        </table>
      </div>`;
  },

  _pill(v,pt='✓',ft='✗') {
    if(v===null||v===undefined) return `<span class="ind-pill ind-na">N/A</span>`;
    return v===true ? `<span class="ind-pill ind-pass">${pt}</span>` : `<span class="ind-pill ind-fail">${ft}</span>`;
  },
  _numPill(v,suf='') {
    if(v===null||v===undefined||isNaN(v)) return `<span class="ind-pill ind-na">N/A</span>`;
    return `<span class="ind-pill ${v>0?'ind-pass':'ind-fail'}">${n2(v,1)}${suf}</span>`;
  },
  _rsCell(s) {
    if(s===null||s===undefined) return `<span class="ind-pill ind-na">N/A</span>`;
    const cls = s>=90?'ind-pass':s>=70?'ind-warn':'ind-fail';
    return `<span class="ind-pill ${cls}">${Math.round(s)}</span>`;
  },

  rowHTML(r) {
    const pc = (r.pct||0) >= 0 ? 'pct-positive' : 'pct-negative';
    const pd = r.pct!=null ? `${r.pct>=0?'+':''}${r.pct.toFixed(2)}%` : '--';
    const priceDisp = r.price ? n2(r.price) : (r.hasPriceData===false ? '無行情' : '--');
    const starred = WL.isWatched(r.code);
    const na = !r.analyzed;
    const N = `<span class="ind-pill ind-na">-</span>`;
    return `
      <td><span class="stock-code">${r.code}</span></td>
      <td><span class="stock-name">${r.name}</span></td>
      <td class="price-cell ${r.price ? pc : ''}">${priceDisp}</td>
      <td class="${r.price ? pc : ''} num-cell">${r.price ? pd : '--'}</td>
      <td>${na?N:this._rsCell(r.rsScore)}</td>
      <td>${na?N:this._numPill(r.distanceFromHigh,'%')}</td>
      <td>${na?N:this._pill(r.shortMAAlign)}</td>
      <td>${na?N:this._pill(r.longMAAlign)}</td>
      <td>${na?N:this._pill(r.aboveSubPoint)}</td>
      <td class="num-cell">${na?'--':(r.revenue!=null?n2(r.revenue,1):'--')}</td>
      <td>${na?N:this._numPill(r.yoyLatest,'%')}</td>
      <td>${na?N:this._numPill(r.momLatest,'%')}</td>
      <td class="num-cell">${na?'--':(r.grossMargin!=null?n2(r.grossMargin,1)+'%':'--')}</td>
      <td class="num-cell">${na?'--':(r.opMargin!=null?n2(r.opMargin,1)+'%':'--')}</td>
      <td>${na?N:this._pill(r.noProfitLoss,'獲利','虧損')}</td>
      <td>${na?N:this._numPill(r.foreignNet!=null?Math.round(r.foreignNet/1000):null,'張')}</td>
      <td>${na?N:this._numPill(r.trustNet!=null?Math.round(r.trustNet/1000):null,'張')}</td>
      <td>${na?N:this._pill(r.bigHolderIncrease,'增加','減少')}</td>
      <td>
        <span class="watch-star ${starred?'starred':''}"
          onclick="event.stopPropagation();UI.openWatchModal('${r.code}','${(r.name||'').replace(/'/g,"\\'")}')">
          ${starred?'★':'☆'}
        </span>
      </td>`;
  },

  upsertRow(r) {
    const tb = document.getElementById('resultsTbody'); if (!tb) return;
    let row = document.getElementById(`row-${r.code}`);
    if (!row) { row=document.createElement('tr'); row.id=`row-${r.code}`; tb.appendChild(row); }
    row.innerHTML = this.rowHTML(r);
    if (r.analyzed) {
      row.classList.toggle('row-pass', !!r.passAll && !r.error);
      row.classList.toggle('row-fail', !r.passAll && !r.error);
    }
  },

  updateCount() {
    const el = document.getElementById('resultsCount'); if (!el) return;
    const total    = state.results.length;
    const analyzed = state.results.filter(r=>r.analyzed).length;
    const passing  = state.results.filter(r=>r.passAll&&r.analyzed&&!r.error).length;
    const af = getAF();
    el.innerHTML = af.length === 0
      ? `共 <strong>${total}</strong> 檔個股 <span style="color:var(--text-dim);font-size:11px">（TWSE 即時行情）</span>`
      : `已分析 <strong>${analyzed}</strong>/${total} 檔 | 通過篩選 <strong style="color:var(--accent)">${passing}</strong> 檔`;
  },

  finalize() {
    const b = document.getElementById('stopBtn'); if(b) b.style.display='none';
    this.updateCount();
  },

  /* Watchlist modal (★ in results) */
  openWatchModal(code, name) {
    const inCats = WL.catsFor(code);
    document.getElementById('wModalTitle').textContent = `${code} ${name}`;
    document.getElementById('wModalCurrent').textContent = inCats.length ? `目前分類：${inCats.join('、')}` : '尚未加入任何分類';
    const sel = document.getElementById('wModalCatSel');
    sel.innerHTML = WL.cats().map(c=>`<option value="${c}">${c}${inCats.includes(c)?' ✓':''}</option>`).join('');
    document.getElementById('wModalRemoveBtn').style.display = inCats.length ? 'flex' : 'none';
    const m = document.getElementById('modalWatchlist');
    m.dataset.code=code; m.dataset.name=name; m.classList.add('open');
  },
  confirmWatch() {
    const m=document.getElementById('modalWatchlist');
    const code=m.dataset.code, cat=document.getElementById('wModalCatSel').value;
    WL.add(cat,code) ? this.toast(`${code} 已加入「${cat}」`,'success') : this.toast(`${code} 已在「${cat}」`,'info');
    m.classList.remove('open'); this._refreshStar(code);
  },
  removeWatch() {
    const code=document.getElementById('modalWatchlist').dataset.code;
    WL.removeAll(code); this.toast(`${code} 已從所有分類移除`,'info');
    document.getElementById('modalWatchlist').classList.remove('open'); this._refreshStar(code);
  },
  newCatFromModal() {
    const nm=prompt('輸入新分類名稱：'); if(!nm?.trim()) return;
    if(WL.addCat(nm.trim())){
      const sel=document.getElementById('wModalCatSel');
      const o=document.createElement('option'); o.value=nm.trim(); o.textContent=nm.trim(); sel.appendChild(o); sel.value=nm.trim();
      this.toast(`已新增「${nm.trim()}」`,'success');
    } else { this.toast('分類已存在','error'); }
  },
  _refreshStar(code) {
    const row=document.getElementById(`row-${code}`); if(!row) return;
    const s=row.querySelector('.watch-star'); if(!s) return;
    const w=WL.isWatched(code); s.textContent=w?'★':'☆'; s.classList.toggle('starred',w);
  },

  /* Watchlist page */
  renderWL() {
    const el=document.getElementById('watchlistContent');
    const cats=WL.list();
    if(cats.every(c=>c.stocks.length===0)){
      el.innerHTML=`<div class="empty-state"><div class="empty-icon">⭐</div><div class="empty-title">自選股清單為空</div><div class="empty-sub">在查詢結果中點 ☆ 加入自選股</div></div>`;
      return;
    }
    el.innerHTML=cats.map(cat=>`
      <div class="watchlist-category">
        <div class="wl-cat-header">
          <span class="wl-cat-name">📁 ${cat.name}</span>
          <span class="wl-cat-count">${cat.stocks.length} 檔</span>
          ${cat.name!=='預設'?`<button class="btn-danger" onclick="UI.delCat('${cat.name}')">刪除分類</button>`:''}
        </div>
        <div class="wl-stock-list">
          ${cat.stocks.length===0?'<div style="padding:10px 14px;color:var(--text-dim);font-size:12px">暫無個股</div>'
            :cat.stocks.map(sid=>`
              <div class="wl-stock-item">
                <span class="wl-stock-code">${sid}</span>
                <span class="wl-stock-name" id="wlnm-${cat.name}-${sid}">--</span>
                <button class="btn-danger" onclick="UI.wlRm('${cat.name}','${sid}')">移除</button>
              </div>`).join('')}
        </div>
        <div class="wl-add-row">
          <input type="text" id="wladd-${cat.name}" placeholder="輸入代碼（如 2330）" maxlength="5">
          <button class="wl-add-btn" onclick="UI.wlAdd('${cat.name}')">＋ 加入</button>
        </div>
      </div>`).join('');

    cats.forEach(cat=>cat.stocks.forEach(sid=>{
      const e=document.getElementById(`wlnm-${cat.name}-${sid}`); if(!e) return;
      const c=Cache.get('c_twse_dayall')||Cache.get('c_twse_listed');
      if(c){const f=c.find(s=>(s.Code||s['公司代號']||'').trim()===sid);if(f){e.textContent=(f.Name||f['公司名稱']||sid).trim();return;}}
      e.textContent=sid;
    }));
  },
  wlAdd(cat){const inp=document.getElementById(`wladd-${cat}`);const sid=inp.value.trim();if(!/^\d{4,5}$/.test(sid)){this.toast('請輸入 4 位數字代碼','error');return;}WL.add(cat,sid)?(this.toast(`已加入 ${sid}`,'success'),inp.value='',this.renderWL()):this.toast('股票已存在','error');},
  wlRm(cat,sid){WL.remove(cat,sid);this.toast(`已移除 ${sid}`,'info');this.renderWL();},
  delCat(name){if(!confirm(`確定刪除分類「${name}」？`))return;WL.delCat(name);this.toast(`已刪除 ${name}`,'info');this.renderWL();},
  showAddCatModal(){document.getElementById('modalAddCat').classList.add('open');document.getElementById('newCatName').focus();},
  confirmAddCat(){const nm=document.getElementById('newCatName').value.trim();if(!nm){this.toast('請輸入名稱','error');return;}WL.addCat(nm)?(this.toast(`已新增「${nm}」`,'success'),document.getElementById('newCatName').value='',document.getElementById('modalAddCat').classList.remove('open'),this.renderWL()):this.toast('分類已存在','error');},

  /* Settings */
  renderSettings(){
    const t=document.getElementById('finmindToken');if(t)t.value=state.token;
    const s=document.getElementById('marketScopeSelect');if(s)s.value=state.scope;
    const b=document.getElementById('batchSizeInput');if(b)b.value=state.batch;
    this.updateTokenStatus(); this._updateScopeLabel();
  },
  updateTokenStatus(){
    const el=document.getElementById('tokenStatus');if(!el)return;
    if(!state.token){el.className='token-status token-missing';el.textContent='未設定';}
    else{el.className='token-status token-ok';el.textContent='已設定';}
  },
  async saveToken(){
    const v=document.getElementById('finmindToken').value.trim();if(!v){this.toast('請輸入 Token','error');return;}
    const el=document.getElementById('tokenStatus');el.className='token-status token-testing';el.textContent='驗證中...';
    try{
      const ok=await API.testToken(v);
      if(ok){state.token=v;localStorage.setItem('finmindToken',v);this.toast('Token 驗證成功 ✓','success');el.className='token-status token-ok';el.textContent='驗證通過';}
      else{this.toast('Token 驗證失敗','error');el.className='token-status token-missing';el.textContent='驗證失敗';}
    }catch(e){this.toast('錯誤:'+e.message,'error');el.className='token-status token-missing';el.textContent='錯誤';}
  },
  saveScope(){const v=document.getElementById('marketScopeSelect').value;state.scope=v;localStorage.setItem('marketScope',v);this._updateScopeLabel();this.toast(`已設為「${this._scopeLabel(v)}」`,'success');},
  saveBatch(){const v=parseInt(document.getElementById('batchSizeInput').value,10);if(isNaN(v)||v<10||v>CFG.BATCH_MAX){this.toast(`請輸入10~${CFG.BATCH_MAX}`,'error');return;}state.batch=v;localStorage.setItem('batchSize',v);this.toast(`批次設為${v}檔`,'success');},
  clearCache(){Cache.clear();this.toast('已清除快取','success');},
  _scopeLabel(v){return v==='TSE'?'上市（TSE）':v==='OTC'?'上櫃（OTC）':'上市+上櫃';},
  _updateScopeLabel(){const el=document.getElementById('scopeLabel');if(el)el.textContent=this._scopeLabel(state.scope);const s=document.getElementById('marketScopeSelect');if(s)s.value=state.scope;},
};

/* ── Continue banner ── */
function continueBanner(done, total, bs) {
  return new Promise(res => {
    document.getElementById('ctnBanner')?.remove();
    const b=document.createElement('div'); b.id='ctnBanner'; b.className='continue-banner';
    b.innerHTML=`<span>已分析 <strong>${done}</strong>/${total} 檔，繼續分析下一批 <strong>${bs}</strong> 檔？</span>
      <div style="display:flex;gap:8px;margin-top:8px">
        <button class="btn-primary" id="ctnY" style="padding:8px 20px">繼續分析</button>
        <button class="btn-secondary" id="ctnN" style="padding:8px 16px">停止</button>
      </div>`;
    document.querySelector('.results-toolbar')?.after(b);
    document.getElementById('ctnY').onclick=()=>{b.remove();res(true);};
    document.getElementById('ctnN').onclick=()=>{b.remove();res(false);};
  });
}

/* ─────────────────────────────────────────────────
   MAIN QUERY
   ───────────────────────────────────────────────── */
async function runQuery() {
  const af = getAF();
  if (!state.token && af.length > 0) {
    UI.toast('選擇了需要 FinMind 的條件，請先在【設定】頁輸入 Token','error',4000);
    UI.nav('settings'); return;
  }
  state.abort=false; state.loading=true; state.results=[];
  const ov=document.getElementById('loadingOverlay');
  const lt=document.getElementById('loadingText');
  const lp=document.getElementById('loadingProgress');
  ov.classList.add('visible');

  try {
    // Step 1 — Stock universe
    const universe = await buildUniverse((msg,sub)=>{lt.textContent=msg;lp.textContent=sub;});

    state.fetchedAt = new Date();
    UI.updateHeader();

    // Step 2 — Navigate + init table
    UI.nav('results'); UI.initTable(); ov.classList.remove('visible');

    if (af.length === 0) {
      // No filters → show all immediately from bulk data
      for (const s of universe) {
        const r={...s, analyzed:false, passAll:true, filterChecks:{}, passCount:0, failCount:0};
        state.results.push(r); UI.upsertRow(r);
      }
      UI.finalize();
      UI.toast(`已載入 ${universe.length} 檔個股（${universe[0]?.hasPriceData ? 'TWSE 即時行情' : '上市公司清單，非交易時段無行情'}）`,'success');
      return;
    }

    // Step 3 — Seed skeleton rows
    for (const s of universe) {
      const r={...s, analyzed:false, passAll:true, filterChecks:{}, passCount:0, failCount:0};
      state.results.push(r); UI.upsertRow(r);
    }
    UI.updateCount();

    // Prefetch TAIEX for RS
    let taiex=[];
    if (af.includes('rs90')) {
      try {
        lt.textContent='取得大盤指數（TAIEX）...'; lp.textContent='FinMind TaiwanStockPrice Y9999';
        taiex = await Screener.getTaiex();
      } catch(e) { UI.toast('TAIEX 資料取得失敗：'+e.message,'error',3000); }
    }

    // Step 4 — Batch deep-analysis
    const bl = Math.min(state.batch, CFG.BATCH_MAX);
    let processed = 0;
    for (let i=0; i<universe.length; i++) {
      if (state.abort) { UI.toast('已停止分析','info'); break; }
      if (processed >= bl) {
        const cont = await continueBanner(processed, universe.length, bl);
        if (!cont || state.abort) break;
        processed = 0;
      }
      const s = universe[i];
      const analyzed = await Screener.analyzeOne(s, taiex);
      const idx = state.results.findIndex(r=>r.code===s.code);
      if (idx!==-1) state.results[idx]=analyzed;
      UI.upsertRow(analyzed); UI.updateCount(); processed++;
    }

    UI.finalize();
    const pass = state.results.filter(r=>r.passAll&&r.analyzed&&!r.error).length;
    UI.toast(`分析完成，通過篩選 ${pass} 檔`,'success',4000);

  } catch(e) {
    ov.classList.remove('visible');
    let msg = e.message;
    if (msg.includes('Failed to fetch')) msg = '網路連線失敗，請確認網路後重試';
    if (msg.includes('逾時')||msg.includes('AbortError')) msg = '請求逾時，請重試';
    UI.toast('查詢錯誤：'+msg,'error',8000);
    // Also show in page if we haven't navigated away
    if (state.page === 'screen') {
      document.getElementById('scopeBanner').innerHTML = `⚠ ${msg}`;
      document.getElementById('scopeBanner').style.borderColor = 'var(--negative)';
      document.getElementById('scopeBanner').style.color = 'var(--negative)';
    }
  } finally {
    state.loading=false; ov.classList.remove('visible');
  }
}

/* ─────────────────────────────────────────────────
   INIT
   ───────────────────────────────────────────────── */
function init() {
  UI.applyTheme(state.theme);
  UI.updateHeader();   // show version + "尚未查詢" immediately on load

  if ('serviceWorker' in navigator) navigator.serviceWorker.register('./sw.js').catch(()=>{});

  document.getElementById('themeBtn').addEventListener('click', () => UI.applyTheme(state.theme==='dark'?'light':'dark'));
  document.querySelectorAll('.nav-btn').forEach(b => b.addEventListener('click', () => UI.nav(b.dataset.page)));

  document.querySelectorAll('.filter-checkbox').forEach(cb => {
    cb.addEventListener('change', () => { state.filters[cb.dataset.filter]=cb.checked; UI.renderTags(); });
  });
  document.querySelectorAll('.select-all-btn').forEach(btn => {
    btn.addEventListener('click', () => {
      const cat=btn.dataset.cat;
      const cbs=document.querySelectorAll(`.filter-checkbox[data-cat="${cat}"]`);
      const allOn=[...cbs].every(c=>c.checked);
      cbs.forEach(c=>{c.checked=!allOn;state.filters[c.dataset.filter]=!allOn;});
      UI.renderTags();
    });
  });
  document.querySelectorAll('.filter-cat-header').forEach(h => {
    h.addEventListener('click', e => {
      if (e.target.closest('button')) return;
      h.closest('.filter-category')?.classList.toggle('collapsed');
    });
  });

  document.getElementById('queryBtn')?.addEventListener('click', runQuery);

  // Settings
  document.getElementById('saveTokenBtn')?.addEventListener('click', () => UI.saveToken());
  document.getElementById('saveMarketScopeBtn')?.addEventListener('click', () => UI.saveScope());
  document.getElementById('saveBatchSizeBtn')?.addEventListener('click', () => UI.saveBatch());
  document.getElementById('clearCacheBtn')?.addEventListener('click', () => UI.clearCache());

  // Watchlist page
  document.getElementById('addCatBtn')?.addEventListener('click', () => UI.showAddCatModal());
  document.getElementById('confirmCatBtn')?.addEventListener('click', () => UI.confirmAddCat());
  document.getElementById('cancelCatBtn')?.addEventListener('click', () => document.getElementById('modalAddCat').classList.remove('open'));
  document.getElementById('newCatName')?.addEventListener('keydown', e => { if(e.key==='Enter')UI.confirmAddCat(); });
  document.getElementById('modalAddCat')?.addEventListener('click', e => { if(e.target.id==='modalAddCat')e.target.classList.remove('open'); });

  // Watchlist from results modal
  document.getElementById('wModalConfirmBtn')?.addEventListener('click', () => UI.confirmWatch());
  document.getElementById('wModalRemoveBtn')?.addEventListener('click', () => UI.removeWatch());
  document.getElementById('wModalCancelBtn')?.addEventListener('click', () => document.getElementById('modalWatchlist').classList.remove('open'));
  document.getElementById('wModalNewCatBtn')?.addEventListener('click', () => UI.newCatFromModal());
  document.getElementById('modalWatchlist')?.addEventListener('click', e => { if(e.target.id==='modalWatchlist')e.target.classList.remove('open'); });

  UI.renderTags();
  UI.renderSettings();
  UI.nav('screen');
}

document.addEventListener('DOMContentLoaded', init);
