/**
 * 台股雷達 Stock Radar — app.js  V1.0
 *
 * 架構變更：
 *  - 讀取 data/screener.json（每日 GitHub Actions 預先計算）
 *  - 篩選完全在瀏覽器端 JS 執行，速度極快
 *  - 不再即時呼叫 FinMind API（除自選股「單股更新」功能）
 *  - 右上角顯示 screener.json 的實際資料日期
 */

const APP_VERSION = 'V1.7';

const CFG = {
  SCREENER_JSON: './data/screener.json',  // 預計算資料
  TIMEOUT_MS:    15000,
};

/* ── State ── */
let state = {
  theme:     localStorage.getItem('theme') || 'dark',
  watchlist: JSON.parse(localStorage.getItem('watchlist') || '{"預設":{"stocks":[]}}'),
  page:      'screen',
  allStocks: [],        // 從 screener.json 載入
  results:   [],        // 篩選後結果
  loading:   false,
  dataDate:  null,      // screener.json 裡的 dataDate
  generated: null,      // screener.json 的產生時間
  dataSource: null,     // 'finmind+twse' | 'twse_only' | 'empty'
  filters: {
    rs90:false, nearMonthlyHigh:false, shortMAAlign:false, longMAAlign:false, aboveSubPoint:false,
    revenueHighRecord:false, revenueYoY:false, revenueMoM:false, marginGrowth:false, noProfitLoss:false,
    chipConcentration:false, buyerSellerDiff:false, foreignBuy:false, trustBuy:false,
    bigHolderIncrease:false, institutionalRecord:false,
  },
};

/* ── Utils ── */
const pad   = n => String(n).padStart(2,'0');
const fmtDT = d => !d ? '--' :
  `${String(d.getFullYear()).slice(2)}/${pad(d.getMonth()+1)}/${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}`;
const fmtD  = s => { if(!s) return '--'; const p=s.split('-'); return p.length===3?`${p[0].slice(2)}/${p[1]}/${p[2]}`:s; };
const n2    = (v,d=2) => (v==null||isNaN(v)) ? '--' : Number(v).toFixed(d);
const getAF = () => Object.entries(state.filters).filter(([,v])=>v).map(([k])=>k);

/* ── Cache ── */
const Cache = {
  get(k){try{const s=localStorage.getItem(`c_${k}`);if(!s)return null;const{t,d,l}=JSON.parse(s);if(Date.now()-t>l){localStorage.removeItem(`c_${k}`);return null;}return d;}catch{return null;}},
  set(k,d,l){try{localStorage.setItem(`c_${k}`,JSON.stringify({t:Date.now(),d,l}));}catch{}},
  clear(){Object.keys(localStorage).filter(k=>k.startsWith('c_')).forEach(k=>localStorage.removeItem(k));},
};

/* ── Fetch helper ── */
async function fx(url) {
  const ctrl=new AbortController();
  const t=setTimeout(()=>ctrl.abort(), CFG.TIMEOUT_MS);
  try {
    const r=await fetch(url,{signal:ctrl.signal});
    clearTimeout(t);
    if(!r.ok) throw new Error(`HTTP ${r.status}`);
    return await r.json();
  } catch(e) { clearTimeout(t); if(e.name==='AbortError') throw new Error('請求逾時'); throw e; }
}

/* ─────────────────────────────────────────────
   LOAD PRE-COMPUTED DATA
   ───────────────────────────────────────────── */
async function loadScreenerData() {
  const ck = 'screener_json';
  // Check localStorage cache first (max 30 min)
  const cached = Cache.get(ck);
  if (cached && cached.stocks && cached.stocks.length > 0) {
    return cached;
  }

  // Add cache-busting query string to bypass browser HTTP cache
  // This ensures we always get the latest screener.json from GitHub Pages
  const url = CFG.SCREENER_JSON + '?_=' + Date.now();
  const data = await fx(url);
  if (!data) throw new Error('screener.json 無法讀取');

  if (data.stocks && data.stocks.length > 0) {
    Cache.set(ck, data, 30 * 60 * 1000);  // cache 30 min in localStorage
  }
  return data;
}

/* ─────────────────────────────────────────────
   CLIENT-SIDE FILTER ENGINE
   All filtering done in-browser on pre-loaded data.
   No API calls needed.
   ───────────────────────────────────────────── */
function applyFilters(stocks) {
  const af = getAF();
  if (af.length === 0) return stocks;

  const FIELD_MAP = {
    rs90:              s => s.rsScore != null ? s.rsScore >= 90 : null,
    nearMonthlyHigh:   s => s.distanceFromHigh != null ? s.distanceFromHigh >= -5 : null,
    shortMAAlign:      s => s.shortMAAlign,
    longMAAlign:       s => s.longMAAlign,
    aboveSubPoint:     s => s.aboveSubPoint,
    revenueHighRecord: s => s.revenueHighRecord,
    revenueYoY:        s => s.yoy2mo,
    revenueMoM:        s => s.mom2mo,
    marginGrowth:      s => s.marginGrowth,
    noProfitLoss:      s => s.noProfitLoss,
    chipConcentration: s => s.chipConcentration,
    buyerSellerDiff:   s => null,  // not available
    foreignBuy:        s => s.foreignBuy,
    trustBuy:          s => s.trustBuy,
    bigHolderIncrease: s => s.bigHolderIncrease,
    institutionalRecord: s => s.institutionalRecord,
  };

  return stocks.filter(s => {
    return af.every(f => {
      const check = FIELD_MAP[f];
      return check ? check(s) === true : false;
    });
  }).map(s => ({
    ...s,
    filterChecks: Object.fromEntries(af.map(f => [f, FIELD_MAP[f]?.(s)])),
    passAll: true,
  }));
}

/* ─────────────────────────────────────────────
   WATCHLIST
   ───────────────────────────────────────────── */
const WL = {
  save(){ localStorage.setItem('watchlist',JSON.stringify(state.watchlist)); },
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

/* ─────────────────────────────────────────────
   UI
   ───────────────────────────────────────────── */
const UI = {
  nav(page){
    state.page=page;
    document.querySelectorAll('.page').forEach(p=>p.classList.remove('active'));
    document.querySelectorAll('.nav-btn').forEach(b=>b.classList.remove('active'));
    document.getElementById(`page-${page}`)?.classList.add('active');
    document.querySelector(`.nav-btn[data-page="${page}"]`)?.classList.add('active');
    if(page==='watchlist')this.renderWL();
    if(page==='settings') this.renderSettings();
  },

  applyTheme(t){
    state.theme=t; document.documentElement.setAttribute('data-theme',t); localStorage.setItem('theme',t);
    document.getElementById('themeBtn').textContent=t==='dark'?'☀️':'🌙';
  },

  /* Header — data date from screener.json, generated time */
  updateHeader(){
    document.getElementById('versionBadge').textContent = APP_VERSION;
    const dEl=document.getElementById('dataDate'), tEl=document.getElementById('fetchTime');
    if(state.dataDate){
      dEl.textContent=`資料 ${fmtD(state.dataDate)}`;
      dEl.classList.add('has-data');
    } else {
      dEl.textContent='資料 --/--/--';
      dEl.classList.remove('has-data');
    }
    if(state.generated){
      const d=new Date(state.generated);
      tEl.textContent=`更新 ${fmtDT(d)}`;
    } else {
      tEl.textContent='尚未載入';
    }
  },

  /* Data source banner */
  updateScopeBanner(){
    const el=document.getElementById('scopeBanner'); if(!el) return;
    if(!state.dataDate){
      el.innerHTML='⚠ 尚無預計算資料。請至 GitHub → Actions 手動觸發一次 <b>每日更新台股資料</b>。';
      el.style.borderColor='var(--negative)'; el.style.color='var(--negative)';
      return;
    }
    const srcLabel = state.dataSource==='finmind+twse' ? 'FinMind + TWSE（完整）'
                   : state.dataSource==='twse_only' ? 'TWSE（技術面）'
                   : '未知';
    const count = state.allStocks.length;
    el.innerHTML=`📦 預計算資料已載入 <strong>${count}</strong> 檔個股 | 來源：${srcLabel} | 篩選在本機執行，<strong>速度極快</strong>`;
    el.style.borderColor=''; el.style.color='';
  },

  toast(msg,type='info',ms=3000){
    const c=document.getElementById('toastContainer');
    const el=document.createElement('div'); el.className=`toast ${type}`; el.textContent=msg; c.appendChild(el);
    setTimeout(()=>el.remove(),ms);
  },

  renderTags(){
    const L={rs90:'RS > 90',nearMonthlyHigh:'距月高 ≤ 5%',shortMAAlign:'短均排列',longMAAlign:'中長均排列',aboveSubPoint:'站上MA5扣抵',revenueHighRecord:'營收創高',revenueYoY:'YoY連2月>20%',revenueMoM:'MoM連2月>20%',marginGrowth:'毛/營益率↑',noProfitLoss:'無虧損',chipConcentration:'籌碼集中↑',buyerSellerDiff:'買賣家數差<0',foreignBuy:'外資買超',trustBuy:'投信買超',bigHolderIncrease:'大戶比例↑',institutionalRecord:'法人持股季高'};
    const el=document.getElementById('activeTags'); if(!el)return;
    const af=getAF();
    el.innerHTML=af.length===0
      ?'<span style="font-size:11px;color:var(--text-dim)">未選條件 — 將顯示全部個股（本機即時篩選，無 API 呼叫）</span>'
      :af.map(f=>`<span class="filter-tag">${L[f]||f}<span class="filter-tag-remove" onclick="UI.rmFilter('${f}')">✕</span></span>`).join('');
    const cm={tech:['rs90','nearMonthlyHigh','shortMAAlign','longMAAlign','aboveSubPoint'],fund:['revenueHighRecord','revenueYoY','revenueMoM','marginGrowth','noProfitLoss'],chip:['chipConcentration','buyerSellerDiff','foreignBuy','trustBuy','bigHolderIncrease','institutionalRecord']};
    for(const[c,ks]of Object.entries(cm)){const b=document.getElementById(`badge-${c}`);if(b)b.textContent=`${ks.filter(k=>state.filters[k]).length}/${ks.length}`;}
  },
  rmFilter(key){state.filters[key]=false;const cb=document.getElementById(`filter-${key}`);if(cb)cb.checked=false;UI.renderTags();},

  /* Results table */
  renderResults(stocks){
    const container=document.getElementById('resultsContainer'); if(!container)return;
    // Ensure min-height:0 so nested flex scroll works (flex min-height cascade fix)
    container.style.cssText='flex:1;min-height:0;overflow:hidden;display:flex;flex-direction:column';
    const af=getAF();
    const count=stocks.length;

    // Empty state
    if(count===0){
      container.innerHTML=`
        <div class="results-toolbar">
          <span class="results-count">符合條件 <strong>0</strong> 檔</span>
          <button class="btn-secondary" onclick="UI.nav('screen')" style="font-size:11px">← 回篩選</button>
        </div>
        <div class="empty-state">
          <div class="empty-icon">🔍</div>
          <div class="empty-title">無個股符合所有篩選條件</div>
          <div class="empty-sub">嘗試減少篩選條件，或等待明日資料更新</div>
        </div>`;
      return;
    }

    const LABELS={rs90:'RS>90',nearMonthlyHigh:'距月高≤5%',shortMAAlign:'短均排列',longMAAlign:'中長均排列',aboveSubPoint:'站上MA5扣抵',revenueHighRecord:'營收創高',revenueYoY:'YoY連2月>20%',revenueMoM:'MoM連2月>20%',marginGrowth:'毛/營益率↑',noProfitLoss:'無虧損',chipConcentration:'籌碼集中↑',buyerSellerDiff:'買賣家數差<0',foreignBuy:'外資買超',trustBuy:'投信買超',bigHolderIncrease:'大戶比例↑',institutionalRecord:'法人持股季高'};
    const filterBar = af.length===0
      ? ''
      : `<div class="results-filter-bar">${af.map(f=>`<span class="filter-tag" style="font-size:10px;padding:2px 7px">${LABELS[f]||f}</span>`).join('')}</div>`;

    container.innerHTML=`
      ${filterBar}
      <div class="results-toolbar">
        <span class="results-count" id="resultsCount">
          ${af.length===0
            ? `全部 <strong>${count}</strong> 檔個股`
            : `符合所有條件 <strong style="color:var(--accent)">${count}</strong> 檔`}
        </span>
        <div style="display:flex;gap:8px;align-items:center">
          <span style="font-size:10px;color:var(--text-dim)">資料 ${fmtD(state.dataDate)}</span>
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
              <th style="color:#4FC3F7">扣抵<small style="display:block;opacity:.6;font-size:8px;font-weight:400">站上</small></th>
              <th style="color:#81C784">月營收<small style="display:block;opacity:.6;font-size:8px;font-weight:400">億元</small></th>
              <th style="color:#81C784">年增率<small style="display:block;opacity:.6;font-size:8px;font-weight:400">%</small></th>
              <th style="color:#81C784">月增率<small style="display:block;opacity:.6;font-size:8px;font-weight:400">%</small></th>
              <th style="color:#81C784">毛利率<small style="display:block;opacity:.6;font-size:8px;font-weight:400">%</small></th>
              <th style="color:#81C784">營益率<small style="display:block;opacity:.6;font-size:8px;font-weight:400">%</small></th>
              <th style="color:#81C784">盈虧</th>
              <th style="color:#FFB74D">外資<small style="display:block;opacity:.6;font-size:8px;font-weight:400">今日淨(張)</small></th>
              <th style="color:#FFB74D">投信<small style="display:block;opacity:.6;font-size:8px;font-weight:400">今日淨(張)</small></th>
              <th style="color:#FFB74D">大戶<small style="display:block;opacity:.6;font-size:8px;font-weight:400">持股變化</small></th>
            </tr>
          </thead>
          <tbody>${stocks.map(s=>this._rowHTML(s)).join('')}</tbody>
        </table>
      </div>`;
  },

  _pill(v,pt='✓',ft='✗'){
    if(v===null||v===undefined)return`<span class="ind-pill ind-na">N/A</span>`;
    return v===true?`<span class="ind-pill ind-pass">${pt}</span>`:`<span class="ind-pill ind-fail">${ft}</span>`;
  },
  _numPill(v,suf=''){
    if(v===null||v===undefined||isNaN(v))return`<span class="ind-pill ind-na">N/A</span>`;
    return`<span class="ind-pill ${v>0?'ind-pass':'ind-fail'}">${n2(v,1)}${suf}</span>`;
  },
  _rsCell(s){
    if(s===null||s===undefined)return`<span class="ind-pill ind-na">N/A</span>`;
    return`<span class="ind-pill ${s>=90?'ind-pass':s>=70?'ind-warn':'ind-fail'}">${Math.round(s)}</span>`;
  },

  _rowHTML(r){
    const pc=(r.pct||0)>=0?'pct-positive':'pct-negative';
    const pd=r.pct!=null?`${r.pct>=0?'+':''}${r.pct.toFixed(2)}%`:'--';
    const starred=WL.isWatched(r.code);
    return`<tr>
      <td><span class="stock-code">${r.code}</span></td>
      <td><span class="stock-name">${r.name}</span></td>
      <td class="price-cell ${r.price?pc:''}">${r.price?n2(r.price):'--'}</td>
      <td class="${r.price?pc:''} num-cell">${r.price?pd:'--'}</td>
      <td>${this._rsCell(r.rsScore)}</td>
      <td>${this._numPill(r.distanceFromHigh,'%')}</td>
      <td>${this._pill(r.shortMAAlign)}</td>
      <td>${this._pill(r.longMAAlign)}</td>
      <td>${this._pill(r.aboveSubPoint)}</td>
      <td class="num-cell">${r.revenue!=null?n2(r.revenue,1):'--'}</td>
      <td>${this._numPill(r.yoyLatest,'%')}</td>
      <td>${this._numPill(r.momLatest,'%')}</td>
      <td class="num-cell">${r.grossMargin!=null?n2(r.grossMargin,1)+'%':'--'}</td>
      <td class="num-cell">${r.opMargin!=null?n2(r.opMargin,1)+'%':'--'}</td>
      <td>${this._pill(r.noProfitLoss,'獲利','虧損')}</td>
      <td>${this._numPill(r.foreignNet!=null?Math.round(r.foreignNet/1000):null,'張')}</td>
      <td>${this._numPill(r.trustNet!=null?Math.round(r.trustNet/1000):null,'張')}</td>
      <td>${this._pill(r.bigHolderIncrease,'增加','減少')}</td>
      <td>
        <span class="watch-star ${starred?'starred':''}"
          onclick="event.stopPropagation();UI.openWatchModal('${r.code}','${(r.name||'').replace(/'/g,"\\'")}')">
          ${starred?'★':'☆'}
        </span>
      </td>
    </tr>`;
  },

  /* Watchlist modal */
  openWatchModal(code,name){
    const inCats=WL.catsFor(code);
    document.getElementById('wModalTitle').textContent=`${code} ${name}`;
    document.getElementById('wModalCurrent').textContent=inCats.length?`目前分類：${inCats.join('、')}`:'尚未加入任何分類';
    const sel=document.getElementById('wModalCatSel');
    sel.innerHTML=WL.cats().map(c=>`<option value="${c}">${c}${inCats.includes(c)?' ✓':''}</option>`).join('');
    document.getElementById('wModalRemoveBtn').style.display=inCats.length?'flex':'none';
    const m=document.getElementById('modalWatchlist');
    m.dataset.code=code; m.dataset.name=name; m.classList.add('open');
  },
  confirmWatch(){const m=document.getElementById('modalWatchlist');const code=m.dataset.code,cat=document.getElementById('wModalCatSel').value;WL.add(cat,code)?this.toast(`${code} 已加入「${cat}」`,'success'):this.toast(`${code} 已在「${cat}」`,'info');m.classList.remove('open');this._refreshStar(code);},
  removeWatch(){const code=document.getElementById('modalWatchlist').dataset.code;WL.removeAll(code);this.toast(`${code} 已從所有分類移除`,'info');document.getElementById('modalWatchlist').classList.remove('open');this._refreshStar(code);},
  newCatFromModal(){const nm=prompt('輸入新分類名稱：');if(!nm?.trim())return;if(WL.addCat(nm.trim())){const sel=document.getElementById('wModalCatSel');const o=document.createElement('option');o.value=nm.trim();o.textContent=nm.trim();sel.appendChild(o);sel.value=nm.trim();this.toast(`已新增「${nm.trim()}」`,'success');}else this.toast('分類已存在','error');},
  _refreshStar(code){const row=document.getElementById(`row-${code}`);if(!row)return;const s=row.querySelector('.watch-star');if(!s)return;const w=WL.isWatched(code);s.textContent=w?'★':'☆';s.classList.toggle('starred',w);},

  /* Watchlist page */
  renderWL(){
    const el=document.getElementById('watchlistContent');
    const cats=WL.list();
    if(cats.every(c=>c.stocks.length===0)){el.innerHTML=`<div class="empty-state"><div class="empty-icon">⭐</div><div class="empty-title">自選股清單為空</div><div class="empty-sub">在查詢結果中點 ☆ 加入自選股</div></div>`;return;}
    el.innerHTML=cats.map(cat=>`
      <div class="watchlist-category">
        <div class="wl-cat-header">
          <span class="wl-cat-name">📁 ${cat.name}</span><span class="wl-cat-count">${cat.stocks.length} 檔</span>
          ${cat.name!=='預設'?`<button class="btn-danger" onclick="UI.delCat('${cat.name}')">刪除分類</button>`:''}
        </div>
        <div class="wl-stock-list">
          ${cat.stocks.length===0?'<div style="padding:10px 14px;color:var(--text-dim);font-size:12px">暫無個股</div>'
            :cat.stocks.map(sid=>{
              const s=state.allStocks.find(x=>x.code===sid);
              const nm=s?s.name:sid;
              const pr=s&&s.price?`${n2(s.price)} (${s.pct>=0?'+':''}${n2(s.pct,2)}%)`:'--';
              const pc=s&&s.pct>=0?'pct-positive':'pct-negative';
              return`<div class="wl-stock-item"><span class="wl-stock-code">${sid}</span><span class="wl-stock-name">${nm}</span><span class="wl-stock-price ${pc}">${pr}</span><button class="btn-danger" onclick="UI.wlRm('${cat.name}','${sid}')">移除</button></div>`;
            }).join('')}
        </div>
        <div class="wl-add-row">
          <input type="text" id="wladd-${cat.name}" placeholder="輸入代碼（如 2330）" maxlength="5">
          <button class="wl-add-btn" onclick="UI.wlAdd('${cat.name}')">＋ 加入</button>
        </div>
      </div>`).join('');
  },
  wlAdd(cat){const inp=document.getElementById(`wladd-${cat}`);const sid=inp.value.trim();if(!/^\d{4,5}$/.test(sid)){this.toast('請輸入 4 位數字代碼','error');return;}WL.add(cat,sid)?(this.toast(`已加入 ${sid}`,'success'),inp.value='',this.renderWL()):this.toast('股票已存在','error');},
  wlRm(cat,sid){WL.remove(cat,sid);this.toast(`已移除 ${sid}`,'info');this.renderWL();},
  delCat(name){if(!confirm(`確定刪除分類「${name}」？`))return;WL.delCat(name);this.toast(`已刪除 ${name}`,'info');this.renderWL();},
  showAddCatModal(){document.getElementById('modalAddCat').classList.add('open');document.getElementById('newCatName').focus();},
  confirmAddCat(){const nm=document.getElementById('newCatName').value.trim();if(!nm){this.toast('請輸入名稱','error');return;}WL.addCat(nm)?(this.toast(`已新增「${nm}」`,'success'),document.getElementById('newCatName').value='',document.getElementById('modalAddCat').classList.remove('open'),this.renderWL()):this.toast('分類已存在','error');},

  /* Settings */
  renderSettings(){
    const di=document.getElementById('dataInfo');
    if(di){
      if(state.dataDate){
        const srcLabel = state.dataSource==='finmind+twse'      ? 'FinMind + TWSE（完整）'
                       : state.dataSource==='yfinance+finmind+twse' ? 'yfinance + FinMind + TWSE（完整）'
                       : state.dataSource==='yfinance+twse'     ? 'yfinance + TWSE（技術面）'
                       : state.dataSource==='twse_only'         ? 'TWSE（技術面）'
                       : state.dataSource || '未知';
        di.innerHTML=`資料日期：<strong>${fmtD(state.dataDate)}</strong><br>
          建置時間：<strong>${state.generated?new Date(state.generated).toLocaleString('zh-TW'):'-'}</strong><br>
          資料來源：<strong>${srcLabel}</strong><br>
          個股數量：<strong>${state.allStocks.length}</strong> 檔`;
      } else {
        di.innerHTML='<span style="color:var(--negative)">⚠ 尚無預計算資料，請執行 GitHub Actions</span>';
      }
    }
  },
  clearCache(){Cache.clear();this.toast('已清除快取，下次查詢將重新讀取 screener.json','success');},
  reloadData(){
    Cache.clear();
    state.allStocks=[];  // force fresh load
    UI.toast('重新載入資料中...','info',2000);
    loadScreenerData().then(data=>{
      state.allStocks  = data.stocks||[];
      state.dataDate   = data.dataDate;
      state.generated  = data.generated;
      state.dataSource = data.source;
      UI.updateHeader();
      UI.updateScopeBanner();
      UI.renderSettings();
      UI.nav('screen');   // 載入完成後回到篩選頁
      UI.toast(`已載入 ${state.allStocks.length} 檔個股（${fmtD(state.dataDate)}）`,'success',3000);
    }).catch(e=>{
      UI.toast('重新載入失敗：'+e.message,'error',5000);
    });
  },
};

/* ─────────────────────────────────────────────
   MAIN QUERY — instant client-side filtering
   ───────────────────────────────────────────── */
async function runQuery() {
  if (state.allStocks.length === 0) {
    // Need to load first
    const ov=document.getElementById('loadingOverlay');
    const lt=document.getElementById('loadingText'), lp=document.getElementById('loadingProgress');
    ov.classList.add('visible'); lt.textContent='載入預計算資料...'; lp.textContent='data/screener.json';
    try {
      const data = await loadScreenerData();
      state.allStocks = data.stocks || [];
      state.dataDate  = data.dataDate;
      state.generated = data.generated;
      state.dataSource= data.source;
      UI.updateHeader();
      UI.updateScopeBanner();
      ov.classList.remove('visible');

      if (state.allStocks.length === 0) {
        UI.toast('⚠ screener.json 無資料。請至 GitHub → Actions 手動執行一次 <每日更新台股資料>。','error',8000);
        return;
      }
      UI.toast(`已載入 ${state.allStocks.length} 檔個股 (${fmtD(state.dataDate)})`, 'success', 3000);
    } catch(e) {
      ov.classList.remove('visible');
      let msg=e.message;
      if(msg.includes('404')||msg.includes('HTTP 404')) msg='screener.json 尚不存在，請先執行 GitHub Actions 建置資料';
      UI.toast('載入失敗：'+msg,'error',8000);
      return;
    }
  }

  // Instant client-side filter
  const filtered = applyFilters(state.allStocks);
  state.results = filtered;

  UI.nav('results');
  UI.renderResults(filtered);

  const af=getAF();
  if(af.length>0){
    UI.toast(`篩選完成：${filtered.length} 檔符合條件（共 ${state.allStocks.length} 檔）`,'success',3000);
  }
}

/* ─────────────────────────────────────────────
   INIT
   ───────────────────────────────────────────── */
function init(){
  UI.applyTheme(state.theme); UI.updateHeader();
  if('serviceWorker' in navigator) navigator.serviceWorker.register('./sw.js').catch(()=>{});

  document.getElementById('themeBtn').addEventListener('click',()=>UI.applyTheme(state.theme==='dark'?'light':'dark'));
  document.querySelectorAll('.nav-btn').forEach(b=>b.addEventListener('click',()=>UI.nav(b.dataset.page)));

  document.querySelectorAll('.filter-checkbox').forEach(cb=>{
    cb.addEventListener('change',()=>{state.filters[cb.dataset.filter]=cb.checked;UI.renderTags();});
  });
  document.querySelectorAll('.select-all-btn').forEach(btn=>{
    btn.addEventListener('click',()=>{
      const cat=btn.dataset.cat;
      const cbs=document.querySelectorAll(`.filter-checkbox[data-cat="${cat}"]`);
      const allOn=[...cbs].every(c=>c.checked);
      cbs.forEach(c=>{c.checked=!allOn;state.filters[c.dataset.filter]=!allOn;});
      UI.renderTags();
    });
  });
  document.querySelectorAll('.filter-cat-header').forEach(h=>{
    h.addEventListener('click',e=>{if(e.target.closest('button'))return;h.closest('.filter-category')?.classList.toggle('collapsed');});
  });

  document.getElementById('queryBtn')?.addEventListener('click',runQuery);
  document.getElementById('reloadDataBtn')?.addEventListener('click',()=>UI.reloadData());
  document.getElementById('saveTokenBtn')?.addEventListener('click',()=>UI.saveToken());
  document.getElementById('clearCacheBtn')?.addEventListener('click',()=>UI.clearCache());

  document.getElementById('addCatBtn')?.addEventListener('click',()=>UI.showAddCatModal());
  document.getElementById('confirmCatBtn')?.addEventListener('click',()=>UI.confirmAddCat());
  document.getElementById('cancelCatBtn')?.addEventListener('click',()=>document.getElementById('modalAddCat').classList.remove('open'));
  document.getElementById('newCatName')?.addEventListener('keydown',e=>{if(e.key==='Enter')UI.confirmAddCat();});
  document.getElementById('modalAddCat')?.addEventListener('click',e=>{if(e.target.id==='modalAddCat')e.target.classList.remove('open');});

  document.getElementById('wModalConfirmBtn')?.addEventListener('click',()=>UI.confirmWatch());
  document.getElementById('wModalRemoveBtn')?.addEventListener('click',()=>UI.removeWatch());
  document.getElementById('wModalCancelBtn')?.addEventListener('click',()=>document.getElementById('modalWatchlist').classList.remove('open'));
  document.getElementById('wModalNewCatBtn')?.addEventListener('click',()=>UI.newCatFromModal());
  document.getElementById('modalWatchlist')?.addEventListener('click',e=>{if(e.target.id==='modalWatchlist')e.target.classList.remove('open');});

  UI.renderTags(); UI.nav('screen');

  // Pre-load data silently on startup
  loadScreenerData().then(data=>{
    state.allStocks = data.stocks||[];
    state.dataDate  = data.dataDate;
    state.generated = data.generated;
    state.dataSource= data.source;
    UI.updateHeader();
    UI.updateScopeBanner();
  }).catch(()=>{ UI.updateScopeBanner(); });
}

document.addEventListener('DOMContentLoaded', init);
