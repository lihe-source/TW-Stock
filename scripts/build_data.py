#!/usr/bin/env python3
"""台股雷達 build_data.py V1.9
Fix 1a: yfinance db locked  Fix 1b: Series ambiguous
Fix 2: log every 50  Fix 3: today data  Fix 4: marketSummary
"""
import json, os, re, time, logging, tempfile, threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, List, Tuple, Any
import requests

try:
    import pandas as pd; PANDAS_OK = True
except ImportError:
    PANDAS_OK = False

try:
    import yfinance as yf
    _CACHE = os.path.join(tempfile.gettempdir(), f'yf_tz_{os.getpid()}')
    os.makedirs(_CACHE, exist_ok=True)
    try: yf.set_tz_cache_location(_CACHE)
    except Exception: pass
    YF_OK = True
except ImportError:
    YF_OK = False

logging.basicConfig(level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s', datefmt='%H:%M:%S')
log = logging.getLogger(__name__)

_RAW_TOKENS    = [os.environ.get(k,'').strip() for k in
                  ('FINMIND_TOKEN','FINMIND_TOKEN_2','FINMIND_TOKEN_3')]
FINMIND_TOKENS = [t for t in _RAW_TOKENS if t]
STOCK_LIMIT    = int(os.environ.get('STOCK_LIMIT','0'))

TWSE_DAY_ALL   = 'https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL'
TWSE_LISTED    = 'https://openapi.twse.com.tw/v1/opendata/t187ap03_L'
TWSE_T86       = 'https://openapi.twse.com.tw/v1/fund/T86'
TWSE_STOCK_DAY = 'https://www.twse.com.tw/exchangeReport/STOCK_DAY'
FINMIND_BASE   = 'https://api.finmindtrade.com/api/v4/data'
OUTPUT_PATH    = 'data/screener.json'
TW_TZ          = timezone(timedelta(hours=8))
YFINANCE_CHUNK = 50
YFINANCE_PERIOD= '7mo'
FINMIND_SLEEP  = 1.2
TWSE_SLEEP     = 0.4
QUOTA_PER_KEY  = 550

SESSION = requests.Session()
SESSION.headers.update({
    'Accept': 'application/json,*/*',
    'Accept-Language': 'zh-TW,zh;q=0.9',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
})

def safe_get_json(url, params=None, retries=3, timeout=30):
    for attempt in range(retries):
        try:
            r = SESSION.get(url, params=params, timeout=timeout)
            r.raise_for_status()
            c = r.content.strip()
            if not c or c[:1] in (b'<',b' ',b'\n'): return None
            return r.json()
        except requests.exceptions.HTTPError as e:
            code = e.response.status_code if e.response else 0
            if 400<=code<500: return None
            if attempt<retries-1: time.sleep(2**attempt)
            else: return None
        except Exception as e:
            if attempt<retries-1: time.sleep(2**attempt)
            else: log.debug(f'sgj: {e}'); return None
    return None

is_equity  = lambda c: bool(c and re.match(r'^[1-9]\d{3}$',c))
date_now   = lambda: datetime.now(TW_TZ)
date_ago_s = lambda m: (date_now()-timedelta(days=m*30)).strftime('%Y-%m-%d')

def load_twse_day_all():
    base,names={},{}
    raw=safe_get_json(TWSE_DAY_ALL)
    for s in (raw or []):
        code=(s.get('Code') or '').strip()
        if not is_equity(code): continue
        try: price=float((s.get('ClosingPrice') or '').replace(',','').strip())
        except ValueError: continue
        if price<=0: continue
        vol_s=(s.get('TradeVolume') or '0').replace(',','')
        vol=float(vol_s) if vol_s.replace('.','').isdigit() else 0
        cr=str(s.get('Change') or '0').strip()
        ca=float(re.sub(r'[▲▼+\-\s,]','',cr) or '0')
        chg=-ca if (cr.startswith('▼') or cr.startswith('-')) else ca
        b=(price-chg) or price; pct=round(chg/b*100,2) if b else 0.0
        base[code]={'code':code,'name':(s.get('Name') or code).strip(),
                    'price':price,'change':round(chg,2),'pct':pct,
                    'high':float((s.get('HighestPrice') or str(price)).replace(',','')),
                    'low':float((s.get('LowestPrice') or str(price)).replace(',','')),
                    'vol':vol}
    log.info(f'  DAY_ALL: {len(base)} 筆')
    raw2=safe_get_json(TWSE_LISTED)
    for s in (raw2 or []):
        code=(s.get('公司代號') or '').strip()
        if is_equity(code): names[code]={'code':code,'name':(s.get('公司名稱') or code).strip()}
    log.info(f'  t187ap03_L: {len(names)} 筆')
    return base,names

def load_t86():
    inst={}
    raw=safe_get_json(TWSE_T86)
    if not raw: log.warning('  T86: 無資料'); return inst
    for s in raw:
        code=(s.get('Code') or s.get('證券代號') or '').strip()
        if not code: continue
        def f(*keys):
            for k in keys:
                v=s.get(k)
                if v is not None and str(v).strip() not in ('','--','-'):
                    try: return float(str(v).replace(',',''))
                    except: pass
            return 0.0
        fgn=f('Foreign_Investor_Net_Buy_or_Sell','外陸資買賣超股數(不含外資自營商)','外資買賣超')*1000
        tst=f('Investment_Trust_Net_Buy_or_Sell','投信買賣超股數','投信買賣超')*1000
        inst[code]={'foreignNet':round(fgn),'trustNet':round(tst),
                    'foreignBuy':fgn>0,'trustBuy':tst>0}
    log.info(f'  T86: {len(inst)} 筆')
    return inst

def calc_market_summary(inst_today):
    if not inst_today: return {}
    fn=sum(v.get('foreignNet',0) or 0 for v in inst_today.values())
    tn=sum(v.get('trustNet',0) or 0 for v in inst_today.values())
    AVG=60
    return {
        'foreignNetYi': round(fn*AVG/1e8,2),
        'trustNetYi':   round(tn*AVG/1e8,2),
        'foreignBuyCnt':  sum(1 for v in inst_today.values() if v.get('foreignBuy')),
        'foreignSellCnt': sum(1 for v in inst_today.values() if not v.get('foreignBuy') and v.get('foreignNet') is not None),
        'trustBuyCnt':    sum(1 for v in inst_today.values() if v.get('trustBuy')),
        'trustSellCnt':   sum(1 for v in inst_today.values() if not v.get('trustBuy') and v.get('trustNet') is not None),
    }

def _scalar(val):
    try:
        if PANDAS_OK and isinstance(val,pd.Series): val=val.iloc[0]
        v=float(val); return None if v!=v else v
    except: return None

def _get_col(row, *keys):
    """Fix 1b: avoid `or` on pandas Series — use explicit None check."""
    for k in keys:
        v=row.get(k)
        if v is None: continue
        r=_scalar(v)
        if r is not None: return r
    return None

def _row_to_ohlc(row, ds):
    c=_get_col(row,'Close','close')
    if c is None or c<=0: return None
    h=_get_col(row,'High','high') or c
    l=_get_col(row,'Low','low') or c
    return {'date':ds,'close':round(c,2),'max':round(h,2),'min':round(l,2)}

def _extract_df(raw, ticker):
    if not PANDAS_OK or raw is None or raw.empty: return None
    try:
        if isinstance(raw.columns,pd.MultiIndex):
            lvl1=raw.columns.get_level_values(1)
            if ticker not in lvl1: return None
            return raw.xs(ticker,level=1,axis=1)
        return raw
    except: return None

def download_price_history(codes):
    if not YF_OK: return {}
    result={}
    tickers=[f'{c}.TW' for c in codes]
    nb=-(-len(tickers)//YFINANCE_CHUNK)
    log.info(f'  下載 {len(tickers)} 支，共 {nb} 批')
    for i in range(0,len(tickers),YFINANCE_CHUNK):
        ct=tickers[i:i+YFINANCE_CHUNK]; cc=codes[i:i+YFINANCE_CHUNK]
        bn=i//YFINANCE_CHUNK+1
        try: raw=yf.download(ct,period=YFINANCE_PERIOD,auto_adjust=True,progress=False)
        except Exception as e: log.warning(f'  批次{bn}失敗:{e}'); continue
        if raw is None or (PANDAS_OK and isinstance(raw,pd.DataFrame) and raw.empty): continue
        ok=0
        for ticker,code in zip(ct,cc):
            try:
                df=_extract_df(raw,ticker) if len(ct)>1 else raw
                if df is None or (PANDAS_OK and isinstance(df,pd.DataFrame) and df.empty): continue
                rows=[r for r in (_row_to_ohlc(row,di.strftime('%Y-%m-%d')) for di,row in df.iterrows()) if r]
                rows.sort(key=lambda x:x['date'],reverse=True)
                if rows: result[code]=rows; ok+=1
            except Exception as e: log.debug(f'  {code}:{e}')
        log.info(f'  批次{bn}/{nb}: {ok}/{len(cc)} 支成功')
        time.sleep(0.5)
    log.info(f'  yfinance 總計：{len(result)}/{len(codes)} 支')
    return result

def download_single(ticker_tw):
    """Fix 1b: handle MultiIndex for single-ticker too."""
    if not YF_OK: return []
    try:
        raw=yf.download(ticker_tw,period=YFINANCE_PERIOD,auto_adjust=True,progress=False)
        if raw is None or (PANDAS_OK and isinstance(raw,pd.DataFrame) and raw.empty): return []
        if PANDAS_OK and isinstance(raw.columns,pd.MultiIndex):
            df=_extract_df(raw,ticker_tw)
            if df is None or df.empty: return []
        else: df=raw
        rows=[r for r in (_row_to_ohlc(row,di.strftime('%Y-%m-%d')) for di,row in df.iterrows()) if r]
        rows.sort(key=lambda x:x['date'],reverse=True)
        return rows
    except Exception as e:
        log.warning(f'  download_single {ticker_tw}: {e}'); return []

def twse_stock_history(code,months=7):
    rows=[]
    for m in range(months):
        t=date_now()-timedelta(days=m*30)
        j=safe_get_json(TWSE_STOCK_DAY,{'response':'json','date':t.strftime('%Y%m01'),'stockNo':code})
        for row in (j or {}).get('data',[]):
            try:
                p=row[0].strip().split('/'); y=int(p[0])+1911
                rows.append({'date':f'{y}-{p[1].zfill(2)}-{p[2].zfill(2)}',
                             'close':float(row[6].replace(',','')),
                             'max':float(row[4].replace(',','')),
                             'min':float(row[5].replace(',',''))})
            except: pass
        time.sleep(TWSE_SLEEP)
    rows.sort(key=lambda x:x['date'],reverse=True)
    return rows

class KeyWorker:
    def __init__(self,token,index,limit=QUOTA_PER_KEY):
        self.token=token; self.index=index; self.limit=limit
        self.count=0; self.exceeded=False
        self._lock=threading.Lock(); self._last=0.0
        self._s=requests.Session(); self._s.headers.update(SESSION.headers)
    @property
    def has_quota(self): return not self.exceeded and self.count<self.limit
    def fetch(self,dataset,code,start_date):
        with self._lock:
            if self.exceeded: return []
            w=FINMIND_SLEEP-(time.monotonic()-self._last)
            if w>0: time.sleep(w)
        p={'dataset':dataset,'data_id':code,'start_date':start_date,'token':self.token}
        try:
            r=self._s.get(FINMIND_BASE,params=p,timeout=25)
            with self._lock: self._last=time.monotonic()
            if r.status_code==402:
                with self._lock:
                    if not self.exceeded: self.exceeded=True; log.warning(f'  Key{self.index} 402({self.count}次)')
                return []
            if r.status_code==422: return []
            r.raise_for_status()
            c=r.content.strip()
            if not c or c[:1] in (b'<',b'\n'): return []
            j=r.json()
            if j.get('status')!=200:
                m=j.get('msg','')
                if any(k in m for k in ('quota','次數','超過','limit')):
                    with self._lock: self.exceeded=True
                return []
            with self._lock:
                self.count+=1
                if self.count>=self.limit: self.exceeded=True; log.info(f'  Key{self.index}達上限')
            return j.get('data',[])
        except Exception as e: log.debug(f'  Key{self.index} {code}:{e}'); return []

def load_revenue_finmind(codes):
    if not FINMIND_TOKENS: log.info('  無Token，月營收N/A'); return {}
    start=date_ago_s(24)
    workers=[KeyWorker(t,i+1) for i,t in enumerate(FINMIND_TOKENS)]
    nk=len(workers)
    log.info(f'  FinMind月營收:{len(codes)}支，{nk}個並發Key')
    log.info(f'  預估:{len(codes)/nk*FINMIND_SLEEP/60:.1f}分鐘')
    result={}; rl=threading.Lock()
    cnt={'n':0}; cl=threading.Lock()
    def fetch_one(code,wk):
        if not wk.has_quota: return
        data=wk.fetch('TaiwanStockMonthRevenue',code,start)
        if data:
            s=sorted(data,key=lambda x:x['date'],reverse=True)
            with rl: result[code]=[{'date':d['date'],'revenue':float(d['revenue'])} for d in s]
        with cl: cnt['n']+=1; n=cnt['n']
        if n%50==0:  # Fix 2: every 50
            sm=', '.join(f'Key{w.index}:{w.count}次' for w in workers)
            log.info(f'  月營收進度:{n}/{len(codes)}，{sm}')
    per=[[] for _ in range(nk)]
    for i,c in enumerate(codes): per[i%nk].append(c)
    def run(wk,batch):
        log.info(f'  Key{wk.index}啟動，負責{len(batch)}支')
        for c in batch:
            if not wk.has_quota: break
            fetch_one(c,wk)
    with ThreadPoolExecutor(max_workers=nk) as pool:
        futs=[pool.submit(run,wk,b) for wk,b in zip(workers,per)]
        for f in as_completed(futs):
            try: f.result()
            except Exception as e: log.warning(f'  Worker異常:{e}')
    sm=', '.join(f'Key{w.index}:{w.count}次' for w in workers)
    log.info(f'  月營收完成:{len(result)}/{len(codes)}支 | {sm}')
    return result

avg=lambda lst: sum(lst)/len(lst) if lst else 0.0

def calc_rs(sp,tp):
    n=min(130,len(sp)-1,len(tp)-1)
    if n<20: return None
    try:
        sN,sP=float(sp[0]['close']),float(sp[n]['close'])
        tN,tP=float(tp[0]['close']),float(tp[n]['close'])
        if not sP or not tP: return None
        ratio=(1+(sN-sP)/sP)/(1+max((tN-tP)/tP,-0.95))
        return max(0.0,min(99.0,round(((ratio-0.85)/0.35)*100,1)))
    except: return None

def calc_technical(prices,proxy):
    if not prices: return {}
    closes=[float(p['close']) for p in prices]; cur=closes[0]
    def ma(n): return avg(closes[:n]) if len(closes)>=n else None
    m5,m10,m20=ma(5),ma(10),ma(20); m60,m120=ma(60),ma(120)
    short=bool(m5>m10>m20) if all(x is not None for x in [m5,m10,m20]) else None
    long =bool(m20>m60>m120) if all(x is not None for x in [m20,m60,m120]) else None
    sub  =bool(cur>float(prices[5]['close'])) if len(prices)>=6 else None
    dh=None
    lk=min(22,len(prices))
    if lk:
        mh=max(float(p.get('max',p['close'])) for p in prices[:lk])
        dh=round(((cur/mh)-1)*100,2) if mh else None
    return {'rsScore':calc_rs(prices,proxy),'shortMAAlign':short,
            'longMAAlign':long,'aboveSubPoint':sub,'distanceFromHigh':dh}

def calc_revenue(rv):
    if len(rv)<3: return {}
    l=rv[0]; lv=float(l['revenue'])
    yoy,mom=[],[]
    for i in range(min(2,len(rv))):
        cur=rv[i]; cd=datetime.strptime(cur['date'][:7],'%Y-%m')
        py=next((d for d in rv if
                 datetime.strptime(d['date'][:7],'%Y-%m').month==cd.month and
                 datetime.strptime(d['date'][:7],'%Y-%m').year==cd.year-1),None)
        if py:
            dn=float(py['revenue'])
            if dn>0: yoy.append((float(cur['revenue'])-dn)/dn)
        if i<len(rv)-1:
            pv=float(rv[i+1]['revenue'])
            if pv>0: mom.append((float(cur['revenue'])-pv)/pv)
    allv=[float(d['revenue']) for d in rv]
    ld=datetime.strptime(l['date'][:7],'%Y-%m')
    sly=next((d for d in rv if
              datetime.strptime(d['date'][:7],'%Y-%m').month==ld.month and
              datetime.strptime(d['date'][:7],'%Y-%m').year==ld.year-1),None)
    return {'revenue':round(lv/1e8,2),'revenueDate':l['date'],
            'yoyLatest':round(yoy[0]*100,2) if yoy else None,
            'momLatest':round(mom[0]*100,2) if mom else None,
            'yoy2mo':len(yoy)==2 and all(v>=0.20 for v in yoy),
            'mom2mo':len(mom)==2 and all(v>=0.20 for v in mom),
            'revenueHighRecord':lv>=max(allv) or (float(sly['revenue'])<lv if sly else False)}

def main():
    log.info('=== 台股雷達資料建置 V1.9 開始 ===')
    log.info(f'yfinance:{"✓" if YF_OK else "✗"} pandas:{"✓" if PANDAS_OK else "✗"}')
    log.info(f'FinMind Keys:{len(FINMIND_TOKENS)}組 配額:{len(FINMIND_TOKENS)*QUOTA_PER_KEY}次')
    data_date=date_now().strftime('%Y-%m-%d')

    log.info('Step 1: TWSE...')
    sb,nm=load_twse_day_all()
    codes_set=set(sb)|set(nm)
    if STOCK_LIMIT: codes_set=set(sorted(codes_set)[:STOCK_LIMIT])
    all_codes=sorted(codes_set)
    log.info(f'  共{len(all_codes)}支')

    log.info('Step 2: T86 法人...')
    inst=load_t86()
    mkt=calc_market_summary(inst)
    if mkt:
        log.info(f'  大盤外資:{mkt["foreignNetYi"]:+.2f}億 ({mkt["foreignBuyCnt"]}買/{mkt["foreignSellCnt"]}賣)')
        log.info(f'  大盤投信:{mkt["trustNetYi"]:+.2f}億 ({mkt["trustBuyCnt"]}買/{mkt["trustSellCnt"]}賣)')

    log.info('Step 3: yfinance K線...')
    ph=download_price_history(all_codes) if YF_OK else {}
    proxy=ph.get('0050',[])
    if not proxy:
        log.info('  單獨下載0050...')
        proxy=download_single('0050.TW')
        if proxy: ph['0050']=proxy; log.info(f'  0050:{len(proxy)}筆')
        else:
            log.info('  0050 yfinance失敗，改用TWSE...')
            proxy=twse_stock_history('0050',7)
            log.info(f'  0050(TWSE):{len(proxy)}筆')

    log.info('Step 4: 月營收...')
    rev_all=load_revenue_finmind(all_codes)

    log.info('Step 5: 指標...')
    p0050=sb.get('0050')
    if p0050 and proxy and proxy[0]['date']<data_date:
        proxy=[{'date':data_date,'close':p0050['price'],'max':p0050['high'],'min':p0050['low']}]+proxy
        log.info(f'  0050今日補入:{data_date} {p0050["price"]}')
    results=[]
    for i,code in enumerate(all_codes):
        if i>0 and i%200==0: log.info(f'  進度:{i}/{len(all_codes)}...')
        base=sb.get(code) or nm.get(code) or {'code':code,'name':code}
        it=inst.get(code,{})
        r={'code':base['code'],'name':base['name'],
           'price':base.get('price',0),'change':base.get('change',0),'pct':base.get('pct',0),
           'high':base.get('high',0),'low':base.get('low',0),'vol':base.get('vol',0),
           'foreignNet':it.get('foreignNet'),'trustNet':it.get('trustNet'),
           'foreignBuy':it.get('foreignBuy'),'trustBuy':it.get('trustBuy'),
           'rsScore':None,'distanceFromHigh':None,'shortMAAlign':None,'longMAAlign':None,'aboveSubPoint':None,
           'revenue':None,'revenueDate':None,'yoyLatest':None,'momLatest':None,
           'yoy2mo':None,'mom2mo':None,'revenueHighRecord':None,
           'grossMargin':None,'opMargin':None,'noProfitLoss':None,'marginGrowth':None,
           'institutionalRecord':None,'bigHolderPct':None,'bigHolderIncrease':None,'chipConcentration':None}
        px=ph.get(code)
        if not px and not YF_OK: px=twse_stock_history(code,7)
        if px:
            tp=base.get('price',0); th=base.get('high',0); tl=base.get('low',0)
            if tp>0 and px[0]['date']<data_date:
                px=[{'date':data_date,'close':tp,'max':th or tp,'min':tl or tp}]+px
            r.update(calc_technical(px,proxy))
            if not r['price']:
                try: r['price']=float(px[0]['close'])
                except: pass
        rl=rev_all.get(code,[])
        if rl: r.update(calc_revenue(rl))
        results.append(r)

    hrs=sum(1 for r in results if r['rsScore'] is not None)
    hrv=sum(1 for r in results if r['revenue'] is not None)
    hfi=sum(1 for r in results if r['foreignBuy'] is not None)
    log.info(f'  RS:{hrs} 月營收:{hrv} 法人:{hfi} / {len(results)}')

    log.info('Step 6: 輸出...')
    os.makedirs('data',exist_ok=True)
    tw_now=date_now()
    out={'version':'V1.9','generated':tw_now.isoformat(),'dataDate':data_date,
         'source':'yfinance+finmind+twse' if FINMIND_TOKENS else 'yfinance+twse',
         'stockCount':len(results),'coverage':{'technical':hrs,'revenue':hrv,'institutional':hfi},
         'marketSummary':mkt,'stocks':results}
    with open(OUTPUT_PATH,'w',encoding='utf-8') as f:
        json.dump(out,f,ensure_ascii=False,separators=(',',':'))
    log.info(f'  ✅ {len(results)}支 → {OUTPUT_PATH} ({os.path.getsize(OUTPUT_PATH)/1024:.0f}KB)')
    log.info('=== 完成 ===')

if __name__=='__main__':
    main()
