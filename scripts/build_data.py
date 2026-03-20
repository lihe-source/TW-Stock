#!/usr/bin/env python3
"""台股雷達 build_data.py V3.0
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

try:
    from bs4 import BeautifulSoup; BS4_OK = True
except ImportError:
    BS4_OK = False

logging.basicConfig(level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s', datefmt='%H:%M:%S')
log = logging.getLogger(__name__)

_RAW_TOKENS    = [os.environ.get(k,'').strip() for k in
                  ('FINMIND_TOKEN','FINMIND_TOKEN_2','FINMIND_TOKEN_3',
                   'FINMIND_TOKEN_4','FINMIND_TOKEN_5','FINMIND_TOKEN_6')]
FINMIND_TOKENS = [t for t in _RAW_TOKENS if t]
STOCK_LIMIT    = int(os.environ.get('STOCK_LIMIT','0'))

TWSE_DAY_ALL   = 'https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL'
TWSE_LISTED    = 'https://openapi.twse.com.tw/v1/opendata/t187ap03_L'
TWSE_T86       = 'https://openapi.twse.com.tw/v1/fund/T86'     # OpenAPI（備援）
TWSE_T86_MAIN  = 'https://www.twse.com.tw/rwd/zh/fund/T86'     # 主要（盤後正式）
TWSE_T86_ALT   = 'https://www.twse.com.tw/fund/T86'            # 備援
TWSE_STOCK_DAY = 'https://www.twse.com.tw/exchangeReport/STOCK_DAY'
FINMIND_BASE   = 'https://api.finmindtrade.com/api/v4/data'
OUTPUT_PATH    = 'data/screener.json'
TW_TZ          = timezone(timedelta(hours=8))
YFINANCE_CHUNK = 50
YFINANCE_PERIOD= '7mo'
FINMIND_SLEEP  = 0.5   # V1.9: 1.2→0.5s（並發模式下更激進）
TWSE_SLEEP     = 0.4
QUOTA_PER_KEY  = 200  # FinMind 免費版實測每 Key 約 207 次，設 200 保守換 Key

TWSE_T86_HIST  = 'https://www.twse.com.tw/fund/T86'  # 歷史 T86（法人5/10日累計）

# MOPS 月營收：一次請求取得所有上市公司，24個月只需24次請求
# 上市(sii)：t21sc03_{民國年}_{月:02d}_0.html
# 上櫃(otc)：另外的 URL（可選）
MOPS_REV_SII = 'https://mops.twse.com.tw/nas/t21/sii/t21sc03_{roc}_{month:02d}_0.html'

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

def _parse_t86_response(raw) -> dict:
    """
    解析 T86 回應，支援兩種格式：
      格式 A（OpenAPI）: list of dict 直接含 Code, Foreign_Investor_Net_Buy_or_Sell 等
      格式 B（twse.com.tw）: {stat, fields, data} 其中 data 是 list of list
    """
    inst = {}
    if not raw:
        return inst

    # 格式 A：list of dict（openapi.twse.com.tw）
    if isinstance(raw, list):
        for s in raw:
            code = (s.get('Code') or s.get('證券代號') or '').strip()
            if not code:
                continue
            def fv(*keys):
                for k in keys:
                    v = s.get(k)
                    if v is not None and str(v).strip() not in ('','--','-'):
                        try: return float(str(v).replace(',',''))
                        except: pass
                return 0.0
            fgn = fv('Foreign_Investor_Net_Buy_or_Sell','外陸資買賣超股數(不含外資自營商)','外資買賣超') * 1000
            tst = fv('Investment_Trust_Net_Buy_or_Sell','投信買賣超股數','投信買賣超') * 1000
            inst[code] = {'foreignNet':round(fgn),'trustNet':round(tst),
                          'foreignBuy':fgn>0,'trustBuy':tst>0}
        return inst

    # 格式 B：{stat, fields, data}（www.twse.com.tw）
    if isinstance(raw, dict):
        stat = raw.get('stat','')
        if stat not in ('OK','ok') and raw.get('status') not in ('OK','ok',200,None):
            return inst
        fields = raw.get('fields', [])
        data   = raw.get('data', [])

        # 找欄位索引（欄位名稱每季可能不同）
        def find_col(*candidates):
            for c in candidates:
                if c in fields:
                    return fields.index(c)
            return -1

        code_col  = find_col('證券代號','代號','Code')
        fgn_col   = find_col('外陸資買賣超股數(不含外資自營商)','外資買賣超股數',
                              'Foreign_Investor_Net_Buy_or_Sell')
        trust_col = find_col('投信買賣超股數','Investment_Trust_Net_Buy_or_Sell')

        if code_col < 0 and data:
            # 欄位找不到時，用固定位置（TWSE T86 已知格式）
            # 典型: [代號, 名稱, ..., 外資淨(col 4), ..., 投信淨(col 7)]
            code_col  = 0
            fgn_col   = 4
            trust_col = 7

        for row in data:
            try:
                code = str(row[code_col]).strip()
                if not is_equity(code):
                    continue
                def pv(col):
                    if col < 0 or col >= len(row): return 0.0
                    s = str(row[col]).replace(',','').replace('+','').strip()
                    return float(s) if s and s not in ('--','-','') else 0.0
                fgn = pv(fgn_col) * 1000
                tst = pv(trust_col) * 1000
                inst[code] = {'foreignNet':round(fgn),'trustNet':round(tst),
                              'foreignBuy':fgn>0,'trustBuy':tst>0}
            except Exception:
                pass
    return inst


def load_t86() -> dict:
    """
    載入今日三大法人買賣超。
    策略：複用 load_t86_historical(1) 取今日資料，格式與歷史相同。
    同時保留 OpenAPI 備援（盤中即時更新用）。
    """
    today_str = date_now().strftime('%Y%m%d')

    # 1. 嘗試 www.twse.com.tw（盤後正式資料）
    for url, label in [(TWSE_T86_MAIN, 'rwd'), (TWSE_T86_ALT, 'fund')]:
        try:
            for sel in ['ALLBUT0999', 'ALL']:
                params = {'response':'json','date':today_str,'selectType':sel}
                raw = safe_get_json(url, params=params, timeout=30)
                if raw is None:
                    continue
                inst = _parse_t86_response(raw)
                if inst:
                    log.info(f'  T86 今日 ({label}/{sel}): {len(inst)} 筆')
                    return inst
        except Exception as e:
            log.debug(f'  T86 {label}: {e}')

    # 2. OpenAPI 備援（盤中）
    try:
        raw = safe_get_json(TWSE_T86, timeout=20)
        if raw:
            inst = _parse_t86_response(raw)
            if inst:
                log.info(f'  T86 今日 (OpenAPI): {len(inst)} 筆')
                return inst
    except Exception as e:
        log.debug(f'  T86 OpenAPI: {e}')

    # 3. 從歷史 T86 的第 1 天（昨日）補用
    log.warning('  T86 今日: 無資料，嘗試使用昨日資料替代')
    return {}

def load_t86_historical(days: int = 10) -> dict:
    """
    TWSE 歷史 T86 — 取最近 N 個交易日，計算 5 日累計法人買賣超。
    與 load_t86 使用相同 URL 和解析邏輯，確保一致性。
    """
    daily: dict = {}   # code → {f:[day1,day2,...], t:[day1,day2,...]}
    fetched = 0
    now = date_now()

    for delta in range(20):
        if fetched >= days:
            break
        d = now - timedelta(days=delta)
        if d.weekday() >= 5:
            continue
        ds = d.strftime('%Y%m%d')

        # 嘗試多個 URL（與 load_t86 相同策略）
        for url, label in [(TWSE_T86_MAIN, 'rwd'), (TWSE_T86_ALT, 'alt')]:
            try:
                j = safe_get_json(url,
                    {'response':'json','date':ds,'selectType':'ALLBUT0999'}, timeout=20)
                if not j:
                    continue
                # 快速驗證是否有資料
                data = j.get('data') if isinstance(j, dict) else j
                if not data:
                    continue

                # 用統一解析器
                day_inst = _parse_t86_response(j if isinstance(j, dict) else j)
                if day_inst:
                    fetched += 1
                    for code, v in day_inst.items():
                        if code not in daily:
                            daily[code] = {'f':[], 't':[]}
                        daily[code]['f'].append(v['foreignNet'])
                        daily[code]['t'].append(v['trustNet'])
                    log.debug(f'  T86 hist {ds} ({label}): {len(day_inst)} 筆')
                    time.sleep(TWSE_SLEEP)
                    break   # 成功就不再嘗試備援 URL
            except Exception as e:
                log.debug(f'  T86 hist {ds} {label}: {e}')

    log.info(f'  T86歷史: {fetched} 交易日，{len(daily)} 支個股')

    out = {}
    for code, v in daily.items():
        fn5 = sum(v['f'][:5])
        tn5 = sum(v['t'][:5])
        out[code] = {'foreignNet5d': round(fn5), 'trustNet5d': round(tn5),
                     'foreignBuy5d': fn5 > 0,    'trustBuy5d': tn5 > 0}
    return out

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
        try: raw=yf.download(ct,period=YFINANCE_PERIOD,auto_adjust=True,progress=False,threads=False)
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

def _flatten_df(df):
    """
    V1.9 Fix: 新版 yfinance 單支下載也可能有 MultiIndex columns (Price×Ticker)。
    用 droplevel(1, axis=1) 攤平，確保後續 row['Close'] 永遠是 scalar。
    """
    if not PANDAS_OK: return df
    try:
        if isinstance(df.columns, pd.MultiIndex):
            # droplevel ticker level → plain ['Close','High','Low',...]
            df = df.droplevel(1, axis=1)
    except Exception:
        pass
    return df

def download_single(ticker_tw):
    """Fix 1b: flatten MultiIndex before iterrows to prevent Series ambiguous error."""
    if not YF_OK: return []
    try:
        raw=yf.download(ticker_tw,period=YFINANCE_PERIOD,auto_adjust=True,progress=False,threads=False)
        if raw is None or (PANDAS_OK and isinstance(raw,pd.DataFrame) and raw.empty): return []
        df = _flatten_df(raw)   # ← flatten so row['Close'] is always scalar
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

def load_revenue_mops(months: int = 24) -> Dict[str, List[dict]]:
    """
    MOPS 月營收 — 完全免費，不需任何 Token。

    每次請求一個月份的 HTML = 所有上市公司的月營收。
    24 個月只需 24 次請求（原本 FinMind 需要 1072 次）。

    URL: https://mops.twse.com.tw/nas/t21/sii/t21sc03_{民國年}_{月:02d}_0.html
    HTML 表格欄位：公司代號 | 公司名稱 | 當月營收 | 上月營收 | 去年當月 | ...

    備註：MOPS 曾對 GitHub Actions IP 封鎖，但 nas/ 路徑的靜態 HTML 通常不受限。
    若連續失敗，自動改用 FinMind 補救（若有 token）。
    """
    now      = date_now()
    result: Dict[str, List[dict]] = {}
    fetched  = 0
    failed   = 0

    log.info(f'  MOPS 月營收：抓取最近 {months} 個月...')

    for m in range(months):
        target    = now - timedelta(days=m * 30)
        roc_year  = target.year - 1911
        month     = target.month
        iso_date  = f'{target.year}-{month:02d}-01'

        # 當月資料通常次月 10 日才公告，跳過可能尚未發布的月份
        if m == 0 and now.day < 12:
            log.debug(f'  跳過 {target.year}/{month:02d}（當月尚未公告）')
            continue

        url = MOPS_REV_SII.format(roc=roc_year, month=month)
        try:
            r = SESSION.get(url, timeout=30)
            r.raise_for_status()

            # 解碼 HTML（MOPS 可能是 big5 或 utf-8）
            for enc in (r.apparent_encoding, 'big5', 'utf-8', 'cp950'):
                try:
                    html = r.content.decode(enc or 'big5', errors='replace')
                    break
                except Exception:
                    html = r.text

            parsed = _parse_mops_revenue(html)
            if not parsed:
                log.debug(f'  MOPS {target.year}/{month:02d}: 解析 0 筆')
                failed += 1
                if failed >= 3:
                    log.warning('  MOPS 連續失敗 3 次，可能被封鎖，停止嘗試')
                    break
                continue

            for code, rev in parsed.items():
                result.setdefault(code, []).append({'date': iso_date, 'revenue': rev})

            fetched += 1
            failed  = 0   # 成功則重設連續失敗計數
            log.info(f'  MOPS {target.year}/{month:02d}: {len(parsed)} 筆')
            time.sleep(1.5)   # MOPS 禮貌等待

        except Exception as e:
            log.warning(f'  MOPS {roc_year}/{month:02d} 失敗: {e}')
            failed += 1
            if failed >= 3:
                log.warning('  MOPS 連續失敗，停止')
                break

    # 每支公司按日期降序排列
    for code in result:
        result[code].sort(key=lambda x: x['date'], reverse=True)

    log.info(f'  MOPS 月營收完成：{fetched} 個月，{len(result)} 支公司')
    return result


def _parse_mops_revenue(html: str) -> Dict[str, float]:
    """
    解析 MOPS 月營收 HTML，回傳 {code: revenue_float}。
    優先用 BeautifulSoup，備援用 regex。
    """
    result: Dict[str, float] = {}

    if BS4_OK:
        try:
            soup = BeautifulSoup(html, 'lxml')
            for row in soup.find_all('tr'):
                cells = row.find_all('td')
                if len(cells) < 3:
                    continue
                code = cells[0].get_text(strip=True)
                if not is_equity(code):
                    continue
                rev_str = cells[2].get_text(strip=True).replace(',', '').replace(' ', '')
                try:
                    rev = float(rev_str)
                    if rev > 0:
                        result[code] = rev
                except ValueError:
                    pass
            if result:
                return result
        except Exception:
            pass

    # Regex 備援
    pattern = (r'<td[^>]*>\s*([1-9]\d{3})\s*</td>'
               r'\s*<td[^>]*>[^<]*</td>'
               r'\s*<td[^>]*>\s*([\d,]+)\s*</td>')
    for m in re.finditer(pattern, html, re.IGNORECASE | re.DOTALL):
        code    = m.group(1).strip()
        rev_str = m.group(2).replace(',', '')
        try:
            rev = float(rev_str)
            if rev > 0:
                result[code] = rev
        except ValueError:
            pass

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
    log.info('=== 台股雷達資料建置 V3.0 開始 ===')
    log.info(f'yfinance:{"✓" if YF_OK else "✗"} pandas:{"✓" if PANDAS_OK else "✗"}')
    log.info(f'yfinance:{"✓" if YF_OK else "✗"} pandas:{"✓" if PANDAS_OK else "✗"} bs4:{"✓" if BS4_OK else "✗(regex備援)"}')
    data_date=date_now().strftime('%Y-%m-%d')

    log.info('Step 1: TWSE...')
    sb,nm=load_twse_day_all()
    codes_set=set(sb)|set(nm)
    if STOCK_LIMIT: codes_set=set(sorted(codes_set)[:STOCK_LIMIT])
    all_codes=sorted(codes_set)
    log.info(f'  共{len(all_codes)}支')

    log.info('Step 2: T86 法人...')
    inst=load_t86()

    log.info('Step 2b: T86 歷史（5日累計法人）...')
    inst5d = load_t86_historical(10)

    # 若今日 T86 無資料，用歷史第 1 天（最新一天）補用
    if not inst and inst5d:
        log.info('  T86 今日無資料，改用歷史最新一天（昨日）補用')
        # 重建今日 inst 從 5d 資料（取最新日）
        for url, label in [(TWSE_T86_MAIN,'rwd'),(TWSE_T86_ALT,'alt')]:
            yesterday = (date_now()-timedelta(days=1)).strftime('%Y%m%d')
            raw = safe_get_json(url, {'response':'json','date':yesterday,'selectType':'ALLBUT0999'}, timeout=20)
            if raw:
                inst = _parse_t86_response(raw)
                if inst:
                    log.info(f'  T86 昨日 ({label}): {len(inst)} 筆（補用）')
                    break

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

    log.info('Step 4: 月營收（MOPS，免費）...')
    rev_all = load_revenue_mops(24)

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
        it5=inst5d.get(code,{})
        r={'code':base['code'],'name':base['name'],
           'price':base.get('price',0),'change':base.get('change',0),'pct':base.get('pct',0),
           'high':base.get('high',0),'low':base.get('low',0),'vol':base.get('vol',0),
           # 今日法人（T86 當日）
           'foreignNet':it.get('foreignNet'),'trustNet':it.get('trustNet'),
           'foreignBuy':it.get('foreignBuy'),'trustBuy':it.get('trustBuy'),
           # 5日累計法人（T86 歷史）
           'foreignNet5d':it5.get('foreignNet5d'),'trustNet5d':it5.get('trustNet5d'),
           'foreignBuy5d':it5.get('foreignBuy5d'),'trustBuy5d':it5.get('trustBuy5d'),
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
    out={'version':'V3.0','generated':tw_now.isoformat(),'dataDate':data_date,
         'source':'yfinance+mops+twse',
         'stockCount':len(results),'coverage':{'technical':hrs,'revenue':hrv,'institutional':hfi},
         'marketSummary':mkt,'stocks':results}
    with open(OUTPUT_PATH,'w',encoding='utf-8') as f:
        json.dump(out,f,ensure_ascii=False,separators=(',',':'))
    log.info(f'  ✅ {len(results)}支 → {OUTPUT_PATH} ({os.path.getsize(OUTPUT_PATH)/1024:.0f}KB)')
    log.info('=== 完成 ===')

if __name__=='__main__':
    main()
