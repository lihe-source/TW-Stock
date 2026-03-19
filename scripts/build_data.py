#!/usr/bin/env python3
"""
台股雷達 — 資料建置腳本  V1.3

新增：
  - 多組 FinMind Token 輪換（FINMIND_TOKEN, FINMIND_TOKEN_2, FINMIND_TOKEN_3）
  - 每組 key 獨立配額守衛，達上限或遇 402 自動切換下一組
  - 3 組 × 550 次 = 1650 次，足夠 1072 支個股月營收

繼承 V1.2 修正：
  - T86 安全 JSON 解析
  - yfinance 新版 MultiIndex 欄位相容
  - MOPS 移除（GitHub Actions IP 被封鎖）

資料來源：
  技術面  → yfinance (Yahoo Finance)  批次 K 線（免費）
  法人    → TWSE OpenAPI T86         今日三大法人（免費）
  行情    → TWSE OpenAPI DAY_ALL     收盤價（免費）
  月營收  → FinMind API              需 FINMIND_TOKEN（支援多組）
"""

import json
import os
import re
import time
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, List, Tuple, Any

import requests

try:
    import pandas as pd
    PANDAS_OK = True
except ImportError:
    PANDAS_OK = False

try:
    import yfinance as yf
    YF_OK = True
except ImportError:
    YF_OK = False

# ─────────────────────────────────────────────
#  設定
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S',
)
log = logging.getLogger(__name__)

# 多組 FinMind Token（最多支援 3 組）
_RAW_TOKENS = [
    os.environ.get('FINMIND_TOKEN',   '').strip(),
    os.environ.get('FINMIND_TOKEN_2', '').strip(),
    os.environ.get('FINMIND_TOKEN_3', '').strip(),
]
FINMIND_TOKENS = [t for t in _RAW_TOKENS if t]   # 只保留有值的

STOCK_LIMIT    = int(os.environ.get('STOCK_LIMIT', '0'))

TWSE_DAY_ALL   = 'https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL'
TWSE_LISTED    = 'https://openapi.twse.com.tw/v1/opendata/t187ap03_L'
TWSE_T86       = 'https://openapi.twse.com.tw/v1/fund/T86'
TWSE_STOCK_DAY = 'https://www.twse.com.tw/exchangeReport/STOCK_DAY'
FINMIND_BASE   = 'https://api.finmindtrade.com/api/v4/data'

OUTPUT_PATH   = 'data/screener.json'
TW_TZ         = timezone(timedelta(hours=8))

YFINANCE_CHUNK  = 50
YFINANCE_PERIOD = '7mo'
FINMIND_SLEEP   = 1.2      # seconds between FinMind calls
TWSE_SLEEP      = 0.4
QUOTA_PER_KEY   = 550      # daily limit per FinMind free key

# ─────────────────────────────────────────────
#  多 Key 配額守衛
# ─────────────────────────────────────────────
class MultiKeyGuard:
    """
    多組 FinMind Token 輪換管理器。
    每組 key 獨立追蹤配額，用盡或 402 後自動切換到下一組。
    """
    def __init__(self, tokens: List[str], limit_per_key: int = QUOTA_PER_KEY):
        self.slots = [{'token': t, 'count': 0, 'exceeded': False}
                      for t in tokens]
        self.limit    = limit_per_key
        self._current = 0
        self._advance_to_valid()

    def _advance_to_valid(self):
        """找到下一個未耗盡的 key"""
        for i in range(len(self.slots)):
            idx = (self._current + i) % len(self.slots)
            if not self.slots[idx]['exceeded']:
                self._current = idx
                return
        self._current = -1  # 全部耗盡

    @property
    def has_quota(self) -> bool:
        return self._current >= 0

    @property
    def current_token(self) -> Optional[str]:
        if self._current < 0:
            return None
        return self.slots[self._current]['token']

    @property
    def key_index(self) -> int:
        return self._current + 1   # 1-based for display

    def inc(self):
        if self._current < 0:
            return
        slot = self.slots[self._current]
        slot['count'] += 1
        if slot['count'] >= self.limit:
            log.info(f'  Key {self.key_index} 配額達 {self.limit} 次，切換下一組')
            slot['exceeded'] = True
            self._advance_to_valid()
            if self._current >= 0:
                log.info(f'  切換到 Key {self.key_index}')
            else:
                log.warning('  所有 FinMind Key 配額耗盡')

    def hit_402(self):
        if self._current < 0:
            return
        log.warning(f'  Key {self.key_index} 402 配額耗盡（已呼叫 {self.slots[self._current]["count"]} 次）')
        self.slots[self._current]['exceeded'] = True
        self._advance_to_valid()
        if self._current >= 0:
            log.info(f'  切換到 Key {self.key_index}')
        else:
            log.warning('  所有 FinMind Key 配額耗盡')

    @property
    def total_calls(self) -> int:
        return sum(s['count'] for s in self.slots)

    def summary(self) -> str:
        parts = [f'Key{i+1}:{s["count"]}次' for i, s in enumerate(self.slots)]
        return ', '.join(parts)


GUARD = MultiKeyGuard(FINMIND_TOKENS)

# ─────────────────────────────────────────────
#  HTTP session
# ─────────────────────────────────────────────
SESSION = requests.Session()
SESSION.headers.update({
    'Accept':          'application/json, text/html, */*',
    'Accept-Language': 'zh-TW,zh;q=0.9',
    'User-Agent':      ('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                        'AppleWebKit/537.36 (KHTML, like Gecko) '
                        'Chrome/120.0.0.0 Safari/537.36'),
})


def safe_get_json(url: str, params: dict = None,
                  retries: int = 3, timeout: int = 30) -> Any:
    """GET + safe JSON parse. Returns None on any error."""
    for attempt in range(retries):
        try:
            r = SESSION.get(url, params=params, timeout=timeout)
            r.raise_for_status()
            content = r.content.strip()
            if not content or content[:1] in (b'<', b' ', b'\n'):
                return None
            return r.json()
        except requests.exceptions.HTTPError as e:
            code = e.response.status_code if e.response is not None else 0
            if 400 <= code < 500:
                return None
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
            else:
                return None
        except (ValueError, json.JSONDecodeError):
            return None
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
            else:
                log.debug(f'  safe_get_json failed: {e}')
                return None
    return None


def finmind_call(dataset: str, data_id: str, start_date: str) -> List[dict]:
    """
    FinMind API with multi-key rotation.
    自動切換 key，全部耗盡後回傳空 list。
    """
    if not GUARD.has_quota:
        return []

    token = GUARD.current_token
    params = {'dataset': dataset, 'data_id': data_id,
              'start_date': start_date, 'token': token}
    try:
        r = SESSION.get(FINMIND_BASE, params=params, timeout=30)

        if r.status_code == 402:
            GUARD.hit_402()
            # Retry with new key if available
            if GUARD.has_quota:
                return finmind_call(dataset, data_id, start_date)
            return []

        if r.status_code == 422:
            log.debug(f'  422 免費版不支援: {dataset}')
            return []

        r.raise_for_status()
        content = r.content.strip()
        if not content:
            return []

        j = r.json()
        if j.get('status') != 200:
            msg = j.get('msg', '')
            if any(k in msg for k in ('quota', '次數', '超過', 'limit')):
                GUARD.hit_402()
                if GUARD.has_quota:
                    return finmind_call(dataset, data_id, start_date)
            else:
                log.debug(f'  FinMind {dataset} {data_id}: {msg}')
            return []

        GUARD.inc()
        time.sleep(FINMIND_SLEEP)
        return j.get('data', [])

    except requests.exceptions.HTTPError as e:
        code = e.response.status_code if e.response is not None else 0
        if code == 402:
            GUARD.hit_402()
            if GUARD.has_quota:
                return finmind_call(dataset, data_id, start_date)
        return []
    except Exception as e:
        log.debug(f'  FinMind {dataset} {data_id}: {e}')
        return []


def is_equity(code: str) -> bool:
    return bool(code and re.match(r'^[1-9]\d{3}$', code))

def date_now() -> datetime:
    return datetime.now(TW_TZ)

def date_ago_str(months: int) -> str:
    return (date_now() - timedelta(days=months * 30)).strftime('%Y-%m-%d')

# ─────────────────────────────────────────────
#  TWSE helpers
# ─────────────────────────────────────────────
def load_twse_day_all() -> Tuple[Dict[str, dict], Dict[str, dict]]:
    stocks_base: Dict[str, dict] = {}
    names_only:  Dict[str, dict] = {}

    raw = safe_get_json(TWSE_DAY_ALL)
    for s in (raw or []):
        code = (s.get('Code') or '').strip()
        if not is_equity(code):
            continue
        price_s = (s.get('ClosingPrice') or '').replace(',', '').strip()
        try:
            price = float(price_s)
        except ValueError:
            continue
        if price <= 0:
            continue
        vol_s = (s.get('TradeVolume') or '0').replace(',', '')
        vol   = float(vol_s) if vol_s.replace('.','').isdigit() else 0
        chg_r = str(s.get('Change') or '0').strip()
        chg_a = float(re.sub(r'[▲▼+\-\s,]', '', chg_r) or '0')
        chg   = -chg_a if (chg_r.startswith('▼') or chg_r.startswith('-')) else chg_a
        base  = (price - chg) or price
        pct   = round(chg / base * 100, 2) if base else 0.0
        stocks_base[code] = {
            'code': code, 'name': (s.get('Name') or code).strip(),
            'price': price, 'change': round(chg, 2), 'pct': pct,
            'high': float((s.get('HighestPrice') or str(price)).replace(',', '')),
            'low':  float((s.get('LowestPrice')  or str(price)).replace(',', '')),
            'vol':  vol,
        }
    log.info(f'  DAY_ALL: {len(stocks_base)} 筆')

    raw2 = safe_get_json(TWSE_LISTED)
    for s in (raw2 or []):
        code = (s.get('公司代號') or '').strip()
        if not is_equity(code):
            continue
        names_only[code] = {'code': code, 'name': (s.get('公司名稱') or code).strip()}
    log.info(f'  t187ap03_L: {len(names_only)} 筆')

    return stocks_base, names_only


def load_t86() -> Dict[str, dict]:
    inst: Dict[str, dict] = {}
    raw = safe_get_json(TWSE_T86)
    if not raw:
        log.warning('  T86: 無資料（可能非交易日）')
        return inst

    for s in raw:
        code = (s.get('Code') or s.get('證券代號') or '').strip()
        if not code:
            continue
        def field(*keys):
            for k in keys:
                v = s.get(k)
                if v is not None and str(v).strip() not in ('', '--', '-'):
                    try: return float(str(v).replace(',', ''))
                    except ValueError: pass
            return 0.0
        fgn   = field('Foreign_Investor_Net_Buy_or_Sell',
                      '外陸資買賣超股數(不含外資自營商)', '外資買賣超') * 1000
        trust = field('Investment_Trust_Net_Buy_or_Sell',
                      '投信買賣超股數', '投信買賣超') * 1000
        inst[code] = {
            'foreignNet': round(fgn), 'trustNet': round(trust),
            'foreignBuy': fgn > 0,    'trustBuy': trust > 0,
        }
    log.info(f'  T86: {len(inst)} 筆')
    return inst

# ─────────────────────────────────────────────
#  yfinance — 相容新版 MultiIndex
# ─────────────────────────────────────────────
def _scalar(val) -> Optional[float]:
    """Safely extract scalar float from value that might be pandas Series."""
    try:
        if PANDAS_OK and isinstance(val, pd.Series):
            val = val.iloc[0]
        v = float(val)
        return None if v != v else v   # filter NaN
    except Exception:
        return None


def _row_to_ohlc(row, date_str: str) -> Optional[dict]:
    close = _scalar(row.get('Close') or row.get('close'))
    if close is None or close <= 0:
        return None
    high = _scalar(row.get('High') or row.get('high')) or close
    low  = _scalar(row.get('Low')  or row.get('low'))  or close
    return {'date': date_str, 'close': round(close,2),
            'max': round(high,2), 'min': round(low,2)}


def _extract_df(raw, ticker: str):
    if not PANDAS_OK or raw is None or raw.empty:
        return None
    try:
        if isinstance(raw.columns, pd.MultiIndex):
            lvl1 = raw.columns.get_level_values(1)
            if ticker not in lvl1:
                return None
            return raw.xs(ticker, level=1, axis=1)
        return raw
    except Exception:
        return None


def download_price_history(codes: List[str]) -> Dict[str, List[dict]]:
    if not YF_OK:
        return {}
    result: Dict[str, List[dict]] = {}
    tickers = [f'{c}.TW' for c in codes]
    n_batches = -(-len(tickers) // YFINANCE_CHUNK)
    log.info(f'  下載 {len(tickers)} 支，共 {n_batches} 批')

    for i in range(0, len(tickers), YFINANCE_CHUNK):
        chunk_t = tickers[i:i+YFINANCE_CHUNK]
        chunk_c = codes[i:i+YFINANCE_CHUNK]
        bn = i // YFINANCE_CHUNK + 1
        try:
            raw = yf.download(chunk_t, period=YFINANCE_PERIOD,
                              auto_adjust=True, progress=False)
        except Exception as e:
            log.warning(f'  批次 {bn} 失敗: {e}')
            continue

        if raw is None or (PANDAS_OK and isinstance(raw, pd.DataFrame) and raw.empty):
            continue

        ok = 0
        for ticker, code in zip(chunk_t, chunk_c):
            try:
                df = _extract_df(raw, ticker) if len(chunk_t) > 1 else raw
                if df is None or (PANDAS_OK and isinstance(df, pd.DataFrame) and df.empty):
                    continue
                rows = [r for r in (
                    _row_to_ohlc(row, di.strftime('%Y-%m-%d'))
                    for di, row in df.iterrows()) if r]
                rows.sort(key=lambda x: x['date'], reverse=True)
                if rows:
                    result[code] = rows
                    ok += 1
            except Exception as e:
                log.debug(f'  {code}: {e}')

        log.info(f'  批次 {bn}/{n_batches}: {ok}/{len(chunk_c)} 支成功')
        time.sleep(0.5)

    log.info(f'  yfinance 總計：{len(result)}/{len(codes)} 支')
    return result


def download_single(ticker_tw: str) -> List[dict]:
    if not YF_OK:
        return []
    try:
        raw = yf.download(ticker_tw, period=YFINANCE_PERIOD,
                          auto_adjust=True, progress=False)
        if raw is None or (PANDAS_OK and isinstance(raw, pd.DataFrame) and raw.empty):
            return []
        rows = [r for r in (
            _row_to_ohlc(row, di.strftime('%Y-%m-%d'))
            for di, row in raw.iterrows()) if r]
        rows.sort(key=lambda x: x['date'], reverse=True)
        return rows
    except Exception as e:
        log.warning(f'  download_single {ticker_tw}: {e}')
        return []


def twse_stock_history(code: str, months: int = 7) -> List[dict]:
    rows = []
    for m in range(months):
        target   = date_now() - timedelta(days=m * 30)
        yyyymmdd = target.strftime('%Y%m01')
        j = safe_get_json(TWSE_STOCK_DAY,
                          {'response':'json','date':yyyymmdd,'stockNo':code})
        for row in (j or {}).get('data', []):
            try:
                parts = row[0].strip().split('/')
                year  = int(parts[0]) + 1911
                iso   = f'{year}-{parts[1].zfill(2)}-{parts[2].zfill(2)}'
                close = float(row[6].replace(',',''))
                high  = float(row[4].replace(',',''))
                low   = float(row[5].replace(',',''))
                rows.append({'date':iso,'close':close,'max':high,'min':low})
            except Exception:
                pass
        time.sleep(TWSE_SLEEP)
    rows.sort(key=lambda x: x['date'], reverse=True)
    return rows

# ─────────────────────────────────────────────
#  FinMind 月營收（多 key 輪換）
# ─────────────────────────────────────────────
def load_revenue_finmind(codes: List[str]) -> Dict[str, List[dict]]:
    if not FINMIND_TOKENS:
        log.info('  無 FINMIND_TOKEN，月營收顯示 N/A')
        return {}

    result: Dict[str, List[dict]] = {}
    start  = date_ago_str(24)
    log.info(f'  FinMind 月營收: {len(codes)} 支，使用 {len(FINMIND_TOKENS)} 組 Key')

    for i, code in enumerate(codes):
        if not GUARD.has_quota:
            log.info(f'  所有 Key 配額耗盡，已取 {i} 支，其餘月營收顯示 N/A')
            break

        data = finmind_call('TaiwanStockMonthRevenue', code, start)
        if data:
            s = sorted(data, key=lambda x: x['date'], reverse=True)
            result[code] = [{'date': d['date'], 'revenue': float(d['revenue'])}
                            for d in s]

        if i > 0 and i % 100 == 0:
            log.info(f'  月營收進度: {i}/{len(codes)}，{GUARD.summary()}')

    log.info(f'  月營收完成: {len(result)}/{len(codes)} 支 | {GUARD.summary()}')
    return result

# ─────────────────────────────────────────────
#  指標計算
# ─────────────────────────────────────────────
def avg(lst: list) -> float:
    return sum(lst)/len(lst) if lst else 0.0


def calc_rs(sp: List[dict], tp: List[dict]) -> Optional[float]:
    n = min(130, len(sp)-1, len(tp)-1)
    if n < 20: return None
    try:
        sN,sP = float(sp[0]['close']),float(sp[n]['close'])
        tN,tP = float(tp[0]['close']),float(tp[n]['close'])
        if not sP or not tP: return None
        ratio = (1+(sN-sP)/sP)/(1+max((tN-tP)/tP,-0.95))
        return max(0.0,min(99.0,round(((ratio-0.85)/0.35)*100,1)))
    except Exception: return None


def calc_technical(prices: List[dict], proxy: List[dict]) -> dict:
    if not prices: return {}
    closes = [float(p['close']) for p in prices]
    cur = closes[0]
    def ma(n): return avg(closes[:n]) if len(closes)>=n else None
    ma5,ma10,ma20 = ma(5),ma(10),ma(20)
    ma60,ma120    = ma(60),ma(120)
    short_align = (bool(ma5>ma10>ma20)
                   if all(x is not None for x in [ma5,ma10,ma20]) else None)
    long_align  = (bool(ma20>ma60>ma120)
                   if all(x is not None for x in [ma20,ma60,ma120]) else None)
    # 站上扣抵值（MA5版）：現價 > 5交易日前股價
    # 意義：今日收盤若高於5日前，MA5 將上升（站上MA5扣抵值）
    above_sub   = (bool(cur > float(prices[5]['close']))
                   if len(prices) >= 6 else None)
    dist_high = None
    lk = min(22,len(prices))
    if lk:
        mh = max(float(p.get('max',p['close'])) for p in prices[:lk])
        dist_high = round(((cur/mh)-1)*100,2) if mh else None
    return {'rsScore':calc_rs(prices,proxy),'shortMAAlign':short_align,
            'longMAAlign':long_align,'aboveSubPoint':above_sub,
            'distanceFromHigh':dist_high}


def calc_revenue(rev_list: List[dict]) -> dict:
    if len(rev_list)<3: return {}
    s  = rev_list
    l  = s[0]; lv = float(l['revenue'])
    yoy,mom = [],[]
    for i in range(min(2,len(s))):
        cur = s[i]
        cd  = datetime.strptime(cur['date'][:7],'%Y-%m')
        py  = next((d for d in s if
                    datetime.strptime(d['date'][:7],'%Y-%m').month==cd.month and
                    datetime.strptime(d['date'][:7],'%Y-%m').year==cd.year-1),None)
        if py:
            dn = float(py['revenue'])
            if dn>0: yoy.append((float(cur['revenue'])-dn)/dn)
        if i<len(s)-1:
            pv=float(s[i+1]['revenue'])
            if pv>0: mom.append((float(cur['revenue'])-pv)/pv)
    all_rev=[float(d['revenue']) for d in s]
    ld=datetime.strptime(l['date'][:7],'%Y-%m')
    sly=next((d for d in s if
              datetime.strptime(d['date'][:7],'%Y-%m').month==ld.month and
              datetime.strptime(d['date'][:7],'%Y-%m').year==ld.year-1),None)
    return {
        'revenue':round(lv/1e8,2), 'revenueDate':l['date'],
        'yoyLatest':round(yoy[0]*100,2) if yoy else None,
        'momLatest':round(mom[0]*100,2) if mom else None,
        'yoy2mo':len(yoy)==2 and all(v>=0.20 for v in yoy),
        'mom2mo':len(mom)==2 and all(v>=0.20 for v in mom),
        'revenueHighRecord':lv>=max(all_rev) or (float(sly['revenue'])<lv if sly else False),
    }

# ─────────────────────────────────────────────
#  主流程
# ─────────────────────────────────────────────
def main():
    log.info('=== 台股雷達資料建置 V1.7 開始 ===')
    log.info(f'yfinance: {"✓" if YF_OK else "✗"}  pandas: {"✓" if PANDAS_OK else "✗"}')
    log.info(f'FinMind Keys: {len(FINMIND_TOKENS)} 組 '
             f'({", ".join(f"Key{i+1}" for i in range(len(FINMIND_TOKENS)))})')
    log.info(f'配額預估: {len(FINMIND_TOKENS)} × {QUOTA_PER_KEY} = '
             f'{len(FINMIND_TOKENS)*QUOTA_PER_KEY} 次')

    # data_date = 今日日期；generated 時間在最後寫檔前才取，才是真正完成時間
    data_date = date_now().strftime('%Y-%m-%d')

    log.info('Step 1: TWSE 個股清單...')
    stocks_base, names_only = load_twse_day_all()

    all_codes_set = set(stocks_base.keys()) | set(names_only.keys())
    if STOCK_LIMIT:
        all_codes_set = set(sorted(all_codes_set)[:STOCK_LIMIT])
        log.info(f'  STOCK_LIMIT={STOCK_LIMIT}')
    all_codes = sorted(all_codes_set)
    log.info(f'  共 {len(all_codes)} 支股票')

    log.info('Step 2: TWSE T86 法人...')
    inst_today = load_t86()

    log.info('Step 3: yfinance K 線...')
    price_history: Dict[str, List[dict]] = {}
    if YF_OK:
        price_history = download_price_history(all_codes)

    proxy = price_history.get('0050', [])
    if not proxy:
        log.info('  單獨下載 0050...')
        proxy = download_single('0050.TW')
        if proxy:
            price_history['0050'] = proxy
            log.info(f'  0050: {len(proxy)} 筆')
        else:
            log.info('  0050 yfinance 失敗，改用 TWSE...')
            proxy = twse_stock_history('0050', 7)
            log.info(f'  0050 (TWSE): {len(proxy)} 筆')

    log.info('Step 4: 月營收（FinMind 多 Key）...')
    revenue_all = load_revenue_finmind(all_codes)

    log.info('Step 5: 計算個股指標...')
    results = []

    # 用 TWSE DAY_ALL 的 0050 資料補全 proxy 最新一筆
    # 讓 RS 用今日收盤計算，而非 yfinance 昨日收盤
    proxy_0050_today = stocks_base.get('0050')
    if proxy_0050_today and proxy and proxy[0]['date'] < data_date:
        today_proxy = {
            'date':  data_date,
            'close': proxy_0050_today['price'],
            'max':   proxy_0050_today['high'],
            'min':   proxy_0050_today['low'],
        }
        proxy = [today_proxy] + proxy
        log.info(f'  0050 今日收盤已補入 proxy（{data_date} {proxy_0050_today["price"]}）')

    for i, code in enumerate(all_codes):
        if i>0 and i%200==0:
            log.info(f'  進度: {i}/{len(all_codes)}...')

        base = stocks_base.get(code) or names_only.get(code) or {'code':code,'name':code}
        inst = inst_today.get(code, {})

        r: dict = {
            'code':base['code'], 'name':base['name'],
            'price':base.get('price',0), 'change':base.get('change',0),
            'pct':base.get('pct',0), 'high':base.get('high',0),
            'low':base.get('low',0), 'vol':base.get('vol',0),
            'foreignNet':inst.get('foreignNet',None), 'trustNet':inst.get('trustNet',None),
            'foreignBuy':inst.get('foreignBuy',None), 'trustBuy':inst.get('trustBuy',None),
            'rsScore':None, 'distanceFromHigh':None,
            'shortMAAlign':None, 'longMAAlign':None, 'aboveSubPoint':None,
            'revenue':None, 'revenueDate':None,
            'yoyLatest':None, 'momLatest':None,
            'yoy2mo':None, 'mom2mo':None, 'revenueHighRecord':None,
            'grossMargin':None, 'opMargin':None,
            'noProfitLoss':None, 'marginGrowth':None,
            'institutionalRecord':None,
            'bigHolderPct':None, 'bigHolderIncrease':None, 'chipConcentration':None,
        }

        px = price_history.get(code)
        if not px and not YF_OK:
            px = twse_stock_history(code, 7)

        if px:
            # ── 核心修正：用 TWSE 今日收盤取代 yfinance 歷史最新一筆 ──
            # yfinance 歷史資料通常比 TWSE 晚一日更新，
            # 若 TWSE 有今日收盤且 yfinance 最新是昨日，補入今日
            twse_price = base.get('price', 0)
            twse_high  = base.get('high',  0)
            twse_low   = base.get('low',   0)
            if twse_price > 0 and px[0]['date'] < data_date:
                today_row = {
                    'date':  data_date,
                    'close': twse_price,
                    'max':   twse_high or twse_price,
                    'min':   twse_low  or twse_price,
                }
                px = [today_row] + px   # 補入今日，讓技術指標以今日收盤計算

            r.update(calc_technical(px, proxy))
            # 確保顯示的股價來自 TWSE（已在 r['price'] 設定）
            if not r['price'] or r['price'] == 0:
                try: r['price'] = float(px[0]['close'])
                except Exception: pass

        rev_list = revenue_all.get(code, [])
        if rev_list:
            r.update(calc_revenue(rev_list))

        results.append(r)

    has_rs      = sum(1 for r in results if r['rsScore']    is not None)
    has_rev     = sum(1 for r in results if r['revenue']    is not None)
    has_foreign = sum(1 for r in results if r['foreignBuy'] is not None)
    log.info(f'  RS:{has_rs}/{len(results)} | 月營收:{has_rev}/{len(results)} | 法人:{has_foreign}/{len(results)}')

    log.info(f'Step 6: 輸出 {OUTPUT_PATH}...')
    os.makedirs('data', exist_ok=True)
    # tw_now 在此取得，反映真正寫入時間（而非腳本啟動時間）
    tw_now = date_now()
    output = {
        'version':    'V1.7',
        'generated':  tw_now.isoformat(),
        'dataDate':   data_date,
        'source':     ('yfinance+finmind+twse'
                       if FINMIND_TOKENS else 'yfinance+twse'),
        'stockCount': len(results),
        'coverage':   {'technical':has_rs,'revenue':has_rev,'institutional':has_foreign},
        'stocks':     results,
    }
    with open(OUTPUT_PATH,'w',encoding='utf-8') as f:
        json.dump(output,f,ensure_ascii=False,separators=(',',':'))
    size_kb = os.path.getsize(OUTPUT_PATH)/1024
    log.info(f'  ✅ {len(results)} 支 → {OUTPUT_PATH} ({size_kb:.0f} KB)')
    log.info('=== 完成 ===')


if __name__ == '__main__':
    main()
