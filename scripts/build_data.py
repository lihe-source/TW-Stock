#!/usr/bin/env python3
"""
台股雷達 — 資料建置腳本  V1.2
build_data.py

修正 V1.1 問題：
  [1] T86 JSON parse error
      → TWSE T86 API 偶爾回傳空字串或 HTML 錯誤頁
      → 修正：安全解析 JSON，失敗時靜默跳過

  [2] download_single 0050 'truth value of a Series is ambiguous'
      → yfinance 新版單支下載的 iterrows() 每格也是 Series 物件
      → 修正：使用 .iloc[0] 或 float() 統一轉換 scalar

  [3] MOPS HTTP 0（GitHub Actions IP 被 MOPS 封鎖）
      → MOPS 對 GitHub Actions IP 段封鎖，無法解決
      → 移除 MOPS，改用 FinMind 取月營收（需 FINMIND_TOKEN）
      → 無 token 時技術面 + 法人仍正常，基本面顯示 N/A

  [4] PWA 只顯示 1 行（CSS 問題，見 style.css）
      → .table-scroll 需加 min-height: 0（在 style.css 修正）

資料來源：
  技術面   → yfinance (Yahoo Finance)  批次 K 線（免費）
  法人     → TWSE OpenAPI T86         今日三大法人（免費）
  行情     → TWSE OpenAPI DAY_ALL     收盤價（免費）
  月營收   → FinMind API              需 FINMIND_TOKEN（選用）

環境變數（GitHub Actions Secrets）：
  FINMIND_TOKEN   → FinMind API Token（選用，用於月營收）
  STOCK_LIMIT     → 限制個股數（測試用，預設 0=全部）
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

FINMIND_TOKEN  = os.environ.get('FINMIND_TOKEN', '').strip()
STOCK_LIMIT    = int(os.environ.get('STOCK_LIMIT', '0'))

TWSE_DAY_ALL   = 'https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL'
TWSE_LISTED    = 'https://openapi.twse.com.tw/v1/opendata/t187ap03_L'
TWSE_T86       = 'https://openapi.twse.com.tw/v1/fund/T86'
TWSE_STOCK_DAY = 'https://www.twse.com.tw/exchangeReport/STOCK_DAY'
FINMIND_BASE   = 'https://api.finmindtrade.com/api/v4/data'

OUTPUT_PATH    = 'data/screener.json'
TW_TZ          = timezone(timedelta(hours=8))

YFINANCE_CHUNK   = 50
YFINANCE_PERIOD  = '7mo'
FINMIND_SLEEP    = 1.2   # seconds between FinMind calls
TWSE_SLEEP       = 0.4
FINMIND_QUOTA    = 550   # daily limit (free tier ~300-600, use 550 as safe limit)

# ─────────────────────────────────────────────
#  配額守衛（FinMind 免費版每日上限）
# ─────────────────────────────────────────────
class QuotaGuard:
    def __init__(self, limit: int):
        self.limit    = limit
        self.count    = 0
        self.exceeded = False

    def ok(self) -> bool:
        if self.exceeded: return False
        if self.count >= self.limit:
            log.warning(f'FinMind 配額達上限 {self.limit} 次，停止呼叫')
            self.exceeded = True
            return False
        return True

    def inc(self): self.count += 1

    def hit_402(self):
        log.warning(f'FinMind 402 配額耗盡（已呼叫 {self.count} 次）')
        self.exceeded = True

QUOTA = QuotaGuard(FINMIND_QUOTA)

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
    """
    GET + JSON parse with safe fallback.
    Returns parsed JSON on success, None on failure.
    Never raises for JSON parse errors.
    """
    for attempt in range(retries):
        try:
            r = SESSION.get(url, params=params, timeout=timeout)
            r.raise_for_status()

            # Safe JSON parse — check content before parsing
            content = r.content.strip()
            if not content or content[:1] in (b'<', b' ', b'\n'):
                log.debug(f'  empty/HTML response from {url[:60]}')
                return None

            return r.json()

        except requests.exceptions.HTTPError as e:
            code = e.response.status_code if e.response is not None else 0
            if 400 <= code < 500:
                log.debug(f'  HTTP {code}: {url[:60]}')
                return None
            if attempt < retries - 1:
                wait = 2 ** attempt
                log.warning(f'  retry {attempt+1}/{retries}: {e} (wait {wait}s)')
                time.sleep(wait)
            else:
                log.warning(f'  failed after {retries} retries: {e}')
                return None

        except (ValueError, json.JSONDecodeError) as e:
            log.debug(f'  JSON parse error: {e}')
            return None

        except Exception as e:
            if attempt < retries - 1:
                wait = 2 ** attempt
                log.warning(f'  retry {attempt+1}/{retries}: {e} (wait {wait}s)')
                time.sleep(wait)
            else:
                log.warning(f'  failed: {e}')
                return None

    return None


def finmind_call(dataset: str, data_id: str, start_date: str) -> List[dict]:
    """
    FinMind API with quota guard.
    402 → quota exceeded, stop all calls.
    422 → dataset not available in free tier, skip silently.
    """
    if not FINMIND_TOKEN or not QUOTA.ok():
        return []

    params = {'dataset': dataset, 'data_id': data_id,
              'start_date': start_date, 'token': FINMIND_TOKEN}
    try:
        r = SESSION.get(FINMIND_BASE, params=params, timeout=30)

        if r.status_code == 402:
            QUOTA.hit_402()
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
                QUOTA.hit_402()
            else:
                log.debug(f'  FinMind {dataset} {data_id}: {msg}')
            return []

        QUOTA.inc()
        time.sleep(FINMIND_SLEEP)
        return j.get('data', [])

    except requests.exceptions.HTTPError as e:
        code = e.response.status_code if e.response is not None else 0
        if code == 402: QUOTA.hit_402()
        elif code != 422: log.debug(f'  FinMind HTTP {code}: {dataset} {data_id}')
        return []
    except Exception as e:
        log.debug(f'  FinMind {dataset} {data_id}: {e}')
        return []


def is_equity(code: str) -> bool:
    return bool(code and re.match(r'^[1-9]\d{3}$', code))


def date_now() -> datetime:
    return datetime.now(TW_TZ)


def date_ago_str(months: int) -> str:
    d = date_now() - timedelta(days=months * 30)
    return d.strftime('%Y-%m-%d')

# ─────────────────────────────────────────────
#  TWSE — 個股清單 + 今日行情
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
        names_only[code] = {
            'code': code,
            'name': (s.get('公司名稱') or code).strip(),
        }
    log.info(f'  t187ap03_L: {len(names_only)} 筆')

    return stocks_base, names_only


def load_t86() -> Dict[str, dict]:
    """
    TWSE T86 今日三大法人。
    修正：安全 JSON 解析，空回應時靜默跳過。
    """
    inst: Dict[str, dict] = {}

    raw = safe_get_json(TWSE_T86)
    if not raw:
        log.warning('  T86: 無法取得法人資料（可能非交易日或伺服器問題）')
        return inst

    for s in raw:
        code = (s.get('Code') or s.get('證券代號') or '').strip()
        if not code:
            continue

        def field(*keys):
            for k in keys:
                v = s.get(k)
                if v is not None and str(v).strip() not in ('', '--', '-'):
                    try:
                        return float(str(v).replace(',', ''))
                    except ValueError:
                        pass
            return 0.0

        fgn   = field('Foreign_Investor_Net_Buy_or_Sell',
                      '外陸資買賣超股數(不含外資自營商)', '外資買賣超') * 1000
        trust = field('Investment_Trust_Net_Buy_or_Sell',
                      '投信買賣超股數', '投信買賣超') * 1000
        inst[code] = {
            'foreignNet': round(fgn),
            'trustNet':   round(trust),
            'foreignBuy': fgn > 0,
            'trustBuy':   trust > 0,
        }

    log.info(f'  T86: {len(inst)} 筆')
    return inst

# ─────────────────────────────────────────────
#  yfinance — 修正版（相容 MultiIndex 欄位）
# ─────────────────────────────────────────────
def _row_to_ohlc(row, date_str: str) -> Optional[dict]:
    """
    從 yfinance DataFrame row 安全取 OHLC 值。
    相容新版（每格可能是 Series 或 scalar）。
    """
    def s(key):
        """Safely extract scalar float from row[key]"""
        try:
            v = row[key]
            if PANDAS_OK and isinstance(v, pd.Series):
                v = v.iloc[0]
            return float(v)
        except Exception:
            return None

    close = s('Close') or s('close')
    if close is None or close <= 0 or (close != close):  # NaN check
        return None
    high = s('High') or s('high') or close
    low  = s('Low')  or s('low')  or close
    return {
        'date':  date_str,
        'close': round(close, 2),
        'max':   round(high, 2),
        'min':   round(low,  2),
    }


def _extract_df(raw, ticker: str):
    """
    從 yfinance.download() 結果取單支股票 DataFrame。
    相容新版 MultiIndex (Price×Ticker) 與舊版 flat columns。
    """
    if not PANDAS_OK or raw is None or raw.empty:
        return None
    try:
        if isinstance(raw.columns, pd.MultiIndex):
            # 新版：Level 0 = Price field, Level 1 = Ticker
            lvl1 = raw.columns.get_level_values(1)
            if ticker not in lvl1:
                return None
            return raw.xs(ticker, level=1, axis=1)
        else:
            return raw  # 舊版 or 單支
    except Exception as e:
        log.debug(f'  _extract_df {ticker}: {e}')
        return None


def download_price_history(codes: List[str]) -> Dict[str, List[dict]]:
    if not YF_OK:
        log.warning('  yfinance 未安裝')
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
            raw = yf.download(
                chunk_t,
                period=YFINANCE_PERIOD,
                auto_adjust=True,
                progress=False,
            )
        except Exception as e:
            log.warning(f'  批次 {bn} 下載失敗: {e}')
            continue

        if raw is None or (PANDAS_OK and isinstance(raw, pd.DataFrame) and raw.empty):
            log.warning(f'  批次 {bn} 空資料')
            continue

        ok = 0
        for ticker, code in zip(chunk_t, chunk_c):
            try:
                df = _extract_df(raw, ticker) if len(chunk_t) > 1 else raw
                if df is None or (PANDAS_OK and isinstance(df, pd.DataFrame) and df.empty):
                    continue
                rows = []
                for date_idx, row in df.iterrows():
                    rec = _row_to_ohlc(row, date_idx.strftime('%Y-%m-%d'))
                    if rec:
                        rows.append(rec)
                rows.sort(key=lambda x: x['date'], reverse=True)
                if rows:
                    result[code] = rows
                    ok += 1
            except Exception as e:
                log.debug(f'  {code}: {e}')

        log.info(f'  批次 {bn}/{n_batches} 完成：{ok}/{len(chunk_c)} 支成功')
        time.sleep(0.5)

    log.info(f'  yfinance 總計：成功 {len(result)}/{len(codes)} 支')
    return result


def download_single(ticker_tw: str) -> List[dict]:
    """
    下載單支股票。修正：使用 _row_to_ohlc 統一處理新舊版 API。
    """
    if not YF_OK:
        return []
    try:
        raw = yf.download(ticker_tw, period=YFINANCE_PERIOD,
                          auto_adjust=True, progress=False)
        if raw is None or (PANDAS_OK and isinstance(raw, pd.DataFrame) and raw.empty):
            return []

        rows = []
        for date_idx, row in raw.iterrows():
            rec = _row_to_ohlc(row, date_idx.strftime('%Y-%m-%d'))
            if rec:
                rows.append(rec)
        rows.sort(key=lambda x: x['date'], reverse=True)
        return rows
    except Exception as e:
        log.warning(f'  download_single {ticker_tw}: {e}')
        return []


def twse_stock_history(code: str, months: int = 7) -> List[dict]:
    rows = []
    now  = date_now()
    for m in range(months):
        target   = now - timedelta(days=m * 30)
        yyyymmdd = target.strftime('%Y%m01')
        j = safe_get_json(TWSE_STOCK_DAY,
                          {'response':'json','date':yyyymmdd,'stockNo':code})
        for row in (j or {}).get('data', []):
            try:
                parts = row[0].strip().split('/')
                year  = int(parts[0]) + 1911
                iso   = f'{year}-{parts[1].zfill(2)}-{parts[2].zfill(2)}'
                close = float(row[6].replace(',', ''))
                high  = float(row[4].replace(',', ''))
                low   = float(row[5].replace(',', ''))
                rows.append({'date': iso, 'close': close, 'max': high, 'min': low})
            except Exception:
                pass
        time.sleep(TWSE_SLEEP)
    rows.sort(key=lambda x: x['date'], reverse=True)
    return rows

# ─────────────────────────────────────────────
#  FinMind — 月營收（有 token 才執行）
# ─────────────────────────────────────────────
def load_revenue_finmind(codes: List[str]) -> Dict[str, List[dict]]:
    """
    透過 FinMind 取月營收（需 FINMIND_TOKEN）。
    每支股票 1 次呼叫，有配額守衛。
    """
    if not FINMIND_TOKEN:
        log.info('  無 FINMIND_TOKEN，月營收指標將顯示 N/A')
        return {}

    result: Dict[str, List[dict]] = {}
    start  = date_ago_str(24)
    log.info(f'  FinMind 月營收: {len(codes)} 支（配額上限 {QUOTA.limit} 次）')

    for i, code in enumerate(codes):
        if not QUOTA.ok():
            log.info(f'  FinMind 配額耗盡，已取 {i} 支，其餘月營收顯示 N/A')
            break

        data = finmind_call('TaiwanStockMonthRevenue', code, start)
        if data:
            s = sorted(data, key=lambda x: x['date'], reverse=True)
            result[code] = [{'date': d['date'], 'revenue': float(d['revenue'])}
                            for d in s]

        if i > 0 and i % 100 == 0:
            log.info(f'  月營收進度: {i}/{len(codes)}，FinMind 已呼叫 {QUOTA.count} 次')

    log.info(f'  月營收完成: {len(result)}/{len(codes)} 支，FinMind 呼叫 {QUOTA.count} 次')
    return result

# ─────────────────────────────────────────────
#  指標計算
# ─────────────────────────────────────────────
def avg(lst: list) -> float:
    return sum(lst) / len(lst) if lst else 0.0


def calc_rs(sp: List[dict], tp: List[dict]) -> Optional[float]:
    n = min(130, len(sp)-1, len(tp)-1)
    if n < 20:
        return None
    try:
        sN, sP = float(sp[0]['close']), float(sp[n]['close'])
        tN, tP = float(tp[0]['close']), float(tp[n]['close'])
        if not sP or not tP:
            return None
        sR = (sN - sP) / sP
        tR = (tN - tP) / tP
        ratio = (1 + sR) / (1 + max(tR, -0.95))
        return max(0.0, min(99.0, round(((ratio - 0.85) / 0.35) * 100, 1)))
    except Exception:
        return None


def calc_technical(prices: List[dict], proxy: List[dict]) -> dict:
    if not prices:
        return {}
    closes = [float(p['close']) for p in prices]
    cur    = closes[0]

    def ma(n): return avg(closes[:n]) if len(closes) >= n else None
    ma5, ma10, ma20 = ma(5), ma(10), ma(20)
    ma60, ma120     = ma(60), ma(120)

    short_align = (bool(ma5 > ma10 > ma20)
                   if all(x is not None for x in [ma5, ma10, ma20]) else None)
    long_align  = (bool(ma20 > ma60 > ma120)
                   if all(x is not None for x in [ma20, ma60, ma120]) else None)
    above_sub   = (bool(cur > float(prices[19]['close']) and
                        cur > float(prices[59]['close']))
                   if len(prices) >= 61 else None)
    dist_high   = None
    lk = min(22, len(prices))
    if lk:
        mh = max(float(p.get('max', p['close'])) for p in prices[:lk])
        dist_high = round(((cur / mh) - 1) * 100, 2) if mh else None

    return {
        'rsScore':          calc_rs(prices, proxy),
        'shortMAAlign':     short_align,
        'longMAAlign':      long_align,
        'aboveSubPoint':    above_sub,
        'distanceFromHigh': dist_high,
    }


def calc_revenue(rev_list: List[dict]) -> dict:
    if len(rev_list) < 3:
        return {}
    s  = rev_list
    l  = s[0]
    lv = float(l['revenue'])

    yoy_list, mom_list = [], []
    for i in range(min(2, len(s))):
        cur = s[i]
        cd  = datetime.strptime(cur['date'][:7], '%Y-%m')
        py  = next((d for d in s if
                    datetime.strptime(d['date'][:7], '%Y-%m').month == cd.month and
                    datetime.strptime(d['date'][:7], '%Y-%m').year  == cd.year - 1), None)
        if py:
            denom = float(py['revenue'])
            if denom > 0:
                yoy_list.append((float(cur['revenue']) - denom) / denom)
        if i < len(s) - 1:
            pv = float(s[i+1]['revenue'])
            if pv > 0:
                mom_list.append((float(cur['revenue']) - pv) / pv)

    all_rev = [float(d['revenue']) for d in s]
    ld = datetime.strptime(l['date'][:7], '%Y-%m')
    sly = next((d for d in s if
                datetime.strptime(d['date'][:7], '%Y-%m').month == ld.month and
                datetime.strptime(d['date'][:7], '%Y-%m').year  == ld.year - 1), None)

    return {
        'revenue':           round(lv / 1e8, 2),
        'revenueDate':       l['date'],
        'yoyLatest':         round(yoy_list[0] * 100, 2) if yoy_list else None,
        'momLatest':         round(mom_list[0] * 100, 2) if mom_list else None,
        'yoy2mo':            len(yoy_list) == 2 and all(v >= 0.20 for v in yoy_list),
        'mom2mo':            len(mom_list) == 2 and all(v >= 0.20 for v in mom_list),
        'revenueHighRecord': lv >= max(all_rev) or (
                             float(sly['revenue']) < lv if sly else False),
    }

# ─────────────────────────────────────────────
#  主流程
# ─────────────────────────────────────────────
def main():
    log.info('=== 台股雷達資料建置 V1.2 開始 ===')
    log.info(f'yfinance:  {"✓" if YF_OK else "✗"}')
    log.info(f'pandas:    {"✓" if PANDAS_OK else "✗"}')
    log.info(f'FINMIND:   {"✓ (月營收已啟用)" if FINMIND_TOKEN else "✗ (月營收 N/A)"}')

    tw_now    = date_now()
    data_date = tw_now.strftime('%Y-%m-%d')

    # ── Step 1: TWSE 個股清單 + 行情 ──
    log.info('Step 1: TWSE 個股清單 + 行情...')
    stocks_base, names_only = load_twse_day_all()

    all_codes_set = set(stocks_base.keys()) | set(names_only.keys())
    if STOCK_LIMIT:
        all_codes_set = set(sorted(all_codes_set)[:STOCK_LIMIT])
        log.info(f'  STOCK_LIMIT={STOCK_LIMIT}')
    all_codes = sorted(all_codes_set)
    log.info(f'  共 {len(all_codes)} 支股票')

    # ── Step 2: T86 法人 ──
    log.info('Step 2: TWSE T86 法人...')
    inst_today = load_t86()

    # ── Step 3: yfinance K 線 ──
    log.info('Step 3: yfinance 批次 K 線...')
    price_history: Dict[str, List[dict]] = {}
    if YF_OK:
        price_history = download_price_history(all_codes)
    else:
        log.info('  yfinance 不可用，逐支從 TWSE 取歷史')

    # 大盤代理 0050
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

    # ── Step 4: 月營收（FinMind，選用）──
    log.info('Step 4: 月營收（FinMind）...')
    revenue_all = load_revenue_finmind(all_codes)

    # ── Step 5: 組合計算 ──
    log.info('Step 5: 計算個股指標...')
    results = []

    for i, code in enumerate(all_codes):
        if i > 0 and i % 200 == 0:
            log.info(f'  進度: {i}/{len(all_codes)}...')

        base = stocks_base.get(code) or names_only.get(code) or {'code':code,'name':code}
        inst = inst_today.get(code, {})

        r: dict = {
            'code':   base['code'],
            'name':   base['name'],
            'price':  base.get('price',  0),
            'change': base.get('change', 0),
            'pct':    base.get('pct',    0),
            'high':   base.get('high',   0),
            'low':    base.get('low',    0),
            'vol':    base.get('vol',    0),
            'foreignNet':  inst.get('foreignNet', None),
            'trustNet':    inst.get('trustNet',   None),
            'foreignBuy':  inst.get('foreignBuy', None),
            'trustBuy':    inst.get('trustBuy',   None),
            'rsScore': None, 'distanceFromHigh': None,
            'shortMAAlign': None, 'longMAAlign': None, 'aboveSubPoint': None,
            'revenue': None, 'revenueDate': None,
            'yoyLatest': None, 'momLatest': None,
            'yoy2mo': None, 'mom2mo': None, 'revenueHighRecord': None,
            'grossMargin': None, 'opMargin': None,
            'noProfitLoss': None, 'marginGrowth': None,
            'institutionalRecord': None,
            'bigHolderPct': None, 'bigHolderIncrease': None, 'chipConcentration': None,
        }

        # 技術指標
        px = price_history.get(code)
        if not px and not YF_OK:
            px = twse_stock_history(code, 7)
        if px:
            r.update(calc_technical(px, proxy))
            if not r['price'] or r['price'] == 0:
                try: r['price'] = float(px[0]['close'])
                except Exception: pass

        # 月營收
        rev_list = revenue_all.get(code, [])
        if rev_list:
            r.update(calc_revenue(rev_list))

        results.append(r)

    # ── 統計 ──
    has_rs      = sum(1 for r in results if r['rsScore']    is not None)
    has_rev     = sum(1 for r in results if r['revenue']    is not None)
    has_foreign = sum(1 for r in results if r['foreignBuy'] is not None)
    log.info(f'  RS 覆蓋: {has_rs}/{len(results)} | 月營收: {has_rev}/{len(results)} | 法人: {has_foreign}/{len(results)}')

    # ── Step 6: 輸出 ──
    log.info(f'Step 6: 輸出 {OUTPUT_PATH}...')
    os.makedirs('data', exist_ok=True)

    output = {
        'version':    'V1.2',
        'generated':  tw_now.isoformat(),
        'dataDate':   data_date,
        'source':     'yfinance+finmind+twse' if FINMIND_TOKEN else 'yfinance+twse',
        'stockCount': len(results),
        'coverage':   {'technical': has_rs, 'revenue': has_rev, 'institutional': has_foreign},
        'stocks':     results,
    }

    with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, separators=(',', ':'))

    size_kb = os.path.getsize(OUTPUT_PATH) / 1024
    log.info(f'  ✅ {len(results)} 支 → {OUTPUT_PATH} ({size_kb:.0f} KB)')
    log.info('=== 完成 ===')


if __name__ == '__main__':
    main()
