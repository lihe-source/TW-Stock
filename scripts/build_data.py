#!/usr/bin/env python3
"""
台股雷達 — 資料建置腳本  V1.1
build_data.py

修正 V1.0 問題：
  [1] yfinance 成功 0/N 支
      → 新版 yfinance (>=0.2.40) 改為 MultiIndex 欄位結構 (Price, Ticker)
      → 修正：使用 df.xs(ticker, level=1, axis=1) 相容新舊版

  [2] MOPS 月營收 404
      → URL 路徑錯誤：s02 → sii，且需加 _0 後綴
      → 錯誤：.../t21/s02/t21sc03_115_03.html
      → 正確：.../t21/sii/t21sc03_115_03_0.html
      → 當月資料尚未公告時（次月 10 日前），靜默跳過

資料來源（完全免費）：
  技術面   → yfinance (Yahoo Finance)  批次 K 線
  基本面   → MOPS 公開資訊觀測站       月營收
  籌碼面   → TWSE OpenAPI T86         今日三大法人
  今日行情 → TWSE OpenAPI DAY_ALL     收盤價
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
    from bs4 import BeautifulSoup
    BS4_OK = True
except ImportError:
    BS4_OK = False

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

TWSE_DAY_ALL   = 'https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL'
TWSE_LISTED    = 'https://openapi.twse.com.tw/v1/opendata/t187ap03_L'
TWSE_T86       = 'https://openapi.twse.com.tw/v1/fund/T86'
TWSE_STOCK_DAY = 'https://www.twse.com.tw/exchangeReport/STOCK_DAY'

# 正確的 MOPS 月營收 URL（上市公司 sii，加 _0 後綴）
MOPS_REVENUE_SII = 'https://mops.twse.com.tw/nas/t21/sii/t21sc03_{roc_year}_{month:02d}_0.html'

ENABLE_FINANCIALS = os.environ.get('ENABLE_FINANCIALS', '0') == '1'
STOCK_LIMIT       = int(os.environ.get('STOCK_LIMIT', '0'))
OUTPUT_PATH       = 'data/screener.json'
TW_TZ             = timezone(timedelta(hours=8))

YFINANCE_CHUNK    = 50      # 降低每批數量，避免 timeout（舊版 100 有時不穩）
YFINANCE_PERIOD   = '7mo'
MOPS_REV_MONTHS   = 24
MOPS_SLEEP        = 1.5     # 稍微加長，避免 MOPS 封鎖
TWSE_SLEEP        = 0.4

# ─────────────────────────────────────────────
#  HTTP session
# ─────────────────────────────────────────────
SESSION = requests.Session()
SESSION.headers.update({
    'Accept':          'text/html,application/json,*/*',
    'Accept-Language': 'zh-TW,zh;q=0.9',
    'User-Agent':      'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                       'AppleWebKit/537.36 (KHTML, like Gecko) '
                       'Chrome/120.0.0.0 Safari/537.36',
})

def get(url: str, params: dict = None, retries: int = 3, timeout: int = 30) -> requests.Response:
    for attempt in range(retries):
        try:
            r = SESSION.get(url, params=params, timeout=timeout)
            r.raise_for_status()
            return r
        except requests.exceptions.HTTPError as e:
            code = e.response.status_code if e.response is not None else 0
            if code == 404:
                raise   # 404 不重試
            if attempt < retries - 1:
                wait = 2 ** attempt
                log.warning(f'  retry {attempt+1}/{retries}: {e} (wait {wait}s)')
                time.sleep(wait)
            else:
                raise
        except Exception as e:
            if attempt < retries - 1:
                wait = 2 ** attempt
                log.warning(f'  retry {attempt+1}/{retries}: {e} (wait {wait}s)')
                time.sleep(wait)
            else:
                raise

def get_json(url: str, params: dict = None) -> Any:
    return get(url, params=params).json()

def is_equity(code: str) -> bool:
    return bool(code and re.match(r'^[1-9]\d{3}$', code))

def date_now() -> datetime:
    return datetime.now(TW_TZ)

# ─────────────────────────────────────────────
#  TWSE helpers
# ─────────────────────────────────────────────
def load_twse_day_all() -> Tuple[Dict[str, dict], Dict[str, dict]]:
    stocks_base: Dict[str, dict] = {}
    names_only:  Dict[str, dict] = {}

    try:
        raw = get_json(TWSE_DAY_ALL)
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
                'code': code,
                'name': (s.get('Name') or code).strip(),
                'price': price, 'change': round(chg, 2), 'pct': pct,
                'high': float((s.get('HighestPrice') or str(price)).replace(',', '')),
                'low':  float((s.get('LowestPrice')  or str(price)).replace(',', '')),
                'vol':  vol,
            }
        log.info(f'  DAY_ALL: {len(stocks_base)} 筆')
    except Exception as e:
        log.warning(f'  DAY_ALL 失敗: {e}')

    try:
        for s in (get_json(TWSE_LISTED) or []):
            code = (s.get('公司代號') or '').strip()
            if not is_equity(code):
                continue
            names_only[code] = {
                'code': code,
                'name': (s.get('公司名稱') or code).strip(),
            }
        log.info(f'  t187ap03_L: {len(names_only)} 筆')
    except Exception as e:
        log.warning(f'  t187ap03_L 失敗: {e}')

    return stocks_base, names_only


def load_t86() -> Dict[str, dict]:
    inst: Dict[str, dict] = {}
    try:
        for s in (get_json(TWSE_T86) or []):
            code = (s.get('Code') or s.get('證券代號') or '').strip()
            if not code:
                continue
            def field(*keys):
                for k in keys:
                    v = s.get(k)
                    if v is not None and str(v).strip() not in ('', '--'):
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
    except Exception as e:
        log.warning(f'  T86 失敗: {e}')
    return inst


# ─────────────────────────────────────────────
#  yfinance — 修正版（相容新舊版 MultiIndex）
# ─────────────────────────────────────────────
def _extract_df_for_ticker(raw, ticker: str):
    """
    從 yfinance.download() 結果中取出單支股票的 DataFrame。
    相容新版（MultiIndex: Price×Ticker）與舊版（直接欄位）。
    """
    if not PANDAS_OK:
        return None

    try:
        if isinstance(raw.columns, pd.MultiIndex):
            # 新版 yfinance (>=0.2.40):
            # columns = MultiIndex [('Close','2330.TW'),('High','2330.TW'), ...]
            # Level 0 = Price field, Level 1 = Ticker
            level1_vals = raw.columns.get_level_values(1)
            if ticker not in level1_vals:
                return None
            df = raw.xs(ticker, level=1, axis=1)
            return df
        else:
            # 舊版 or 單支下載：直接是 OHLCV columns
            return raw
    except Exception as e:
        log.debug(f'  _extract_df_for_ticker {ticker}: {e}')
        return None


def _safe_float(val) -> Optional[float]:
    """安全轉 float，處理 Series/scalar 兩種情況"""
    try:
        if PANDAS_OK and isinstance(val, pd.Series):
            v = val.iloc[0]
        else:
            v = val
        f = float(v)
        return f if not (f != f) else None  # check NaN
    except Exception:
        return None


def download_price_history(codes: List[str]) -> Dict[str, List[dict]]:
    if not YF_OK:
        log.warning('  yfinance 未安裝，跳過技術指標')
        return {}

    result: Dict[str, List[dict]] = {}
    tickers = [f'{c}.TW' for c in codes]
    total_batches = -(-len(tickers) // YFINANCE_CHUNK)
    log.info(f'  下載 {len(tickers)} 支，共 {total_batches} 批（每批 {YFINANCE_CHUNK} 支）')

    for i in range(0, len(tickers), YFINANCE_CHUNK):
        chunk_tickers = tickers[i:i+YFINANCE_CHUNK]
        chunk_codes   = codes[i:i+YFINANCE_CHUNK]
        bn = i // YFINANCE_CHUNK + 1
        log.info(f'  批次 {bn}/{total_batches}: {chunk_tickers[0]} ~ {chunk_tickers[-1]}')

        try:
            raw = yf.download(
                chunk_tickers,
                period=YFINANCE_PERIOD,
                auto_adjust=True,
                progress=False,
                # 不傳 group_by 讓 yfinance 用預設（新版自動 MultiIndex）
            )
        except Exception as e:
            log.warning(f'  批次 {bn} 下載失敗: {e}')
            continue

        if raw is None or raw.empty:
            log.warning(f'  批次 {bn} 回傳空資料')
            continue

        success = 0
        for ticker, code in zip(chunk_tickers, chunk_codes):
            try:
                if len(chunk_tickers) == 1:
                    # 單支下載：直接是 DataFrame
                    df = raw
                else:
                    df = _extract_df_for_ticker(raw, ticker)

                if df is None or df.empty:
                    continue

                rows = []
                for date_idx, row in df.iterrows():
                    close = _safe_float(row.get('Close') or row.get('close'))
                    high  = _safe_float(row.get('High')  or row.get('high'))
                    low   = _safe_float(row.get('Low')   or row.get('low'))
                    if close is None or close <= 0:
                        continue
                    rows.append({
                        'date':  date_idx.strftime('%Y-%m-%d'),
                        'close': round(close, 2),
                        'max':   round(high or close, 2),
                        'min':   round(low  or close, 2),
                    })

                rows.sort(key=lambda x: x['date'], reverse=True)
                if rows:
                    result[code] = rows
                    success += 1

            except Exception as e:
                log.debug(f'  {code}: {e}')

        log.info(f'  批次 {bn} 完成：{success}/{len(chunk_codes)} 支成功')
        time.sleep(0.8)

    log.info(f'  yfinance 總計：成功 {len(result)}/{len(codes)} 支')
    return result


def download_single(ticker_tw: str) -> List[dict]:
    """下載單支股票（如 0050.TW）"""
    if not YF_OK:
        return []
    try:
        raw = yf.download(ticker_tw, period=YFINANCE_PERIOD,
                          auto_adjust=True, progress=False)
        if raw is None or raw.empty:
            return []
        rows = []
        for date_idx, row in raw.iterrows():
            close = _safe_float(row.get('Close') or row.get('close'))
            high  = _safe_float(row.get('High')  or row.get('high'))
            low   = _safe_float(row.get('Low')   or row.get('low'))
            if close is None or close <= 0:
                continue
            rows.append({
                'date':  date_idx.strftime('%Y-%m-%d'),
                'close': round(close, 2),
                'max':   round(high or close, 2),
                'min':   round(low  or close, 2),
            })
        rows.sort(key=lambda x: x['date'], reverse=True)
        return rows
    except Exception as e:
        log.warning(f'  download_single {ticker_tw}: {e}')
        return []


# ─────────────────────────────────────────────
#  TWSE STOCK_DAY — 免費備援
# ─────────────────────────────────────────────
def twse_stock_history(code: str, months: int = 7) -> List[dict]:
    rows = []
    now  = date_now()
    for m in range(months):
        target   = now - timedelta(days=m * 30)
        yyyymmdd = target.strftime('%Y%m01')
        try:
            j = get_json(TWSE_STOCK_DAY,
                         {'response':'json','date':yyyymmdd,'stockNo':code})
            for row in j.get('data', []):
                try:
                    parts = row[0].strip().split('/')
                    year  = int(parts[0]) + 1911
                    iso   = f'{year}-{parts[1].zfill(2)}-{parts[2].zfill(2)}'
                    close = float(row[6].replace(',', ''))
                    high  = float(row[4].replace(',', ''))
                    low   = float(row[5].replace(',', ''))
                    rows.append({'date': iso, 'close': close,
                                 'max': high, 'min': low})
                except Exception:
                    pass
            time.sleep(TWSE_SLEEP)
        except Exception as e:
            log.debug(f'  twse_history {code} {yyyymmdd}: {e}')
    rows.sort(key=lambda x: x['date'], reverse=True)
    return rows


# ─────────────────────────────────────────────
#  MOPS 月營收 — 修正版
#  正確 URL：.../t21/sii/t21sc03_{roc}_{mm:02d}_0.html
# ─────────────────────────────────────────────
def load_mops_revenue_all() -> Dict[str, List[dict]]:
    """
    批次下載 MOPS 所有公司月營收（過去 N 個月）。
    每次 1 個 HTML 檔 = 所有上市公司，共 N 次請求。
    回傳 {code: [{date, revenue}, ...]} 降序。
    """
    revenue_map: Dict[str, List[dict]] = {}
    now     = date_now()
    fetched = 0

    log.info(f'  MOPS 月營收：最多抓 {MOPS_REV_MONTHS} 個月...')

    for m in range(MOPS_REV_MONTHS):
        target   = now - timedelta(days=m * 30)
        roc_year = target.year - 1911
        month    = target.month
        iso_date = f'{target.year}-{month:02d}-01'

        # 月營收通常次月 10 日後才公告；當月 / 次月初可能尚未發布
        # 以目前日期判斷：若資料月份為本月，靜默跳過
        if target.year == now.year and target.month == now.month:
            log.debug(f'  跳過 {target.year}/{month:02d}（當月未公告）')
            continue

        url = MOPS_REVENUE_SII.format(roc_year=roc_year, month=month)

        try:
            r    = get(url, timeout=35)
            enc  = r.apparent_encoding or 'big5'
            html = r.content.decode(enc, errors='replace')

            parsed = _parse_revenue_html(html, iso_date)
            if not parsed:
                log.debug(f'  {target.year}/{month:02d}: 解析 0 筆（可能格式變更）')
                continue

            for code, rev in parsed.items():
                revenue_map.setdefault(code, []).append(
                    {'date': iso_date, 'revenue': rev})
            fetched += 1
            log.info(f'  {target.year}/{month:02d}: {len(parsed)} 筆')
            time.sleep(MOPS_SLEEP)

        except requests.exceptions.HTTPError as e:
            code_http = e.response.status_code if e.response else 0
            if code_http == 404:
                # 資料尚未發布，靜默跳過
                log.debug(f'  {target.year}/{month:02d}: 尚未發布 (404)')
            else:
                log.warning(f'  {target.year}/{month:02d}: HTTP {code_http}')
        except Exception as e:
            log.warning(f'  MOPS {roc_year}/{month:02d} 失敗: {e}')

    # Sort descending
    for code in revenue_map:
        revenue_map[code].sort(key=lambda x: x['date'], reverse=True)

    log.info(f'  MOPS 完成：{fetched} 個月，{len(revenue_map)} 支公司')
    return revenue_map


def _parse_revenue_html(html: str, iso_date: str) -> Dict[str, float]:
    result: Dict[str, float] = {}

    if BS4_OK:
        try:
            soup = BeautifulSoup(html, 'lxml')
            # Try to find rows with 4-digit code in first cell
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
        except Exception:
            pass

    # Regex fallback (or supplement)
    if not result:
        pattern = (r'<td[^>]*>\s*([1-9]\d{3})\s*</td>'
                   r'\s*<td[^>]*>[^<]*</td>'
                   r'\s*<td[^>]*>\s*([\d,]+)\s*</td>')
        for m in re.finditer(pattern, html, re.IGNORECASE | re.DOTALL):
            code    = m.group(1)
            rev_str = m.group(2).replace(',', '')
            try:
                rev = float(rev_str)
                if rev > 0:
                    result[code] = rev
            except ValueError:
                pass

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
        score = ((ratio - 0.85) / 0.35) * 100
        return max(0.0, min(99.0, round(score, 1)))
    except Exception:
        return None


def calc_technical(prices: List[dict], proxy: List[dict]) -> dict:
    if not prices:
        return {}
    closes = [float(p['close']) for p in prices]
    cur    = closes[0]

    def ma(n):
        return avg(closes[:n]) if len(closes) >= n else None

    ma5, ma10, ma20 = ma(5), ma(10), ma(20)
    ma60, ma120     = ma(60), ma(120)

    short_align = (bool(ma5 > ma10 > ma20)
                   if all(x is not None for x in [ma5, ma10, ma20]) else None)
    long_align  = (bool(ma20 > ma60 > ma120)
                   if all(x is not None for x in [ma20, ma60, ma120]) else None)
    above_sub   = (bool(cur > float(prices[19]['close']) and
                        cur > float(prices[59]['close']))
                   if len(prices) >= 61 else None)

    dist_high = None
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
    s  = rev_list   # already sorted descending
    l  = s[0]
    lv = float(l['revenue'])

    yoy_list, mom_list = [], []
    for i in range(min(2, len(s))):
        cur = s[i]
        cd  = datetime.strptime(cur['date'], '%Y-%m-%d')
        py  = next((d for d in s if
                    datetime.strptime(d['date'], '%Y-%m-%d').month == cd.month and
                    datetime.strptime(d['date'], '%Y-%m-%d').year  == cd.year - 1), None)
        if py:
            denom = float(py['revenue'])
            if denom > 0:
                yoy_list.append((float(cur['revenue']) - denom) / denom)
        if i < len(s) - 1:
            pv = float(s[i+1]['revenue'])
            if pv > 0:
                mom_list.append((float(cur['revenue']) - pv) / pv)

    all_rev = [float(d['revenue']) for d in s]
    ld      = datetime.strptime(l['date'], '%Y-%m-%d')
    sly     = next((d for d in s if
                    datetime.strptime(d['date'], '%Y-%m-%d').month == ld.month and
                    datetime.strptime(d['date'], '%Y-%m-%d').year  == ld.year - 1), None)

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
    log.info('=== 台股雷達資料建置 V1.1 開始 ===')
    log.info(f'yfinance:  {"✓" if YF_OK else "✗ 使用 TWSE 備援"}')
    log.info(f'pandas:    {"✓" if PANDAS_OK else "✗"}')
    log.info(f'bs4:       {"✓" if BS4_OK else "✗ 使用 regex 備援"}')

    tw_now    = date_now()
    data_date = tw_now.strftime('%Y-%m-%d')

    # ── Step 1: TWSE 個股清單 + 今日行情 ──
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

    # ── Step 3: yfinance 批次 K 線 ──
    log.info('Step 3: yfinance 批次下載 K 線...')
    price_history: Dict[str, List[dict]] = {}
    if YF_OK:
        price_history = download_price_history(all_codes)
    else:
        log.info('  yfinance 不可用，逐支從 TWSE 取歷史（較慢）')

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

    # ── Step 4: MOPS 月營收 ──
    log.info('Step 4: MOPS 月營收...')
    revenue_all = load_mops_revenue_all()

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
            'rsScore':           None,
            'distanceFromHigh':  None,
            'shortMAAlign':      None,
            'longMAAlign':       None,
            'aboveSubPoint':     None,
            'revenue':           None,
            'revenueDate':       None,
            'yoyLatest':         None,
            'momLatest':         None,
            'yoy2mo':            None,
            'mom2mo':            None,
            'revenueHighRecord': None,
            'grossMargin':       None,
            'opMargin':          None,
            'noProfitLoss':      None,
            'marginGrowth':      None,
            'institutionalRecord': None,
            'bigHolderPct':        None,
            'bigHolderIncrease':   None,
            'chipConcentration':   None,
        }

        # 技術指標
        px = price_history.get(code)
        if not px and not YF_OK:
            px = twse_stock_history(code, 7)
        if px:
            r.update(calc_technical(px, proxy))
            if not r['price'] or r['price'] == 0:
                try:
                    r['price'] = float(px[0]['close'])
                except Exception:
                    pass

        # 月營收
        rev_list = revenue_all.get(code, [])
        if rev_list:
            r.update(calc_revenue(rev_list))

        results.append(r)

    # ── 統計 ──
    has_rs      = sum(1 for r in results if r['rsScore']    is not None)
    has_rev     = sum(1 for r in results if r['revenue']    is not None)
    has_foreign = sum(1 for r in results if r['foreignBuy'] is not None)
    log.info(f'  RS 覆蓋: {has_rs}/{len(results)}  '
             f'月營收: {has_rev}/{len(results)}  '
             f'法人: {has_foreign}/{len(results)}')

    # ── Step 6: 輸出 ──
    log.info(f'Step 6: 輸出 {OUTPUT_PATH}...')
    os.makedirs('data', exist_ok=True)

    output = {
        'version':    'V1.1',
        'generated':  tw_now.isoformat(),
        'dataDate':   data_date,
        'source':     'yfinance+mops+twse',
        'stockCount': len(results),
        'coverage':   {'technical': has_rs,
                       'revenue':   has_rev,
                       'institutional': has_foreign},
        'stocks':     results,
    }
    with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, separators=(',',':'))

    size_kb = os.path.getsize(OUTPUT_PATH) / 1024
    log.info(f'  ✅ {len(results)} 支 → {OUTPUT_PATH} ({size_kb:.0f} KB)')
    log.info('=== 完成 ===')


if __name__ == '__main__':
    main()
