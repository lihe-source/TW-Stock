#!/usr/bin/env python3
"""
台股雷達 — 資料建置腳本  V1.0
build_data.py

方案 E：Yahoo Finance + MOPS + TWSE T86（完全免費）

資料來源：
  技術面   → yfinance (Yahoo Finance)     : 個股 7 個月 K 線歷史，批次下載
  基本面   → MOPS 公開資訊觀測站           : 月營收（批次 HTML），財報（選用）
  籌碼面   → TWSE OpenAPI T86            : 今日三大法人買賣超（全市場）
  今日行情 → TWSE OpenAPI DAY_ALL        : 收盤價、漲跌、成交量

執行：
  本機測試  : python3 scripts/build_data.py
  GitHub Actions : 自動執行（.github/workflows/update_data.yml）

環境變數（選用）：
  ENABLE_FINANCIALS=1  → 啟用 MOPS 財報爬取（毛利率、營益率）
  STOCK_LIMIT=200      → 限制個股數量（測試用）
"""

import json
import os
import re
import sys
import time
import logging
import traceback
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, List, Tuple, Any

import requests
try:
    from bs4 import BeautifulSoup
    BS4_OK = True
except ImportError:
    BS4_OK = False
    print("[WARNING] beautifulsoup4 not installed. MOPS parsing will use regex fallback.")

try:
    import yfinance as yf
    YF_OK = True
except ImportError:
    YF_OK = False
    print("[WARNING] yfinance not installed. Technical indicators will use TWSE fallback.")

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
MOPS_REVENUE   = 'https://mops.twse.com.tw/nas/t21/s02/t21sc03_{roc_year}_{month:02d}.html'
MOPS_FIN_URL   = 'https://mops.twse.com.tw/mops/web/ajax_t163sb04'

ENABLE_FINANCIALS = os.environ.get('ENABLE_FINANCIALS', '0') == '1'
STOCK_LIMIT       = int(os.environ.get('STOCK_LIMIT', '0'))  # 0 = no limit
OUTPUT_PATH       = 'data/screener.json'
TW_TZ             = timezone(timedelta(hours=8))

YFINANCE_CHUNK    = 100     # stocks per yfinance batch call
YFINANCE_PERIOD   = '7mo'   # 7 months of history
MOPS_REV_MONTHS   = 24      # how many months of revenue to fetch
MOPS_SLEEP        = 1.2     # seconds between MOPS requests
TWSE_SLEEP        = 0.4

# ─────────────────────────────────────────────
#  HTTP session
# ─────────────────────────────────────────────
SESSION = requests.Session()
SESSION.headers.update({
    'Accept':          'text/html,application/json,*/*',
    'Accept-Language': 'zh-TW,zh;q=0.9,en;q=0.8',
    'User-Agent':      'Mozilla/5.0 (compatible; TaiwanStockRadar/1.0)',
})

def get(url: str, params: dict = None, method: str = 'GET',
        data: dict = None, retries: int = 3, timeout: int = 30) -> requests.Response:
    for attempt in range(retries):
        try:
            if method == 'POST':
                r = SESSION.post(url, data=data, timeout=timeout)
            else:
                r = SESSION.get(url, params=params, timeout=timeout)
            r.raise_for_status()
            return r
        except Exception as e:
            if attempt < retries - 1:
                wait = 2 ** attempt
                log.warning(f'  retry {attempt+1}/{retries}: {e} (wait {wait}s)')
                time.sleep(wait)
            else:
                raise

def get_json(url: str, params: dict = None, retries: int = 3) -> Any:
    return get(url, params=params, retries=retries).json()

def date_ago(months: int) -> datetime:
    return datetime.now(TW_TZ) - timedelta(days=months * 30)

# ─────────────────────────────────────────────
#  TWSE helpers
# ─────────────────────────────────────────────
def is_equity(code: str) -> bool:
    """4 位數字，非 0 開頭（排除 ETF、權證）"""
    return bool(code and re.match(r'^[1-9]\d{3}$', code))

def load_twse_day_all() -> Tuple[Dict[str, dict], Dict[str, dict]]:
    """
    回傳 (stocks_base, names_only)
    stocks_base: 有行情資料的股票  {code: {name,price,change,pct,high,low,vol}}
    names_only:  只有名稱的股票（非交易日備援）
    """
    stocks_base: Dict[str, dict] = {}
    names_only:  Dict[str, dict] = {}

    # Layer 1: DAY_ALL
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
            name  = (s.get('Name') or code).strip()
            stocks_base[code] = {
                'code': code, 'name': name,
                'price': price, 'change': round(chg, 2), 'pct': pct,
                'high': float((s.get('HighestPrice') or str(price)).replace(',', '')),
                'low':  float((s.get('LowestPrice')  or str(price)).replace(',', '')),
                'vol':  vol,
            }
        log.info(f'  DAY_ALL: {len(stocks_base)} 筆')
    except Exception as e:
        log.warning(f'  DAY_ALL 失敗: {e}')

    # Layer 2: company list (always available)
    try:
        raw = get_json(TWSE_LISTED)
        for s in (raw or []):
            code = (s.get('公司代號') or '').strip()
            if not is_equity(code):
                continue
            name = (s.get('公司名稱') or code).strip()
            names_only[code] = {'code': code, 'name': name}
        log.info(f'  t187ap03_L: {len(names_only)} 筆')
    except Exception as e:
        log.warning(f'  t187ap03_L 失敗: {e}')

    return stocks_base, names_only

def load_t86() -> Dict[str, dict]:
    """T86 三大法人今日買賣超 → {code: {foreignNet, trustNet, foreignBuy, trustBuy}}"""
    inst: Dict[str, dict] = {}
    try:
        raw = get_json(TWSE_T86)
        for s in (raw or []):
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
#  yfinance — 批次下載 K 線
# ─────────────────────────────────────────────
def download_price_history(codes: List[str]) -> Dict[str, List[dict]]:
    """
    批次下載所有個股 7 個月 K 線，回傳 {code: [{date,close,max,min}, ...] 最新在前}
    使用 yfinance 的群組下載，比逐支呼叫快約 20 倍。
    """
    if not YF_OK:
        log.warning('  yfinance 未安裝，跳過技術指標計算')
        return {}

    result: Dict[str, List[dict]] = {}
    tickers = [f'{c}.TW' for c in codes]

    log.info(f'  yfinance 批次下載 {len(tickers)} 支（分 {-(-len(tickers)//YFINANCE_CHUNK)} 批）...')

    for i in range(0, len(tickers), YFINANCE_CHUNK):
        chunk = tickers[i:i+YFINANCE_CHUNK]
        chunk_codes = codes[i:i+YFINANCE_CHUNK]
        batch_num = i // YFINANCE_CHUNK + 1
        total_batches = -(-len(tickers) // YFINANCE_CHUNK)
        log.info(f'    批次 {batch_num}/{total_batches}: {chunk[0]} ~ {chunk[-1]}')

        try:
            # auto_adjust=True 自動調整還原權息
            raw = yf.download(
                chunk,
                period=YFINANCE_PERIOD,
                auto_adjust=True,
                progress=False,
                group_by='ticker',
                threads=True,    # 並行下載
            )
        except Exception as e:
            log.warning(f'    批次 {batch_num} 失敗: {e}')
            continue

        for ticker, code in zip(chunk, chunk_codes):
            try:
                if len(chunk) == 1:
                    # single ticker: raw 是 DataFrame
                    df = raw
                else:
                    df = raw[ticker] if ticker in raw.columns.get_level_values(1) else None
                    if df is None:
                        continue

                if df is None or df.empty:
                    continue

                rows = []
                for date_idx, row in df.iterrows():
                    try:
                        close = float(row['Close'])
                        high  = float(row.get('High', close))
                        low   = float(row.get('Low',  close))
                        if close <= 0:
                            continue
                        rows.append({
                            'date':  date_idx.strftime('%Y-%m-%d'),
                            'close': round(close, 2),
                            'max':   round(high,  2),
                            'min':   round(low,   2),
                        })
                    except Exception:
                        pass
                rows.sort(key=lambda x: x['date'], reverse=True)
                if rows:
                    result[code] = rows
            except Exception as e:
                log.debug(f'    {code}: {e}')

        time.sleep(0.5)  # polite pause between batches

    log.info(f'  yfinance 完成: 成功 {len(result)}/{len(codes)} 支')
    return result


# ─────────────────────────────────────────────
#  TWSE STOCK_DAY — 免費備援（無 yfinance 時）
# ─────────────────────────────────────────────
def twse_stock_history(code: str, months: int = 7) -> List[dict]:
    rows = []
    now  = datetime.now(TW_TZ)
    for m in range(months):
        target   = now - timedelta(days=m * 30)
        yyyymmdd = target.strftime('%Y%m01')
        try:
            j = get_json(TWSE_STOCK_DAY, {'response':'json','date':yyyymmdd,'stockNo':code})
            for row in j.get('data', []):
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
        except Exception as e:
            log.debug(f'  twse_history {code} {yyyymmdd}: {e}')
    rows.sort(key=lambda x: x['date'], reverse=True)
    return rows


# ─────────────────────────────────────────────
#  MOPS 月營收 — 批次抓取（每次 1 個月，全部公司）
# ─────────────────────────────────────────────
def load_mops_revenue_all_months(months: int = MOPS_REV_MONTHS) -> Dict[str, List[dict]]:
    """
    從 MOPS 批次下載所有公司月營收資料（過去 N 個月）。
    回傳 {code: [{date:'YYYY-MM-01', revenue:float}, ...]} 按日期降序。

    MOPS URL: https://mops.twse.com.tw/nas/t21/s02/t21sc03_{民國年}_{月:02d}.html
    HTML 表格欄位（上市公司）：
      公司代號 | 公司名稱 | 當月營收 | 上月營收 | 去年當月營收 | 上月增減(%) | 去年同月增減(%)
    """
    revenue_map: Dict[str, List[dict]] = {}  # code → list of {date, revenue}
    now = datetime.now(TW_TZ)

    log.info(f'  MOPS 月營收：抓取最近 {months} 個月...')

    for m in range(months):
        target    = now - timedelta(days=m * 30)
        roc_year  = target.year - 1911
        month     = target.month
        url       = MOPS_REVENUE.format(roc_year=roc_year, month=month)
        iso_date  = f'{target.year}-{month:02d}-01'

        try:
            r   = get(url, timeout=30)
            # MOPS HTML uses big5 or utf-8 encoding; detect from response
            enc = r.encoding or 'big5'
            html = r.content.decode(enc, errors='replace')

            parsed = _parse_mops_revenue_html(html, iso_date)
            for code, rev in parsed.items():
                revenue_map.setdefault(code, []).append({'date': iso_date, 'revenue': rev})
            log.info(f'    {target.year}/{month:02d}: {len(parsed)} 筆')
            time.sleep(MOPS_SLEEP)

        except Exception as e:
            log.warning(f'    MOPS {roc_year}/{month:02d} 失敗: {e}')

    # Sort each company's revenue descending by date
    for code in revenue_map:
        revenue_map[code].sort(key=lambda x: x['date'], reverse=True)

    log.info(f'  MOPS 月營收完成：{len(revenue_map)} 支公司')
    return revenue_map


def _parse_mops_revenue_html(html: str, iso_date: str) -> Dict[str, float]:
    """解析 MOPS 月營收 HTML，回傳 {code: revenue_float}"""
    result: Dict[str, float] = {}

    if BS4_OK:
        try:
            soup = BeautifulSoup(html, 'html.parser')
            tables = soup.find_all('table')
            for table in tables:
                rows = table.find_all('tr')
                for row in rows:
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
        except Exception as e:
            log.debug(f'  BeautifulSoup 解析失敗: {e}，改用 regex')
            result = _parse_mops_revenue_regex(html)
    else:
        result = _parse_mops_revenue_regex(html)

    return result


def _parse_mops_revenue_regex(html: str) -> Dict[str, float]:
    """regex 備援解析 MOPS 月營收"""
    result: Dict[str, float] = {}
    # Pattern: <td>4-digit code</td><td>name</td><td>revenue with commas</td>
    pattern = r'<td[^>]*>\s*([1-9]\d{3})\s*</td>\s*<td[^>]*>[^<]*</td>\s*<td[^>]*>\s*([\d,]+)\s*</td>'
    for m in re.finditer(pattern, html, re.IGNORECASE):
        code    = m.group(1).strip()
        rev_str = m.group(2).replace(',', '').strip()
        try:
            rev = float(rev_str)
            if rev > 0:
                result[code] = rev
        except ValueError:
            pass
    return result


# ─────────────────────────────────────────────
#  MOPS 財報（選用，逐支公司）
# ─────────────────────────────────────────────
def load_mops_financials(code: str) -> List[dict]:
    """
    從 MOPS 取得個股最近季度財報。
    只有 ENABLE_FINANCIALS=1 時才呼叫。
    回傳 [{date, type, value}, ...] 格式（與 FinMind 相容）
    """
    if not ENABLE_FINANCIALS:
        return []
    try:
        now     = datetime.now(TW_TZ)
        season  = (now.month - 1) // 3  # 0~3
        if season == 0:
            year, season = now.year - 1, 4
        else:
            year = now.year

        result = []
        for q_back in range(4):  # 最近 4 季
            q      = season - q_back
            yr     = year
            if q <= 0:
                q  += 4
                yr -= 1
            date_str = f'{yr}-{q*3:02d}-01'

            data = {'encodeURIComponent': 1,
                    'step':    1,
                    'CO_ID':   code,
                    'SYEAR':   yr,
                    'SSEASON': q,
                    'report_id': 'C'}
            r    = get(MOPS_FIN_URL, method='POST', data=data, timeout=25)
            html = r.text

            # Parse gross profit margin, operating income from HTML
            for match in re.finditer(
                r'(毛利率|營業利益率|本期淨利)[^\d\-]*([\-\d\.]+)\s*%', html):
                label = match.group(1)
                val   = float(match.group(2))
                type_map = {'毛利率': 'gross_margin_pct',
                            '營業利益率': 'op_margin_pct',
                            '本期淨利': 'NetIncome'}
                result.append({'date': date_str,
                                'type': type_map.get(label, label),
                                'value': val})
            time.sleep(MOPS_SLEEP)

        return result
    except Exception as e:
        log.debug(f'  MOPS 財報 {code}: {e}')
        return []


# ─────────────────────────────────────────────
#  指標計算
# ─────────────────────────────────────────────
def avg(lst: list) -> float:
    return sum(lst) / len(lst) if lst else 0.0


def calc_technical(prices: List[dict], proxy: List[dict]) -> dict:
    if not prices:
        return {}
    closes = [float(p['close']) for p in prices]
    cur    = closes[0]

    def ma(n):
        return avg(closes[:n]) if len(closes) >= n else None

    ma5, ma10, ma20 = ma(5), ma(10), ma(20)
    ma60, ma120     = ma(60), ma(120)

    short_align = None
    if all(x is not None for x in [ma5, ma10, ma20]):
        short_align = bool(ma5 > ma10 > ma20)

    long_align = None
    if all(x is not None for x in [ma20, ma60, ma120]):
        long_align = bool(ma20 > ma60 > ma120)

    above_sub = None
    if len(prices) >= 61:
        above_sub = bool(cur > float(prices[19]['close']) and
                         cur > float(prices[59]['close']))

    dist_high = None
    lk = min(22, len(prices))
    if lk:
        mh = max(float(p.get('max', p['close'])) for p in prices[:lk])
        dist_high = round(((cur / mh) - 1) * 100, 2) if mh else None

    # RS 指標（個股 vs 0050 大盤代理，26 週）
    rs = _calc_rs(prices, proxy)

    return {
        'rsScore':          rs,
        'shortMAAlign':     short_align,
        'longMAAlign':      long_align,
        'aboveSubPoint':    above_sub,
        'distanceFromHigh': dist_high,
    }


def _calc_rs(sp: List[dict], tp: List[dict]) -> Optional[float]:
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


def calc_revenue(rev_list: List[dict]) -> dict:
    """
    rev_list: [{date:'YYYY-MM-01', revenue:float}, ...] 降序
    """
    if len(rev_list) < 3:
        return {}
    s  = rev_list  # already sorted descending
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


def calc_financials_from_mops(data: List[dict]) -> dict:
    """解析 MOPS 財報資料（毛利率、營益率格式）"""
    if not data:
        return {}
    # Group by date
    by_date: Dict[str, dict] = {}
    for item in data:
        d = item.get('date', '')
        by_date.setdefault(d, {})[item.get('type', '')] = float(item.get('value', 0))

    dates = sorted(by_date.keys(), reverse=True)
    if not dates:
        return {}
    l  = by_date[dates[0]]
    ly = by_date[dates[4]] if len(dates) > 4 else None

    gm = l.get('gross_margin_pct')
    om = l.get('op_margin_pct')
    ni = l.get('NetIncome')

    gm_grow = om_grow = None
    if ly:
        ly_gm = ly.get('gross_margin_pct')
        ly_om = ly.get('op_margin_pct')
        if gm is not None and ly_gm is not None:
            gm_grow = gm > ly_gm
        if om is not None and ly_om is not None:
            om_grow = om > ly_om

    return {
        'grossMargin':  round(gm, 2) if gm is not None else None,
        'opMargin':     round(om, 2) if om is not None else None,
        'noProfitLoss': (ni > 0) if ni is not None else None,
        'marginGrowth': (gm_grow and om_grow) if (
                         gm_grow is not None and om_grow is not None) else None,
    }


# ─────────────────────────────────────────────
#  主流程
# ─────────────────────────────────────────────
def main():
    log.info('=== 台股雷達資料建置 V1.0 開始 ===')
    log.info(f'yfinance: {"✓" if YF_OK else "✗ (用 TWSE 備援)"}')
    log.info(f'BeautifulSoup4: {"✓" if BS4_OK else "✗ (用 regex 備援)"}')
    log.info(f'ENABLE_FINANCIALS: {ENABLE_FINANCIALS}')
    log.info(f'STOCK_LIMIT: {STOCK_LIMIT if STOCK_LIMIT else "無限制"}')

    tw_now    = datetime.now(TW_TZ)
    data_date = tw_now.strftime('%Y-%m-%d')

    # ── Step 1: TWSE 個股清單 + 今日行情 ──
    log.info('Step 1: TWSE 個股清單 + 行情...')
    stocks_base, names_only = load_twse_day_all()

    # Merge: use stocks_base primarily, fill in names_only for those with prices
    all_codes_set = set(stocks_base.keys()) | set(names_only.keys())
    if STOCK_LIMIT:
        all_codes_set = set(sorted(all_codes_set)[:STOCK_LIMIT])
        log.info(f'  STOCK_LIMIT 生效，限制 {STOCK_LIMIT} 支')

    all_codes = sorted(all_codes_set)
    log.info(f'  總計 {len(all_codes)} 支股票')

    # ── Step 2: TWSE T86 法人 ──
    log.info('Step 2: TWSE T86 法人買賣超...')
    inst_today = load_t86()

    # ── Step 3: yfinance 批次下載 K 線 ──
    log.info('Step 3: yfinance 批次下載 K 線歷史...')
    price_history: Dict[str, List[dict]] = {}
    if YF_OK:
        price_history = download_price_history(all_codes)
    else:
        log.info('  yfinance 不可用，將逐支從 TWSE 取歷史（較慢）')

    # 大盤代理（0050）
    proxy: List[dict] = price_history.get('0050', [])
    if not proxy:
        log.info('  0050 未在批次結果中，單獨下載...')
        if YF_OK:
            try:
                raw  = yf.download('0050.TW', period=YFINANCE_PERIOD,
                                   auto_adjust=True, progress=False)
                proxy = [{'date': d.strftime('%Y-%m-%d'),
                          'close': round(float(row['Close']),2),
                          'max':   round(float(row['High']),2),
                          'min':   round(float(row['Low']),2)}
                         for d, row in raw.iterrows() if float(row['Close']) > 0]
                proxy.sort(key=lambda x: x['date'], reverse=True)
                log.info(f'  0050: {len(proxy)} 筆')
            except Exception as e:
                log.warning(f'  0050 下載失敗: {e}')
        if not proxy:
            proxy = twse_stock_history('0050', 7)
            log.info(f'  0050 (TWSE backup): {len(proxy)} 筆')

    # ── Step 4: MOPS 月營收 ──
    log.info('Step 4: MOPS 月營收（批次）...')
    revenue_all = load_mops_revenue_all_months(MOPS_REV_MONTHS)

    # ── Step 5: 組合計算 ──
    log.info('Step 5: 計算個股指標...')
    results = []

    for i, code in enumerate(all_codes):
        if i > 0 and i % 200 == 0:
            log.info(f'  進度: {i}/{len(all_codes)}...')

        base = stocks_base.get(code) or names_only.get(code) or {'code': code, 'name': code}
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
            # 法人（T86 今日）
            'foreignNet':  inst.get('foreignNet', None),
            'trustNet':    inst.get('trustNet',   None),
            'foreignBuy':  inst.get('foreignBuy', None),
            'trustBuy':    inst.get('trustBuy',   None),
            # 技術指標（預設 None）
            'rsScore':           None,
            'distanceFromHigh':  None,
            'shortMAAlign':      None,
            'longMAAlign':       None,
            'aboveSubPoint':     None,
            # 基本面（預設 None）
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
            # 籌碼（無免費來源，保留欄位）
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
            # 補行情（非交易日 TWSE 無收盤）
            if not r['price'] or r['price'] == 0:
                try:
                    r['price'] = float(px[0]['close'])
                except Exception:
                    pass

        # 月營收
        rev_list = revenue_all.get(code, [])
        if rev_list:
            r.update(calc_revenue(rev_list))

        # 財報（選用）
        if ENABLE_FINANCIALS:
            fin_data = load_mops_financials(code)
            if fin_data:
                r.update(calc_financials_from_mops(fin_data))

        results.append(r)

    log.info(f'  指標計算完成：{len(results)} 支')

    # ── Step 6: 統計 ──
    has_rs      = sum(1 for r in results if r['rsScore']   is not None)
    has_rev     = sum(1 for r in results if r['revenue']   is not None)
    has_foreign = sum(1 for r in results if r['foreignBuy'] is not None)
    log.info(f'  RS 指標覆蓋: {has_rs}/{len(results)} 支')
    log.info(f'  月營收覆蓋:  {has_rev}/{len(results)} 支')
    log.info(f'  法人覆蓋:    {has_foreign}/{len(results)} 支')

    # ── Step 7: 輸出 ──
    log.info(f'Step 6: 輸出 {OUTPUT_PATH}...')
    os.makedirs('data', exist_ok=True)

    output = {
        'version':     'V1.0',
        'generated':   tw_now.isoformat(),
        'dataDate':    data_date,
        'source':      'yfinance+mops+twse',
        'stockCount':  len(results),
        'coverage': {
            'technical': has_rs,
            'revenue':   has_rev,
            'institutional': has_foreign,
        },
        'stocks': results,
    }

    with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, separators=(',', ':'))

    size_kb = os.path.getsize(OUTPUT_PATH) / 1024
    log.info(f'  ✅ {len(results)} 支 → {OUTPUT_PATH} ({size_kb:.0f} KB)')
    log.info('=== 完成 ===')


if __name__ == '__main__':
    main()
