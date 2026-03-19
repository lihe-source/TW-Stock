#!/usr/bin/env python3
"""
台股雷達 — 資料建置腳本
build_data.py

執行方式：
  本機測試：python3 scripts/build_data.py
  GitHub Actions：自動執行（見 .github/workflows/update_data.yml）

輸出：data/screener.json

資料來源：
  - TWSE OpenAPI   → 收盤行情、法人買賣超（免費，無需 token）
  - FinMind API    → 個股歷史、財報、籌碼（需 token，可選）
  - TWSE STOCK_DAY → 個股歷史（免費備援，無需 token）
"""

import json
import os
import sys
import time
import math
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, List, Any

import requests

# ─────────────────────────────────────────────────────────────
#  設定
# ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S',
)
log = logging.getLogger(__name__)

FINMIND_TOKEN  = os.environ.get('FINMIND_TOKEN', '')
FINMIND_BASE   = 'https://api.finmindtrade.com/api/v4/data'
TWSE_DAY_ALL   = 'https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL'
TWSE_LISTED    = 'https://openapi.twse.com.tw/v1/opendata/t187ap03_L'
TWSE_T86       = 'https://openapi.twse.com.tw/v1/fund/T86'
TWSE_STOCK_DAY = 'https://www.twse.com.tw/exchangeReport/STOCK_DAY'

# FinMind 速率控制（免費版 300 次/小時，付費版 30000 次/小時）
RATE_SLEEP     = 0.35 if FINMIND_TOKEN else 0   # seconds between calls
TWSE_SLEEP     = 0.6                            # TWSE per-stock calls

TAIEX_PROXY    = '0050'     # 元大台灣50（大盤代理）
MONTHS_PRICE   = 7          # 取多少個月歷史股價
MONTHS_REVENUE = 24         # 月營收取多少個月
MONTHS_FIN     = 18         # 財報取多少個月

OUTPUT_PATH    = 'data/screener.json'
EMPTY_PATH     = 'data/screener_empty.json'

TW_TZ = timezone(timedelta(hours=8))

# ─────────────────────────────────────────────────────────────
#  HTTP 工具
# ─────────────────────────────────────────────────────────────
SESSION = requests.Session()
SESSION.headers.update({'Accept': 'application/json', 'User-Agent': 'TaiwanStockRadar/0.6'})

def get_json(url: str, params: dict = None, retries: int = 3, timeout: int = 20) -> Any:
    for attempt in range(retries):
        try:
            r = SESSION.get(url, params=params, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            if attempt < retries - 1:
                wait = 2 ** attempt
                log.warning(f'  retry {attempt+1}/{retries} ({e}) wait {wait}s')
                time.sleep(wait)
            else:
                raise

def finmind(dataset: str, data_id: str, start_date: str) -> List[dict]:
    """呼叫 FinMind API，回傳 data list，若無 token 回傳空 list"""
    if not FINMIND_TOKEN:
        return []
    params = {'dataset': dataset, 'data_id': data_id,
              'start_date': start_date, 'token': FINMIND_TOKEN}
    j = get_json(FINMIND_BASE, params)
    if j.get('status') != 200:
        log.debug(f'  FinMind {dataset} {data_id}: {j.get("msg","?")}')
        return []
    if RATE_SLEEP:
        time.sleep(RATE_SLEEP)
    return j.get('data', [])

def date_ago(months: int) -> str:
    d = datetime.now(TW_TZ) - timedelta(days=months * 30)
    return d.strftime('%Y-%m-%d')

def date_str(d: datetime) -> str:
    return d.strftime('%Y-%m-%d')

# ─────────────────────────────────────────────────────────────
#  TWSE 個股月份歷史（免費備援，不需 token）
# ─────────────────────────────────────────────────────────────
def twse_stock_history(code: str, months: int = 7) -> List[dict]:
    """
    使用 TWSE STOCK_DAY 端點取得個股歷史（每月一個 call）
    回傳格式：[{date, open, high, low, close, volume}, ...]  最新在前
    """
    rows = []
    now = datetime.now(TW_TZ)
    for m in range(months):
        target = now - timedelta(days=m * 30)
        yyyymmdd = target.strftime('%Y%m01')  # 每月第一天
        try:
            j = get_json(TWSE_STOCK_DAY, {
                'response': 'json',
                'date': yyyymmdd,
                'stockNo': code,
            })
            data = j.get('data', [])
            for row in data:
                # TWSE format: [date_tw, vol, turnover, open, high, low, close, change, txn]
                try:
                    tw_date = row[0].strip()           # 民國年，如 114/03/18
                    parts = tw_date.split('/')
                    year = int(parts[0]) + 1911
                    mon  = parts[1].zfill(2)
                    day  = parts[2].zfill(2)
                    iso  = f'{year}-{mon}-{day}'
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

# ─────────────────────────────────────────────────────────────
#  計算指標
# ─────────────────────────────────────────────────────────────
def avg(lst):
    return sum(lst) / len(lst) if lst else 0.0

def calc_rs(sp: List[dict], tp: List[dict]) -> Optional[float]:
    """RS 相對強度（個股 vs 0050 大盤代理，26週）"""
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

def calc_mas(prices: List[dict]) -> dict:
    closes = [float(p['close']) for p in prices]
    return {
        'ma5':   avg(closes[:5])   if len(closes) >= 5   else None,
        'ma10':  avg(closes[:10])  if len(closes) >= 10  else None,
        'ma20':  avg(closes[:20])  if len(closes) >= 20  else None,
        'ma60':  avg(closes[:60])  if len(closes) >= 60  else None,
        'ma120': avg(closes[:120]) if len(closes) >= 120 else None,
    }

def calc_technical(prices: List[dict], proxy: List[dict]) -> dict:
    if not prices:
        return {}
    ma = calc_mas(prices)
    closes = [float(p['close']) for p in prices]
    current = closes[0]

    # 短均排列：MA5 > MA10 > MA20
    short_align = None
    if ma['ma5'] and ma['ma10'] and ma['ma20']:
        short_align = bool(ma['ma5'] > ma['ma10'] > ma['ma20'])

    # 中長均排列：MA20 > MA60 > MA120
    long_align = None
    if ma['ma20'] and ma['ma60'] and ma['ma120']:
        long_align = bool(ma['ma20'] > ma['ma60'] > ma['ma120'])

    # 扣抵值
    above_sub = None
    if len(prices) >= 61:
        above_sub = bool(current > float(prices[19]['close']) and
                         current > float(prices[59]['close']))

    # 距月高點
    dist_high = None
    lookback = min(22, len(prices))
    if lookback > 0:
        month_high = max(float(p.get('max', p['close'])) for p in prices[:lookback])
        dist_high = round(((current / month_high) - 1) * 100, 2) if month_high else None

    # RS
    rs = calc_rs(prices, proxy)

    return {
        'rsScore':          rs,
        'shortMAAlign':     short_align,
        'longMAAlign':      long_align,
        'aboveSubPoint':    above_sub,
        'distanceFromHigh': dist_high,
    }

def calc_revenue(raw: List[dict]) -> dict:
    if len(raw) < 3:
        return {}
    s = sorted(raw, key=lambda x: x['date'], reverse=True)
    latest = s[0]
    lv = float(latest['revenue'])

    yoy_list, mom_list = [], []
    for i in range(min(2, len(s))):
        cur = s[i]
        cd = datetime.strptime(cur['date'], '%Y-%m-%d')
        py = next((d for d in s if
            datetime.strptime(d['date'], '%Y-%m-%d').month == cd.month and
            datetime.strptime(d['date'], '%Y-%m-%d').year  == cd.year - 1), None)
        if py:
            yoy_list.append((float(cur['revenue']) - float(py['revenue'])) / float(py['revenue']))
        if i < len(s) - 1:
            prev = float(s[i+1]['revenue'])
            if prev > 0:
                mom_list.append((float(cur['revenue']) - prev) / prev)

    all_rev = [float(d['revenue']) for d in s]
    same_last_year = next((d for d in s if
        datetime.strptime(d['date'], '%Y-%m-%d').month == datetime.strptime(latest['date'], '%Y-%m-%d').month and
        datetime.strptime(d['date'], '%Y-%m-%d').year  == datetime.strptime(latest['date'], '%Y-%m-%d').year - 1), None)

    return {
        'revenue':           round(lv / 1e8, 2),
        'revenueDate':       latest['date'],
        'yoyLatest':         round(yoy_list[0] * 100, 2) if yoy_list else None,
        'momLatest':         round(mom_list[0] * 100, 2) if mom_list else None,
        'yoy2mo':            len(yoy_list) == 2 and all(v >= 0.20 for v in yoy_list),
        'mom2mo':            len(mom_list) == 2 and all(v >= 0.20 for v in mom_list),
        'revenueHighRecord': lv >= max(all_rev) or (
            float(same_last_year['revenue']) < lv if same_last_year else False),
    }

def calc_financials(data: List[dict]) -> dict:
    if len(data) < 2:
        return {}
    s = sorted(data, key=lambda x: x['date'], reverse=True)
    latest = s[0]
    latest_dt = datetime.strptime(latest['date'], '%Y-%m-%d')

    ly = next((d for d in s if
        datetime.strptime(d['date'], '%Y-%m-%d').month == latest_dt.month and
        datetime.strptime(d['date'], '%Y-%m-%d').year  == latest_dt.year - 1), None)

    def gv(row, types):
        for t in types:
            item = next((d for d in data if d['date'] == row['date'] and d['type'] == t), None)
            if item:
                return float(item['value'])
        return None

    rev = gv(latest, ['Revenue', 'revenue'])
    gp  = gv(latest, ['GrossProfit', 'gross_profit'])
    op  = gv(latest, ['OperatingIncome', 'operating_income'])
    ni  = gv(latest, ['NetIncome', 'net_income', 'AfterTaxProfit'])

    gm = (gp / rev * 100) if rev and gp else None
    om = (op / rev * 100) if rev and op else None

    gm_grow, om_grow = None, None
    if ly:
        ly_rev = gv(ly, ['Revenue', 'revenue'])
        ly_gp  = gv(ly, ['GrossProfit', 'gross_profit'])
        ly_op  = gv(ly, ['OperatingIncome', 'operating_income'])
        ly_gm  = (ly_gp / ly_rev * 100) if ly_rev and ly_gp else None
        ly_om  = (ly_op / ly_rev * 100) if ly_rev and ly_op else None
        if gm is not None and ly_gm is not None:
            gm_grow = gm > ly_gm
        if om is not None and ly_om is not None:
            om_grow = om > ly_om

    return {
        'grossMargin':  round(gm, 2) if gm is not None else None,
        'opMargin':     round(om, 2) if om is not None else None,
        'noProfitLoss': (ni > 0) if ni is not None else None,
        'marginGrowth': (gm_grow and om_grow) if (gm_grow is not None and om_grow is not None) else None,
    }

def calc_institutional(data: List[dict]) -> dict:
    if not data:
        return {}
    s = sorted(data, key=lambda x: x['date'], reverse=True)
    fgn = trust = 0.0
    for r in s[:30]:
        n = (r.get('name', '') or '').lower()
        net = float(r.get('buy', 0)) - float(r.get('sell', 0))
        if '外資' in n or 'foreign' in n:
            fgn += net
        if '投信' in n or 'investment_trust' in n:
            trust += net

    # 法人持股季高
    three_months_ago = date_ago(3)
    daily: Dict[str, float] = {}
    for r in data:
        if r['date'] >= three_months_ago:
            daily[r['date']] = daily.get(r['date'], 0) + (float(r.get('buy',0)) - float(r.get('sell',0)))
    da = sorted(daily.items(), key=lambda x: x[0], reverse=True)
    rs5 = sum(v for _, v in da[:5])
    qmax = max((v for _, v in da), default=0)
    inst_record = len(da) > 10 and rs5 >= qmax

    return {
        'foreignNet':        round(fgn),
        'trustNet':          round(trust),
        'foreignBuy':        fgn > 0,
        'trustBuy':          trust > 0,
        'institutionalRecord': inst_record,
    }

def calc_shareholder(data: List[dict]) -> dict:
    if len(data) < 2:
        return {}
    s = sorted(data, key=lambda x: x['date'], reverse=True)
    l = s[0]
    p = s[min(4, len(s)-1)]
    pct  = float(l.get('percent_above_1000') or l.get('HoldingSharesRatio') or 0)
    ppct = float(p.get('percent_above_1000') or p.get('HoldingSharesRatio') or 0)
    return {
        'bigHolderPct':      round(pct, 2),
        'bigHolderIncrease': pct > ppct,
        'chipConcentration': pct > ppct,
    }

# ─────────────────────────────────────────────────────────────
#  主流程
# ─────────────────────────────────────────────────────────────
def main():
    log.info('=== 台股雷達資料建置開始 ===')
    log.info(f'FinMind Token: {"有設定" if FINMIND_TOKEN else "未設定（只使用 TWSE 免費資料）"}')

    tw_now = datetime.now(TW_TZ)
    data_date = date_str(tw_now)

    # ── Step 1: TWSE DAY_ALL + T86 同時取得 ──
    log.info('Step 1: 取得 TWSE DAY_ALL + T86...')
    day_all_data, t86_data = [], []
    try:
        day_all_data = get_json(TWSE_DAY_ALL) or []
        log.info(f'  DAY_ALL: {len(day_all_data)} 筆')
    except Exception as e:
        log.warning(f'  DAY_ALL 失敗: {e}')

    try:
        t86_data = get_json(TWSE_T86) or []
        log.info(f'  T86: {len(t86_data)} 筆')
    except Exception as e:
        log.warning(f'  T86 失敗: {e}')

    # ── Step 2: 解析 DAY_ALL → 個股基本資料 ──
    log.info('Step 2: 解析個股清單...')
    stocks_base: Dict[str, dict] = {}
    for s in day_all_data:
        code = (s.get('Code') or '').strip()
        if not code or not code.isdigit() or len(code) != 4 or code.startswith('0'):
            continue
        price_str = (s.get('ClosingPrice') or '0').replace(',', '')
        price = float(price_str) if price_str and price_str not in ['+', '-', '--'] else 0
        if price <= 0:
            continue
        vol_str = (s.get('TradeVolume') or '0').replace(',', '')
        vol = float(vol_str) if vol_str.replace('.','').isdigit() else 0
        chg_raw = str(s.get('Change') or '0').strip()
        chg_abs = float(chg_raw.replace('▲','').replace('▼','').replace('+','').replace('-','').replace(',','').strip() or '0')
        chg = -chg_abs if (chg_raw.startswith('▼') or chg_raw.startswith('-')) else chg_abs
        base = price - chg or price
        pct = (chg / base * 100) if base else 0

        stocks_base[code] = {
            'code':   code,
            'name':   (s.get('Name') or code).strip(),
            'price':  price,
            'change': round(chg, 2),
            'pct':    round(pct, 2),
            'high':   float((s.get('HighestPrice') or price).replace(',', '') if isinstance(s.get('HighestPrice'), str) else price),
            'low':    float((s.get('LowestPrice')  or price).replace(',', '') if isinstance(s.get('LowestPrice'), str) else price),
            'vol':    vol,
        }

    # Fallback: TWSE listed company list (no prices)
    if not stocks_base:
        log.warning('  DAY_ALL 無資料，改用 t187ap03_L...')
        try:
            listed = get_json(TWSE_LISTED) or []
            for s in listed:
                code = (s.get('公司代號') or '').strip()
                if not code or not code.isdigit() or len(code) != 4 or code.startswith('0'):
                    continue
                stocks_base[code] = {
                    'code': code, 'name': (s.get('公司名稱') or code).strip(),
                    'price': 0, 'change': 0, 'pct': 0, 'high': 0, 'low': 0, 'vol': 0,
                }
        except Exception as e:
            log.error(f'  t187ap03_L 失敗: {e}')

    log.info(f'  共 {len(stocks_base)} 支股票')

    # ── Step 3: 解析 T86 → 法人買賣超 ──
    log.info('Step 3: 解析法人買賣超 (T86)...')
    inst_map: Dict[str, dict] = {}
    for s in t86_data:
        code = (s.get('Code') or s.get('證券代號') or '').strip()
        if not code:
            continue
        fgn = float(
            s.get('Foreign_Investor_Net_Buy_or_Sell') or
            s.get('外陸資買賣超股數(不含外資自營商)') or
            s.get('外資買賣超') or 0
        ) * 1000  # T86 unit = 千股 → 股
        trust = float(
            s.get('Investment_Trust_Net_Buy_or_Sell') or
            s.get('投信買賣超股數') or
            s.get('投信買賣超') or 0
        ) * 1000
        inst_map[code] = {
            'foreignNet': round(fgn),
            'trustNet':   round(trust),
            'foreignBuy': fgn > 0,
            'trustBuy':   trust > 0,
        }
    log.info(f'  T86 解析: {len(inst_map)} 支')

    # ── Step 4: 大盤代理 (0050) ──
    log.info(f'Step 4: 取得大盤代理 {TAIEX_PROXY}...')
    proxy_prices = []
    if FINMIND_TOKEN:
        proxy_prices = finmind('TaiwanStockPrice', TAIEX_PROXY, date_ago(MONTHS_PRICE))
        proxy_prices.sort(key=lambda x: x['date'], reverse=True)
        log.info(f'  0050 (FinMind): {len(proxy_prices)} 筆')
    else:
        log.info(f'  無 FinMind token，改用 TWSE 歷史...')
        proxy_prices = twse_stock_history(TAIEX_PROXY, MONTHS_PRICE)
        log.info(f'  0050 (TWSE): {len(proxy_prices)} 筆')

    # ── Step 5: 個股深度分析 ──
    log.info('Step 5: 個股指標計算...')
    codes = sorted(stocks_base.keys())
    results = []
    total = len(codes)

    for i, code in enumerate(codes):
        if i % 50 == 0:
            log.info(f'  進度: {i}/{total}...')

        base = stocks_base[code]
        inst = inst_map.get(code, {})

        result = {
            **base,
            # Pre-fill from T86
            'foreignNet':  inst.get('foreignNet', None),
            'trustNet':    inst.get('trustNet',   None),
            'foreignBuy':  inst.get('foreignBuy', None),
            'trustBuy':    inst.get('trustBuy',   None),
            # Defaults for FinMind data
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
            'bigHolderPct':      None,
            'bigHolderIncrease': None,
            'chipConcentration': None,
        }

        if FINMIND_TOKEN:
            # FinMind: 個股歷史股價
            try:
                px = finmind('TaiwanStockPrice', code, date_ago(MONTHS_PRICE))
                px.sort(key=lambda x: x['date'], reverse=True)
                if px:
                    tech = calc_technical(px, proxy_prices)
                    result.update(tech)
                    # 若 TWSE 無收盤價，從 FinMind 補
                    if not result['price'] and px:
                        result['price'] = float(px[0]['close'])
            except Exception as e:
                log.debug(f'  {code} price: {e}')

            # FinMind: 月營收
            try:
                rev_data = finmind('TaiwanStockMonthRevenue', code, date_ago(MONTHS_REVENUE))
                result.update(calc_revenue(rev_data))
            except Exception:
                pass

            # FinMind: 財報
            try:
                fin_data = finmind('TaiwanStockFinancialStatements', code, date_ago(MONTHS_FIN))
                result.update(calc_financials(fin_data))
            except Exception:
                pass

            # FinMind: 法人（季高）
            try:
                inst_hist = finmind('TaiwanStockInstitutionalInvestors', code, date_ago(3))
                inst_calc = calc_institutional(inst_hist)
                # 更新 foreignNet/trustNet（FinMind 更精確），保留 T86 foreignBuy/trustBuy
                result['institutionalRecord'] = inst_calc.get('institutionalRecord')
                if inst_hist:  # use FinMind if available
                    result['foreignNet'] = inst_calc.get('foreignNet', result['foreignNet'])
                    result['trustNet']   = inst_calc.get('trustNet',   result['trustNet'])
                    result['foreignBuy'] = inst_calc.get('foreignBuy', result['foreignBuy'])
                    result['trustBuy']   = inst_calc.get('trustBuy',   result['trustBuy'])
            except Exception:
                pass

            # FinMind: 股東結構
            try:
                sh_data = finmind('TaiwanStockShareholderStructure', code, date_ago(3))
                result.update(calc_shareholder(sh_data))
            except Exception:
                pass

        else:
            # 無 FinMind token: 使用 TWSE STOCK_DAY 計算技術指標
            try:
                px = twse_stock_history(code, MONTHS_PRICE)
                if px:
                    tech = calc_technical(px, proxy_prices)
                    result.update(tech)
            except Exception as e:
                log.debug(f'  {code} twse_history: {e}')

        results.append(result)

    # ── Step 6: 輸出 JSON ──
    log.info(f'Step 6: 輸出 {OUTPUT_PATH}...')
    os.makedirs('data', exist_ok=True)

    output = {
        'generated':  tw_now.isoformat(),
        'dataDate':   data_date,
        'source':     'finmind+twse' if FINMIND_TOKEN else 'twse_only',
        'stockCount': len(results),
        'stocks':     results,
    }
    with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, separators=(',', ':'))

    size_kb = os.path.getsize(OUTPUT_PATH) / 1024
    log.info(f'  完成！{len(results)} 支股票 → {OUTPUT_PATH} ({size_kb:.0f} KB)')

    # ── Step 7: 輸出空白樣本（供初次部署測試）──
    sample = {
        'generated':  tw_now.isoformat(),
        'dataDate':   data_date,
        'source':     'sample',
        'stockCount': 0,
        'stocks':     [],
        '_note':      '此為空白樣本，請執行 GitHub Actions 建置真實資料',
    }
    with open(EMPTY_PATH, 'w', encoding='utf-8') as f:
        json.dump(sample, f, ensure_ascii=False, indent=2)

    log.info('=== 完成 ===')


if __name__ == '__main__':
    main()
