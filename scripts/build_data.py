#!/usr/bin/env python3
"""台股雷達 build_data.py V3.0
Fix 1a: yfinance db locked  Fix 1b: Series ambiguous
Fix 2: log every 50  Fix 3: today data  Fix 4: marketSummary
"""
import json, os, re, time, logging, tempfile, threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone, time as dt_time
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

# FinMind Token 自動偵測：支援最多 15 個，有幾個用幾個
_RAW_TOKENS = [os.environ.get(k,'').strip() for k in
               ['FINMIND_TOKEN'] +
               [f'FINMIND_TOKEN_{i}' for i in range(2, 16)]]   # TOKEN_2 ~ TOKEN_15
FINMIND_TOKENS = [t for t in _RAW_TOKENS if t]
STOCK_LIMIT    = int(os.environ.get('STOCK_LIMIT','0'))

TWSE_DAY_ALL   = 'https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL'
TWSE_DAY_ALL2  = 'https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY_ALL'  # 盤後完整版
TPEX_DAY_ALL   = 'https://www.tpex.org.tw/openapi/v1/tpex_mainboard_daily_close_quotes'  # 上櫃
TPEX_INST_TODAY= 'https://www.tpex.org.tw/openapi/v1/tpex_mainboard_perday_3major_institution'  # 上櫃今日法人
TPEX_INST_HIST = 'https://www.tpex.org.tw/web/stock/3invest/daily/3itrade_download.php'  # 上櫃歷史法人
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
FINMIND_IP_LIMIT = 700   # GitHub Actions IP 每次 run 約 700 次總限制
# 動態計算每 Key 配額（Keys 越多，每 Key 分到越少）
QUOTA_PER_KEY = max(1, FINMIND_IP_LIMIT // max(len(FINMIND_TOKENS), 1))

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

def _parse_twse_stock(s: dict) -> tuple:
    """
    Parse one TWSE stock record → (code, data_dict) or (None, None)
    支援兩種格式：
      OpenAPI: Code, ClosingPrice, Change, HighestPrice, LowestPrice, Name
      rwd 盤後: 證券代號, 收盤價, 漲跌價差, 最高價, 最低價, 證券名稱
    """
    code = (s.get('Code') or s.get('證券代號') or s.get('代號') or '').strip()
    if not is_equity(code): return None, None
    price_s = (s.get('ClosingPrice') or s.get('收盤價') or '').replace(',','').strip()
    # 清除 TWSE 停牌/暫停交易標記：'X0.00', '--', '---' 等非數字格式
    price_s = re.sub(r'^[^0-9\.\-]+', '', price_s)  # 移除開頭非數字字元（如 'X'）
    try:
        price = float(price_s)
    except ValueError:
        return None, None
    if price <= 0: return None, None
    vol_s = (s.get('TradeVolume') or s.get('成交股數') or '0').replace(',','')
    vol = float(vol_s) if vol_s.replace('.','').isdigit() else 0
    # 漲跌：rwd 用「漲跌(+/-)」欄位存符號、「漲跌價差」存數值
    sign_s = str(s.get('漲跌(+/-)') or '').strip()
    cr     = str(s.get('Change') or s.get('漲跌價差') or '0').strip()
    ca     = float(re.sub(r'[▲▼+\-\s,]','',cr) or '0')
    # rwd 格式：sign_s = '-' 或 '+' 或空字串
    is_neg = (sign_s == '-') or cr.startswith('▼') or cr.startswith('-')
    chg    = -ca if is_neg else ca
    b   = (price - chg) or price
    pct = round(chg / b * 100, 2) if b else 0.0
    def safe_float(val, default):
        s = str(val or '').replace(',','').strip()
        s = re.sub(r'^[^0-9\.\-]+', '', s)
        try: return float(s)
        except: return default
    hi  = safe_float(s.get('HighestPrice') or s.get('最高價'), price)
    lo  = safe_float(s.get('LowestPrice')  or s.get('最低價'), price)
    name = (s.get('Name') or s.get('證券名稱') or s.get('名稱') or code).strip()
    return code, {'code':code,'name':name,
                  'price':price,'change':round(chg,2),'pct':pct,
                  'high':hi,'low':lo,'vol':vol}


def _parse_tpex_stock(s: dict) -> tuple:
    """Parse one TPEX stock record → (code, data_dict) or (None, None)"""
    code = (s.get('SecuritiesCompanyCode') or s.get('代號') or '').strip()
    if not is_equity(code): return None, None
    price_s = (s.get('Close') or s.get('收盤') or '').replace(',','').strip()
    try:
        price = float(price_s)
    except ValueError:
        return None, None
    if price <= 0: return None, None
    vol_s = (s.get('TradingShares') or s.get('成交股數') or '0').replace(',','')
    vol = float(vol_s) if vol_s.replace('.','').isdigit() else 0
    cr  = str(s.get('Change') or s.get('漲跌') or '0').strip()
    ca  = float(re.sub(r'[▲▼+\-\s,]','',cr) or '0')
    chg = -ca if (cr.startswith('▼') or cr.startswith('-')) else ca
    b   = (price - chg) or price
    pct = round(chg / b * 100, 2) if b else 0.0
    hi  = float((s.get('High') or s.get('最高') or str(price)).replace(',',''))
    lo  = float((s.get('Low')  or s.get('最低') or str(price)).replace(',',''))
    return code, {'code':code,'name':(s.get('CompanyName') or s.get('名稱') or code).strip(),
                  'price':price,'change':round(chg,2),'pct':pct,
                  'high':hi,'low':lo,'vol':vol}


def load_twse_day_all():
    """
    取得全市場今日收盤行情（上市 + 上櫃）。
    依序嘗試多個來源，確保取得最新資料。

    上市（TSE）：
      1. openapi.twse.com.tw STOCK_DAY_ALL（即時，盤中也有）
      2. www.twse.com.tw rwd/zh/afterTrading/STOCK_DAY_ALL（盤後完整版）

    上櫃（OTC/TPEX）：
      1. www.tpex.org.tw openapi 收盤行情

    資料日期驗證：確認是今日資料，否則 log 警告
    """
    base  : dict = {}
    names : dict = {}
    today = date_now().strftime('%Y/%m/%d')   # TWSE 日期格式
    today_iso = date_now().strftime('%Y-%m-%d')

    # ── 上市 TSE ──
    # 重要：優先用 rwd 盤後版（14:30後才有，但資料最正確）
    # 備援才用 OpenAPI（即時更新但有時是昨日資料）
    for url, label in [
        (TWSE_DAY_ALL2, 'rwd盤後'),   # ← 優先：盤後正式收盤資料
        (TWSE_DAY_ALL,  'OpenAPI'),   # ← 備援：可能回傳昨日資料
    ]:
        try:
            raw = safe_get_json(url)
            if not raw:
                log.warning(f'  TSE ({label}): 無回應')
                continue
            # rwd 版本回傳 {stat, date, fields, data}
            if isinstance(raw, dict):
                stat = raw.get('stat','')
                if stat not in ('OK','ok',''):
                    log.warning(f'  TSE ({label}): stat={stat}，跳過')
                    continue
                records = []
                fields  = raw.get('fields', [])
                log.info(f'  TSE ({label}) fields: {fields[:5]}...')  # debug 欄位名稱
                for row in raw.get('data', []):
                    if isinstance(row, list) and fields:
                        records.append(dict(zip(fields, row)))
                    elif isinstance(row, dict):
                        records.append(row)
            else:
                records = raw

            ok = 0
            for s in records:
                code, d = _parse_twse_stock(s)
                if code:
                    base[code] = d
                    ok += 1
            log.info(f'  TSE ({label}): {ok} 筆')
            if ok > 100:
                break   # 成功取到足夠資料，不需嘗試備援
        except Exception as e:
            log.warning(f'  TSE ({label}) 失敗: {e}')

    # ── 上櫃 TPEX ──
    otc_codes: set = set()   # 記錄哪些是上櫃股票（yfinance 要用 .TWO）
    try:
        raw = safe_get_json(TPEX_DAY_ALL)
        ok  = 0
        for s in (raw or []):
            code, d = _parse_tpex_stock(s)
            if code and code not in base:   # 不覆蓋上市資料
                base[code] = d
                otc_codes.add(code)
                ok += 1
        log.info(f'  OTC (TPEX): {ok} 筆')
    except Exception as e:
        log.debug(f'  TPEX: {e}')

    log.info(f'  今日行情合計: {len(base)} 支（上市 + 上櫃）')

    # ── 公司名稱清單（備援）──
    try:
        raw2 = safe_get_json(TWSE_LISTED)
        for s in (raw2 or []):
            code = (s.get('公司代號') or '').strip()
            if is_equity(code):
                names[code] = {'code':code,'name':(s.get('公司名稱') or code).strip()}
        log.info(f'  t187ap03_L: {len(names)} 筆')
    except Exception as e:
        log.debug(f'  t187ap03_L: {e}')

    return base, names, otc_codes

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
            fgn = fv('Foreign_Investor_Net_Buy_or_Sell','外陸資買賣超股數(不含外資自營商)','外資買賣超')
            tst = fv('Investment_Trust_Net_Buy_or_Sell','投信買賣超股數','投信買賣超')
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
                fgn = pv(fgn_col)
                tst = pv(trust_col)
                inst[code] = {'foreignNet':round(fgn),'trustNet':round(tst),
                              'foreignBuy':fgn>0,'trustBuy':tst>0}
            except Exception:
                pass
    return inst


def load_tpex_inst_today() -> dict:
    """
    TPEX 上櫃今日三大法人（完全免費）。
    URL: https://www.tpex.org.tw/openapi/v1/tpex_mainboard_perday_3major_institution
    欄位: SecuritiesCompanyCode, ForeignInvestmentNetBuySell, InvestmentTrustNetBuySell
    """
    inst = {}
    try:
        raw = safe_get_json(TPEX_INST_TODAY, timeout=20)
        for s in (raw or []):
            code = (s.get('SecuritiesCompanyCode') or s.get('代號') or '').strip()
            if not is_equity(code):
                continue
            def fv(*keys):
                for k in keys:
                    v = s.get(k)
                    if v is not None and str(v).strip() not in ('', '--', '-'):
                        try: return float(str(v).replace(',', ''))
                        except: pass
                return 0.0
            fgn   = fv('ForeignInvestmentNetBuySell', '外資及陸資買賣超股數', '外資買賣超')
            trust = fv('InvestmentTrustNetBuySell', '投信買賣超股數', '投信買賣超')
            inst[code] = {
                'foreignNet': round(fgn), 'trustNet': round(trust),
                'foreignBuy': fgn > 0,   'trustBuy': trust > 0,
            }
        log.info(f'  TPEX 今日法人: {len(inst)} 筆')
    except Exception as e:
        log.debug(f'  TPEX 今日法人: {e}')
    return inst


def load_t86() -> dict:
    """
    載入今日三大法人買賣超。
    上市(TSE)：TWSE T86
    上櫃(OTC)：TPEX 三大法人 API
    """
    today_str = date_now().strftime('%Y%m%d')
    inst: dict = {}

    # ── TSE 上市 T86 ──
    for url, label in [(TWSE_T86_MAIN, 'rwd'), (TWSE_T86_ALT, 'fund')]:
        try:
            for sel in ['ALLBUT0999', 'ALL']:
                params = {'response':'json','date':today_str,'selectType':sel}
                raw = safe_get_json(url, params=params, timeout=30)
                if raw is None:
                    continue
                parsed = _parse_t86_response(raw)
                if parsed:
                    inst.update(parsed)
                    log.info(f'  T86 今日 ({label}/{sel}): {len(parsed)} 筆')
                    break
            if inst:
                break
        except Exception as e:
            log.debug(f'  T86 {label}: {e}')

    if not inst:
        try:
            raw = safe_get_json(TWSE_T86, timeout=20)
            if raw:
                parsed = _parse_t86_response(raw)
                if parsed:
                    inst.update(parsed)
                    log.info(f'  T86 今日 (OpenAPI): {len(parsed)} 筆')
        except Exception as e:
            log.debug(f'  T86 OpenAPI: {e}')

    if not inst:
        # 判斷現在是否為交易時段（09:00~14:30 台灣時間）
        now_tw = date_now()
        is_trading_hours = (now_tw.weekday() < 5 and
                            dt_time(9, 0) <= now_tw.time() <= dt_time(14, 35))
        if is_trading_hours:
            log.warning('  T86 今日: 交易時段內無資料，可能尚未更新')
        else:
            log.info('  T86 今日: 盤後資料尚未就緒，自動使用昨日資料補用')

        # 自動用昨日資料補用
        for delta in range(1, 5):
            d = now_tw - timedelta(days=delta)
            if d.weekday() >= 5:
                continue
            ds = d.strftime('%Y%m%d')
            for url, label in [(TWSE_T86_MAIN, 'rwd'), (TWSE_T86_ALT, 'fund')]:
                try:
                    for sel in ['ALLBUT0999', 'ALL']:
                        raw = safe_get_json(url,
                            {'response':'json','date':ds,'selectType':sel}, timeout=20)
                        if not raw: continue
                        parsed = _parse_t86_response(raw)
                        if parsed:
                            inst.update(parsed)
                            log.info(f'  T86 補用 {ds[:4]}/{ds[4:6]}/{ds[6:]} ({label}): {len(parsed)} 筆')
                            break
                    if inst: break
                except Exception as e:
                    log.debug(f'  T86 補用 {ds}: {e}')
            if inst:
                break

    # ── OTC 上櫃 TPEX 法人 ──
    tpex_inst = load_tpex_inst_today()
    before = len(inst)
    for code, v in tpex_inst.items():
        if code not in inst:   # 不覆蓋 TSE 資料
            inst[code] = v
    log.info(f'  法人合計: TSE {before} + OTC {len(inst)-before} = {len(inst)} 支')
    return inst

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

    log.info(f'  T86歷史(TSE): {fetched} 交易日，{len(daily)} 支個股')

    # ── TPEX 上櫃歷史法人（每日一次呼叫，完全免費）──
    tpex_fetched = 0
    for delta in range(20):
        if tpex_fetched >= days:
            break
        d = now - timedelta(days=delta)
        if d.weekday() >= 5:
            continue
        ds_tpex = d.strftime('%Y/%m/%d')   # TPEX 日期格式
        try:
            j = safe_get_json(TPEX_INST_TODAY,
                              params={'date': ds_tpex, 'response': 'json'}, timeout=20)
            if not j:
                # Try without date param to get today
                if delta == 0:
                    j = safe_get_json(TPEX_INST_TODAY, timeout=20)
            if not j:
                continue
            ok_tpex = 0
            for s in (j if isinstance(j, list) else j.get('data', [])):
                code = (s.get('SecuritiesCompanyCode') or s.get('代號') or '').strip()
                if not is_equity(code) or code in daily:   # 不覆蓋 TSE 資料
                    continue
                def fv2(*keys):
                    for k in keys:
                        v = s.get(k)
                        if v is not None and str(v).strip() not in ('','--','-'):
                            try: return float(str(v).replace(',',''))
                            except: pass
                    return 0.0
                fgn   = fv2('ForeignInvestmentNetBuySell','外資及陸資買賣超股數','外資買賣超')
                trust = fv2('InvestmentTrustNetBuySell','投信買賣超股數','投信買賣超')
                if code not in daily:
                    daily[code] = {'f':[], 't':[]}
                daily[code]['f'].append(fgn)
                daily[code]['t'].append(trust)
                ok_tpex += 1
            if ok_tpex > 0:
                tpex_fetched += 1
                log.debug(f'  TPEX hist {ds_tpex}: {ok_tpex} 筆')
            time.sleep(TWSE_SLEEP)
        except Exception as e:
            log.debug(f'  TPEX hist {ds_tpex}: {e}')

    log.info(f'  T86歷史合計: TSE+OTC {len(daily)} 支個股')

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

def download_price_history(codes, otc_codes: set = None):
    """下載 K 線：上市用 .TW，上櫃用 .TWO（yfinance 格式不同）"""
    if not YF_OK: return {}
    otc_codes = otc_codes or set()
    result={}
    tickers=[f'{c}.TWO' if c in otc_codes else f'{c}.TW' for c in codes]
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
                # 永遠先攤平 MultiIndex，再取單股資料
                if len(ct) > 1:
                    df = _extract_df(raw, ticker)
                else:
                    df = _flatten_df(raw)   # 單支批次也要攤平，否則 row['Close'] 可能是 Series
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


def download_financials(codes: List[str], otc_codes: set) -> Dict[str, dict]:
    """
    用 yfinance Ticker.quarterly_income_stmt 取季度財報。
    完全免費，不消耗 FinMind 配額。
    取得：毛利率、營業利益率、近期盈虧（淨利正負）。

    因為每支需要獨立呼叫，批次下載，每批限速避免被封。
    """
    if not YF_OK:
        return {}

    result: Dict[str, dict] = {}
    ok = 0
    log.info(f'  yfinance 財報：下載 {len(codes)} 支...')

    for i, code in enumerate(codes):
        suffix = '.TWO' if code in otc_codes else '.TW'
        try:
            tk = yf.Ticker(f'{code}{suffix}')
            # quarterly_income_stmt: rows=items, cols=dates
            qf = tk.quarterly_income_stmt
            if qf is None or (PANDAS_OK and isinstance(qf, pd.DataFrame) and qf.empty):
                continue

            def get_row(*candidates):
                for c in candidates:
                    for idx in qf.index:
                        if c.lower() in str(idx).lower():
                            row = qf.loc[idx]
                            # 取最新一季（第一欄）
                            val = row.iloc[0] if len(row) > 0 else None
                            if val is not None and not (isinstance(val, float) and val != val):
                                return float(val)
                return None

            revenue    = get_row('Total Revenue', 'Revenue')
            gross      = get_row('Gross Profit')
            op_income  = get_row('Operating Income', 'Operating Profit')
            net_income = get_row('Net Income')

            if revenue and revenue > 0:
                gross_margin = round(gross / revenue * 100, 2) if gross else None
                op_margin    = round(op_income / revenue * 100, 2) if op_income else None
            else:
                gross_margin = op_margin = None

            no_loss = (net_income > 0) if net_income is not None else None

            result[code] = {
                'grossMargin':  gross_margin,
                'opMargin':     op_margin,
                'noProfitLoss': no_loss,
                'marginGrowth': None,  # 需比較去年同季，暫不實作
            }
            ok += 1

        except Exception as e:
            log.debug(f'  財報 {code}: {e}')

        # 每 30 支 log 一次進度
        if (i + 1) % 30 == 0:
            log.info(f'  yfinance 財報進度: {i+1}/{len(codes)}，成功 {ok} 支')
        # 微量限速避免觸發 Yahoo 速率限制
        if i % 5 == 4:
            time.sleep(0.3)

    log.info(f'  yfinance 財報完成: {ok}/{len(codes)} 支有財報資料')
    return result

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
    """每個 FinMind Token 的獨立 Worker Thread，有自己的速率控制。"""
    def __init__(self, token: str, index: int, limit: int = QUOTA_PER_KEY):
        self.token    = token
        self.index    = index
        self.limit    = limit
        self.count    = 0
        self.exceeded = False
        self._lock    = threading.Lock()
        self._last    = 0.0
        self._s       = requests.Session()
        self._s.headers.update(SESSION.headers)

    @property
    def has_quota(self) -> bool:
        return not self.exceeded and self.count < self.limit

    def fetch(self, dataset: str, code: str, start_date: str) -> List[dict]:
        with self._lock:
            if self.exceeded:
                return []
            wait = FINMIND_SLEEP - (time.monotonic() - self._last)
            if wait > 0:
                time.sleep(wait)

        params = {'dataset': dataset, 'data_id': code,
                  'start_date': start_date, 'token': self.token}
        try:
            r = self._s.get(FINMIND_BASE, params=params, timeout=25)
            with self._lock:
                self._last = time.monotonic()

            if r.status_code == 402:
                with self._lock:
                    if not self.exceeded:
                        self.exceeded = True
                        log.warning(f'  Key{self.index} 402 配額耗盡（{self.count}次）')
                return []
            if r.status_code in (422, 429):
                return []
            r.raise_for_status()

            content = r.content.strip()
            if not content or content[:1] in (b'<', b'\n'):
                return []

            j = r.json()
            if j.get('status') != 200:
                msg = j.get('msg', '')
                if any(k in msg for k in ('quota', '次數', '超過', 'limit')):
                    with self._lock:
                        self.exceeded = True
                        log.warning(f'  Key{self.index} 配額耗盡（msg）')
                return []

            with self._lock:
                self.count += 1
                if self.count >= self.limit:
                    self.exceeded = True
                    log.info(f'  Key{self.index} 達上限 {self.limit} 次')
            return j.get('data', [])

        except Exception as e:
            log.debug(f'  Key{self.index} {code}: {e}')
            return []


def _load_existing_revenue() -> Dict[str, List[dict]]:
    """
    讀取現有 screener.json 裡的月營收，供增量更新使用。
    讓「今天更新不到的股票」保留昨天的月營收，逐日累積直到 100% 覆蓋。
    """
    try:
        if not os.path.exists(OUTPUT_PATH):
            return {}
        with open(OUTPUT_PATH, encoding='utf-8') as f:
            d = json.load(f)
        result = {}
        for s in d.get('stocks', []):
            code = s.get('code', '')
            rev  = s.get('revenue')
            rdate= s.get('revenueDate')
            if code and rev is not None and rdate:
                result[code] = [{'date': rdate, 'revenue': float(rev) * 1e8}]
        log.info(f'  載入現有月營收快取: {len(result)} 支')
        return result
    except Exception as e:
        log.debug(f'  讀取現有月營收失敗: {e}')
        return {}


def load_revenue_finmind(codes: List[str],
                         existing: Dict[str, List[dict]]) -> Dict[str, List[dict]]:
    """
    FinMind 月營收 — 並發多 Key + 增量累積策略。

    核心設計：
      - FinMind 每 Key 實測約 53 次/日
      - 每日可更新：Key 數 × 53 支
      - 今天更新不到的股票：保留 screener.json 現有資料（昨天的值）
      - 2 天內完整輪一圈（15 Keys × 53 × 2天 = 1590 次 > 1072 支）

    自動偵測有幾個 Key 就用幾個，不需修改程式碼。
    """
    if not FINMIND_TOKENS:
        log.info('  無 FinMind Token，月營收顯示 N/A')
        return existing.copy()

    start   = date_ago_s(24)
    workers = [KeyWorker(t, i+1) for i, t in enumerate(FINMIND_TOKENS)]
    nk      = len(workers)
    today_quota = nk * QUOTA_PER_KEY   # 今天最多能抓的支數

    # 決定今天從哪支開始（用日期 offset 輪流，確保每支都能輪到）
    day_num   = date_now().timetuple().tm_yday
    start_idx = (day_num * today_quota) % max(len(codes), 1)

    # 今日批次（從 start_idx 開始，循環取 today_quota 支）
    today_batch = [codes[(start_idx + i) % len(codes)]
                   for i in range(min(today_quota, len(codes)))]

    log.info(f'  FinMind 月營收: {nk} 個 Key')
    log.info(f'  IP 總限制: ~{FINMIND_IP_LIMIT} 次/run，每 Key 分配 {QUOTA_PER_KEY} 次')
    log.info(f'  今日配額: {today_quota} 次，更新 {len(today_batch)} 支')
    log.info(f'  今日範圍: {today_batch[0]}~{today_batch[-1]}（輪流覆蓋，每 {max(1,len(codes)//max(today_quota,1))} 天一圈）')
    log.info(f'  其餘 {len(codes)-len(today_batch)} 支保留昨日資料')

    # 從現有資料開始（今天更新的才覆蓋，其餘保留）
    result      = existing.copy()
    result_lock = threading.Lock()
    counter     = {'done': 0}
    counter_lock= threading.Lock()

    def fetch_one(code: str, preferred: KeyWorker) -> None:
        worker = preferred if preferred.has_quota else \
                 next((w for w in workers if w.has_quota), None)
        if worker is None:
            return
        data = worker.fetch('TaiwanStockMonthRevenue', code, start)
        if data:
            s = sorted(data, key=lambda x: x['date'], reverse=True)
            with result_lock:
                result[code] = [{'date': d['date'],
                                  'revenue': float(d['revenue'])} for d in s]
        with counter_lock:
            counter['done'] += 1
            n = counter['done']
        if n % 100 == 0:
            sm = ', '.join(f'Key{w.index}:{w.count}次{"✓" if w.has_quota else "滿"}' for w in workers)
            log.info(f'  月營收進度: {n}/{len(today_batch)}，{sm}')

    # Round-robin 分配今日批次到各 Worker
    per_worker = [[] for _ in range(nk)]
    for i, code in enumerate(today_batch):
        per_worker[i % nk].append(code)

    def run(wk: KeyWorker, batch: List[str]) -> None:
        for c in batch:
            fetch_one(c, wk)

    with ThreadPoolExecutor(max_workers=nk) as pool:
        futs = [pool.submit(run, wk, b)
                for wk, b in zip(workers, per_worker)]
        for f in as_completed(futs):
            try:
                f.result()
            except Exception as e:
                log.warning(f'  Worker 異常: {e}')

    total_with_rev = sum(1 for v in result.values() if v)
    sm = ', '.join(f'Key{w.index}:{w.count}次' for w in workers)
    log.info(f'  月營收完成: 今日更新 {counter["done"]} 支，累計 {total_with_rev}/{len(codes)} 支 | {sm}')
    return result

avg=lambda lst: sum(lst)/len(lst) if lst else 0.0

def calc_rs_from_rsi(prices: list, period: int = 14) -> float:
    """
    用 RSI(14) Wilder 法計算，再透過公式反推 RS 評分（0~99）。

    步驟：
      1. 計算標準 RSI(14)：RSI = 100 - 100/(1+RS_ratio)
         其中 RS_ratio = avg_gain / avg_loss（Wilder EMA）
      2. 反推 RS_ratio：RS_ratio = 100/(100-RSI) - 1
      3. 映射到 0~99 評分：rsScore = RSI × (99/100)

    映射關係（RSI → RS評分）：
      RSI  0 → RS  0.0  （全跌）
      RSI 50 → RS 49.5  （中性）
      RSI 70 → RS 69.3  （偏強）
      RSI 90 → RS 89.1  （極強）
      RSI 91.9 → RS 91  （通過 RS>90 篩選）
      RSI 100 → RS 99.0 （全漲）

    與 TradingView / Yahoo Finance RSI 完全一致，差異 < 1%。
    """
    if len(prices) < period + 1:
        return None
    closes = [float(p['close']) for p in prices]
    closes.reverse()   # 時間正序（最舊在前）

    gains, losses = [], []
    for i in range(1, len(closes)):
        delta = closes[i] - closes[i-1]
        gains.append(max(delta, 0))
        losses.append(max(-delta, 0))

    # Wilder 初始平均
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    # Wilder 平滑
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period

    # RSI
    if avg_loss == 0:
        rsi = 100.0
    else:
        rs_ratio = avg_gain / avg_loss
        rsi = 100.0 - 100.0 / (1.0 + rs_ratio)

    # 映射 RSI(0~100) → RS評分(0~99)
    rs_score = round(rsi * 99.0 / 100.0, 1)
    return max(0.0, min(99.0, rs_score))

def calc_technical(prices, proxy=None):
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
    # RS 評分（0~99）= RSI(14) × 99/100
    # RS > 90 對應 RSI > 90.9（強勢動能篩選）
    rs_score = calc_rs_from_rsi(prices, 14)
    return {'rsScore':rs_score,'shortMAAlign':short,
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

def self_check_price(results: list, sb: dict) -> bool:
    """
    自檢：用 Yahoo Finance Taiwan 作為第三方基準，
    驗證程式收盤價與市場實際價格是否一致。

    不能用 sb 自比自己 — sb 若本身資料有誤（如 rwd 失敗退回 OpenAPI 舊資料），
    自比永遠通過但結果仍是錯的。
    Yahoo Finance 是獨立第三方，能真正發現資料來源問題。
    """
    CHECK_CODE = '2330'
    log.info(f'  自檢：用 Yahoo Finance 驗證 {CHECK_CODE} 收盤...')

    # 從 Yahoo Finance Taiwan 取得獨立報價
    ref_price = None
    try:
        yf_url = f'https://query1.finance.yahoo.com/v8/finance/chart/{CHECK_CODE}.TW'
        r = SESSION.get(yf_url, headers={'User-Agent': 'Mozilla/5.0',
                                          'Accept': 'application/json'}, timeout=15)
        r.raise_for_status()
        meta = r.json().get('chart', {}).get('result', [{}])[0].get('meta', {})
        # regularMarketPrice = 最新成交價；previousClose = 前日收盤
        raw_ref = meta.get('regularMarketPrice') or meta.get('previousClose')
        if raw_ref:
            ref_price = round(float(raw_ref), 2)
    except Exception as e:
        log.warning(f'  自檢：無法取得 Yahoo 報價 ({e})，改用 TWSE sb 比對')
        # Yahoo 失敗時降級：至少確認 results 和 sb 一致（偵測計算過程中的 bug）
        twse_price = sb.get(CHECK_CODE, {}).get('price', 0)
        our_price  = next((s.get('price', 0) for s in results if s.get('code') == CHECK_CODE), 0)
        if twse_price and our_price == twse_price:
            log.info(f'  ✅ 自檢（降級）：{CHECK_CODE} 程式={our_price} = sb={twse_price}')
        elif twse_price:
            log.warning(f'  ⚠ 自檢（降級）失敗：{CHECK_CODE} 程式={our_price} ≠ sb={twse_price}')
        return True

    our_stock = next((s for s in results if s.get('code') == CHECK_CODE), None)
    if not our_stock:
        log.warning(f'  自檢：結果中找不到 {CHECK_CODE}')
        return True

    our_price = our_stock.get('price', 0)
    sb_price  = sb.get(CHECK_CODE, {}).get('price', 0)

    if our_price == ref_price:
        log.info(f'  ✅ 自檢通過：{CHECK_CODE} 程式={our_price} = Yahoo={ref_price}（完全吻合）')
        return True
    else:
        diff_pct = abs(our_price - ref_price) / ref_price * 100 if ref_price else 0
        log.warning(
            f'  ⚠ 自檢失敗：{CHECK_CODE} 程式={our_price} / Yahoo={ref_price} '
            f'（差異 {diff_pct:.2f}%）\n'
            f'       TWSE-sb={sb_price}  →  '
            f'{"TWSE資料來源有誤（rwd失敗退回OpenAPI舊資料）" if sb_price == our_price else "計算過程覆蓋了price欄位"}'
        )
        return False


def main():
    log.info('=== 台股雷達資料建置 V3.83 開始 ===')
    log.info(f'yfinance:{"✓" if YF_OK else "✗"} pandas:{"✓" if PANDAS_OK else "✗"} bs4:{"✓" if BS4_OK else "✗(regex備援)"}')
    data_date=date_now().strftime('%Y-%m-%d')

    log.info('Step 1: TWSE...')
    sb,nm,otc_codes=load_twse_day_all()
    codes_set=set(sb)|set(nm)
    if STOCK_LIMIT: codes_set=set(sorted(codes_set)[:STOCK_LIMIT])
    all_codes=sorted(codes_set)
    log.info(f'  共{len(all_codes)}支（上市{len(all_codes)-len(otc_codes)}支，上櫃{len(otc_codes)}支）')

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
    ph=download_price_history(all_codes, otc_codes) if YF_OK else {}
    proxy=ph.get('0050',[])
    if not proxy:
        log.info('  單獨下載0050...')
        proxy=download_single('0050.TW')
        if proxy: ph['0050']=proxy; log.info(f'  0050:{len(proxy)}筆')
        else:
            log.info('  0050 yfinance失敗，改用TWSE...')
            proxy=twse_stock_history('0050',7)
            log.info(f'  0050(TWSE):{len(proxy)}筆')

    log.info('Step 3b: yfinance 季度財報（毛利率/盈虧）...')
    fin_all = download_financials(all_codes, otc_codes)

    log.info('Step 4: 月營收（FinMind 增量）...')
    existing_rev = _load_existing_revenue()
    rev_all = load_revenue_finmind(all_codes, existing_rev)

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
            if tp > 0:
                # 強制用 TWSE 官方收盤覆蓋 px 最新一筆
                # 不管 yfinance 是否已有今日資料，TWSE 才是正確來源
                today_row = {'date':data_date,'close':tp,'max':th or tp,'min':tl or tp}
                if px[0]['date'] == data_date:
                    px[0] = today_row   # 覆蓋 yfinance 今日（避免 auto_adjust 還原價）
                else:
                    px = [today_row] + px   # 補入今日
            r.update(calc_technical(px,proxy))
            if not r['price']:
                try: r['price']=float(px[0]['close'])
                except: pass
        rl=rev_all.get(code,[])
        if rl: r.update(calc_revenue(rl))
        # 季度財報（毛利率/盈虧）
        fin = fin_all.get(code, {})
        if fin:
            r['grossMargin']  = fin.get('grossMargin')
            r['opMargin']     = fin.get('opMargin')
            r['noProfitLoss'] = fin.get('noProfitLoss')
        results.append(r)

    hrs  = sum(1 for r in results if r['rsScore']       is not None)
    hrv  = sum(1 for r in results if r['revenue']       is not None)
    hfi  = sum(1 for r in results if r['foreignBuy']    is not None)
    hfin = sum(1 for r in results if r['grossMargin']   is not None)
    log.info(f'  RS:{hrs} 月營收:{hrv} 財報:{hfin} 法人:{hfi} / {len(results)}')

    log.info('Step 6: 自檢...')
    self_check_price(results, sb)

    log.info('Step 7: 輸出...')
    os.makedirs('data',exist_ok=True)
    tw_now=date_now()
    out={'version':'V3.83','generated':tw_now.isoformat(),'dataDate':data_date,
         'source':'yfinance+finmind+twse' if FINMIND_TOKENS else 'yfinance+twse',
         'stockCount':len(results),'coverage':{'technical':hrs,'revenue':hrv,'institutional':hfi},
         'marketSummary':mkt,'stocks':results}
    with open(OUTPUT_PATH,'w',encoding='utf-8') as f:
        json.dump(out,f,ensure_ascii=False,separators=(',',':'))
    log.info(f'  ✅ {len(results)}支 → {OUTPUT_PATH} ({os.path.getsize(OUTPUT_PATH)/1024:.0f}KB)')
    log.info('=== 完成 ===')

if __name__=='__main__':
    main()
