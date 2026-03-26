#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ==========================================================
# multi_pairs_bot.py — TL Breaks + Multi-Filter Bot (v3.1)
# XAUUSD + BTCUSD + EURUSD + GBPUSD + US100 + US500
# Capital.com API
# ✅ v3.0: منطق معكوس (شراء من قاع، بيع من قمة)
# ✅ v3.1: Partial TP متدرج + Progressive SL (لا انحداف للربح)
# ==========================================================

import os, csv, json, time, sqlite3, requests
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timezone
from threading import Lock
from dotenv import load_dotenv

load_dotenv()

# ═══════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════
# ═══════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════
API_KEY    = os.getenv('CAPITAL_API_KEY',  'BbmFhEF3FffkcR0Y')
EMAIL      = os.getenv('CAPITAL_EMAIL',    'almorese2013@gmail.com')
PASSWORD   = os.getenv('CAPITAL_PASSWORD', 'Ba050326>')
TG_TOKEN   = os.getenv('TG_TOKEN',         '8782238258:AAEtuQg7OYAmoemhWfLqKdYpqIxfWwyKRSQ')
TG_CHAT_ID = os.getenv('TG_CHAT_ID',       '533243705')


if not all([API_KEY, EMAIL, PASSWORD]):
    raise ValueError("CAPITAL_API_KEY, CAPITAL_EMAIL, CAPITAL_PASSWORD must be set!")

BASE_URL  = 'https://api-capital.backend-capital.com'
DEMO_MODE = os.getenv('DEMO_MODE', 'false').lower() == 'true'

PAIRS = {
    'GOLD': {'epic': 'GOLD', 'allow_buy': True, 'allow_sell': True, 'size_override': None},
    'BTCUSD': {'epic': 'BTCUSD', 'allow_buy': True, 'allow_sell': True, 'size_override': None},
    'EURUSD': {'epic': 'EURUSD', 'allow_buy': True, 'allow_sell': True, 'size_override': None},
    'GBPUSD': {'epic': 'GBPUSD', 'allow_buy': True, 'allow_sell': True, 'size_override': None},
    'US100': {'epic': 'US100', 'allow_buy': True, 'allow_sell': True, 'size_override': None},
    'US500': {'epic': 'US500', 'allow_buy': True, 'allow_sell': True, 'size_override': None},
}

STRATEGY_TF   = os.getenv('STRATEGY_TF', 'MINUTE_15')
CANDLES_COUNT = 500
SCAN_INTERVAL = int(os.getenv('SCAN_INTERVAL', '300'))

LENGTH       = int(os.getenv('LENGTH', '10'))
SLOPE_MULT   = float(os.getenv('SLOPE_MULT', '1.0'))
SLOPE_METHOD = os.getenv('SLOPE_METHOD', 'ATR')
ATR_PERIOD   = 14
SL_ATR_MULT  = 1.5
TP_ATR_MULT  = 3.0

# ═══════════════════════════════════════════════════════
# ✅ v3.1: إعدادات Partial TP المتدرج + Progressive SL
# ═══════════════════════════════════════════════════════
STAGE1_TP_R     = 1.5   # أول Partial TP
STAGE1_PCT      = 0.50  # 50% من الحجم

STAGE2_TP_R     = 2.5   # ثاني Partial TP  
STAGE2_PCT      = 0.30  # 30% من الباقي (15% إجمالي)

FINAL_TP_R      = 3.5   # ثالث Partial TP
FINAL_PCT       = 0.50  # 50% من المتبقي (17.5% إجمالي)

# الباقي ~17.5% يجري مع Trailing حتى النهاية

# Progressive SL: تأمين ربح تدريجي للباقي
PROGRESSIVE_LOCK = {
    2.0: 0.5,   # عند 2.0R: 0.5R مضمون
    2.5: 1.0,   # عند 2.5R: 1.0R مضمون (بعد Stage 2)
    3.0: 1.5,   # عند 3.0R: 1.5R مضمون
    3.5: 2.0,   # عند 3.5R: 2.0R مضمون
    4.5: 3.0,   # عند 4.5R: 3.0R مضمون
    6.0: 4.0,   # عند 6.0R: 4.0R مضمون
}

TRAILING_ATR_MULT = 2.0  # للباقي النهائي

RSI_SELL_MIN, RSI_SELL_MAX = 55, 78   # ذروة شراء = بيع
RSI_BUY_MIN, RSI_BUY_MAX   = 22, 45   # ذروة بيع = شراء

SPREAD_ATR_MAX = 0.25
SESSION_START, SESSION_END = 0, 23

RISK_PERCENT         = float(os.getenv('RISK_PERCENT', '0.01'))
MAX_OPEN_TRADES      = int(os.getenv('MAX_OPEN_TRADES', '6'))
MAX_CONSECUTIVE_LOSS = int(os.getenv('MAX_CONSEC_LOSS', '3'))
ACCOUNT_BALANCE      = float(os.getenv('ACCOUNT_BALANCE', '1000'))

_BASE_DIR  = os.getenv('DATA_DIR', '/tmp')
DB_FILE    = os.path.join(_BASE_DIR, 'multi_bot.db')
TRADES_CSV = os.path.join(_BASE_DIR, 'trades_log.csv')

db_lock, session_headers, _meta_cache = Lock(), {}, {}

CSV_HEADERS = [
    'date', 'time_utc', 'pair', 'direction', 'entry', 'sl', 'tp', 'exit_price',
    'atr', 'size', 'sl_dist', 'pnl_usd', 'pnl_r', 'result', 'bars_held', 'spread', 'tf',
    'stage1_done', 'stage2_done', 'stage3_done', 'final_locked_r',
    'supertrend', 'rsi', 'ema_fast', 'ema_slow',
]


# ═══════════════════════════════════════════════════════
# UTILS & DATABASE
# ═══════════════════════════════════════════════════════
def utc_now(): return datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
def log(msg): print(f'[{utc_now()}] {msg}', flush=True)

def db_init():
    with sqlite3.connect(DB_FILE) as conn:
        conn.execute('''CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY, key TEXT UNIQUE, pair TEXT, direction TEXT,
            timestamp TEXT, entry REAL, sl REAL, tp REAL, atr REAL, size REAL,
            spread REAL DEFAULT 0, status TEXT DEFAULT 'PENDING',
            stage1_done INTEGER DEFAULT 0, stage2_done INTEGER DEFAULT 0, 
            stage3_done INTEGER DEFAULT 0, final_locked_r REAL DEFAULT 0
        )''')
        conn.execute('''CREATE TABLE IF NOT EXISTS open_positions (
            deal_id TEXT PRIMARY KEY, pair TEXT, direction TEXT, entry REAL,
            sl REAL, tp REAL, atr REAL, size REAL, db_key TEXT, opened_at TEXT,
            stage1_done INTEGER DEFAULT 0, stage2_done INTEGER DEFAULT 0,
            stage3_done INTEGER DEFAULT 0, final_locked_r REAL DEFAULT 0
        )''')
        conn.commit()

def db_save(key, pair, direction, entry, sl, tp, atr, size, spread=0.0):
    with db_lock:
        with sqlite3.connect(DB_FILE) as conn:
            try:
                conn.execute(
                    'INSERT INTO trades (key,pair,direction,timestamp,entry,sl,tp,atr,size,spread) VALUES (?,?,?,?,?,?,?,?,?,?)',
                    (key, pair, direction, utc_now(), entry, sl, tp, atr, size, spread)
                )
                conn.commit()
            except sqlite3.IntegrityError: pass

def db_update(key, status): 
    with db_lock:
        with sqlite3.connect(DB_FILE) as conn:
            conn.execute('UPDATE trades SET status=? WHERE key=?', (status, key))
            conn.commit()

def db_is_dup(key):
    with db_lock:
        with sqlite3.connect(DB_FILE) as conn:
            return conn.execute('SELECT id FROM trades WHERE key=?', (key,)).fetchone() is not None

def op_save(deal_id, pair, direction, entry, sl, tp, atr, size, db_key):
    with db_lock:
        with sqlite3.connect(DB_FILE) as conn:
            try:
                conn.execute(
                    'INSERT OR IGNORE INTO open_positions (deal_id,pair,direction,entry,sl,tp,atr,size,db_key,opened_at) VALUES (?,?,?,?,?,?,?,?,?,?)',
                    (deal_id, pair, direction, entry, sl, tp, atr, size, db_key, utc_now())
                )
                conn.commit()
            except Exception as ex: log(f'op_save ERROR: {ex}')

def op_get_all():
    with db_lock:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            return [dict(r) for r in conn.execute('SELECT * FROM open_positions').fetchall()]

def op_update(deal_id, **kwargs):
    with db_lock:
        with sqlite3.connect(DB_FILE) as conn:
            for col, val in kwargs.items():
                conn.execute(f'UPDATE open_positions SET {col}=? WHERE deal_id=?', (val, deal_id))
            conn.commit()

def op_delete(deal_id):
    with db_lock:
        with sqlite3.connect(DB_FILE) as conn:
            conn.execute('DELETE FROM open_positions WHERE deal_id=?', (deal_id,))
            conn.commit()


# ═══════════════════════════════════════════════════════
# CSV & API HELPERS
# ═══════════════════════════════════════════════════════
def csv_init():
    if not Path(TRADES_CSV).exists():
        with open(TRADES_CSV, 'w', newline='', encoding='utf-8-sig') as f:
            csv.DictWriter(f, fieldnames=CSV_HEADERS).writeheader()

def csv_log_trade(pos, exit_price, stage1=0, stage2=0, stage3=0, final_r=0):
    try:
        entry, sl, size, dir_, pair = pos['entry'], pos['sl'], pos['size'], pos['direction'], pos['pair']
        sl_dist = abs(entry - sl)
        pnl_pts = (exit_price - entry) if dir_ == 'BUY' else (entry - exit_price)
        pnl_r = round(pnl_pts / sl_dist, 2) if sl_dist > 0 else 0
        result = 'WIN' if pnl_pts > 0 else ('LOSS' if pnl_pts < 0 else 'BE')
        
        meta = _meta_cache.get(pair, {}).get('data')
        pnl_usd = round(pnl_pts * size * (meta[3] if meta else 1), 2)
        
        now = datetime.now(timezone.utc)
        row = {
            'date': now.strftime('%Y-%m-%d'), 'time_utc': now.strftime('%H:%M'),
            'pair': pair, 'direction': dir_, 'entry': entry, 'sl': sl, 'tp': pos['tp'],
            'exit_price': exit_price, 'atr': pos['atr'], 'size': size,
            'sl_dist': round(sl_dist, 5), 'pnl_usd': pnl_usd, 'pnl_r': pnl_r,
            'result': result, 'bars_held': 0, 'spread': pos.get('spread', 0),
            'tf': STRATEGY_TF, 'stage1_done': stage1, 'stage2_done': stage2,
            'stage3_done': stage3, 'final_locked_r': final_r,
            'supertrend': 0, 'rsi': 0, 'ema_fast': 0, 'ema_slow': 0,
        }
        with open(TRADES_CSV, 'a', newline='', encoding='utf-8-sig') as f:
            csv.DictWriter(f, fieldnames=CSV_HEADERS).writerow(row)

        icon = '✅' if result == 'WIN' else ('❌' if result == 'LOSS' else '🔵')
        stages = f' | S1:{stage1} S2:{stage2} S3:{stage3} Locked:{final_r}R' if any([stage1,stage2,stage3]) else ''
        log(f'  {icon} CLOSED {pair} {dir_} | PnL=${pnl_usd:+.2f} ({pnl_r:+.2f}R){stages}')
        
        nl = '\n'
        tg(f'{icon} *{pair} {dir_} — {result}*{stages}{nl}Entry: `{entry}` → Exit: `{exit_price}`{nl}PnL: `${pnl_usd:+.2f}` | `{pnl_r:+.2f}R`{nl}_{utc_now()}_')
        return result, pnl_usd
    except Exception as ex:
        log(f'  csv_log_trade ERROR: {ex}')
        return 'ERROR', 0

def _get(path, params=None, retries=3):
    for attempt in range(retries):
        try:
            r = requests.get(BASE_URL + path, headers=session_headers, params=params, timeout=15)
            if r.status_code == 429: time.sleep(5 * (attempt + 1)); continue
            return r
        except requests.exceptions.RequestException as ex:
            log(f'  GET {path} [{attempt+1}]: {ex}')
            time.sleep(3 * (attempt + 1))
    return None

def _post(path, body, retries=2):
    for attempt in range(retries):
        try:
            return requests.post(BASE_URL + path, headers=session_headers, json=body, timeout=15)
        except requests.exceptions.RequestException as ex:
            log(f'  POST {path} [{attempt+1}]: {ex}')
            time.sleep(3 * (attempt + 1))
    return None

def _put(path, body):
    try: return requests.put(BASE_URL + path, headers=session_headers, json=body, timeout=10)
    except Exception as ex: log(f'  PUT {path}: {ex}')

def create_session():
    url, hdrs = BASE_URL + '/api/v1/session', {'X-CAP-API-KEY': API_KEY, 'Content-Type': 'application/json'}
    try:
        r = requests.post(url, headers=hdrs, json={'identifier': EMAIL, 'password': PASSWORD, 'encryptedPassword': False}, timeout=15)
        if r.status_code == 200:
            session_headers.update({
                'X-SECURITY-TOKEN': r.headers.get('X-SECURITY-TOKEN'),
                'CST': r.headers.get('CST'), 'Content-Type': 'application/json'
            })
            log('✅ Session OK'); return True
        log(f'❌ Session FAILED [{r.status_code}]')
    except Exception as ex: log(f'❌ Session ERROR: {ex}')
    return False

def ping_session(): _get('/api/v1/ping')
def get_balance():
    global ACCOUNT_BALANCE
    r = _get('/api/v1/accounts')
    if r and r.status_code == 200:
        accs = r.json().get('accounts', [])
        if accs: ACCOUNT_BALANCE = float(accs[0].get('balance', {}).get('available', ACCOUNT_BALANCE))

def get_open_positions():
    r = _get('/api/v1/positions')
    return r.json().get('positions', []) if r and r.status_code == 200 else []

def get_instrument_meta(epic):
    now = time.time()
    cached = _meta_cache.get(epic)
    if cached and (now - cached['ts']) < 300: return cached['data']
    r = _get(f'/api/v1/markets/{epic}')
    if not r or r.status_code != 200: return 0.0, 0.0, 0.0, 100.0, 0.1, 1000.0
    data = r.json()
    snap, inst, deal = data.get('snapshot', {}), data.get('instrument', {}), data.get('dealingRules', {})
    bid = float(snap.get('bid', 0) or 0)
    ask = float(snap.get('offer', 0) or 0)
    result = (bid, ask, round(ask - bid, 5), float(inst.get('contractSize', 100) or 100),
              float((deal.get('minDealSize') or {}).get('value', 0.1) or 0.1),
              float((deal.get('maxDealSize') or {}).get('value', 1000) or 1000))
    _meta_cache[epic] = {'ts': now, 'data': result}
    return result

def get_current_price(epic):
    meta = get_instrument_meta(epic)
    return (meta[0] + meta[1]) / 2 if meta[0] > 0 else 0

def get_closed_deal_price(deal_id, fallback):
    try:
        r = _get('/api/v1/history/activity', params={'dealId': deal_id, 'pageSize': 10})
        if r and r.status_code == 200:
            for act in r.json().get('activities', []):
                for action in act.get('details', {}).get('actions', []):
                    if action.get('actionType') in ('POSITION_CLOSED', 'STOP_LIMIT_AMENDED'):
                        lvl = action.get('level') or action.get('stopLevel')
                        if lvl: return float(lvl)
    except Exception as ex: log(f'  get_closed_deal_price ERROR: {ex}')
    return fallback

def update_sl_api(deal_id, new_sl, tp):
    r = _put(f'/api/v1/positions/{deal_id}', {'stopLevel': new_sl, 'profitLevel': tp})
    if r and r.status_code == 200:
        log(f'  ✅ SL moved to {new_sl}'); return True
    log(f'  ❌ SL update failed'); return False

def close_partial_api(deal_id, size):
    r = _post(f'/api/v1/positions/{deal_id}', {'size': size})
    if r and r.status_code == 200:
        log(f'  💰 Partial close: {size} units'); return True
    log(f'  ❌ Partial close failed: {r.text[:100] if r else "no response"}'); return False


# ═══════════════════════════════════════════════════════
# TELEGRAM
# ═══════════════════════════════════════════════════════
def tg(text):
    if not TG_TOKEN: return
    try:
        requests.post(f'https://api.telegram.org/bot{TG_TOKEN}/sendMessage',
                      data={'chat_id': TG_CHAT_ID, 'text': text, 'parse_mode': 'Markdown'}, timeout=10)
    except: pass

def tg_signal(sig, filters_info):
    icon, mode = ('🟢' if sig['direction'] == 'BUY' else '🔴'), ('DEMO' if DEMO_MODE else 'LIVE')
    entry_type, nl = ('قاع📉' if sig['direction'] == 'BUY' else 'قمة📈'), '\n'
    tg(f'{icon} *{sig["pair"]} {sig["direction"]}* [{mode}] {entry_type}{nl}'
       f'Entry: `{sig["entry"]}` | SL: `{sig["sl"]}` | TP: `{sig["tp"]}`{nl}'
       f'Stages: `1.5R→50%` | `2.5R→30%` | `3.5R→50% of rest`{nl}'
       f'Progressive: `2R→0.5R` | `2.5R→1R` | `3R→1.5R`...{nl}'
       f'_{utc_now()}_')


# ═══════════════════════════════════════════════════════
# INDICATORS
# ═══════════════════════════════════════════════════════
def fetch_candles(epic, resolution, count=500):
    r = _get(f'/api/v1/prices/{epic}', params={'resolution': resolution, 'max': count})
    if not r or r.status_code != 200: return pd.DataFrame()
    prices = r.json().get('prices', [])
    if len(prices) < LENGTH * 3 + ATR_PERIOD: return pd.DataFrame()
    try:
        rows = [{'time': p['snapshotTimeUTC'],
                 'open': (p['openPrice']['bid'] + p['openPrice']['ask']) / 2,
                 'high': (p['highPrice']['bid'] + p['highPrice']['ask']) / 2,
                 'low': (p['lowPrice']['bid'] + p['lowPrice']['ask']) / 2,
                 'close': (p['closePrice']['bid'] + p['closePrice']['ask']) / 2,
                 'volume': p.get('lastTradedVolume', 0) or 0} for p in prices]
    except (KeyError, TypeError) as ex: log(f'  candle parse ERROR: {ex}'); return pd.DataFrame()
    df = pd.DataFrame(rows)
    df['time'] = pd.to_datetime(df['time'], utc=True)
    return df.sort_values('time').reset_index(drop=True)

def calc_atr_series(df, period=14):
    tr = pd.concat([df['high'] - df['low'], 
                    (df['high'] - df['close'].shift()).abs(),
                    (df['low'] - df['close'].shift()).abs()], axis=1).max(axis=1)
    return tr.ewm(span=period, adjust=False).mean()

def calc_supertrend(df, period=10, mult=3.0):
    atr, hl2 = calc_atr_series(df, period), (df['high'] + df['low']) / 2
    upper, lower = (hl2 + mult * atr).values, (hl2 - mult * atr).values
    close, n = df['close'].values, len(df)
    final_u, final_l, st, dir = upper.copy(), lower.copy(), np.zeros(n), np.ones(n, dtype=int)
    for i in range(1, n):
        final_u[i] = upper[i] if (upper[i] < final_u[i-1] or close[i-1] > final_u[i-1]) else final_u[i-1]
        final_l[i] = lower[i] if (lower[i] > final_l[i-1] or close[i-1] < final_l[i-1]) else final_l[i-1]
        dir[i] = 1 if (st[i-1] == final_u[i-1] and close[i] > final_u[i]) else (-1 if close[i] < final_l[i] else dir[i-1])
        st[i] = final_l[i] if dir[i] == 1 else final_u[i]
    return pd.Series(st, index=df.index), pd.Series(dir, index=df.index)

def calc_ema(s, p): return s.ewm(span=p, adjust=False).mean()
def calc_rsi(s, p=14):
    d = s.diff()
    g, l = d.clip(lower=0).ewm(span=p, adjust=False).mean(), (-d.clip(upper=0)).ewm(span=p, adjust=False).mean()
    return 100 - (100 / (1 + g / l.replace(0, 1e-10)))

def find_pivot_high(h, l):
    n, p = len(h), [np.nan] * len(h)
    for i in range(l, n - l):
        if h.iloc[i] >= h.iloc[i-l:i+l+1].max(): p[i] = h.iloc[i]
    return p

def find_pivot_low(lo, l):
    n, p = len(lo), [np.nan] * len(lo)
    for i in range(l, n - l):
        if lo.iloc[i] <= lo.iloc[i-l:i+l+1].min(): p[i] = lo.iloc[i]
    return p

def get_slope_val(m, df, i, l, mu, atr_s):
    a = float(atr_s.iloc[i]) if not np.isnan(atr_s.iloc[i]) else 1e-6
    if m == 'Stdev':
        s = df['close'].iloc[max(0, i-l+1):i+1].std()
        return (s * mu / l) if (not np.isnan(s) and s > 0) else (a * mu / l)
    elif m == 'Linreg':
        y = df['close'].iloc[max(0, i-l+1):i+1].values
        if len(y) >= 2: return abs(np.polyfit(np.arange(len(y)), y, 1)[0]) * mu
    return a * mu / l

def tl_value(ai, av, s, up, ci):
    return av + s * (ci - ai) if up else av - s * (ci - ai)


# ═══════════════════════════════════════════════════════
# ✅ v3.1: MANAGE OPEN POSITIONS — Partial TP متدرج + Progressive SL
# ═══════════════════════════════════════════════════════
def manage_open_positions():
    tracked = op_get_all()
    if not tracked: return
    
    live_pos = get_open_positions()
    live_ids = {p.get('position', {}).get('dealId', '') for p in live_pos}

    for pos in tracked:
        deal_id = pos['deal_id']
        
        # ✅ تسجيل الإغلاق الكامل
        if deal_id not in live_ids:
            log(f'  📋 {pos["pair"]} {deal_id} أُغلقت — نسجّلها')
            exit_price = get_closed_deal_price(deal_id, get_current_price(pos['pair']))
            if exit_price > 0:
                result, _ = csv_log_trade(pos, exit_price, pos['stage1_done'], 
                                          pos['stage2_done'], pos['stage3_done'], pos['final_locked_r'])
                if result != 'ERROR':
                    db_update(pos['db_key'], result.upper() if result in ('WIN','LOSS','BE') else 'CLOSED')
                    op_delete(deal_id)
            continue

        # ✅ حساب الربح الحالي
        cur_price = get_current_price(pos['pair'])
        if cur_price <= 0: continue
        
        entry, sl, tp, size, dir_ = pos['entry'], pos['sl'], pos['tp'], pos['size'], pos['direction']
        sl_dist = abs(entry - sl)
        if sl_dist <= 0: continue
        
        profit_pts = (cur_price - entry) if dir_ == 'BUY' else (entry - cur_price)
        profit_r = profit_pts / sl_dist
        
        s1, s2, s3 = pos['stage1_done'], pos['stage2_done'], pos['stage3_done']
        log(f'  📊 {pos["pair"]} {dir_} | R={profit_r:.2f} | S1:{s1} S2:{s2} S3:{s3} | Locked:{pos["final_locked_r"]}R')

        # ═══════════════════════════════════════════════════════
        # المرحلة 1: Partial TP 50% عند 1.5R
        # ═══════════════════════════════════════════════════════
        if not s1 and profit_r >= STAGE1_TP_R:
            partial_size = round(size * STAGE1_PCT, 2)
            _, _, _, cs, min_sz, _ = get_instrument_meta(pos['pair'])
            
            if partial_size >= min_sz and close_partial_api(deal_id, partial_size):
                op_update(deal_id, stage1_done=1)
                remaining = size - partial_size
                
                nl = '\n'
                tg(f'💰 *Stage 1 — {pos["pair"]} {dir_}*{nl}'
                   f'أغلقنا `{partial_size}` وحطة ({int(STAGE1_PCT*100)}%){nl}'
                   f'عند `+{profit_r:.2f}R` | السعر: `{round(cur_price,5)}`{nl}'
                   f'متبقي: `{remaining}` وحدة | SL الأصلي: 🔓 مفتوح{nl}'
                   f'_{utc_now()}_')
                log(f'  💰 Stage 1: {partial_size} closed, {remaining} remaining, SL unchanged')

        # ═══════════════════════════════════════════════════════
        # المرحلة 2: Partial TP 30% عند 2.5R (من الباقي)
        # ═══════════════════════════════════════════════════════
        elif s1 and not s2 and profit_r >= STAGE2_TP_R:
            remaining_after_s1 = size * (1 - STAGE1_PCT)
            stage2_size = round(remaining_after_s1 * (STAGE2_PCT / (1 - STAGE1_PCT)), 2)
            _, _, _, cs, min_sz, _ = get_instrument_meta(pos['pair'])
            
            if stage2_size >= min_sz and close_partial_api(deal_id, stage2_size):
                # ✅ الآن نحرك SL إلى 0.5R مضمون (ليس BE!)
                new_sl = calculate_sl_at_r(pos, 0.5)
                if new_sl and update_sl_api(deal_id, new_sl, tp):
                    op_update(deal_id, stage2_done=1, final_locked_r=0.5, sl=new_sl)
                    
                    remaining = size * (1 - STAGE1_PCT) * (1 - STAGE2_PCT / (1 - STAGE1_PCT))
                    
                    nl = '\n'
                    tg(f'💰💰 *Stage 2 — {pos["pair"]} {dir_}*{nl}'
                       f'أغلقنا `{stage2_size}` وحدة إضافية ({int(STAGE2_PCT*100)}% من الباقي){nl}'
                       f'عند `+{profit_r:.2f}R` | SL → `+0.5R` مضمون 🔒{nl}'
                       f'متبقي: `{round(remaining,2)}` وحدة | Progressive SL يعمل...{nl}'
                       f'_{utc_now()}_')
                    log(f'  💰💰 Stage 2: {stage2_size} closed, SL locked at +0.5R')

        # ═══════════════════════════════════════════════════════
        # المرحلة 3: Partial TP 50% من المتبقي عند 3.5R
        # ═══════════════════════════════════════════════════════
        elif s2 and not s3 and profit_r >= FINAL_TP_R:
            remaining_after_s2 = size * (1 - STAGE1_PCT) * (1 - STAGE2_PCT / (1 - STAGE1_PCT))
            final_size = round(remaining_after_s2 * FINAL_PCT, 2)
            _, _, _, cs, min_sz, _ = get_instrument_meta(pos['pair'])
            
            if final_size >= min_sz and close_partial_api(deal_id, final_size):
                # ✅ SL إلى 2R مضمون
                new_sl = calculate_sl_at_r(pos, 2.0)
                if new_sl and update_sl_api(deal_id, new_sl, tp):
                    final_remaining = remaining_after_s2 * (1 - FINAL_PCT)
                    op_update(deal_id, stage3_done=1, final_locked_r=2.0, sl=new_sl)
                    
                    nl = '\n'
                    tg(f'💰💰💰 *Stage 3 — {pos["pair"]} {dir_}*{nl}'
                       f'أغلقنا `{final_size}` وحدة أخيرة ({int(FINAL_PCT*100)}% من المتبقي){nl}'
                       f'عند `+{profit_r:.2f}R` | SL → `+2R` مضمون 🔒🔒{nl}'
                       f'باقي نهائي: `{round(final_remaining,2)}` وحدة | Trailing فقط{nl}'
                       f'_{utc_now()}_')
                    log(f'  💰💰💰 Stage 3: {final_size} closed, SL locked at +2R, {final_remaining} trailing')

        # ═══════════════════════════════════════════════════════
        # Progressive SL: تأمين ربح تدريجي للباقي (بعد Stage 2)
        # ═══════════════════════════════════════════════════════
        elif s2:
            new_locked_r, reason = get_progressive_lock(profit_r)
            if new_locked_r > pos['final_locked_r']:
                new_sl = calculate_sl_at_r(pos, new_locked_r)
                if new_sl and should_move_sl(pos['sl'], new_sl, dir_) and update_sl_api(deal_id, new_sl, tp):
                    op_update(deal_id, final_locked_r=new_locked_r, sl=new_sl)
                    
                    nl = '\n'
                    tg(f'📈 *Progressive Lock — {pos["pair"]} {dir_}*{nl}'
                       f'ربح مضمون ↑ `{pos["final_locked_r"]}R` → `{new_locked_r}R`{nl}'
                       f'السعر: `+{profit_r:.2f}R` | SL: `{new_sl}`{nl}'
                       f'_{utc_now()}_')
                    log(f'  📈 Progressive: locked {new_locked_r}R (was {pos["final_locked_r"]}R)')

        # ═══════════════════════════════════════════════════════
        # Trailing للباقي النهائي (بعد Stage 3)
        # ═══════════════════════════════════════════════════════
        elif s3:
            # Trailing بسيط: SL = السعر الحالي - 2×ATR
            atr = calc_atr_for_trailing(pos['pair'])
            if atr:
                trail_sl = calculate_trailing_sl(pos, cur_price, atr)
                if trail_sl and should_move_sl(pos['sl'], trail_sl, dir_) and update_sl_api(deal_id, trail_sl, tp):
                    op_update(deal_id, sl=trail_sl)
                    log(f'  📉 Trailing: SL moved to {trail_sl}')


def calculate_sl_at_r(pos, locked_r):
    """احسب SL عند locked_r من الربح"""
    entry, dir_, sl_dist = pos['entry'], pos['direction'], abs(pos['entry'] - pos['sl'])
    if dir_ == 'BUY': return round(entry + (locked_r * sl_dist), 5)
    else: return round(entry - (locked_r * sl_dist), 5)

def get_progressive_lock(profit_r):
    """احدد مستوى الربح المضمون حسب الربح الحالي"""
    for threshold, locked in sorted(PROGRESSIVE_LOCK.items(), reverse=True):
        if profit_r >= threshold: return locked, f'@ {threshold}R profit'
    return 0, 'none'

def should_move_sl(current_sl, new_sl, direction):
    """تحقق إذا كان SL الجديد أفضل"""
    if direction == 'BUY': return new_sl > current_sl + 0.00001
    else: return new_sl < current_sl - 0.00001

def calc_atr_for_trailing(pair):
    """احسب ATR الحالي للـ Trailing"""
    df = fetch_candles(PAIRS[pair]['epic'], STRATEGY_TF, 50)
    if df.empty: return None
    return float(calc_atr_series(df.iloc[:-1], ATR_PERIOD).iloc[-1])

def calculate_trailing_sl(pos, cur_price, atr):
    """احسب SL trailing: السعر - 2×ATR"""
    dir_ = pos['direction']
    if dir_ == 'BUY': return round(cur_price - (TRAILING_ATR_MULT * atr), 5)
    else: return round(cur_price + (TRAILING_ATR_MULT * atr), 5)


# ═══════════════════════════════════════════════════════
# SIGNAL DETECTION — v3.0: منطق معكوس
# ═══════════════════════════════════════════════════════
def check_signal(pair_name, config):
    epic, allow_buy, allow_sell = config['epic'], config['allow_buy'], config['allow_sell']
    if not allow_buy and not allow_sell: return None

    df = fetch_candles(epic, STRATEGY_TF, CANDLES_COUNT)
    if df.empty or len(df) < max(LENGTH * 3 + ATR_PERIOD, 200): 
        log(f'  {pair_name}: بيانات غير كافية'); return None

    df_c = df.iloc[:-1].copy().reset_index(drop=True)
    n = len(df_c)

    atr_s = calc_atr_series(df_c, ATR_PERIOD)
    ph_arr, pl_arr = find_pivot_high(df_c['high'], LENGTH), find_pivot_low(df_c['low'], LENGTH)
    st_line, st_dir = calc_supertrend(df_c, 10, 3.0)
    ema_f, ema_s = calc_ema(df_c['close'], 20), calc_ema(df_c['close'], 50)
    ema_tf, ema_tm, ema_ts = calc_ema(df_c['close'], 20), calc_ema(df_c['close'], 50), calc_ema(df_c['close'], 200)
    rsi_s = calc_rsi(df_c['close'], 14)

    upper_tl = lower_tl = None
    for i in range(LENGTH + ATR_PERIOD, n):
        atr_i = float(atr_s.iloc[i])
        if np.isnan(atr_i) or atr_i <= 0: continue
        if not np.isnan(ph_arr[i]): upper_tl = (i, float(ph_arr[i]), get_slope_val(SLOPE_METHOD, df_c, i, LENGTH, SLOPE_MULT, atr_s))
        if not np.isnan(pl_arr[i]): lower_tl = (i, float(pl_arr[i]), get_slope_val(SLOPE_METHOD, df_c, i, LENGTH, SLOPE_MULT, atr_s))

    li, lc, la = n - 1, float(df_c['close'].iloc[-1]), float(atr_s.iloc[-1])
    if np.isnan(la) or la <= 0: return None

    csd, cef, ces = int(st_dir.iloc[li]), float(ema_f.iloc[li]), float(ema_s.iloc[li])
    cet, cem, cesl = float(ema_tf.iloc[li]), float(ema_tm.iloc[li]), float(ema_ts.iloc[li])
    cr = float(rsi_s.iloc[li])

    bid, ask, sp, cs, min_sz, max_sz = get_instrument_meta(epic)
    if bid <= 0 or sp > la * SPREAD_ATR_MAX: 
        log(f'  {pair_name}: Spread عالٍ'); return None

    signal, entry = None, None

    # ✅ SELL: كسر ترند علوي (ذروة)
    if allow_sell and upper_tl:
        ai, av, sv = upper_tl
        if ai < li - 1:
            u_last, u_prev = tl_value(ai, av, sv, False, li), tl_value(ai, av, sv, False, li - 1)
            pc = float(df_c['close'].iloc[li - 1])
            if lc > u_last and pc > u_prev:
                filters = [csd == -1, cef < ces, cet < cem < cesl, cr > 55, cr < 78]
                if all(filters):
                    signal, entry = 'SELL', bid
                    log(f'  {pair_name}: 🔴 SELL @ قمة | RSI={cr:.1f}')

    # ✅ BUY: كسر ترند سفلي (قاع)
    if allow_buy and not signal and lower_tl:
        ai, av, sv = lower_tl
        if ai < li - 1:
            l_last, l_prev = tl_value(ai, av, sv, True, li), tl_value(ai, av, sv, True, li - 1)
            pc = float(df_c['close'].iloc[li - 1])
            if lc < l_last and pc < l_prev:
                filters = [csd == 1, cef > ces, cet > cem > cesl, cr > 22, cr < 45]
                if all(filters):
                    signal, entry = 'BUY', ask
                    log(f'  {pair_name}: 🟢 BUY @ قاع | RSI={cr:.1f}')

    if not signal: return None

    if signal == 'SELL':
        sl, tp = round(entry + SL_ATR_MULT * la + sp, 5), round(entry - TP_ATR_MULT * la, 5)
    else:
        sl, tp = round(entry - SL_ATR_MULT * la - sp, 5), round(entry + TP_ATR_MULT * la, 5)

    sld = abs(entry - sl)
    if sld < la * 0.1: return None

    sz = config.get('size_override')
    if sz: size = max(min(float(sz), max_sz), min_sz)
    else:
        risk = ACCOUNT_BALANCE * RISK_PERCENT
        size = max(min(round(risk / (sld * cs), 2), max_sz), min_sz)

    return {
        'pair': pair_name, 'epic': epic, 'direction': signal, 'entry': round(entry, 5),
        'sl': sl, 'tp': tp, 'atr': round(la, 5), 'size': size, 'spread': round(sp, 5),
        'filters_info': {'rsi': cr, 'st_dir': csd, 'ema_cross': cef > ces if signal == 'BUY' else cef < ces}
    }


# ═══════════════════════════════════════════════════════
# EXECUTE & SCAN
# ═══════════════════════════════════════════════════════
def execute_order(sig):
    body = {'epic': sig['epic'], 'direction': sig['direction'], 'size': sig['size'],
            'guaranteedStop': False, 'trailingStop': False, 'stopLevel': sig['sl'], 'profitLevel': sig['tp']}
    log(f'  📤 {sig["pair"]} {sig["direction"]} | entry≈{sig["entry"]} SL={sig["sl"]} TP={sig["tp"]}')
    
    r = _post('/api/v1/positions', body)
    if not r: tg_result(sig['pair'], sig['direction'], 'ERROR', 'N/A', 'no response'); return 'ERROR', 'no response'
    
    data = r.json()
    if r.status_code == 200:
        ref = data.get('dealReference', 'N/A')
        time.sleep(2)
        rc = _get(f'/api/v1/confirms/{ref}')
        if rc and rc.status_code == 200:
            c = rc.json()
            status, deal_id = c.get('dealStatus', 'UNKNOWN'), c.get('dealId', ref)
            tg_result(sig['pair'], sig['direction'], status, ref, c.get('reason', ''))
            if status in ('ACCEPTED', 'SUCCESS'):
                db_key = f'{sig["pair"]}_{datetime.now(timezone.utc).strftime("%Y-%m-%d_%H%M")}'
                op_save(deal_id, sig['pair'], sig['direction'], sig['entry'], sig['sl'], sig['tp'], sig['atr'], sig['size'], db_key)
            return status, ref
        return 'UNKNOWN', ref
    tg_result(sig['pair'], sig['direction'], 'FAILED', 'N/A', data.get('errorCode', str(data)[:80]))
    return 'FAILED', data.get('errorCode')

def tg_result(pair, direction, status, ref, error=''):
    icon, nl = ('✅' if status in ('ACCEPTED', 'SUCCESS') else '❌'), '\n'
    tg(f'{icon} *{pair} {direction} {status}*{nl}Ref: `{ref}`' + (f'{nl}Err: `{error[:80]}`' if error else ''))

def run_scan():
    now = datetime.now(timezone.utc)
    if now.weekday() >= 5 or not (SESSION_START <= now.hour < SESSION_END):
        log(f'⏸ خارج الجلسة/عطلة'); return

    log('─' * 60)
    log(f'🔍 Scan v3.1 | Partial TP متدرج + Progressive SL | {"DEMO" if DEMO_MODE else "LIVE"}')
    log('─' * 60)

    get_balance()
    manage_open_positions()

    open_pos = get_open_positions()
    log(f'  مفتوحة: {len(open_pos)} / {MAX_OPEN_TRADES}')
    if len(open_pos) >= MAX_OPEN_TRADES: log('  ⏸ الحد الأقصى'); return

    ts_key = now.strftime('%Y-%m-%d_%H%M')
    for pair_name, config in PAIRS.items():
        if len(open_pos) >= MAX_OPEN_TRADES: break
        if db_consec_losses(pair_name) >= MAX_CONSECUTIVE_LOSS: 
            log(f'  {pair_name}: تخطّي بسبب خسائر'); continue
        key = f'{pair_name}_{ts_key}'
        if db_is_dup(key): log(f'  {pair_name}: مكرر'); continue
        
        log(f'  {pair_name}: فحص...')
        sig = check_signal(pair_name, config)
        if not sig: log(f'  {pair_name}: لا إشارة'); continue
        
        db_save(key, pair_name, sig['direction'], sig['entry'], sig['sl'], sig['tp'], sig['atr'], sig['size'], sig['spread'])
        tg_signal(sig, sig['filters_info'])
        status, ref = execute_order(sig)
        db_update(key, status)
        log(f'  {pair_name}: {status} | {ref}')
        open_pos = get_open_positions()
        time.sleep(2)

def db_consec_losses(pair):
    with db_lock:
        with sqlite3.connect(DB_FILE) as conn:
            rows = conn.execute("SELECT status FROM trades WHERE pair=? AND status IN ('WIN','LOSS') ORDER BY id DESC LIMIT 8", (pair,)).fetchall()
            c = 0
            for r in rows:
                if r[0] == 'LOSS': c += 1
                else: break
            return c


# ═══════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════
def start_bot():
    db_init()
    csv_init()
    mode, nl = ('DEMO' if DEMO_MODE else 'LIVE'), '\n'

    print('=' * 60, flush=True)
    print(f'  Multi-Pairs Bot v3.1 — Partial TP متدرج + Progressive SL', flush=True)
    print(f'  🟢 BUY: قاع | 🔴 SELL: قمة (منطق معكوس v3.0)', flush=True)
    print(f'  💰 Stage 1: {STAGE1_TP_R}R → {int(STAGE1_PCT*100)}% | Stage 2: {STAGE2_TP_R}R → {int(STAGE2_PCT*100)}% | Stage 3: {FINAL_TP_R}R → {int(FINAL_PCT*100)}%', flush=True)
    print(f'  📈 Progressive: 2R→0.5R | 2.5R→1R | 3R→1.5R | 3.5R→2R | 4.5R→3R | 6R→4R', flush=True)
    print(f'  🏃 Trailing: الباقي النهائي مع ATR×{TRAILING_ATR_MULT}', flush=True)
    print('=' * 60, flush=True)

    tg(f'🚀 *Bot v3.1* [{mode}]{nl}'
       f'💰 Stage 1: `{STAGE1_TP_R}R→{int(STAGE1_PCT*100)}%`{nl}'
       f'💰💰 Stage 2: `{STAGE2_TP_R}R→{int(STAGE2_PCT*100)}%` + Lock `0.5R`{nl}'
       f'💰💰💰 Stage 3: `{FINAL_TP_R}R→{int(FINAL_PCT*100)}%` + Lock `2R`{nl}'
       f'🏃 Trailing: الباقي مع ATR×{TRAILING_ATR_MULT}{nl}'
       f'📈 Progressive: `2R→0.5R` → `2.5R→1R` → `3R→1.5R`...{nl}'
       f'_{utc_now()}_')

    session_age = 0
    while True:
        try:
            if session_age == 0:
                if not create_session(): time.sleep(60); continue
            else: ping_session()
            session_age = (session_age + 1) % 15
            run_scan()
        except KeyboardInterrupt: log('🛑 Bot stopped'); tg('🛑 Bot stopped'); break
        except Exception as ex: log(f'LOOP ERROR: {ex}'); tg(f'❌ Error: `{str(ex)[:100]}`')
        time.sleep(SCAN_INTERVAL)

if __name__ == '__main__':
    start_bot()
