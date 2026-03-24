#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ==========================================================
# multi_pairs_bot.py — TL Breaks + Multi-Filter Bot
# XAUUSD + BTCUSD + EURUSD + GBPUSD | Capital.com API
# ==========================================================
# الاستراتيجية (من الصورة):
#   TL Breaks: Pivot Highs/Lows + ATR Trendlines
#   Filters: Supertrend | EMA Fast/Slow | EMA Trend×3
#            EMA Slope  | RSI           | Volume
#   M15 | 500 candles | SL=1.5×ATR | TP=3.0×ATR
#   Supertrend Period=10 | RSI Period=14
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
API_KEY    = os.getenv('CAPITAL_API_KEY',  'BbmFhEF3FffkcR0Y')
EMAIL      = os.getenv('CAPITAL_EMAIL',    'almorese2013@gmail.com')
PASSWORD   = os.getenv('CAPITAL_PASSWORD', 'Ba050326>')
TG_TOKEN   = os.getenv('TG_TOKEN',         '8782238258:AAEtuQg7OYAmoemhWfLqKdYpqIxfWwyKRSQ')
TG_CHAT_ID = os.getenv('TG_CHAT_ID',       '533243705')

BASE_URL  = 'https://api-capital.backend-capital.com'
DEMO_MODE = os.getenv('DEMO_MODE', 'false').lower() == 'true'

# ── الأزواج — من الصورة ──
PAIRS = {
    'GOLD': {
        'epic':          'GOLD',       # XAUUSD على Capital.com
        'allow_buy':     True,
        'allow_sell':    True,
        'size_override': 0.05,            # الذهب: حجم 1 لوت
    },
    'BTCUSD': {
        'epic':          'BTCUSD',
        'allow_buy':     True,
        'allow_sell':    True,
        'size_override': None,         # تلقائي
    },
    'EURUSD': {
        'epic':          'EURUSD',
        'allow_buy':     True,
        'allow_sell':    True,
        'size_override': 1000,         # من الصورة
    },
    'GBPUSD': {
        'epic':          'GBPUSD',
        'allow_buy':     True,
        'allow_sell':    True,
        'size_override': 1000,         # من الصورة
    },
}

# ── الإعدادات من الصورة ──
STRATEGY_TF    = os.getenv('STRATEGY_TF',    'MINUTE_15')  # M15
CANDLES_COUNT  = 500                                         # من الصورة
SCAN_INTERVAL  = int(os.getenv('SCAN_INTERVAL', '300'))

# ── Strategy Parameters من الصورة ──
LENGTH         = int(os.getenv('LENGTH',   '10'))
SLOPE_MULT     = float(os.getenv('SLOPE_MULT', '1.0'))
SLOPE_METHOD   = os.getenv('SLOPE_METHOD', 'ATR')
ATR_PERIOD     = 14
SL_ATR_MULT    = 1.5    # من الصورة
TP_ATR_MULT    = 3.0    # من الصورة

# ── Filter Parameters من الصورة ──
SUPERTREND_PERIOD = 10   # من الصورة
SUPERTREND_MULT   = 3.0
RSI_PERIOD        = 14   # من الصورة
EMA_FAST          = 20
EMA_SLOW          = 50
EMA_TREND3_FAST   = 20
EMA_TREND3_MID    = 50
EMA_TREND3_SLOW   = 200
EMA_SLOPE_PERIOD  = 20
VOLUME_MA_PERIOD  = 20

# ── فلتر الجلسة (UTC) ──
SESSION_START = 00   # 06:00 بتوقيتك
SESSION_END   = 23  # 23:00 بتوقيتك

# ── Risk ──
RISK_PERCENT         = float(os.getenv('RISK_PERCENT',     '0.01'))
MAX_OPEN_TRADES      = int(os.getenv('MAX_OPEN_TRADES',    '4'))
MAX_CONSECUTIVE_LOSS = int(os.getenv('MAX_CONSEC_LOSS',    '5'))
ACCOUNT_BALANCE      = float(os.getenv('ACCOUNT_BALANCE', '1000'))

# ── Files ──
_BASE_DIR  = os.getenv('DATA_DIR', '/tmp')
DB_FILE    = os.path.join(_BASE_DIR, 'multi_bot.db')
TRADES_CSV = os.path.join(_BASE_DIR, 'trades_log.csv')

db_lock         = Lock()
session_headers = {}
_meta_cache: dict = {}

CSV_HEADERS = [
    'date', 'time_utc', 'pair', 'direction',
    'entry', 'sl', 'tp', 'exit_price',
    'atr', 'size', 'sl_dist',
    'pnl_usd', 'pnl_r', 'result',
    'bars_held', 'spread', 'tf',
    'supertrend', 'rsi', 'ema_fast', 'ema_slow',
]


# ═══════════════════════════════════════════════════════
# UTILS
# ═══════════════════════════════════════════════════════
def utc_now():
    return datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')

def log(msg):
    print(f'[{utc_now()}] {msg}', flush=True)


# ═══════════════════════════════════════════════════════
# DATABASE
# ═══════════════════════════════════════════════════════
def db_init():
    with sqlite3.connect(DB_FILE) as conn:
        conn.execute('''CREATE TABLE IF NOT EXISTS trades (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            key       TEXT UNIQUE,
            pair      TEXT,
            direction TEXT,
            timestamp TEXT,
            entry     REAL, sl REAL, tp REAL,
            atr       REAL, size REAL,
            spread    REAL DEFAULT 0,
            status    TEXT DEFAULT 'PENDING'
        )''')
        conn.commit()

def db_save(key, pair, direction, entry, sl, tp, atr, size, spread=0.0):
    with db_lock:
        with sqlite3.connect(DB_FILE) as conn:
            try:
                conn.execute(
                    'INSERT INTO trades'
                    ' (key,pair,direction,timestamp,entry,sl,tp,atr,size,spread)'
                    ' VALUES (?,?,?,?,?,?,?,?,?,?)',
                    (key, pair, direction, utc_now(),
                     entry, sl, tp, atr, size, spread)
                )
                conn.commit()
            except sqlite3.IntegrityError:
                pass

def db_update(key, status):
    with db_lock:
        with sqlite3.connect(DB_FILE) as conn:
            conn.execute('UPDATE trades SET status=? WHERE key=?', (status, key))
            conn.commit()

def db_is_dup(key):
    with db_lock:
        with sqlite3.connect(DB_FILE) as conn:
            return conn.execute(
                'SELECT id FROM trades WHERE key=?', (key,)
            ).fetchone() is not None

def db_consec_losses(pair):
    with db_lock:
        with sqlite3.connect(DB_FILE) as conn:
            rows = conn.execute(
                "SELECT status FROM trades"
                " WHERE pair=? AND status IN ('WIN','LOSS')"
                " ORDER BY id DESC LIMIT 8", (pair,)
            ).fetchall()
            count = 0
            for r in rows:
                if r[0] == 'LOSS': count += 1
                else: break
            return count


# ═══════════════════════════════════════════════════════
# CSV LOGGER
# ═══════════════════════════════════════════════════════
def csv_init():
    if not Path(TRADES_CSV).exists():
        with open(TRADES_CSV, 'w', newline='', encoding='utf-8-sig') as f:
            csv.DictWriter(f, fieldnames=CSV_HEADERS).writeheader()

def csv_log_trade(pair, direction, entry, sl, tp, exit_price,
                  atr, size, spread=0.0, bars_held=0,
                  st_val=0, rsi_val=0, ema_f=0, ema_s=0):
    try:
        sl_dist = abs(entry - sl)
        pnl_pts = (exit_price - entry) if direction == 'BUY' else (entry - exit_price)
        cached  = _meta_cache.get(pair)
        cs_val  = cached['data'][3] if cached else 100.0
        pnl_usd = round(pnl_pts * size * cs_val, 2)
        pnl_r   = round(pnl_pts / sl_dist, 2) if sl_dist > 0 else 0.0
        result  = 'WIN' if pnl_pts > 0 else ('LOSS' if pnl_pts < 0 else 'BE')

        now = datetime.now(timezone.utc)
        row = {
            'date':       now.strftime('%Y-%m-%d'),
            'time_utc':   now.strftime('%H:%M'),
            'pair':       pair, 'direction': direction,
            'entry':      entry, 'sl': sl, 'tp': tp,
            'exit_price': exit_price, 'atr': atr, 'size': size,
            'sl_dist':    round(sl_dist, 5),
            'pnl_usd':    pnl_usd, 'pnl_r': pnl_r, 'result': result,
            'bars_held':  bars_held, 'spread': spread, 'tf': STRATEGY_TF,
            'supertrend': round(st_val, 5),
            'rsi':        round(rsi_val, 2),
            'ema_fast':   round(ema_f, 5),
            'ema_slow':   round(ema_s, 5),
        }
        with open(TRADES_CSV, 'a', newline='', encoding='utf-8-sig') as f:
            csv.DictWriter(f, fieldnames=CSV_HEADERS).writerow(row)

        icon = '✅' if result == 'WIN' else ('❌' if result == 'LOSS' else '🔵')
        log(f'  {icon} CLOSED {pair} {direction} | PnL=${pnl_usd:+.2f} ({pnl_r:+.2f}R)')

        nl = '
'
        tg(
            f'{icon} *{pair} {direction} CLOSED — {result}*{nl}'
            f'Entry: `{entry}` → Exit: `{exit_price}`{nl}'
            f'PnL: `${pnl_usd:+.2f}` | `{pnl_r:+.2f}R`{nl}'
            f'RSI: `{round(rsi_val,1)}` | ST: `{round(st_val,2)}`{nl}'
            f'_{now.strftime("%Y-%m-%d %H:%M UTC")}_'
        )
        return result
    except Exception as ex:
        log(f'  csv_log_trade ERROR: {ex}')
        return 'ERROR'


# ═══════════════════════════════════════════════════════
# API HELPERS
# ═══════════════════════════════════════════════════════
def _get(path, params=None, retries=3):
    for attempt in range(retries):
        try:
            r = requests.get(
                BASE_URL + path, headers=session_headers,
                params=params, timeout=15
            )
            if r.status_code == 429:
                time.sleep(5 * (attempt + 1)); continue
            return r
        except requests.exceptions.RequestException as ex:
            log(f'  GET {path} [{attempt+1}/{retries}]: {ex}')
            time.sleep(3 * (attempt + 1))
    return None

def _post(path, body, retries=2):
    for attempt in range(retries):
        try:
            return requests.post(
                BASE_URL + path, headers=session_headers,
                json=body, timeout=15
            )
        except requests.exceptions.RequestException as ex:
            log(f'  POST {path} [{attempt+1}/{retries}]: {ex}')
            time.sleep(3 * (attempt + 1))
    return None

def _put(path, body):
    try:
        return requests.put(
            BASE_URL + path, headers=session_headers,
            json=body, timeout=10
        )
    except Exception as ex:
        log(f'  PUT {path}: {ex}')
    return None


# ═══════════════════════════════════════════════════════
# SESSION
# ═══════════════════════════════════════════════════════
def create_session():
    url  = BASE_URL + '/api/v1/session'
    hdrs = {'X-CAP-API-KEY': API_KEY, 'Content-Type': 'application/json'}
    body = {'identifier': EMAIL, 'password': PASSWORD, 'encryptedPassword': False}
    try:
        r = requests.post(url, headers=hdrs, json=body, timeout=15)
        if r.status_code == 200:
            session_headers.update({
                'X-SECURITY-TOKEN': r.headers.get('X-SECURITY-TOKEN'),
                'CST':              r.headers.get('CST'),
                'Content-Type':     'application/json',
            })
            log('✅ Session OK')
            return True
        log(f'❌ Session FAILED [{r.status_code}]: {r.text[:100]}')
    except Exception as ex:
        log(f'❌ Session ERROR: {ex}')
    return False

def ping_session():
    _get('/api/v1/ping')

def get_balance():
    global ACCOUNT_BALANCE
    r = _get('/api/v1/accounts')
    if r and r.status_code == 200:
        accs = r.json().get('accounts', [])
        if accs:
            ACCOUNT_BALANCE = float(
                accs[0].get('balance', {}).get('available', ACCOUNT_BALANCE)
            )
            log(f'💰 Balance: ${round(ACCOUNT_BALANCE, 2)}')

def get_open_positions():
    r = _get('/api/v1/positions')
    if r and r.status_code == 200:
        return r.json().get('positions', [])
    return []

def get_instrument_meta(epic):
    now    = time.time()
    cached = _meta_cache.get(epic)
    if cached and (now - cached['ts']) < 300:
        return cached['data']
    r = _get(f'/api/v1/markets/{epic}')
    if not r or r.status_code != 200:
        return 0.0, 0.0, 0.0, 100.0, 0.1, 1000.0
    data       = r.json()
    snap       = data.get('snapshot',     {})
    instrument = data.get('instrument',   {})
    dealing    = data.get('dealingRules', {})
    bid    = float(snap.get('bid',   0) or 0)
    ask    = float(snap.get('offer', 0) or 0)
    spread = round(ask - bid, 5)
    cs     = float(instrument.get('contractSize', 100) or 100)
    min_sz = float((dealing.get('minDealSize') or {}).get('value', 0.1)  or 0.1)
    max_sz = float((dealing.get('maxDealSize') or {}).get('value', 1000) or 1000)
    result = (bid, ask, spread, cs, min_sz, max_sz)
    _meta_cache[epic] = {'ts': now, 'data': result}
    return result


# ═══════════════════════════════════════════════════════
# TELEGRAM
# ═══════════════════════════════════════════════════════
def tg(text):
    if not TG_TOKEN:
        return
    try:
        requests.post(
            f'https://api.telegram.org/bot{TG_TOKEN}/sendMessage',
            data={'chat_id': TG_CHAT_ID, 'text': text, 'parse_mode': 'Markdown'},
            timeout=10
        )
    except Exception:
        pass

def tg_signal(sig, filters_info):
    icon = '🟢' if sig['direction'] == 'BUY' else '🔴'
    mode = 'DEMO' if DEMO_MODE else 'LIVE'
    rr   = round(TP_ATR_MULT / SL_ATR_MULT, 1)
    nl   = '
'
    tg(
        f'{icon} *{sig["pair"]} {sig["direction"]}* [{mode}]{nl}'
        f'Entry: `{sig["entry"]}` | SL: `{sig["sl"]}` | TP: `{sig["tp"]}`{nl}'
        f'R:R: `1:{rr}` | Size: `{sig["size"]}`{nl}'
        f'ATR: `{sig["atr"]}` | Spread: `{sig["spread"]}`{nl}'
        f'── Filters ──{nl}'
        f'ST: `{"↑UP" if filters_info["st_dir"]==1 else "↓DN"}`'
        f' | RSI: `{filters_info["rsi"]:.1f}`{nl}'
        f'EMA {EMA_FAST}/{EMA_SLOW}: `{"✅" if filters_info["ema_cross"] else "❌"}`'
        f' | Trend×3: `{"✅" if filters_info["ema3"] else "❌"}`{nl}'
        f'EMA Slope: `{"↑" if filters_info["slope"] else "↓"}`'
        f' | Volume: `{"✅" if filters_info["vol"] else "❌"}`{nl}'
        f'TF: `{STRATEGY_TF}`{nl}'
        f'_{utc_now()}_'
    )

def tg_result(pair, direction, status, ref, error=''):
    icon = '✅' if status in ('ACCEPTED', 'SUCCESS') else '❌'
    nl   = '
'
    msg  = f'{icon} *{pair} {direction} {status}*{nl}Ref: `{ref}`'
    if error:
        msg += f'{nl}Err: `{error[:80]}`'
    tg(msg)


# ═══════════════════════════════════════════════════════
# CANDLES
# ═══════════════════════════════════════════════════════
def fetch_candles(epic, resolution, count=500):
    r = _get(f'/api/v1/prices/{epic}',
             params={'resolution': resolution, 'max': count})
    if not r or r.status_code != 200:
        log(f'  fetch_candles FAILED ({epic})')
        return pd.DataFrame()
    prices = r.json().get('prices', [])
    if len(prices) < LENGTH * 3 + ATR_PERIOD:
        return pd.DataFrame()
    try:
        rows = [{
            'time':   p['snapshotTimeUTC'],
            'open':   (p['openPrice']['bid']  + p['openPrice']['ask'])  / 2,
            'high':   (p['highPrice']['bid']  + p['highPrice']['ask'])  / 2,
            'low':    (p['lowPrice']['bid']   + p['lowPrice']['ask'])   / 2,
            'close':  (p['closePrice']['bid'] + p['closePrice']['ask']) / 2,
            'volume': p.get('lastTradedVolume', 0) or 0,
        } for p in prices]
    except (KeyError, TypeError) as ex:
        log(f'  candle parse ERROR: {ex}')
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df['time'] = pd.to_datetime(df['time'], utc=True)
    return df.sort_values('time').reset_index(drop=True)


# ═══════════════════════════════════════════════════════
# INDICATORS — من الصورة بالضبط
# ═══════════════════════════════════════════════════════
def calc_atr_series(df, period=14):
    tr = pd.concat([
        df['high'] - df['low'],
        (df['high'] - df['close'].shift()).abs(),
        (df['low']  - df['close'].shift()).abs()
    ], axis=1).max(axis=1)
    return tr.ewm(span=period, adjust=False).mean()

# ── [1] Supertrend (Period=10 من الصورة) ──
def calc_supertrend(df, period=10, multiplier=3.0):
    atr  = calc_atr_series(df, period)
    hl2  = (df['high'] + df['low']) / 2
    n    = len(df)

    upper = (hl2 + multiplier * atr).values
    lower = (hl2 - multiplier * atr).values
    close = df['close'].values

    final_upper = upper.copy()
    final_lower = lower.copy()
    supertrend  = np.zeros(n)
    direction   = np.ones(n, dtype=int)  # 1=up, -1=down

    for i in range(1, n):
        final_upper[i] = upper[i] if (upper[i] < final_upper[i-1]
                                       or close[i-1] > final_upper[i-1]) \
                         else final_upper[i-1]
        final_lower[i] = lower[i] if (lower[i] > final_lower[i-1]
                                       or close[i-1] < final_lower[i-1]) \
                         else final_lower[i-1]

        if supertrend[i-1] == final_upper[i-1]:
            direction[i] = 1 if close[i] > final_upper[i] else -1
        else:
            direction[i] = -1 if close[i] < final_lower[i] else 1

        supertrend[i] = final_lower[i] if direction[i] == 1 else final_upper[i]

    return pd.Series(supertrend, index=df.index), pd.Series(direction, index=df.index)

# ── [2] EMA Fast + EMA Slow ──
def calc_ema(series, period):
    return series.ewm(span=period, adjust=False).mean()

# ── [3] EMA Slope — ميل EMA ──
def calc_ema_slope(series, period, lookback=3):
    ema = calc_ema(series, period)
    return ema.diff(lookback)  #양수 = صاعد

# ── [4] RSI (Period=14 من الصورة) ──
def calc_rsi(series, period=14):
    delta = series.diff()
    gain  = delta.clip(lower=0).ewm(span=period, adjust=False).mean()
    loss  = (-delta.clip(upper=0)).ewm(span=period, adjust=False).mean()
    rs    = gain / loss.replace(0, 1e-10)
    return 100 - (100 / (1 + rs))

# ── [5] Volume فوق المتوسط ──
def calc_volume_filter(volume_series, period=20):
    vol_ma = volume_series.rolling(period).mean()
    return volume_series > vol_ma

# ── Pivot Highs/Lows ──
def find_pivot_high(high_series, length):
    n, pivots = len(high_series), [np.nan] * len(high_series)
    for i in range(length, n - length):
        if high_series.iloc[i] == high_series.iloc[i-length: i+length+1].max():
            pivots[i] = high_series.iloc[i]
    return pivots

def find_pivot_low(low_series, length):
    n, pivots = len(low_series), [np.nan] * len(low_series)
    for i in range(length, n - length):
        if low_series.iloc[i] == low_series.iloc[i-length: i+length+1].min():
            pivots[i] = low_series.iloc[i]
    return pivots

def get_slope_val(method, df, idx, length, mult, atr_series):
    atr_val = float(atr_series.iloc[idx]) if not np.isnan(atr_series.iloc[idx]) else 1e-6
    if method == 'Stdev':
        start = max(0, idx - length + 1)
        stdev = df['close'].iloc[start:idx+1].std()
        return (stdev * mult / length) if (not np.isnan(stdev) and stdev > 0) \
               else (atr_val * mult / length)
    elif method == 'Linreg':
        start = max(0, idx - length + 1)
        y = df['close'].iloc[start:idx+1].values
        if len(y) >= 2:
            return abs(np.polyfit(np.arange(len(y)), y, 1)[0]) * mult
    return atr_val * mult / length

def tl_value(anchor_idx, anchor_val, slope, goes_up, cur_idx):
    bars = cur_idx - anchor_idx
    return anchor_val + slope * bars if goes_up else anchor_val - slope * bars


# ═══════════════════════════════════════════════════════
# POSITION SIZING
# ═══════════════════════════════════════════════════════
def calc_position_size(risk_usd, sl_dist, contract_size, min_size, max_size):
    if sl_dist <= 0 or contract_size <= 0:
        return min_size
    size = round(risk_usd / (sl_dist * contract_size), 2)
    return max(min(size, max_size), min_size)


# ═══════════════════════════════════════════════════════
# SIGNAL DETECTION — TL Breaks + 7 Filters
# ═══════════════════════════════════════════════════════
def check_signal(pair_name, config):
    epic       = config['epic']
    allow_buy  = config['allow_buy']
    allow_sell = config['allow_sell']
    if not allow_buy and not allow_sell:
        return None

    df = fetch_candles(epic, STRATEGY_TF, CANDLES_COUNT)
    if df.empty or len(df) < max(LENGTH * 3 + ATR_PERIOD, EMA_TREND3_SLOW + 10):
        log(f'  {pair_name}: بيانات غير كافية')
        return None

    df_c = df.iloc[:-1].copy().reset    },
    'BTCUSD': {
        'epic':          'BTCUSD',
        'allow_buy':     True,
        'allow_sell':    True,
        'size_override': None,
    },
    'EURUSD': {
        'epic':          'EURUSD',
        'allow_buy':     True,
        'allow_sell':    True,
        'size_override': 1000,
    },
    'GBPUSD': {
        'epic':          'GBPUSD',
        'allow_buy':     True,
        'allow_sell':    True,
        'size_override': 1000,
    },
}

STRATEGY_TF   = os.getenv('STRATEGY_TF', 'MINUTE_15')
CANDLES_COUNT = 500
SCAN_INTERVAL = int(os.getenv('SCAN_INTERVAL', '300'))

LENGTH       = int(os.getenv('LENGTH',      '10'))
SLOPE_MULT   = float(os.getenv('SLOPE_MULT', '1.0'))
SLOPE_METHOD = os.getenv('SLOPE_METHOD',    'ATR')
ATR_PERIOD   = 14
SL_ATR_MULT  = 1.5
TP_ATR_MULT  = 3.0

# ── ✅ إعدادات Break-even + Partial TP الجديدة ──
BE_TRIGGER_R      = 1.0    # انقل SL للدخول عند تحقيق 1R ربح
PARTIAL_TP_R      = 1.5    # أغلق نصف الصفقة عند 1.5R
PARTIAL_TP_RATIO  = 0.5    # نسبة الإغلاق الجزئي (50%)
ENABLE_BE         = True   # تفعيل Break-even
ENABLE_PARTIAL_TP = True   # تفعيل Partial TP

SUPERTREND_PERIOD = 10
SUPERTREND_MULT   = 3.0
RSI_PERIOD        = 14
EMA_FAST          = 20
EMA_SLOW          = 50
EMA_TREND3_FAST   = 20
EMA_TREND3_MID    = 50
EMA_TREND3_SLOW   = 200
EMA_SLOPE_PERIOD  = 20
VOLUME_MA_PERIOD  = 20

SESSION_START = 3
SESSION_END   = 20

RISK_PERCENT         = float(os.getenv('RISK_PERCENT',    '0.01'))
MAX_OPEN_TRADES      = int(os.getenv('MAX_OPEN_TRADES',   '4'))
MAX_CONSECUTIVE_LOSS = int(os.getenv('MAX_CONSEC_LOSS',   '5'))
ACCOUNT_BALANCE      = float(os.getenv('ACCOUNT_BALANCE', '1000'))

_BASE_DIR  = os.getenv('DATA_DIR', os.path.dirname(os.path.abspath(__file__)))
DB_FILE    = os.path.join(_BASE_DIR, 'multi_bot.db')
TRADES_CSV = os.path.join(_BASE_DIR, 'trades_log.csv')

db_lock         = Lock()
session_headers = {}
_meta_cache: dict = {}

CSV_HEADERS = [
    'date', 'time_utc', 'pair', 'direction',
    'entry', 'sl', 'tp', 'exit_price',
    'atr', 'size', 'sl_dist', 'pnl_usd', 'pnl_r',
    'result', 'bars_held', 'spread', 'tf',
    'be_triggered', 'partial_done',
    'supertrend', 'rsi', 'ema_fast', 'ema_slow',
]


# ═══════════════════════════════════════════════════════
# UTILS
# ═══════════════════════════════════════════════════════
def utc_now():
    return datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')

def log(msg):
    print(f'[{utc_now()}] {msg}', flush=True)


# ═══════════════════════════════════════════════════════
# DATABASE
# ═══════════════════════════════════════════════════════
def db_init():
    with sqlite3.connect(DB_FILE) as conn:
        conn.execute('''CREATE TABLE IF NOT EXISTS trades (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            key          TEXT UNIQUE,
            pair         TEXT,
            direction    TEXT,
            timestamp    TEXT,
            entry        REAL, sl REAL, tp REAL,
            atr          REAL, size REAL, spread REAL DEFAULT 0,
            be_triggered INTEGER DEFAULT 0,
            partial_done INTEGER DEFAULT 0,
            status       TEXT DEFAULT 'PENDING'
        )''')
        # ── جدول الصفقات المفتوحة لمتابعة BE/Partial ──
        conn.execute('''CREATE TABLE IF NOT EXISTS open_positions (
            deal_id      TEXT PRIMARY KEY,
            pair         TEXT,
            direction    TEXT,
            entry        REAL,
            sl           REAL,
            tp           REAL,
            atr          REAL,
            size         REAL,
            db_key       TEXT,
            be_triggered INTEGER DEFAULT 0,
            partial_done INTEGER DEFAULT 0,
            opened_at    TEXT
        )''')
        conn.commit()

def db_save(key, pair, direction, entry, sl, tp, atr, size, spread=0.0):
    with db_lock:
        with sqlite3.connect(DB_FILE) as conn:
            try:
                conn.execute(
                    'INSERT INTO trades'
                    ' (key,pair,direction,timestamp,entry,sl,tp,atr,size,spread)'
                    ' VALUES (?,?,?,?,?,?,?,?,?,?)',
                    (key, pair, direction, utc_now(),
                     entry, sl, tp, atr, size, spread)
                )
                conn.commit()
            except sqlite3.IntegrityError:
                pass

def db_update(key, status):
    with db_lock:
        with sqlite3.connect(DB_FILE) as conn:
            conn.execute('UPDATE trades SET status=? WHERE key=?', (status, key))
            conn.commit()

def db_is_dup(key):
    with db_lock:
        with sqlite3.connect(DB_FILE) as conn:
            return conn.execute(
                'SELECT id FROM trades WHERE key=?', (key,)
            ).fetchone() is not None

def db_consec_losses(pair):
    with db_lock:
        with sqlite3.connect(DB_FILE) as conn:
            rows = conn.execute(
                "SELECT status FROM trades"
                " WHERE pair=? AND status IN ('WIN','LOSS')"
                " ORDER BY id DESC LIMIT 8", (pair,)
            ).fetchall()
            count = 0
            for r in rows:
                if r[0] == 'LOSS': count += 1
                else: break
            return count

# ── Open Positions tracking ──
def op_save(deal_id, pair, direction, entry, sl, tp, atr, size, db_key):
    with db_lock:
        with sqlite3.connect(DB_FILE) as conn:
            try:
                conn.execute(
                    'INSERT OR IGNORE INTO open_positions'
                    ' (deal_id,pair,direction,entry,sl,tp,atr,size,db_key,opened_at)'
                    ' VALUES (?,?,?,?,?,?,?,?,?,?)',
                    (deal_id, pair, direction, entry, sl, tp,
                     atr, size, db_key, utc_now())
                )
                conn.commit()
            except Exception as ex:
                log(f'op_save ERROR: {ex}')

def op_get_all():
    with db_lock:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute('SELECT * FROM open_positions').fetchall()
            return [dict(r) for r in rows]

def op_update(deal_id, be_triggered=None, partial_done=None, sl=None):
    with db_lock:
        with sqlite3.connect(DB_FILE) as conn:
            if be_triggered is not None:
                conn.execute('UPDATE open_positions SET be_triggered=? WHERE deal_id=?',
                             (int(be_triggered), deal_id))
            if partial_done is not None:
                conn.execute('UPDATE open_positions SET partial_done=? WHERE deal_id=?',
                             (int(partial_done), deal_id))
            if sl is not None:
                conn.execute('UPDATE open_positions SET sl=? WHERE deal_id=?',
                             (sl, deal_id))
            conn.commit()

def op_delete(deal_id):
    with db_lock:
        with sqlite3.connect(DB_FILE) as conn:
            conn.execute('DELETE FROM open_positions WHERE deal_id=?', (deal_id,))
            conn.commit()


# ═══════════════════════════════════════════════════════
# CSV LOGGER
# ═══════════════════════════════════════════════════════
def csv_init():
    if not Path(TRADES_CSV).exists():
        with open(TRADES_CSV, 'w', newline='', encoding='utf-8-sig') as f:
            csv.DictWriter(f, fieldnames=CSV_HEADERS).writeheader()

def csv_log_trade(pos, exit_price, be_triggered=False, partial_done=False):
    try:
        entry   = pos['entry']
        sl      = pos['sl']
        size    = pos['size']
        dir_    = pos['direction']
        pair    = pos['pair']

        sl_dist = abs(entry - sl)
        pnl_pts = (exit_price - entry) if dir_ == 'BUY' else (entry - exit_price)
        pnl_r   = round(pnl_pts / sl_dist, 2) if sl_dist > 0 else 0.0
        result  = 'WIN' if pnl_pts > 0 else ('LOSS' if pnl_pts < 0 else 'BE')

        meta    = _meta_cache.get(pos['pair'], {}).get('data')
        cs_val  = meta[3] if meta else 1.0
        pnl_usd = round(pnl_pts * size * cs_val, 2)

        now = datetime.now(timezone.utc)
        row = {
            'date':         now.strftime('%Y-%m-%d'),
            'time_utc':     now.strftime('%H:%M'),
            'pair':         pair, 'direction': dir_,
            'entry':        entry, 'sl': sl, 'tp': pos['tp'],
            'exit_price':   exit_price,
            'atr':          pos['atr'], 'size': size,
            'sl_dist':      round(sl_dist, 5),
            'pnl_usd':      pnl_usd, 'pnl_r': pnl_r,
            'result':       result, 'bars_held': 0,
            'spread':       0, 'tf': STRATEGY_TF,
            'be_triggered': int(be_triggered),
            'partial_done': int(partial_done),
            'supertrend':   0, 'rsi': 0,
            'ema_fast':     0, 'ema_slow': 0,
        }
        with open(TRADES_CSV, 'a', newline='', encoding='utf-8-sig') as f:
            csv.DictWriter(f, fieldnames=CSV_HEADERS).writerow(row)

        icon = '✅' if result == 'WIN' else ('❌' if result == 'LOSS' else '🔵')
        log(f'  {icon} CLOSED {pair} {dir_} | PnL=${pnl_usd:+.2f} ({pnl_r:+.2f}R)')

        nl = '
'
        be_txt = ' | BE ✅' if be_triggered else ''
        pt_txt = ' | Partial ✅' if partial_done else ''
        tg(
            f'{icon} *{pair} {dir_} — {result}*{be_txt}{pt_txt}{nl}'
            f'Entry: `{entry}` → Exit: `{exit_price}`{nl}'
            f'PnL: `${pnl_usd:+.2f}` | `{pnl_r:+.2f}R`{nl}'
            f'_{now.strftime("%Y-%m-%d %H:%M UTC")}_'
        )
        return result, pnl_usd
    except Exception as ex:
        log(f'  csv_log_trade ERROR: {ex}')
        return 'ERROR', 0.0


# ═══════════════════════════════════════════════════════
# API HELPERS
# ═══════════════════════════════════════════════════════
def _get(path, params=None, retries=3):
    for attempt in range(retries):
        try:
            r = requests.get(BASE_URL + path, headers=session_headers,
                             params=params, timeout=15)
            if r.status_code == 429:
                time.sleep(5 * (attempt + 1)); continue
            return r
        except requests.exceptions.RequestException as ex:
            log(f'  GET {path} [{attempt+1}]: {ex}')
            time.sleep(3 * (attempt + 1))
    return None

def _post(path, body, retries=2):
    for attempt in range(retries):
        try:
            return requests.post(BASE_URL + path, headers=session_headers,
                                 json=body, timeout=15)
        except requests.exceptions.RequestException as ex:
            log(f'  POST {path} [{attempt+1}]: {ex}')
            time.sleep(3 * (attempt + 1))
    return None

def _put(path, body):
    try:
        return requests.put(BASE_URL + path, headers=session_headers,
                            json=body, timeout=10)
    except Exception as ex:
        log(f'  PUT {path}: {ex}')
    return None


# ═══════════════════════════════════════════════════════
# SESSION
# ═══════════════════════════════════════════════════════
def create_session():
    url  = BASE_URL + '/api/v1/session'
    hdrs = {'X-CAP-API-KEY': API_KEY, 'Content-Type': 'application/json'}
    body = {'identifier': EMAIL, 'password': PASSWORD, 'encryptedPassword': False}
    try:
        r = requests.post(url, headers=hdrs, json=body, timeout=15)
        if r.status_code == 200:
            session_headers.update({
                'X-SECURITY-TOKEN': r.headers.get('X-SECURITY-TOKEN'),
                'CST':              r.headers.get('CST'),
                'Content-Type':     'application/json',
            })
            log('✅ Session OK')
            return True
        log(f'❌ Session FAILED [{r.status_code}]: {r.text[:100]}')
    except Exception as ex:
        log(f'❌ Session ERROR: {ex}')
    return False

def ping_session():
    _get('/api/v1/ping')

def get_balance():
    global ACCOUNT_BALANCE
    r = _get('/api/v1/accounts')
    if r and r.status_code == 200:
        accs = r.json().get('accounts', [])
        if accs:
            ACCOUNT_BALANCE = float(
                accs[0].get('balance', {}).get('available', ACCOUNT_BALANCE)
            )
            log(f'💰 Balance: ${round(ACCOUNT_BALANCE, 2)}')

def get_open_positions():
    r = _get('/api/v1/positions')
    if r and r.status_code == 200:
        return r.json().get('positions', [])
    return []

def get_instrument_meta(epic):
    now    = time.time()
    cached = _meta_cache.get(epic)
    if cached and (now - cached['ts']) < 300:
        return cached['data']
    r = _get(f'/api/v1/markets/{epic}')
    if not r or r.status_code != 200:
        return 0.0, 0.0, 0.0, 100.0, 0.1, 1000.0
    data       = r.json()
    snap       = data.get('snapshot',     {})
    instrument = data.get('instrument',   {})
    dealing    = data.get('dealingRules', {})
    bid    = float(snap.get('bid',   0) or 0)
    ask    = float(snap.get('offer', 0) or 0)
    spread = round(ask - bid, 5)
    cs     = float(instrument.get('contractSize', 100) or 100)
    min_sz = float((dealing.get('minDealSize') or {}).get('value', 0.1)  or 0.1)
    max_sz = float((dealing.get('maxDealSize') or {}).get('value', 1000) or 1000)
    result = (bid, ask, spread, cs, min_sz, max_sz)
    _meta_cache[epic] = {'ts': now, 'data': result}
    return result

def get_current_price(epic):
    """جلب السعر الحالي للزوج"""
    meta = get_instrument_meta(epic)
    bid, ask = meta[0], meta[1]
    return (bid + ask) / 2 if bid > 0 else 0.0

def update_sl_api(deal_id, new_sl, tp):
    """تعديل SL عبر API"""
    r = _put(f'/api/v1/positions/{deal_id}',
             {'stopLevel': new_sl, 'profitLevel': tp})
    if r and r.status_code == 200:
        log(f'  ✅ SL moved to {new_sl} (deal={deal_id})')
        return True
    log(f'  ❌ SL update failed (deal={deal_id})')
    return False

def close_partial_api(deal_id, size):
    """إغلاق جزئي عبر API"""
    r = _post(f'/api/v1/positions/{deal_id}',
              {'size': size})
    if r and r.status_code == 200:
        log(f'  💰 Partial close: {size} units (deal={deal_id})')
        return True
    log(f'  ❌ Partial close failed (deal={deal_id}): {r.text[:100] if r else "no response"}')
    return False


# ═══════════════════════════════════════════════════════
# TELEGRAM
# ═══════════════════════════════════════════════════════
def tg(text):
    if not TG_TOKEN:
        return
    try:
        requests.post(
            f'https://api.telegram.org/bot{TG_TOKEN}/sendMessage',
            data={'chat_id': TG_CHAT_ID, 'text': text, 'parse_mode': 'Markdown'},
            timeout=10
        )
    except Exception:
        pass

def tg_signal(sig, filters_info):
    icon = '🟢' if sig['direction'] == 'BUY' else '🔴'
    mode = 'DEMO' if DEMO_MODE else 'LIVE'
    rr   = round(TP_ATR_MULT / SL_ATR_MULT, 1)
    nl   = '
'
    tg(
        f'{icon} *{sig["pair"]} {sig["direction"]}* [{mode}]{nl}'
        f'Entry: `{sig["entry"]}` | SL: `{sig["sl"]}` | TP: `{sig["tp"]}`{nl}'
        f'R:R `1:{rr}` | Size: `{sig["size"]}`{nl}'
        f'ATR: `{sig["atr"]}` | Spread: `{sig["spread"]}`{nl}'
        f'BE عند `1R` | Partial TP عند `1.5R`{nl}'
        f'ST: `{"↑UP" if filters_info["st_dir"]==1 else "↓DN"}`'
        f' | RSI: `{filters_info["rsi"]:.1f}`{nl}'
        f'EMA: `{"✅" if filters_info["ema_cross"] else "❌"}`'
        f' | Trend×3: `{"✅" if filters_info["ema3"] else "❌"}`{nl}'
        f'TF: `{STRATEGY_TF}`{nl}'
        f'_{utc_now()}_'
    )

def tg_result(pair, direction, status, ref, error=''):
    icon = '✅' if status in ('ACCEPTED', 'SUCCESS') else '❌'
    nl   = '
'
    msg  = f'{icon} *{pair} {direction} {status}*{nl}Ref: `{ref}`'
    if error:
        msg += f'{nl}Err: `{error[:80]}`'
    tg(msg)


# ═══════════════════════════════════════════════════════
# CANDLES
# ═══════════════════════════════════════════════════════
def fetch_candles(epic, resolution, count=500):
    r = _get(f'/api/v1/prices/{epic}',
             params={'resolution': resolution, 'max': count})
    if not r or r.status_code != 200:
        return pd.DataFrame()
    prices = r.json().get('prices', [])
    if len(prices) < LENGTH * 3 + ATR_PERIOD:
        return pd.DataFrame()
    try:
        rows = [{
            'time':   p['snapshotTimeUTC'],
            'open':   (p['openPrice']['bid']  + p['openPrice']['ask'])  / 2,
            'high':   (p['highPrice']['bid']  + p['highPrice']['ask'])  / 2,
            'low':    (p['lowPrice']['bid']   + p['lowPrice']['ask'])   / 2,
            'close':  (p['closePrice']['bid'] + p['closePrice']['ask']) / 2,
            'volume': p.get('lastTradedVolume', 0) or 0,
        } for p in prices]
    except (KeyError, TypeError) as ex:
        log(f'  candle parse ERROR: {ex}')
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df['time'] = pd.to_datetime(df['time'], utc=True)
    return df.sort_values('time').reset_index(drop=True)


# ═══════════════════════════════════════════════════════
# INDICATORS
# ═══════════════════════════════════════════════════════
def calc_atr_series(df, period=14):
    tr = pd.concat([
        df['high'] - df['low'],
        (df['high'] - df['close'].shift()).abs(),
        (df['low']  - df['close'].shift()).abs()
    ], axis=1).max(axis=1)
    return tr.ewm(span=period, adjust=False).mean()

def calc_supertrend(df, period=10, multiplier=3.0):
    atr  = calc_atr_series(df, period)
    hl2  = (df['high'] + df['low']) / 2
    n    = len(df)
    upper = (hl2 + multiplier * atr).values
    lower = (hl2 - multiplier * atr).values
    close = df['close'].values
    final_upper = upper.copy()
    final_lower = lower.copy()
    supertrend  = np.zeros(n)
    direction   = np.ones(n, dtype=int)
    for i in range(1, n):
        final_upper[i] = upper[i] if (upper[i] < final_upper[i-1]
                                       or close[i-1] > final_upper[i-1]) \
                         else final_upper[i-1]
        final_lower[i] = lower[i] if (lower[i] > final_lower[i-1]
                                       or close[i-1] < final_lower[i-1]) \
                         else final_lower[i-1]
        if supertrend[i-1] == final_upper[i-1]:
            direction[i] = 1 if close[i] > final_upper[i] else -1
        else:
            direction[i] = -1 if close[i] < final_lower[i] else 1
        supertrend[i] = final_lower[i] if direction[i] == 1 else final_upper[i]
    return pd.Series(supertrend, index=df.index), pd.Series(direction, index=df.index)

def calc_ema(series, period):
    return series.ewm(span=period, adjust=False).mean()

def calc_ema_slope(series, period, lookback=3):
    return calc_ema(series, period).diff(lookback)

def calc_rsi(series, period=14):
    delta = series.diff()
    gain  = delta.clip(lower=0).ewm(span=period, adjust=False).mean()
    loss  = (-delta.clip(upper=    },
    'BTCUSD': {
        'epic':          'BTCUSD',
        'allow_buy':     True,
        'allow_sell':    True,
        'size_override': None,
    },
    'EURUSD': {
        'epic':          'EURUSD',
        'allow_buy':     True,
        'allow_sell':    True,
        'size_override': 1000,
    },
    'GBPUSD': {
        'epic':          'GBPUSD',
        'allow_buy':     True,
        'allow_sell':    True,
        'size_override': 1000,
    },
}

STRATEGY_TF   = os.getenv('STRATEGY_TF', 'MINUTE_15')
CANDLES_COUNT = 500
SCAN_INTERVAL = int(os.getenv('SCAN_INTERVAL', '180'))

LENGTH       = int(os.getenv('LENGTH',      '10'))
SLOPE_MULT   = float(os.getenv('SLOPE_MULT', '1.0'))
SLOPE_METHOD = os.getenv('SLOPE_METHOD',    'ATR')
ATR_PERIOD   = 14
SL_ATR_MULT  = 1.7
TP_ATR_MULT  = 1.0

# ── ✅ إعدادات Break-even + Partial TP الجديدة ──
BE_TRIGGER_R      = 1.0    # انقل SL للدخول عند تحقيق 1R ربح
PARTIAL_TP_R      = 1.5    # أغلق نصف الصفقة عند 1.5R
PARTIAL_TP_RATIO  = 0.5    # نسبة الإغلاق الجزئي (50%)
ENABLE_BE         = True   # تفعيل Break-even
ENABLE_PARTIAL_TP = True   # تفعيل Partial TP

SUPERTREND_PERIOD = 10
SUPERTREND_MULT   = 3.0
RSI_PERIOD        = 14
EMA_FAST          = 20
EMA_SLOW          = 50
EMA_TREND3_FAST   = 20
EMA_TREND3_MID    = 50
EMA_TREND3_SLOW   = 200
EMA_SLOPE_PERIOD  = 20
VOLUME_MA_PERIOD  = 20

SESSION_START = 00
SESSION_END   = 23

RISK_PERCENT         = float(os.getenv('RISK_PERCENT',    '0.01'))
MAX_OPEN_TRADES      = int(os.getenv('MAX_OPEN_TRADES',   '5'))
MAX_CONSECUTIVE_LOSS = int(os.getenv('MAX_CONSEC_LOSS',   '3'))
ACCOUNT_BALANCE      = float(os.getenv('ACCOUNT_BALANCE', '1000'))

_BASE_DIR  = os.getenv('DATA_DIR', os.path.dirname(os.path.abspath(__file__)))
DB_FILE    = os.path.join(_BASE_DIR, 'multi_bot.db')
TRADES_CSV = os.path.join(_BASE_DIR, 'trades_log.csv')

db_lock         = Lock()
session_headers = {}
_meta_cache: dict = {}

CSV_HEADERS = [
    'date', 'time_utc', 'pair', 'direction',
    'entry', 'sl', 'tp', 'exit_price',
    'atr', 'size', 'sl_dist', 'pnl_usd', 'pnl_r',
    'result', 'bars_held', 'spread', 'tf',
    'be_triggered', 'partial_done',
    'supertrend', 'rsi', 'ema_fast', 'ema_slow',
]


# ═══════════════════════════════════════════════════════
# UTILS
# ═══════════════════════════════════════════════════════
def utc_now():
    return datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')

def log(msg):
    print(f'[{utc_now()}] {msg}', flush=True)


# ═══════════════════════════════════════════════════════
# DATABASE
# ═══════════════════════════════════════════════════════
def db_init():
    with sqlite3.connect(DB_FILE) as conn:
        conn.execute('''CREATE TABLE IF NOT EXISTS trades (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            key          TEXT UNIQUE,
            pair         TEXT,
            direction    TEXT,
            timestamp    TEXT,
            entry        REAL, sl REAL, tp REAL,
            atr          REAL, size REAL, spread REAL DEFAULT 0,
            be_triggered INTEGER DEFAULT 0,
            partial_done INTEGER DEFAULT 0,
            status       TEXT DEFAULT 'PENDING'
        )''')
        # ── جدول الصفقات المفتوحة لمتابعة BE/Partial ──
        conn.execute('''CREATE TABLE IF NOT EXISTS open_positions (
            deal_id      TEXT PRIMARY KEY,
            pair         TEXT,
            direction    TEXT,
            entry        REAL,
            sl           REAL,
            tp           REAL,
            atr          REAL,
            size         REAL,
            db_key       TEXT,
            be_triggered INTEGER DEFAULT 0,
            partial_done INTEGER DEFAULT 0,
            opened_at    TEXT
        )''')
        conn.commit()

def db_save(key, pair, direction, entry, sl, tp, atr, size, spread=0.0):
    with db_lock:
        with sqlite3.connect(DB_FILE) as conn:
            try:
                conn.execute(
                    'INSERT INTO trades'
                    ' (key,pair,direction,timestamp,entry,sl,tp,atr,size,spread)'
                    ' VALUES (?,?,?,?,?,?,?,?,?,?)',
                    (key, pair, direction, utc_now(),
                     entry, sl, tp, atr, size, spread)
                )
                conn.commit()
            except sqlite3.IntegrityError:
                pass

def db_update(key, status):
    with db_lock:
        with sqlite3.connect(DB_FILE) as conn:
            conn.execute('UPDATE trades SET status=? WHERE key=?', (status, key))
            conn.commit()

def db_is_dup(key):
    with db_lock:
        with sqlite3.connect(DB_FILE) as conn:
            return conn.execute(
                'SELECT id FROM trades WHERE key=?', (key,)
            ).fetchone() is not None

def db_consec_losses(pair):
    with db_lock:
        with sqlite3.connect(DB_FILE) as conn:
            rows = conn.execute(
                "SELECT status FROM trades"
                " WHERE pair=? AND status IN ('WIN','LOSS')"
                " ORDER BY id DESC LIMIT 8", (pair,)
            ).fetchall()
            count = 0
            for r in rows:
                if r[0] == 'LOSS': count += 1
                else: break
            return count

# ── Open Positions tracking ──
def op_save(deal_id, pair, direction, entry, sl, tp, atr, size, db_key):
    with db_lock:
        with sqlite3.connect(DB_FILE) as conn:
            try:
                conn.execute(
                    'INSERT OR IGNORE INTO open_positions'
                    ' (deal_id,pair,direction,entry,sl,tp,atr,size,db_key,opened_at)'
                    ' VALUES (?,?,?,?,?,?,?,?,?,?)',
                    (deal_id, pair, direction, entry, sl, tp,
                     atr, size, db_key, utc_now())
                )
                conn.commit()
            except Exception as ex:
                log(f'op_save ERROR: {ex}')

def op_get_all():
    with db_lock:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute('SELECT * FROM open_positions').fetchall()
            return [dict(r) for r in rows]

def op_update(deal_id, be_triggered=None, partial_done=None, sl=None):
    with db_lock:
        with sqlite3.connect(DB_FILE) as conn:
            if be_triggered is not None:
                conn.execute('UPDATE open_positions SET be_triggered=? WHERE deal_id=?',
                             (int(be_triggered), deal_id))
            if partial_done is not None:
                conn.execute('UPDATE open_positions SET partial_done=? WHERE deal_id=?',
                             (int(partial_done), deal_id))
            if sl is not None:
                conn.execute('UPDATE open_positions SET sl=? WHERE deal_id=?',
                             (sl, deal_id))
            conn.commit()

def op_delete(deal_id):
    with db_lock:
        with sqlite3.connect(DB_FILE) as conn:
            conn.execute('DELETE FROM open_positions WHERE deal_id=?', (deal_id,))
            conn.commit()


# ═══════════════════════════════════════════════════════
# CSV LOGGER
# ═══════════════════════════════════════════════════════
def csv_init():
    if not Path(TRADES_CSV).exists():
        with open(TRADES_CSV, 'w', newline='', encoding='utf-8-sig') as f:
            csv.DictWriter(f, fieldnames=CSV_HEADERS).writeheader()

def csv_log_trade(pos, exit_price, be_triggered=False, partial_done=False):
    try:
        entry   = pos['entry']
        sl      = pos['sl']
        size    = pos['size']
        dir_    = pos['direction']
        pair    = pos['pair']

        sl_dist = abs(entry - sl)
        pnl_pts = (exit_price - entry) if dir_ == 'BUY' else (entry - exit_price)
        pnl_r   = round(pnl_pts / sl_dist, 2) if sl_dist > 0 else 0.0
        result  = 'WIN' if pnl_pts > 0 else ('LOSS' if pnl_pts < 0 else 'BE')

        meta    = _meta_cache.get(pos['pair'], {}).get('data')
        cs_val  = meta[3] if meta else 1.0
        pnl_usd = round(pnl_pts * size * cs_val, 2)

        now = datetime.now(timezone.utc)
        row = {
            'date':         now.strftime('%Y-%m-%d'),
            'time_utc':     now.strftime('%H:%M'),
            'pair':         pair, 'direction': dir_,
            'entry':        entry, 'sl': sl, 'tp': pos['tp'],
            'exit_price':   exit_price,
            'atr':          pos['atr'], 'size': size,
            'sl_dist':      round(sl_dist, 5),
            'pnl_usd':      pnl_usd, 'pnl_r': pnl_r,
            'result':       result, 'bars_held': 0,
            'spread':       0, 'tf': STRATEGY_TF,
            'be_triggered': int(be_triggered),
            'partial_done': int(partial_done),
            'supertrend':   0, 'rsi': 0,
            'ema_fast':     0, 'ema_slow': 0,
        }
        with open(TRADES_CSV, 'a', newline='', encoding='utf-8-sig') as f:
            csv.DictWriter(f, fieldnames=CSV_HEADERS).writerow(row)

        icon = '✅' if result == 'WIN' else ('❌' if result == 'LOSS' else '🔵')
        log(f'  {icon} CLOSED {pair} {dir_} | PnL=${pnl_usd:+.2f} ({pnl_r:+.2f}R)')

        nl = '\n'
        be_txt = ' | BE ✅' if be_triggered else ''
        pt_txt = ' | Partial ✅' if partial_done else ''
        tg(
            f'{icon} *{pair} {dir_} — {result}*{be_txt}{pt_txt}{nl}'
            f'Entry: `{entry}` → Exit: `{exit_price}`{nl}'
            f'PnL: `${pnl_usd:+.2f}` | `{pnl_r:+.2f}R`{nl}'
            f'_{now.strftime("%Y-%m-%d %H:%M UTC")}_'
        )
        return result, pnl_usd
    except Exception as ex:
        log(f'  csv_log_trade ERROR: {ex}')
        return 'ERROR', 0.0


# ═══════════════════════════════════════════════════════
# API HELPERS
# ═══════════════════════════════════════════════════════
def _get(path, params=None, retries=3):
    for attempt in range(retries):
        try:
            r = requests.get(BASE_URL + path, headers=session_headers,
                             params=params, timeout=15)
            if r.status_code == 429:
                time.sleep(5 * (attempt + 1)); continue
            return r
        except requests.exceptions.RequestException as ex:
            log(f'  GET {path} [{attempt+1}]: {ex}')
            time.sleep(3 * (attempt + 1))
    return None

def _post(path, body, retries=2):
    for attempt in range(retries):
        try:
            return requests.post(BASE_URL + path, headers=session_headers,
                                 json=body, timeout=15)
        except requests.exceptions.RequestException as ex:
            log(f'  POST {path} [{attempt+1}]: {ex}')
            time.sleep(3 * (attempt + 1))
    return None

def _put(path, body):
    try:
        return requests.put(BASE_URL + path, headers=session_headers,
                            json=body, timeout=10)
    except Exception as ex:
        log(f'  PUT {path}: {ex}')
    return None


# ═══════════════════════════════════════════════════════
# SESSION
# ═══════════════════════════════════════════════════════
def create_session():
    url  = BASE_URL + '/api/v1/session'
    hdrs = {'X-CAP-API-KEY': API_KEY, 'Content-Type': 'application/json'}
    body = {'identifier': EMAIL, 'password': PASSWORD, 'encryptedPassword': False}
    try:
        r = requests.post(url, headers=hdrs, json=body, timeout=15)
        if r.status_code == 200:
            session_headers.update({
                'X-SECURITY-TOKEN': r.headers.get('X-SECURITY-TOKEN'),
                'CST':              r.headers.get('CST'),
                'Content-Type':     'application/json',
            })
            log('✅ Session OK')
            return True
        log(f'❌ Session FAILED [{r.status_code}]: {r.text[:100]}')
    except Exception as ex:
        log(f'❌ Session ERROR: {ex}')
    return False

def ping_session():
    _get('/api/v1/ping')

def get_balance():
    global ACCOUNT_BALANCE
    r = _get('/api/v1/accounts')
    if r and r.status_code == 200:
        accs = r.json().get('accounts', [])
        if accs:
            ACCOUNT_BALANCE = float(
                accs[0].get('balance', {}).get('available', ACCOUNT_BALANCE)
            )
            log(f'💰 Balance: ${round(ACCOUNT_BALANCE, 2)}')

def get_open_positions():
    r = _get('/api/v1/positions')
    if r and r.status_code == 200:
        return r.json().get('positions', [])
    return []

def get_instrument_meta(epic):
    now    = time.time()
    cached = _meta_cache.get(epic)
    if cached and (now - cached['ts']) < 300:
        return cached['data']
    r = _get(f'/api/v1/markets/{epic}')
    if not r or r.status_code != 200:
        return 0.0, 0.0, 0.0, 100.0, 0.1, 1000.0
    data       = r.json()
    snap       = data.get('snapshot',     {})
    instrument = data.get('instrument',   {})
    dealing    = data.get('dealingRules', {})
    bid    = float(snap.get('bid',   0) or 0)
    ask    = float(snap.get('offer', 0) or 0)
    spread = round(ask - bid, 5)
    cs     = float(instrument.get('contractSize', 100) or 100)
    min_sz = float((dealing.get('minDealSize') or {}).get('value', 0.1)  or 0.1)
    max_sz = float((dealing.get('maxDealSize') or {}).get('value', 1000) or 1000)
    result = (bid, ask, spread, cs, min_sz, max_sz)
    _meta_cache[epic] = {'ts': now, 'data': result}
    return result

def get_current_price(epic):
    """جلب السعر الحالي للزوج"""
    meta = get_instrument_meta(epic)
    bid, ask = meta[0], meta[1]
    return (bid + ask) / 2 if bid > 0 else 0.0

def update_sl_api(deal_id, new_sl, tp):
    """تعديل SL عبر API"""
    r = _put(f'/api/v1/positions/{deal_id}',
             {'stopLevel': new_sl, 'profitLevel': tp})
    if r and r.status_code == 200:
        log(f'  ✅ SL moved to {new_sl} (deal={deal_id})')
        return True
    log(f'  ❌ SL update failed (deal={deal_id})')
    return False

def close_partial_api(deal_id, size):
    """إغلاق جزئي عبر API"""
    r = _post(f'/api/v1/positions/{deal_id}',
              {'size': size})
    if r and r.status_code == 200:
        log(f'  💰 Partial close: {size} units (deal={deal_id})')
        return True
    log(f'  ❌ Partial close failed (deal={deal_id}): {r.text[:100] if r else "no response"}')
    return False


# ═══════════════════════════════════════════════════════
# TELEGRAM
# ═══════════════════════════════════════════════════════
def tg(text):
    if not TG_TOKEN:
        return
    try:
        requests.post(
            f'https://api.telegram.org/bot{TG_TOKEN}/sendMessage',
            data={'chat_id': TG_CHAT_ID, 'text': text, 'parse_mode': 'Markdown'},
            timeout=10
        )
    except Exception:
        pass

def tg_signal(sig, filters_info):
    icon = '🟢' if sig['direction'] == 'BUY' else '🔴'
    mode = 'DEMO' if DEMO_MODE else 'LIVE'
    rr   = round(TP_ATR_MULT / SL_ATR_MULT, 1)
    nl   = '\n'
    tg(
        f'{icon} *{sig["pair"]} {sig["direction"]}* [{mode}]{nl}'
        f'Entry: `{sig["entry"]}` | SL: `{sig["sl"]}` | TP: `{sig["tp"]}`{nl}'
        f'R:R `1:{rr}` | Size: `{sig["size"]}`{nl}'
        f'ATR: `{sig["atr"]}` | Spread: `{sig["spread"]}`{nl}'
        f'BE عند `1R` | Partial TP عند `1.5R`{nl}'
        f'ST: `{"↑UP" if filters_info["st_dir"]==1 else "↓DN"}`'
        f' | RSI: `{filters_info["rsi"]:.1f}`{nl}'
        f'EMA: `{"✅" if filters_info["ema_cross"] else "❌"}`'
        f' | Trend×3: `{"✅" if filters_info["ema3"] else "❌"}`{nl}'
        f'TF: `{STRATEGY_TF}`{nl}'
        f'_{utc_now()}_'
    )

def tg_result(pair, direction, status, ref, error=''):
    icon = '✅' if status in ('ACCEPTED', 'SUCCESS') else '❌'
    nl   = '\n'
    msg  = f'{icon} *{pair} {direction} {status}*{nl}Ref: `{ref}`'
    if error:
        msg += f'{nl}Err: `{error[:80]}`'
    tg(msg)


# ═══════════════════════════════════════════════════════
# CANDLES
# ═══════════════════════════════════════════════════════
def fetch_candles(epic, resolution, count=500):
    r = _get(f'/api/v1/prices/{epic}',
             params={'resolution': resolution, 'max': count})
    if not r or r.status_code != 200:
        return pd.DataFrame()
    prices = r.json().get('prices', [])
    if len(prices) < LENGTH * 3 + ATR_PERIOD:
        return pd.DataFrame()
    try:
        rows = [{
            'time':   p['snapshotTimeUTC'],
            'open':   (p['openPrice']['bid']  + p['openPrice']['ask'])  / 2,
            'high':   (p['highPrice']['bid']  + p['highPrice']['ask'])  / 2,
            'low':    (p['lowPrice']['bid']   + p['lowPrice']['ask'])   / 2,
            'close':  (p['closePrice']['bid'] + p['closePrice']['ask']) / 2,
            'volume': p.get('lastTradedVolume', 0) or 0,
        } for p in prices]
    except (KeyError, TypeError) as ex:
        log(f'  candle parse ERROR: {ex}')
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df['time'] = pd.to_datetime(df['time'], utc=True)
    return df.sort_values('time').reset_index(drop=True)


# ═══════════════════════════════════════════════════════
# INDICATORS
# ═══════════════════════════════════════════════════════
def calc_atr_series(df, period=14):
    tr = pd.concat([
        df['high'] - df['low'],
        (df['high'] - df['close'].shift()).abs(),
        (df['low']  - df['close'].shift()).abs()
    ], axis=1).max(axis=1)
    return tr.ewm(span=period, adjust=False).mean()

def calc_supertrend(df, period=10, multiplier=3.0):
    atr  = calc_atr_series(df, period)
    hl2  = (df['high'] + df['low']) / 2
    n    = len(df)
    upper = (hl2 + multiplier * atr).values
    lower = (hl2 - multiplier * atr).values
    close = df['close'].values
    final_upper = upper.copy()
    final_lower = lower.copy()
    supertrend  = np.zeros(n)
    direction   = np.ones(n, dtype=int)
    for i in range(1, n):
        final_upper[i] = upper[i] if (upper[i] < final_upper[i-1]
                                       or close[i-1] > final_upper[i-1]) \
                         else final_upper[i-1]
        final_lower[i] = lower[i] if (lower[i] > final_lower[i-1]
                                       or close[i-1] < final_lower[i-1]) \
                         else final_lower[i-1]
        if supertrend[i-1] == final_upper[i-1]:
            direction[i] = 1 if close[i] > final_upper[i] else -1
        else:
            direction[i] = -1 if close[i] < final_lower[i] else 1
        supertrend[i] = final_lower[i] if direction[i] == 1 else final_upper[i]
    return pd.Series(supertrend, index=df.index), pd.Series(direction, index=df.index)

def calc_ema(series, period):
    return series.ewm(span=period, adjust=False).mean()

def calc_ema_slope(series, period, lookback=3):
    return calc_ema(series, period).diff(lookback)

def calc_rsi(series, period=14):
    delta = series.diff()
    gain  = delta.clip(lower=0).ewm(span=period, adjust=False).mean()
    loss  = (-delta.clip(upper=0)).ewm(span=period, adjust=False).mean()
    rs    = gain / loss.replace(0, 1e-10)
    return 100 - (100 / (1 + rs))

def calc_volume_filter(volume_series, period=20):
    return volume_series > volume_series.rolling(period).mean()

def find_pivot_high(high_series, length):
    n, pivots = len(high_series), [np.nan] * len(high_series)
    for i in range(length, n - length):
        if high_series.iloc[i] == high_series.iloc[i-length: i+length+1].max():
            pivots[i] = high_series.iloc[i]
    return pivots

def find_pivot_low(low_series, length):
    n, pivots = len(low_series), [np.nan] * len(low_series)
    for i in range(length, n - length):
        if low_series.iloc[i] == low_series.iloc[i-length: i+length+1].min():
            pivots[i] = low_series.iloc[i]
    return pivots

def get_slope_val(method, df, idx, length, mult, atr_series):
    atr_val = float(atr_series.iloc[idx]) if not np.isnan(atr_series.iloc[idx]) else 1e-6
    if method == 'Stdev':
        stdev = df['close'].iloc[max(0, idx-length+1):idx+1].std()
        return (stdev * mult / length) if (not np.isnan(stdev) and stdev > 0) \
               else (atr_val * mult / length)
    elif method == 'Linreg':
        y = df['close'].iloc[max(0, idx-length+1):idx+1].values
        if len(y) >= 2:
            return abs(np.polyfit(np.arange(len(y)), y, 1)[0]) * mult
    return atr_val * mult / length

def tl_value(anchor_idx, anchor_val, slope, goes_up, cur_idx):
    bars = cur_idx - anchor_idx
    return anchor_val + slope * bars if goes_up else anchor_val - slope * bars


# ═══════════════════════════════════════════════════════
# ✅ MANAGE OPEN POSITIONS — Break-even + Partial TP
# ═══════════════════════════════════════════════════════
def manage_open_positions():
    """
    يُشغَّل في كل scan لمتابعة الصفقات المفتوحة:
    1. Break-even: انقل SL للدخول فور تحقيق BE_TRIGGER_R
    2. Partial TP: أغلق PARTIAL_TP_RATIO% عند PARTIAL_TP_R
    """
    if not ENABLE_BE and not ENABLE_PARTIAL_TP:
        return

    tracked = op_get_all()
    if not tracked:
        return

    live_pos = get_open_positions()
    live_ids = {p.get('position', {}).get('dealId', '') for p in live_pos}

    for pos in tracked:
        deal_id = pos['deal_id']

        # ── الصفقة أُغلقت بالسوق (SL/TP ضُرب) ──
        if deal_id not in live_ids:
            log(f'  📋 {pos["pair"]} {deal_id} أُغلقت — نسجّلها')
            # جلب سعر الإغلاق من API إن أمكن
            exit_price = get_current_price(pos['pair'])
            if exit_price > 0:
                result, pnl = csv_log_trade(
                    pos, exit_price,
                    be_triggered=bool(pos['be_triggered']),
                    partial_done=bool(pos['partial_done'])
                )
                db_update(pos['db_key'], result.upper() if result in ('WIN','LOSS','BE') else 'CLOSED')
            op_delete(deal_id)
            continue

        # ── احسب الربح الحالي بالـ R ──
        cur_price = get_current_price(pos['pair'])
        if cur_price <= 0:
            continue

        entry   = pos['entry']
        sl      = pos['sl']
        tp      = pos['tp']
        atr     = pos['atr']
        size    = pos['size']
        dir_    = pos['direction']
        sl_dist = abs(entry - sl)
        if sl_dist <= 0:
            continue

        profit_pts = (cur_price - entry) if dir_ == 'BUY' else (entry - cur_price)
        profit_r   = profit_pts / sl_dist

        log(f'  📊 {pos["pair"]} {dir_} | R={profit_r:.2f} | BE={pos["be_triggered"]} | Partial={pos["partial_done"]}')

        # ── [1] Partial TP عند PARTIAL_TP_R ──
        if ENABLE_PARTIAL_TP and not pos['partial_done'] and profit_r >= PARTIAL_TP_R:
            _, _, _, cs, min_sz, _ = get_instrument_meta(pos['pair'])
            partial_size = round(size * PARTIAL_TP_RATIO, 2)
            if partial_size >= min_sz:
                ok = close_partial_api(deal_id, partial_size)
                if ok:
                    op_update(deal_id, partial_done=True)
                    tg(
                        f'💰 *Partial TP — {pos["pair"]} {dir_}*\n'
                        f'أغلقنا `{partial_size}` وحدة عند `{round(cur_price,5)}`\n'
                        f'R: `+{profit_r:.2f}R` | الباقي يجري مع BE\n'
                        f'_{utc_now()}_'
                    )

        # ── [2] Break-even عند BE_TRIGGER_R ──
        if ENABLE_BE and not pos['be_triggered'] and profit_r >= BE_TRIGGER_R:
            new_sl = round(entry + 0.00001, 5) if dir_ == 'BUY' \
                     else round(entry - 0.00001, 5)
            ok = update_sl_api(deal_id, new_sl, tp)
            if ok:
                op_update(deal_id, be_triggered=True, sl=new_sl)
                tg(
                    f'🔒 *Break-even — {pos["pair"]} {dir_}*\n'
                    f'SL نُقل للدخول: `{new_sl}`\n'
                    f'السعر الحالي: `{round(cur_price,5)}` | R: `+{profit_r:.2f}R`\n'
                    f'_{utc_now()}_'
                )


# ═══════════════════════════════════════════════════════
# SIGNAL DETECTION
# ═══════════════════════════════════════════════════════
def check_signal(pair_name, config):
    epic       = config['epic']
    allow_buy  = config['allow_buy']
    allow_sell = config['allow_sell']
    if not allow_buy and not allow_sell:
        return None

    df = fetch_candles(epic, STRATEGY_TF, CANDLES_COUNT)
    if df.empty or len(df) < max(LENGTH * 3 + ATR_PERIOD, EMA_TREND3_SLOW + 10):
        log(f'  {pair_name}: بيانات غير كافية')
        return None

    df_c = df.iloc[:-1].copy().reset_index(drop=True)
    n    = len(df_c)

    atr_s      = calc_atr_series(df_c, ATR_PERIOD)
    ph_arr     = find_pivot_high(df_c['high'], LENGTH)
    pl_arr     = find_pivot_low(df_c['low'],   LENGTH)
    st_line, st_dir = calc_supertrend(df_c, SUPERTREND_PERIOD, SUPERTREND_MULT)
    ema_fast   = calc_ema(df_c['close'], EMA_FAST)
    ema_slow   = calc_ema(df_c['close'], EMA_SLOW)
    ema_t3_f   = calc_ema(df_c['close'], EMA_TREND3_FAST)
    ema_t3_m   = calc_ema(df_c['close'], EMA_TREND3_MID)
    ema_t3_s   = calc_ema(df_c['close'], EMA_TREND3_SLOW)
    ema_slope  = calc_ema_slope(df_c['close'], EMA_SLOPE_PERIOD)
    rsi_s      = calc_rsi(df_c['close'], RSI_PERIOD)
    vol_filter = calc_volume_filter(df_c['volume'], VOLUME_MA_PERIOD)

    upper_tl = lower_tl = None
    for i in range(LENGTH + ATR_PERIOD, n):
        atr_i = float(atr_s.iloc[i])
        if np.isnan(atr_i) or atr_i <= 0:
            continue
        ph = ph_arr[i]
        pl = pl_arr[i]
        if not np.isnan(ph):
            upper_tl = (i, float(ph), get_slope_val(SLOPE_METHOD, df_c, i, LENGTH, SLOPE_MULT, atr_s))
        if not np.isnan(pl):
            lower_tl = (i, float(pl), get_slope_val(SLOPE_METHOD, df_c, i, LENGTH, SLOPE_MULT, atr_s))

    last_idx   = n - 1
    last_close = float(df_c['close'].iloc[last_idx])
    last_atr   = float(atr_s.iloc[last_idx])
    if np.isnan(last_atr) or last_atr <= 0:
        return None

    cur_st_dir = int(st_dir.iloc[last_idx])
    cur_ema_f  = float(ema_fast.iloc[last_idx])
    cur_ema_s  = float(ema_slow.iloc[last_idx])
    cur_ema3_f = float(ema_t3_f.iloc[last_idx])
    cur_ema3_m = float(ema_t3_m.iloc[last_idx])
    cur_ema3_s = float(ema_t3_s.iloc[last_idx])
    cur_slope  = float(ema_slope.iloc[last_idx])
    cur_rsi    = float(rsi_s.iloc[last_idx])
    cur_vol    = bool(vol_filter.iloc[last_idx])

    signal = None

    if allow_buy and upper_tl is not None:
        ai, av, sv = upper_tl
        if ai < last_idx - 1:
            u_val = tl_value(ai, av, sv, False, last_idx)
            if last_close > u_val:
                filters = [
                    cur_st_dir == 1,
                    cur_ema_f > cur_ema_s,
                    cur_ema3_f > cur_ema3_m > cur_ema3_s,
                    cur_slope > 0,
                    cur_rsi > 50,
                    cur_vol,
                ]
                if all(filters):
                    signal = 'BUY'
                    log(f'  {pair_name}: 🟢 BUY | 6/6 ✅')
                else:
                    failed = [['ST','EMA','Trend×3','Slope',f'RSI({cur_rsi:.0f})','Vol'][i]
                              for i, f in enumerate(filters) if not f]
                    log(f'  {pair_name}: BUY TL ✅ فلاتر فشلت: {", ".join(failed)}')

    if allow_sell and signal is None and lower_tl is not None:
        ai, av, sv = lower_tl
        if ai < last_idx - 1:
            l_val = tl_value(ai, av, sv, True, last_idx)
            if last_close < l_val:
                filters = [
                    cur_st_dir == -1,
                    cur_ema_f < cur_ema_s,
                    cur_ema3_f < cur_ema3_m < cur_ema3_s,
                    cur_slope < 0,
                    cur_rsi < 50,
                    cur_vol,
                ]
                if all(filters):
                    signal = 'SELL'
                    log(f'  {pair_name}: 🔴 SELL | 6/6 ✅')
                else:
                    failed = [['ST','EMA','Trend×3','Slope',f'RSI({cur_rsi:.0f})','Vol'][i]
                              for i, f in enumerate(filters) if not f]
                    log(f'  {pair_name}: SELL TL ✅ فلاتر فشلت: {", ".join(failed)}')

    if not signal:
        return None

    bid, ask, spread, cs, min_sz, max_sz = get_instrument_meta(epic)
    if bid <= 0 or ask <= 0:
        return None

    entry = ask if signal == 'BUY' else bid
    sl    = round(entry - SL_ATR_MULT * last_atr, 5) if signal == 'BUY' \
            else round(entry + SL_ATR_MULT * last_atr, 5)
    tp    = round(entry + TP_ATR_MULT * last_atr, 5) if signal == 'BUY' \
            else round(entry - TP_ATR_MULT * last_atr, 5)

    sl_dist = abs(entry - sl)
    if sl_dist < last_atr * 0.1:
        return None

    if config.get('size_override') is not None:
        size = max(min(float(config['size_override']), max_sz), min_sz)
    else:
        risk_usd = ACCOUNT_BALANCE * RISK_PERCENT
        raw_size = round(risk_usd / (sl_dist * cs), 2)
        size     = max(min(raw_size, max_sz), min_sz)

    return {
        'pair':      pair_name, 'epic': epic,
        'direction': signal,
        'entry':     round(entry, 5),
        'sl': sl,   'tp': tp,
        'atr':       round(last_atr, 5),
        'size':      size,
        'spread':    round(spread, 5),
        'filters_info': {
            'st_dir':    cur_st_dir,
            'rsi':       cur_rsi,
            'ema_cross': cur_ema_f > cur_ema_s if signal=='BUY' else cur_ema_f < cur_ema_s,
            'ema3':      (cur_ema3_f > cur_ema3_m > cur_ema3_s) if signal=='BUY'
                         else (cur_ema3_f < cur_ema3_m < cur_ema3_s),
            'slope':     cur_slope > 0 if signal=='BUY' else cur_slope < 0,
            'vol':       cur_vol,
        },
    }


# ═══════════════════════════════════════════════════════
# EXECUTE ORDER
# ═══════════════════════════════════════════════════════
def execute_order(sig):
    body = {
        'epic':           sig['epic'],
        'direction':      sig['direction'],
        'size':           sig['size'],
        'guaranteedStop': False,
        'trailingStop':   False,
        'stopLevel':      sig['sl'],
        'profitLevel':    sig['tp'],
    }
    log(f'  📤 {sig["pair"]} {sig["direction"]} | '
        f'entry≈{sig["entry"]} SL={sig["sl"]} TP={sig["tp"]} size={sig["size"]}')

    r = _post('/api/v1/positions', body)
    if not r:
        tg_result(sig['pair'], sig['direction'], 'ERROR', 'N/A', 'no response')
        return 'ERROR', 'no response'

    data = r.json()
    log(f'  RESP [{r.status_code}]: {json.dumps(data)[:200]}')

    if r.status_code == 200:
        deal_ref = data.get('dealReference', 'N/A')
        time.sleep(2)
        rc = _get(f'/api/v1/confirms/{deal_ref}')
        if rc and rc.status_code == 200:
            confirm  = rc.json()
            status   = confirm.get('dealStatus', 'UNKNOWN')
            deal_id  = confirm.get('dealId', deal_ref)
            reason   = confirm.get('reason', '')
            tg_result(sig['pair'], sig['direction'], status, deal_ref, reason)

            # ── سجّل الصفقة لمتابعة BE/Partial ──
            if status in ('ACCEPTED', 'SUCCESS'):
                op_save(
                    deal_id, sig['pair'], sig['direction'],
                    sig['entry'], sig['sl'], sig['tp'],
                    sig['atr'], sig['size'],
                    f'{sig["pair"]}_{datetime.now(timezone.utc).strftime("%Y-%m-%d_%H")}'
                )
            return status, deal_ref
        return 'UNKNOWN', deal_ref
    else:
        err = data.get('errorCode', str(data)[:80])
        tg_result(sig['pair'], sig['direction'], 'FAILED', 'N/A', err)
        return 'FAILED', err


# ═══════════════════════════════════════════════════════
# SCAN
# ═══════════════════════════════════════════════════════
def run_scan():
    now = datetime.now(timezone.utc)
    if now.weekday() >= 7:
        log('⏸  عطلة نهاية الأسبوع')
        return
    if not (SESSION_START <= now.hour < SESSION_END):
        log(f'⏸  خارج الجلسة ({now.hour:02d}:00 UTC)')
        return

    log('─' * 60)
    log(f'🔍 Scan | TF={STRATEGY_TF} | {"DEMO" if DEMO_MODE else "LIVE"}')
    log('─' * 60)

    get_balance()

    # ── ✅ تشغيل إدارة الصفقات المفتوحة أولاً ──
    manage_open_positions()

    open_pos = get_open_positions()
    log(f'  مفتوحة: {len(open_pos)} / {MAX_OPEN_TRADES}')
    if len(open_pos) >= MAX_OPEN_TRADES:
        log('  ⏸  الحد الأقصى')
        return

    ts_key = now.strftime('%Y-%m-%d_%H')

    for pair_name, config in PAIRS.items():
        if len(open_pos) >= MAX_OPEN_TRADES:
            break
        consec = db_consec_losses(pair_name)
        if consec >= MAX_CONSECUTIVE_LOSS:
            log(f'  {pair_name}: ⚠️  {consec} خسائر — تخطّي')
            continue
        key = f'{pair_name}_{ts_key}'
        if db_is_dup(key):
            log(f'  {pair_name}: ⏸  مكرر')
            continue
        log(f'  {pair_name}: فحص ...')
        sig = check_signal(pair_name, config)
        if sig is None:
            log(f'  {pair_name}: لا إشارة')
            continue
        db_save(key, pair_name, sig['direction'],
                sig['entry'], sig['sl'], sig['tp'],
                sig['atr'], sig['size'], sig['spread'])
        tg_signal(sig, sig['filters_info'])
        status, ref = execute_order(sig)
        db_update(key, status)
        log(f'  {pair_name}: {status} | {ref}')
        open_pos = get_open_positions()
        time.sleep(2)


# ═══════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════
def start_bot():
    db_init()
    csv_init()
    mode = 'DEMO' if DEMO_MODE else 'LIVE'
    nl   = '\n'

    print('=' * 60, flush=True)
    print(f'  Multi-Pairs TL Breaks Bot [{mode}]',              flush=True)
    print(f'  TF: {STRATEGY_TF} | Candles: {CANDLES_COUNT}',   flush=True)
    print(f'  SL/TP: {SL_ATR_MULT}/{TP_ATR_MULT}×ATR',         flush=True)
    print(f'  ✅ Break-even عند {BE_TRIGGER_R}R',               flush=True)
    print(f'  ✅ Partial TP {int(PARTIAL_TP_RATIO*100)}% عند {PARTIAL_TP_R}R', flush=True)
    print(f'  Filters: ST({SUPERTREND_PERIOD}) | EMA {EMA_FAST}/{EMA_SLOW}'
          f' | Trend×3 | Slope | RSI({RSI_PERIOD}) | Volume',   flush=True)
    for pn, pc in PAIRS.items():
        sz = pc.get('size_override', 'AUTO')
        print(f'    {pn:<8}: BUY={"✅" if pc["allow_buy"] else "❌"}'
              f' SELL={"✅" if pc["allow_sell"] else "❌"} Size={sz}', flush=True)
    print('=' * 60, flush=True)

    tg(
        f'🚀 *Multi-Pairs Bot* [{mode}]{nl}'
        f'TF: `{STRATEGY_TF}` | SL/TP: `{SL_ATR_MULT}/{TP_ATR_MULT}×ATR`{nl}'
        f'🔒 Break-even: عند `{BE_TRIGGER_R}R`{nl}'
        f'💰 Partial TP: `{int(PARTIAL_TP_RATIO*100)}%` عند `{PARTIAL_TP_R}R`{nl}'
        f'Filters: `ST | EMA | Trend×3 | Slope | RSI | Volume`{nl}'
        f'Pairs: `{"` | `".join(PAIRS.keys())}`{nl}'
        f'_{utc_now()}_'
    )

    session_age = 0
    while True:
        try:
            if session_age == 0:
                if not create_session():
                    time.sleep(60)
                    continue
            else:
                ping_session()
            session_age = (session_age + 1) % 25
            run_scan()
        except KeyboardInterrupt:
            log('🛑 Bot stopped')
            tg('🛑 Bot stopped')
            break
        except Exception as ex:
            log(f'LOOP ERROR: {ex}')
            tg(f'❌ Bot Error: `{str(ex)[:100]}`')
        time.sleep(SCAN_INTERVAL)


if __name__ == '__main__':
    start_bot()
