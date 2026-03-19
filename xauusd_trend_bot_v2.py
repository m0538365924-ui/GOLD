#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ==========================================================
# xauusd_trend_bot_v2.py — XAUUSD BUY Only | H4 Trend + H1
# النسخة المُحسَّنة — 6 إصلاحات جوهرية
# ==========================================================
# الإصلاحات:
# 1. Position Size: يجلب contractSize من API بدل افتراض ثابت
# 2. Entry: Limit Order بدل Market (انتظار retrace 0.2 ATR)
# 3. TP: R:R ثابت 1:2 بدل EMA50 كهدف
# 4. RSI: ضيّقنا من 30-60 إلى 40-55
# 5. Trailing Stop ديناميكي: 2 ATR أول 1R، ثم 3 ATR بعده
# 6. Spread Filter: نتخطى إذا Spread > حد معين
# 7. Pullback أعمق: 0.2 ATR كحد أدنى للعمق
# ==========================================================

import os, requests, json, time, sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timezone
from threading import Lock
from dotenv import load_dotenv

load_dotenv()

# ================================================
# ⚙️  CONFIG
# ================================================
API_KEY    = os.getenv('CAPITAL_API_KEY',  'BbmFhEF3FffkcR0Y')
EMAIL      = os.getenv('CAPITAL_EMAIL',    'almorese2013@gmail.com')
PASSWORD   = os.getenv('CAPITAL_PASSWORD', 'Ba050326>')
TG_TOKEN   = os.getenv('TG_TOKEN',         '8782238258:AAEtuQg7OYAmoemhWfLqKdYpqIxfWwyKRSQ')
TG_CHAT_ID = os.getenv('TG_CHAT_ID',       '533243705')

GOLD_EPIC  = 'GOLD'      # جرّب GOLD أو XAUUSD
DEMO_MODE  = False        # ← False للـ Live
BASE_URL   = 'https://api-capital.backend-capital.com'

# ================================================
# 💰 RISK
# ================================================
RISK_PERCENT         = 0.01
MAX_OPEN_TRADES      = 1
MAX_CONSECUTIVE_LOSS = 4
ACCOUNT_BALANCE      = 1000.0

# ================================================
# 📊 STRATEGY
# ================================================
H4_EMA_FAST    = 21
H4_EMA_MID     = 50
H4_EMA_SLOW    = 200
H4_ADX_MIN     = 25

H1_EMA_ENTRY   = 21
H1_RSI_MIN     = 40      # إصلاح 4: ضيّقنا من 30 إلى 40
H1_RSI_MAX     = 55      # إصلاح 4: ضيّقنا من 60 إلى 55
H1_RSI_PERIOD  = 14

ATR_PERIOD     = 14
SL_ATR_MULT    = 1.0
TP_RR          = 2.0     # إصلاح 3: R:R ثابت 1:2

# إصلاح 5: Trailing ديناميكي
TRAIL_INITIAL  = 2.0     # 2 ATR قبل 1R
TRAIL_AFTER_1R = 3.0     # 3 ATR بعد 1R (نعطي مساحة أكثر)

# إصلاح 2: Limit Order
LIMIT_RETRACE_ATR = 0.2  # ننتظر retrace 0.2 ATR عن الـ EMA21

# إصلاح 7: Pullback عمق
PULLBACK_MIN_ATR = 0.2   # low[-2] لازم ينزل 0.2 ATR تحت EMA21

# إصلاح 6: Spread Filter
MAX_SPREAD       = 1.5   # دولار — فوق هذا نتخطى

TRADE_START_H  = 00
TRADE_END_H    = 23:49
SCAN_INTERVAL  = 300
DB_FILE        = 'xauusd_v2.db'

session_headers = {}
db_lock         = Lock()

# لتتبع الـ Limit Orders المعلقة
pending_limit   = {}   # {key: {entry_limit, sl, tp, expiry_time}}


# ================================================
# 🗄️ DATABASE
# ================================================
def db_init():
    with sqlite3.connect(DB_FILE) as conn:
        conn.execute('''CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            key TEXT UNIQUE, timestamp TEXT,
            entry_limit REAL, entry_actual REAL,
            sl REAL, tp REAL, atr REAL,
            adx_h4 REAL, rsi_h1 REAL,
            spread REAL, status TEXT DEFAULT 'PENDING')''')
        conn.commit()

def db_save_signal(key, data):
    with db_lock:
        with sqlite3.connect(DB_FILE) as conn:
            try:
                conn.execute(
                    '''INSERT INTO signals
                    (key,timestamp,entry_limit,sl,tp,atr,adx_h4,rsi_h1,spread)
                    VALUES (?,?,?,?,?,?,?,?,?)''',
                    (key, data['timestamp'], data['entry_limit'],
                     data['sl'], data['tp'], data['atr'],
                     data['adx_h4'], data['rsi_h1'], data['spread'])
                )
                conn.commit()
            except sqlite3.IntegrityError:
                pass

def db_update(key, status, entry_actual=None):
    with db_lock:
        with sqlite3.connect(DB_FILE) as conn:
            if entry_actual:
                conn.execute(
                    'UPDATE signals SET status=?, entry_actual=? WHERE key=?',
                    (status, entry_actual, key)
                )
            else:
                conn.execute('UPDATE signals SET status=? WHERE key=?', (status, key))
            conn.commit()

def db_is_duplicate(key):
    with db_lock:
        with sqlite3.connect(DB_FILE) as conn:
            return conn.execute(
                'SELECT id FROM signals WHERE key=?', (key,)
            ).fetchone() is not None

def db_consecutive_losses():
    with db_lock:
        with sqlite3.connect(DB_FILE) as conn:
            rows = conn.execute(
                "SELECT status FROM signals WHERE status IN ('ACCEPTED','FAILED')"
                " ORDER BY id DESC LIMIT 6"
            ).fetchall()
            count = 0
            for r in rows:
                if r[0] == 'FAILED': count += 1
                else: break
            return count


# ================================================
# 🔧 UTILITIES
# ================================================
def utc_now():
    return datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')

def today_key():
    return datetime.now(timezone.utc).strftime('%Y-%m-%d')

def log(msg):
    print(f'[{utc_now()}] {msg}', flush=True)

def is_trade_window():
    now = datetime.now(timezone.utc)
    if now.weekday() >= 5: return False
    return TRADE_START_H <= now.hour < TRADE_END_H


# ================================================
# 🌐 SESSION
# ================================================
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
                'Content-Type':     'application/json'
            })
            log('✅ Session OK')
            return True
        log('❌ Session FAILED: ' + r.text[:80])
    except Exception as ex:
        log('❌ Session ERROR: ' + str(ex))
    return False

def ping_session():
    try: requests.get(BASE_URL + '/api/v1/ping',
                      headers=session_headers, timeout=10)
    except: pass

def get_balance():
    global ACCOUNT_BALANCE
    try:
        r = requests.get(BASE_URL + '/api/v1/accounts',
                         headers=session_headers, timeout=10)
        if r.status_code == 200:
            accs = r.json().get('accounts', [])
            if accs:
                ACCOUNT_BALANCE = float(
                    accs[0].get('balance', {}).get('available', ACCOUNT_BALANCE))
                log(f'💰 Balance: ${round(ACCOUNT_BALANCE, 2)}')
    except Exception as ex:
        log('Balance ERROR: ' + str(ex))

def get_open_positions():
    try:
        r = requests.get(BASE_URL + '/api/v1/positions',
                         headers=session_headers, timeout=10)
        if r.status_code == 200:
            return [p for p in r.json().get('positions', [])
                    if p.get('position', {}).get('dealStatus', 'OPEN')
                    not in ('CLOSED', 'DELETED', 'REJECTED')]
    except Exception as ex:
        log('Positions ERROR: ' + str(ex))
    return []

def get_market_info():
    """
    إصلاح 1: يجلب contractSize و minDealSize من API
    يُرجع: (bid, ask, spread, contract_size, min_size)
    """
    try:
        r = requests.get(BASE_URL + '/api/v1/markets/' + GOLD_EPIC,
                         headers=session_headers, timeout=10)
        if r.status_code == 200:
            data         = r.json()
            snap         = data.get('snapshot', {})
            instrument   = data.get('instrument', {})
            dealing      = data.get('dealingRules', {})

            bid          = snap.get('bid', 0)
            ask          = snap.get('offer', 0)
            spread       = round(ask - bid, 3)

            # contractSize: حجم العقد الواحد بالأوقية
            contract_size = float(instrument.get('contractSize', 100))

            # minDealSize: أصغر حجم مسموح
            min_size = float(
                dealing.get('minDealSize', {}).get('value', 0.1)
            )

            return bid, ask, spread, contract_size, min_size
    except Exception as ex:
        log(f'market_info ERROR: {ex}')
    return 0, 0, 0, 100, 0.1


# ================================================
# 📡 TELEGRAM
# ================================================
def tg(text):
    try:
        requests.post(
            f'https://api.telegram.org/bot{TG_TOKEN}/sendMessage',
            data={'chat_id': TG_CHAT_ID, 'text': text, 'parse_mode': 'Markdown'},
            timeout=10
        )
    except: pass

def tg_signal(data):
    nl = '\n'
    mode = 'DEMO' if DEMO_MODE else 'LIVE'
    tg(
        f'🟡 *XAUUSD BUY Limit* [{mode}]{nl}'
        f'Limit Entry: `{data["entry_limit"]}`{nl}'
        f'SL: `{data["sl"]}` | TP: `{data["tp"]}`{nl}'
        f'R:R: `1:{TP_RR}` | Size: `{data["size"]}`{nl}'
        f'Spread: `{data["spread"]}$` | ADX: `{data["adx_h4"]}`{nl}'
        f'RSI: `{data["rsi_h1"]}` | ATR: `{data["atr"]}`{nl}'
        f'_ينتظر retrace للدخول..._'
        f'{nl}_{utc_now()}_'
    )

def tg_entry_filled(entry_actual, sl, tp):
    nl = '\n'
    tg(f'✅ *دخول مُنفَّذ* XAUUSD{nl}'
       f'Entry: `{entry_actual}` | SL: `{sl}` | TP: `{tp}`{nl}'
       f'_{utc_now()}_')

def tg_limit_expired(entry_limit):
    tg(f'⏰ *Limit انتهت صلاحيته* XAUUSD{chr(10)}'
       f'لم يصل السعر إلى `{entry_limit}` — ألغيت الإشارة')

def tg_result(status, deal_ref, error=''):
    icon = '✅' if status in ('ACCEPTED', 'SUCCESS') else '❌'
    mode = 'DEMO' if DEMO_MODE else 'LIVE'
    msg  = f'{icon} *XAUUSD {status}* [{mode}]{chr(10)}Ref:`{deal_ref}`'
    if error: msg += f'{chr(10)}Err:`{error}`'
    tg(msg)

def tg_trail(deal_id, old_sl, new_sl, phase):
    tg(f'📈 *Trail* XAUUSD [{phase}]{chr(10)}'
       f'SL: `{old_sl}` → `{new_sl}`')

def tg_be(deal_id):
    tg(f'🔒 *Breakeven* XAUUSD — SL إلى Entry')


# ================================================
# 📈 MARKET DATA
# ================================================
def fetch_candles(tf, count=250):
    res_map = {'H4': 'HOUR_4', 'H1': 'HOUR', 'M15': 'MINUTE_15'}
    try:
        r = requests.get(
            BASE_URL + '/api/v1/prices/' + GOLD_EPIC,
            headers=session_headers,
            params={'resolution': res_map[tf], 'max': count},
            timeout=15
        )
        if r.status_code == 429:
            time.sleep(3)
            r = requests.get(BASE_URL + '/api/v1/prices/' + GOLD_EPIC,
                             headers=session_headers,
                             params={'resolution': res_map[tf], 'max': count},
                             timeout=15)
        if r.status_code != 200: return pd.DataFrame()
        prices = r.json().get('prices', [])
        if len(prices) < 20: return pd.DataFrame()
        rows = [{
            'time':  p['snapshotTimeUTC'],
            'open':  (p['openPrice']['bid']  + p['openPrice']['ask'])  / 2,
            'high':  (p['highPrice']['bid']  + p['highPrice']['ask'])  / 2,
            'low':   (p['lowPrice']['bid']   + p['lowPrice']['ask'])   / 2,
            'close': (p['closePrice']['bid'] + p['closePrice']['ask']) / 2,
            'vol':   p.get('lastTradedVolume', 1),
        } for p in prices]
        df = pd.DataFrame(rows)
        df['time'] = pd.to_datetime(df['time'], utc=True)
        return df.sort_values('time').reset_index(drop=True)
    except Exception as ex:
        log(f'  fetch_candles {tf}: {ex}')
        return pd.DataFrame()


# ================================================
# 📐 INDICATORS
# ================================================
def ema(s, p): return s.ewm(span=p, adjust=False).mean()

def get_atr(df, p=14):
    """ATR يدوي — بدون pandas_ta"""
    tr = pd.concat([
        df['high'] - df['low'],
        (df['high'] - df['close'].shift()).abs(),
        (df['low']  - df['close'].shift()).abs()
    ], axis=1).max(axis=1)
    return float(tr.ewm(span=p, adjust=False).mean().iloc[-1])

def get_adx(df, p=14):
    """ADX يدوي — بدون pandas_ta"""
    try:
        high  = df['high']
        low   = df['low']
        close = df['close']

        tr = pd.concat([
            high - low,
            (high - close.shift()).abs(),
            (low  - close.shift()).abs()
        ], axis=1).max(axis=1)
        atr = tr.ewm(span=p, adjust=False).mean()

        plus_dm  = high.diff().clip(lower=0)
        minus_dm = (-low.diff()).clip(lower=0)
        plus_dm[plus_dm  < minus_dm] = 0
        minus_dm[minus_dm < plus_dm] = 0

        plus_di  = 100 * plus_dm.ewm(span=p, adjust=False).mean() / (atr + 1e-10)
        minus_di = 100 * minus_dm.ewm(span=p, adjust=False).mean() / (atr + 1e-10)
        dx       = (abs(plus_di - minus_di) / (plus_di + minus_di + 1e-10)) * 100
        adx      = dx.ewm(span=p, adjust=False).mean()
        return round(float(adx.iloc[-1]), 1)
    except:
        return 0.0

def get_rsi(s, p=14):
    """RSI يدوي — بدون pandas_ta"""
    try:
        delta = s.diff()
        gain  = delta.clip(lower=0).ewm(com=p-1, adjust=False).mean()
        loss  = (-delta.clip(upper=0)).ewm(com=p-1, adjust=False).mean()
        rs    = gain / (loss + 1e-10)
        rsi   = 100 - (100 / (1 + rs))
        return round(float(rsi.iloc[-1]), 1)
    except:
        return 50.0

def is_momentum_bull(df):
    """شمعة زخم صعودية على H1: body ≥ 40% من النطاق"""
    c    = df.iloc[-2]
    body = c['close'] - c['open']
    rng  = c['high']  - c['low']
    return body > 0 and rng > 0 and (body / rng) >= 0.40


# ================================================
# 💵 POSITION SIZING — إصلاح 1
# ================================================
def calc_size(sl_dist, contract_size, min_size):
    """
    حساب صحيح بناءً على contractSize الفعلي من API.

    للذهب على Capital.com:
    - contract_size = عدد الأوقيات في اللوت الواحد (عادة 100)
    - pip_value = contract_size × pip_size (0.01 للذهب)
    - risk_usd = size × sl_dist × contract_size × pip_size_factor

    المعادلة الصحيحة:
    size = risk_usd / (sl_dist × contract_size × 0.01)
    """
    if sl_dist <= 0 or contract_size <= 0:
        return min_size

    risk_usd = ACCOUNT_BALANCE * RISK_PERCENT

    # قيمة النقطة الواحدة (1$) = contract_size × 0.01
    point_value = contract_size * 0.01

    # عدد النقاط في الـ SL
    sl_points = sl_dist / 0.01

    size = risk_usd / (sl_points * point_value / 100)

    # تقريب للحد الأدنى المسموح
    size = max(round(size, 2), min_size)

    log(f'  💵 Size: {size} | SL_dist={sl_dist} | ContractSize={contract_size}'
        f' | Risk=${round(risk_usd, 2)}')
    return size


# ================================================
# 🔍 SIGNAL DETECTION — كل الإصلاحات مطبّقة
# ================================================
def check_signal():
    # ===== جلب بيانات السوق =====
    bid, ask, spread, contract_size, min_size = get_market_info()

    # إصلاح 6: Spread Filter
    if spread > MAX_SPREAD:
        log(f'  ⏸  Spread={spread}$ > حد {MAX_SPREAD}$ — تخطّي')
        return None

    if bid <= 0:
        log('  ⏸  لا يمكن جلب السعر')
        return None

    # ===== H4 TREND =====
    h4 = fetch_candles('H4', 60)
    h1 = fetch_candles('H1', 100)
    if h4.empty or h1.empty:
        log('  ❌ بيانات فارغة')
        return None

    e21_h4  = float(ema(h4['close'], H4_EMA_FAST).iloc[-1])
    e50_h4  = float(ema(h4['close'], H4_EMA_MID).iloc[-1])
    e200_h4 = float(ema(h4['close'], H4_EMA_SLOW).iloc[-1])
    adx_h4  = get_adx(h4, 14)

    if not (e21_h4 > e50_h4 > e200_h4):
        log(f'  ⏸  H4 ليس UP')
        return None
    if adx_h4 < H4_ADX_MIN:
        log(f'  ⏸  ADX={adx_h4} < {H4_ADX_MIN}')
        return None

    log(f'  ✅ H4 UP | ADX={adx_h4}')

    # ===== H1 ENTRY =====
    e21_series = ema(h1['close'], H1_EMA_ENTRY)
    e21_h1     = float(e21_series.iloc[-2])   # آخر شمعة مكتملة
    rsi_h1     = get_rsi(h1['close'].iloc[:-1], H1_RSI_PERIOD)
    atr_h1     = get_atr(h1.iloc[:-1], ATR_PERIOD)

    low_prev   = float(h1['low'].iloc[-2])
    close_prev = float(h1['close'].iloc[-2])

    # إصلاح 7: Pullback أعمق — low[-2] لازم ينزل 0.2 ATR تحت EMA21
    min_pullback_depth = e21_h1 - atr_h1 * PULLBACK_MIN_ATR
    pullback_deep   = low_prev <= min_pullback_depth
    pullback_closed = close_prev >= e21_h1 * 0.999   # أُغلق فوق EMA21

    if not pullback_deep:
        log(f'  ⏸  Pullback سطحي | Low={round(low_prev,2)} '
            f'حد={round(min_pullback_depth,2)} EMA21={round(e21_h1,2)}')
        return None

    if not pullback_closed:
        log(f'  ⏸  Close تحت EMA21 | Close={round(close_prev,2)}')
        return None

    # إصلاح 4: RSI ضيّق 40-55
    if not (H1_RSI_MIN <= rsi_h1 <= H1_RSI_MAX):
        log(f'  ⏸  RSI={rsi_h1} خارج [{H1_RSI_MIN}-{H1_RSI_MAX}]')
        return None

    if not is_momentum_bull(h1):
        log(f'  ⏸  لا شمعة زخم صعودية')
        return None

    if atr_h1 <= 0:
        log('  ⏸  ATR=0')
        return None

    # ===== إصلاح 2: Limit Order Entry =====
    # ننتظر retrace 0.2 ATR تحت close الشمعة الحالية
    price_now   = float(h1['close'].iloc[-1])
    entry_limit = round(price_now - atr_h1 * LIMIT_RETRACE_ATR, 2)

    # إصلاح 3: SL و TP بـ R:R ثابت
    sl      = round(low_prev - atr_h1 * SL_ATR_MULT, 2)
    sl_dist = abs(entry_limit - sl)
    if sl_dist <= 0:
        log('  ⏸  SL_dist=0')
        return None

    tp = round(entry_limit + sl_dist * TP_RR, 2)

    # حساب الحجم الصحيح
    size = calc_size(sl_dist, contract_size, min_size)

    signal = {
        'timestamp':     utc_now(),
        'entry_limit':   entry_limit,
        'price_now':     price_now,
        'sl':            sl,
        'tp':            tp,
        'sl_dist':       sl_dist,
        'atr':           round(atr_h1, 2),
        'adx_h4':        adx_h4,
        'rsi_h1':        rsi_h1,
        'spread':        spread,
        'contract_size': contract_size,
        'min_size':      min_size,
        'size':          size,
    }

    log(f'  🟡 SIGNAL | Limit={entry_limit} SL={sl} TP={tp} '
        f'RR=1:{TP_RR} Size={size} Spread={spread}$')
    return signal


# ================================================
# 📤 OPEN POSITION
# ================================================
def open_position(signal):
    """فتح Limit Order"""
    body = {
        'epic':           GOLD_EPIC,
        'direction':      'BUY',
        'size':           signal['size'],
        'guaranteedStop': False,
        'trailingStop':   False,
        'stopLevel':      signal['sl'],
        'profitLevel':    signal['tp'],
        'level':          signal['entry_limit'],   # Limit price
        'type':           'LIMIT',
    }

    log(f'📤 LIMIT ORDER: entry={signal["entry_limit"]} '
        f'SL={signal["sl"]} TP={signal["tp"]} size={signal["size"]}')

    try:
        # نحاول Limit Order أولاً
        r    = requests.post(BASE_URL + '/api/v1/workingorders',
                             headers=session_headers, json=body, timeout=15)
        data = r.json()
        log(f'  RESP [{r.status_code}]: {json.dumps(data)[:150]}')

        if r.status_code == 200:
            deal_ref = data.get('dealReference', 'N/A')
            time.sleep(2)
            confirm  = requests.get(
                BASE_URL + '/api/v1/confirms/' + deal_ref,
                headers=session_headers, timeout=10
            ).json()
            status = confirm.get('dealStatus', 'UNKNOWN')
            reason = confirm.get('reason', '')
            tg_result(status, deal_ref, reason)
            return status, deal_ref

        # إذا فشل Limit، ندخل Market
        log('  ⚠️  Limit فشل — محاولة Market')
        body_market = {
            'epic':           GOLD_EPIC,
            'direction':      'BUY',
            'size':           signal['size'],
            'guaranteedStop': False,
            'trailingStop':   False,
            'stopLevel':      signal['sl'],
            'profitLevel':    signal['tp'],
        }
        r    = requests.post(BASE_URL + '/api/v1/positions',
                             headers=session_headers, json=body_market, timeout=15)
        data = r.json()
        log(f'  Market RESP [{r.status_code}]: {json.dumps(data)[:150]}')

        if r.status_code == 200:
            deal_ref = data.get('dealReference', 'N/A')
            time.sleep(2)
            confirm  = requests.get(
                BASE_URL + '/api/v1/confirms/' + deal_ref,
                headers=session_headers, timeout=10
            ).json()
            status = confirm.get('dealStatus', 'UNKNOWN')
            reason = confirm.get('reason', '')
            tg_result(status, deal_ref, reason)
            return status, deal_ref
        else:
            err = data.get('errorCode', str(data))
            tg_result('FAILED', 'N/A', err)
            return 'FAILED', err

    except Exception as ex:
        log(f'  EXCEPTION: {ex}')
        return 'ERROR', str(ex)


# ================================================
# 🔒 TRAILING STOP — إصلاح 5: ديناميكي
# ================================================
def manage_trailing(positions):
    for pos in positions:
        p         = pos.get('position', {})
        entry     = p.get('level', 0)
        deal_id   = p.get('dealId', '')
        direction = p.get('direction', '')
        cur_sl    = p.get('stopLevel', 0)
        if not deal_id or direction != 'BUY' or not entry: continue

        h1 = fetch_candles('H1', 30)
        if h1.empty: continue
        atr = get_atr(h1, ATR_PERIOD)

        bid, _, _, _, _ = get_market_info()
        if not bid: continue
        price = bid   # نستخدم Bid للـ BUY

        sl_dist_orig = abs(entry - cur_sl)
        if sl_dist_orig <= 0: continue
        one_r        = entry + sl_dist_orig   # مستوى 1R

        # إصلاح 5: Trailing ديناميكي حسب المرحلة
        if price >= one_r:
            # بعد 1R: trail أوسع (3 ATR) لإعطاء مساحة للترند
            new_trail = round(price - atr * TRAIL_AFTER_1R, 2)
            phase     = 'Phase2 (3ATR)'
        else:
            # قبل 1R: trail عادي (2 ATR)
            new_trail = round(price - atr * 2.0, 2)
            phase     = 'Phase1 (2ATR)'

        # تحريك SL للأعلى فقط، بحد أدنى 0.5$ بوفر
        if new_trail > cur_sl + 0.5:
            old_sl = cur_sl
            update_sl(deal_id, new_trail)
            log(f'  📈 Trail [{phase}]: {old_sl} → {new_trail} (price={price})')
            tg_trail(deal_id, old_sl, new_trail, phase)

        # Breakeven عند 1R إذا SL لا يزال تحت Entry
        elif price >= one_r and cur_sl < entry:
            be_sl = round(entry + 0.50, 2)
            update_sl(deal_id, be_sl)
            log(f'  🔒 Breakeven: SL → {be_sl}')
            tg_be(deal_id)


def update_sl(deal_id, new_sl):
    try:
        r = requests.put(
            BASE_URL + '/api/v1/positions/' + deal_id,
            headers=session_headers,
            json={'stopLevel': new_sl},
            timeout=10
        )
        log(f'  SL UPDATE [{r.status_code}]')
    except Exception as ex:
        log(f'  SL UPDATE ERROR: {ex}')


# ================================================
# 🔄 MAIN LOOP
# ================================================
def run_scan(execute=True):
    if not is_trade_window():
        log(f'⏸  خارج {TRADE_START_H}:00-{TRADE_END_H}:00 UTC')
        return

    consec = db_consecutive_losses()
    if consec >= MAX_CONSECUTIVE_LOSS:
        log(f'⚠️  {consec} خسائر متتالية — توقف ساعة')
        tg(f'⚠️ *PAUSE* {consec} خسائر — توقف ساعة')
        time.sleep(3600)
        return

    log('—' * 50)
    log(f'🔍 XAUUSD v2 | {"DEMO" if DEMO_MODE else "LIVE"}')
    log('—' * 50)

    get_balance()
    positions = get_open_positions()
    log(f'  مفتوحة: {len(positions)}')

    # إدارة Trailing
    if positions:
        manage_trailing(positions)

    if len(positions) >= MAX_OPEN_TRADES:
        log('  ⏸  صفقة مفتوحة')
        return

    key = f'XAUUSD_BUY_{today_key()}'
    if db_is_duplicate(key):
        log(f'  ⏸  إشارة اليوم منفّذة ({key})')
        return

    signal = check_signal()
    if signal is None:
        log('  ⏸  لا إشارة')
        return

    db_save_signal(key, signal)
    tg_signal(signal)

    if execute:
        status, ref = open_position(signal)
        db_update(key, status, signal['entry_limit'])
        log(f'  ORDER: {status} | {ref}')
    else:
        log('  [DRY RUN]')


def start_bot(execute=True):
    db_init()
    mode = 'DEMO' if DEMO_MODE else 'LIVE'
    print('=' * 55, flush=True)
    print(f'  XAUUSD BUY Bot v2 [{mode}]', flush=True)
    print(f'  Epic     : {GOLD_EPIC}', flush=True)
    print(f'  Entry    : Limit (-{LIMIT_RETRACE_ATR} ATR)', flush=True)
    print(f'  TP       : R:R 1:{TP_RR}', flush=True)
    print(f'  RSI      : {H1_RSI_MIN}-{H1_RSI_MAX}', flush=True)
    print(f'  Spread   : max ${MAX_SPREAD}', flush=True)
    print(f'  Trail    : {TRAIL_INITIAL}→{TRAIL_AFTER_1R} ATR', flush=True)
    print(f'  Pullback : ≥ {PULLBACK_MIN_ATR} ATR', flush=True)
    print('=' * 55, flush=True)

    nl = '\n'
    tg(f'🚀 *XAUUSD Bot v2* [{mode}]{nl}'
       f'Entry: Limit | TP: 1:{TP_RR} | RSI: {H1_RSI_MIN}-{H1_RSI_MAX}{nl}'
       f'Spread max: ${MAX_SPREAD} | Trail: {TRAIL_INITIAL}→{TRAIL_AFTER_1R} ATR{nl}'
       f'_{utc_now()}_')

    session_age = 0
    while True:
        try:
            if session_age == 0:
                if not create_session():
                    time.sleep(60); continue
            else:
                ping_session()
            session_age = (session_age + 1) % 30
            run_scan(execute=execute)
        except KeyboardInterrupt:
            log('🛑 Bot stopped')
            tg('🛑 XAUUSD Bot v2 stopped')
            break
        except Exception as ex:
            log(f'LOOP ERROR: {ex}')
            tg(f'❌ Error: `{ex}`')
        time.sleep(SCAN_INTERVAL)


if __name__ == '__main__':
    start_bot(execute=True)
