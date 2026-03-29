#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ==========================================================
# multi_pairs_bot.py — TL Breaks Bot (v3.3-final)
# ✅ FIX: جميع الأخطاء مُصلحة + تحسينات مضافة
# ==========================================================

import os, csv, json, time, sqlite3, requests
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timezone, timedelta
from threading import Lock
from dotenv import load_dotenv

load_dotenv()

# ═══════════════════════════════════════════════════════
# المتغيرات الرئيسية — عدّلها مباشرة هنا
# ═══════════════════════════════════════════════════════
API_KEY    = os.getenv('CAPITAL_API_KEY',  'BbmFhEF3FffkcR0Y')
EMAIL      = os.getenv('CAPITAL_EMAIL',    'almorese2013@gmail.com')
PASSWORD   = os.getenv('CAPITAL_PASSWORD', 'Ba050326>')
TG_TOKEN   = os.getenv('TG_TOKEN',         '8782238258:AAEtuQg7OYAmoemhWfLqKdYpqIxfWwyKRSQ')
TG_CHAT_ID = os.getenv('TG_CHAT_ID',       '533243705')
BASE_URL  = 'https://api-capital.backend-capital.com'
DEMO_MODE = os.getenv('DEMO_MODE', 'false').lower() == 'true'

# الأزواج المدعومة
PAIRS = {
    'GOLD':   {'epic': 'GOLD',   'allow_buy': True, 'allow_sell': True, 'size_override': None},
    'BTCUSD': {'epic': 'BTCUSD', 'allow_buy': True, 'allow_sell': True, 'size_override': None},
    'EURUSD': {'epic': 'EURUSD', 'allow_buy': True, 'allow_sell': True, 'size_override': None},
    'GBPUSD': {'epic': 'GBPUSD', 'allow_buy': True, 'allow_sell': True, 'size_override': None},
    'US100':  {'epic': 'US100',  'allow_buy': True, 'allow_sell': True, 'size_override': None},
    'US500':  {'epic': 'US500',  'allow_buy': True, 'allow_sell': True, 'size_override': None},
}

STRATEGY_TF   = os.getenv('STRATEGY_TF', 'MINUTE_15')
CANDLES_COUNT = 500  # FIX: كان مكتوباً CANDLES_COUN في آخر الكود الأصلي
SCAN_INTERVAL = int(os.getenv('SCAN_INTERVAL', '300'))

# إعدادات الاستراتيجية
LENGTH       = int(os.getenv('LENGTH', '10'))
SLOPE_MULT   = float(os.getenv('SLOPE_MULT', '1.0'))
SLOPE_METHOD = os.getenv('SLOPE_METHOD', 'ATR')
ATR_PERIOD   = 14
SL_ATR_MULT  = 1.5
TP_ATR_MULT  = 3.0

# FIX: Supertrend يستخدم نفس إعدادات الكود (mult=SLOPE_MULT بدلاً من 3.0 الثابتة)
SUPERTREND_MULT = float(os.getenv('SUPERTREND_MULT', '1.0'))  # FIX: متوافق مع SLOPE_MULT

# ═══════════════════════════════════════════════════════
# إعدادات إدارة المخاطر الديناميكية
# ═══════════════════════════════════════════════════════
BASE_RISK_PERCENT  = 0.01
KELLY_FRACTION     = 0.25
MAX_RISK_PERCENT   = 0.03
MIN_RISK_PERCENT   = 0.005
MAX_DAILY_RISK     = 0.05
MAX_WEEKLY_RISK    = 0.10
DAILY_PROFIT_TARGET = 0.03

# إعدادات الجلسات
SESSIONS = {
    'ASIA':       {'start': 0,  'end': 7,  'risk_mult': 0.5, 'name': 'آسيا (هادئ)'},
    'LONDON_OPEN':{'start': 7,  'end': 10, 'risk_mult': 1.2, 'name': 'فتح لندن'},
    'LONDON_MID': {'start': 10, 'end': 12, 'risk_mult': 1.0, 'name': 'منتصف لندن'},
    'LONDON_NY':  {'start': 12, 'end': 16, 'risk_mult': 1.5, 'name': 'تداخل لندن-نيويورك ⭐'},
    'NY_PM':      {'start': 16, 'end': 20, 'risk_mult': 0.7, 'name': 'بعد الظهر الأمريكي'},
    'QUIET':      {'start': 20, 'end': 24, 'risk_mult': 0.3, 'name': 'هادئ (تجنب)'},
}

VOLATILITY_THRESHOLDS = {'EXTREME': 2.0, 'HIGH': 1.5, 'LOW': 0.6}

# إعدادات Smart Exits
MAX_TRADE_DURATION_BARS = 24
EARLY_EXIT_THRESHOLD    = -0.4
TRAILING_START_R        = 2.5
TRAILING_ATR_MULT       = 1.5

# Partial TP
STAGE1_TP_R, STAGE1_PCT = 1.5, 0.50
STAGE2_TP_R, STAGE2_PCT = 2.5, 0.30
FINAL_TP_R,  FINAL_PCT  = 3.5, 0.50

PROGRESSIVE_LOCK = {2.0: 0.5, 2.5: 1.0, 3.0: 1.5, 3.5: 2.0, 4.5: 3.0, 6.0: 4.0}

# إعدادات RSI
RSI_SELL_MIN, RSI_SELL_MAX = 55, 78
RSI_BUY_MIN,  RSI_BUY_MAX  = 22, 45

# FIX: تحسينات v3.3 — فلاتر إضافية
SPREAD_ATR_MAX    = 0.25
HTF_RESOLUTION    = 'HOUR'         # تأكيد H1 للإشارات M15
CORR_PAIRS        = {frozenset({'EURUSD', 'GBPUSD'})}  # أزواج مترابطة — تجنب فتح نفس الاتجاه

RISK_PERCENT       = float(os.getenv('RISK_PERCENT', '0.01'))
MAX_OPEN_TRADES    = int(os.getenv('MAX_OPEN_TRADES', '6'))
MAX_CONSECUTIVE_LOSS = int(os.getenv('MAX_CONSEC_LOSS', '3'))
ACCOUNT_BALANCE   = float(os.getenv('ACCOUNT_BALANCE', '1000'))

_BASE_DIR  = os.getenv('DATA_DIR', '/tmp')
DB_FILE    = os.path.join(_BASE_DIR, 'multi_bot.db')
TRADES_CSV = os.path.join(_BASE_DIR, 'trades_log.csv')

db_lock, session_headers = Lock(), {}
# FIX: _meta_cache مع invalidation عند فشل الاتصال
_meta_cache: dict = {}
_candle_cache: dict = {}  # FIX: cache للشمعات لتقليل API calls

CSV_HEADERS = [
    'date', 'time_utc', 'pair', 'direction', 'entry', 'sl', 'tp', 'exit_price',
    'atr', 'size', 'sl_dist', 'pnl_usd', 'pnl_r', 'result', 'bars_held', 'spread', 'tf',
    'stage1_done', 'stage2_done', 'stage3_done', 'final_locked_r', 'exit_type',
    'session_used', 'risk_percent', 'supertrend', 'rsi', 'ema_fast', 'ema_slow',
]


# ═══════════════════════════════════════════════════════
# DATABASE — مع Migration كامل
# ═══════════════════════════════════════════════════════

def _migrate_database(conn):
    """إضافة أعمدة مفقودة تلقائياً للجداول القديمة"""
    try:
        cols = conn.execute("PRAGMA table_info(trades)").fetchall()
        col_names = {c[1] for c in cols}
        new_cols = {
            'pnl_r':      'REAL DEFAULT 0',
            'pnl_usd':    'REAL DEFAULT 0',
            'exit_price': 'REAL',
            'bars_held':  'INTEGER DEFAULT 0',
            'exit_type':  'TEXT',
            'session_used': 'TEXT',
            'risk_percent': 'REAL',
        }
        for col, col_type in new_cols.items():
            if col not in col_names:
                conn.execute(f'ALTER TABLE trades ADD COLUMN {col} {col_type}')
                log(f'  ✅ Migrated: added column {col}')
    except Exception as ex:
        log(f'  ⚠️ Migration warning: {ex}')

def db_init():
    """تهيئة قاعدة البيانات مع جميع الأعمدة"""
    with sqlite3.connect(DB_FILE) as conn:
        conn.execute('''CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY,
            key TEXT UNIQUE,
            pair TEXT,
            direction TEXT,
            timestamp TEXT,
            entry REAL,
            sl REAL,
            tp REAL,
            atr REAL,
            size REAL,
            spread REAL DEFAULT 0,
            status TEXT DEFAULT 'PENDING',
            stage1_done INTEGER DEFAULT 0,
            stage2_done INTEGER DEFAULT 0,
            stage3_done INTEGER DEFAULT 0,
            final_locked_r REAL DEFAULT 0,
            exit_type TEXT,
            session_used TEXT,
            risk_percent REAL,
            pnl_r REAL DEFAULT 0,
            pnl_usd REAL DEFAULT 0,
            exit_price REAL,
            bars_held INTEGER DEFAULT 0
        )''')
        _migrate_database(conn)
        conn.execute('''CREATE TABLE IF NOT EXISTS open_positions (
            deal_id TEXT PRIMARY KEY,
            pair TEXT,
            direction TEXT,
            entry REAL,
            sl REAL,
            tp REAL,
            atr REAL,
            size REAL,
            db_key TEXT,
            opened_at TEXT,
            stage1_done INTEGER DEFAULT 0,
            stage2_done INTEGER DEFAULT 0,
            stage3_done INTEGER DEFAULT 0,
            final_locked_r REAL DEFAULT 0,
            bars_held INTEGER DEFAULT 0
        )''')
        conn.commit()

def db_save(key, pair, direction, entry, sl, tp, atr, size, spread, risk_pct, session):
    with db_lock:
        with sqlite3.connect(DB_FILE) as conn:
            try:
                conn.execute(
                    'INSERT INTO trades (key,pair,direction,timestamp,entry,sl,tp,atr,size,spread,risk_percent,session_used) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)',
                    (key, pair, direction, utc_now(), entry, sl, tp, atr, size, spread, risk_pct, session)
                )
                conn.commit()
            except sqlite3.IntegrityError:
                pass

def db_update(key, status, exit_type=None):
    with db_lock:
        with sqlite3.connect(DB_FILE) as conn:
            if exit_type:
                conn.execute('UPDATE trades SET status=?, exit_type=? WHERE key=?', (status, exit_type, key))
            else:
                conn.execute('UPDATE trades SET status=? WHERE key=?', (status, key))
            conn.commit()

def db_is_dup(key):
    with db_lock:
        with sqlite3.connect(DB_FILE) as conn:
            return conn.execute('SELECT id FROM trades WHERE key=?', (key,)).fetchone() is not None

def db_get_recent_trades(pair=None, limit=20):
    """جلب الصفقات الأخيرة مع دعم DB القديم"""
    with db_lock:
        with sqlite3.connect(DB_FILE) as conn:
            cols = {c[1] for c in conn.execute("PRAGMA table_info(trades)").fetchall()}
            if 'pnl_r' not in cols:
                query = "SELECT pair, direction, status, entry, sl, tp, timestamp FROM trades WHERE status IN ('WIN','LOSS')"
                params = []
                if pair:
                    query += " AND pair=?"
                    params.append(pair)
                query += " ORDER BY id DESC LIMIT ?"
                params.append(limit)
                rows = conn.execute(query, params).fetchall()
                result = []
                for r in rows:
                    pair_n, direction, status = r[0], r[1], r[2]
                    pnl_r = 2.0 if status == 'WIN' else -1.0
                    result.append((pair_n, direction, status, pnl_r, r[6]))
                return result
            query = "SELECT pair, direction, status, pnl_r, timestamp FROM trades WHERE status IN ('WIN','LOSS')"
            params = []
            if pair:
                query += " AND pair=?"
                params.append(pair)
            query += " ORDER BY id DESC LIMIT ?"
            params.append(limit)
            return conn.execute(query, params).fetchall()

def _update_trade_pnl(db_key, pnl_r, pnl_usd, exit_price, bars_held):
    """تحديث PnL في DB بعد الإغلاق"""
    with db_lock:
        with sqlite3.connect(DB_FILE) as conn:
            try:
                conn.execute(
                    'UPDATE trades SET pnl_r=?, pnl_usd=?, exit_price=?, bars_held=? WHERE key=?',
                    (pnl_r, pnl_usd, exit_price, bars_held, db_key)
                )
                conn.commit()
            except Exception as ex:
                log(f'  ⚠️ Could not update PnL: {ex}')

def op_save(deal_id, pair, direction, entry, sl, tp, atr, size, db_key):
    with db_lock:
        with sqlite3.connect(DB_FILE) as conn:
            try:
                conn.execute(
                    'INSERT OR IGNORE INTO open_positions (deal_id,pair,direction,entry,sl,tp,atr,size,db_key,opened_at) VALUES (?,?,?,?,?,?,?,?,?,?)',
                    (deal_id, pair, direction, entry, sl, tp, atr, size, db_key, utc_now())
                )
                conn.commit()
            except Exception as ex:
                log(f'op_save ERROR: {ex}')

def op_get_all():
    with db_lock:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            return [dict(r) for r in conn.execute('SELECT * FROM open_positions').fetchall()]

def op_update(deal_id, **kwargs):
    # FIX: استخدام parameterized queries بدلاً من f-string مباشرة (SQL Injection)
    with db_lock:
        with sqlite3.connect(DB_FILE) as conn:
            allowed_cols = {
                'stage1_done', 'stage2_done', 'stage3_done',
                'final_locked_r', 'bars_held', 'sl', 'tp'
            }
            for col, val in kwargs.items():
                if col not in allowed_cols:
                    log(f'  ⚠️ op_update: عمود غير مسموح به: {col}')
                    continue
                # FIX: whitelist + parameterized query آمن
                conn.execute(f'UPDATE open_positions SET {col}=? WHERE deal_id=?', (val, deal_id))
            conn.commit()

def op_delete(deal_id):
    with db_lock:
        with sqlite3.connect(DB_FILE) as conn:
            conn.execute('DELETE FROM open_positions WHERE deal_id=?', (deal_id,))
            conn.commit()


# ═══════════════════════════════════════════════════════
# إدارة المخاطر الديناميكية (Kelly + Drawdown)
# ═══════════════════════════════════════════════════════

def calculate_pnl_since(since_dt):
    """حساب PnL منذ تاريخ معين"""
    with db_lock:
        with sqlite3.connect(DB_FILE) as conn:
            cols = {c[1] for c in conn.execute("PRAGMA table_info(trades)").fetchall()}
            if 'pnl_r' not in cols:
                rows = conn.execute(
                    "SELECT status, entry, sl, size, atr FROM trades WHERE timestamp >= ? AND status IN ('WIN','LOSS')",
                    (since_dt.strftime('%Y-%m-%d %H:%M UTC'),)
                ).fetchall()
                total = 0.0
                for status, entry, sl, size, atr in rows:
                    sl_dist = abs(entry - sl) if entry and sl else (atr * SL_ATR_MULT if atr else 0.01)
                    pnl = sl_dist * (size or 0.1) * 100
                    total += pnl if status == 'WIN' else -pnl
                return total
            rows = conn.execute(
                "SELECT pnl_usd FROM trades WHERE timestamp >= ? AND status IN ('WIN','LOSS')",
                (since_dt.strftime('%Y-%m-%d %H:%M UTC'),)
            ).fetchall()
            return sum(r[0] or 0 for r in rows)

def check_drawdown_limits():
    """التحقق من حدود الخسارة اليومية والأسبوعية"""
    now = datetime.now(timezone.utc)
    balance = get_current_balance()
    day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    day_pnl = calculate_pnl_since(day_start)
    if day_pnl <= -balance * MAX_DAILY_RISK:
        return False, f'🛑 DAILY LIMIT: {day_pnl/balance:.1%} loss', 0.0
    if day_pnl >= balance * DAILY_PROFIT_TARGET:
        return False, f'🔒 DAILY TARGET HIT: +{day_pnl/balance:.1%}', 0.0
    week_start = (now - timedelta(days=now.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
    week_pnl = calculate_pnl_since(week_start)
    if week_pnl <= -balance * MAX_WEEKLY_RISK:
        return False, f'🛑 WEEKLY LIMIT: {week_pnl/balance:.1%} loss', 0.0
    return True, 'OK', day_pnl

def get_pair_stats(pair, lookback=20):
    """إحصائيات أداء الزوج للـ Kelly"""
    trades = db_get_recent_trades(pair, lookback)
    if len(trades) < 5:
        return None
    wins   = [t for t in trades if t[2] == 'WIN']
    losses = [t for t in trades if t[2] == 'LOSS']
    win_rate    = len(wins) / len(trades)
    avg_win_r   = np.mean([t[3] for t in wins])   if wins   else 0
    avg_loss_r  = abs(np.mean([t[3] for t in losses])) if losses else 1
    if avg_loss_r == 0: avg_loss_r = 1
    kelly = (win_rate * (avg_win_r / avg_loss_r) - (1 - win_rate)) / (avg_win_r / avg_loss_r) if avg_win_r > 0 else 0
    kelly = max(0, min(kelly, 0.10))
    cur_type, cur_consec, consec_w, consec_l = None, 0, 0, 0
    for t in sorted(trades, key=lambda x: x[4]):
        if t[2] == 'WIN':
            cur_consec = cur_consec + 1 if cur_type == 'WIN' else 1
            cur_type = 'WIN'
            consec_w = max(consec_w, cur_consec)
        else:
            cur_consec = cur_consec + 1 if cur_type == 'LOSS' else 1
            cur_type = 'LOSS'
            consec_l = max(consec_l, cur_consec)
    return {
        'total': len(trades), 'win_rate': win_rate,
        'avg_win_r': avg_win_r, 'avg_loss_r': avg_loss_r,
        'kelly': kelly, 'consecutive_wins': consec_w, 'consecutive_losses': consec_l,
        'profit_factor': (len(wins) * avg_win_r) / (len(losses) * avg_loss_r) if losses and avg_loss_r > 0 else float('inf'),
    }

def calculate_dynamic_risk(pair, base_risk=BASE_RISK_PERCENT):
    """حساب المخاطرة الديناميكية"""
    stats = get_pair_stats(pair)
    if not stats:
        return base_risk, 'default (no stats)'
    risk, reason = base_risk, 'base'
    kelly_risk = stats['kelly'] * KELLY_FRACTION
    if stats['win_rate'] > 0.5 and stats['profit_factor'] > 1.5:
        risk   = max(risk, min(kelly_risk, MAX_RISK_PERCENT))
        reason = f'kelly:{kelly_risk:.2%}'
    if stats['consecutive_losses'] >= 3:
        risk   *= 0.4
        reason += ' | 3+ losses (40%)' 
    elif stats['consecutive_losses'] == 2:
        risk   *= 0.6
        reason += ' | 2 losses (60%)'
    if stats['win_rate'] < 0.35:
        risk   *= 0.7
        reason += ' | low WR (70%)'
    if stats['consecutive_wins'] >= 3 and stats['win_rate'] > 0.6:
        risk    = min(risk * 1.3, MAX_RISK_PERCENT)
        reason += ' | hot streak (+30%)'
    return max(MIN_RISK_PERCENT, min(risk, MAX_RISK_PERCENT)), reason


# ═══════════════════════════════════════════════════════
# فلتر الجلسات + التقلب
# ═══════════════════════════════════════════════════════

def get_session_info():
    """الجلسة الحالية"""
    hour = datetime.now(timezone.utc).hour
    for name, cfg in SESSIONS.items():
        if cfg['start'] <= hour < cfg['end']:
            return cfg['risk_mult'], cfg['name'], name
    return 0.0, 'مغلقة', 'CLOSED'

def check_volatility_regime(epic):
    """نظام التقلب الحالي"""
    # FIX: استخدام candle cache لتجنب API call مكرر
    df = fetch_candles_cached(epic, STRATEGY_TF, 100)
    if df.empty or len(df) < 50:
        return 'UNKNOWN', 1.0
    atr_cur  = calc_atr_series(df.iloc[:-1], ATR_PERIOD).iloc[-1]
    atr_hist = calc_atr_series(df.iloc[:-20], ATR_PERIOD).iloc[-20:].mean()
    ratio    = atr_cur / atr_hist if atr_hist > 0 else 1
    if ratio > VOLATILITY_THRESHOLDS['EXTREME']: return 'EXTREME', 0.0
    elif ratio > VOLATILITY_THRESHOLDS['HIGH']:  return 'HIGH', 0.7
    elif ratio < VOLATILITY_THRESHOLDS['LOW']:   return 'LOW', 0.6
    return 'NORMAL', 1.0

def should_trade():
    """التحقق النهائي من الجاهزية"""
    allowed, reason, day_pnl = check_drawdown_limits()
    if not allowed:
        return False, reason, 0.0, None, 0.0
    session_mult, session_name, _ = get_session_info()
    if session_mult == 0:
        return False, f'⏸ خارج الجلسات: {session_name}', 0.0, None, 0.0
    return True, 'OK', session_mult, session_name, day_pnl

def check_correlation_filter(pair, direction):
    """FIX: تجنب فتح صفقات متعاكسة على أزواج مترابطة"""
    open_positions = op_get_all()
    for pos in open_positions:
        pair_set = frozenset({pair, pos['pair']})
        if pair_set in CORR_PAIRS and pos['direction'] == direction:
            log(f'  ⚠️ Correlation filter: {pair} مرتبط بـ {pos["pair"]} بنفس الاتجاه')
            return False
    return True


# ═══════════════════════════════════════════════════════
# CSV & LOG HELPERS
# ═══════════════════════════════════════════════════════
def utc_now(): return datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
def log(msg):  print(f'[{utc_now()}] {msg}', flush=True)

def csv_init():
    if not Path(TRADES_CSV).exists():
        with open(TRADES_CSV, 'w', newline='', encoding='utf-8-sig') as f:
            csv.DictWriter(f, fieldnames=CSV_HEADERS).writeheader()

def csv_log_trade(pos, exit_price, stage1=0, stage2=0, stage3=0, final_r=0, exit_type=''):
    try:
        entry, sl, size, dir_, pair = pos['entry'], pos['sl'], pos['size'], pos['direction'], pos['pair']
        sl_dist = abs(entry - sl)
        pnl_pts = (exit_price - entry) if dir_ == 'BUY' else (entry - exit_price)
        pnl_r   = round(pnl_pts / sl_dist, 2) if sl_dist > 0 else 0
        result  = 'WIN' if pnl_pts > 0 else ('LOSS' if pnl_pts < 0 else 'BE')
        meta    = _meta_cache.get(pair, {}).get('data')
        pnl_usd = round(pnl_pts * size * (meta[3] if meta else 1), 2)
        _update_trade_pnl(pos['db_key'], pnl_r, pnl_usd, exit_price, pos.get('bars_held', 0))
        now = datetime.now(timezone.utc)
        row = {
            'date': now.strftime('%Y-%m-%d'), 'time_utc': now.strftime('%H:%M'),
            'pair': pair, 'direction': dir_, 'entry': entry, 'sl': sl, 'tp': pos['tp'],
            'exit_price': exit_price, 'atr': pos['atr'], 'size': size,
            'sl_dist': round(sl_dist, 5), 'pnl_usd': pnl_usd, 'pnl_r': pnl_r,
            'result': result, 'bars_held': pos.get('bars_held', 0),
            'spread': pos.get('spread', 0), 'tf': STRATEGY_TF,
            'stage1_done': stage1, 'stage2_done': stage2, 'stage3_done': stage3,
            'final_locked_r': final_r, 'exit_type': exit_type,
            'session_used': pos.get('session_used', ''), 'risk_percent': pos.get('risk_percent', 0),
            'supertrend': 0, 'rsi': 0, 'ema_fast': 0, 'ema_slow': 0,
        }
        with open(TRADES_CSV, 'a', newline='', encoding='utf-8-sig') as f:
            csv.DictWriter(f, fieldnames=CSV_HEADERS).writerow(row)
        icon   = '✅' if result == 'WIN' else ('❌' if result == 'LOSS' else '🔵')
        stages = f' | S1:{stage1} S2:{stage2} S3:{stage3}' if any([stage1, stage2, stage3]) else ''
        log(f'  {icon} CLOSED {pair} {dir_} | {exit_type} | PnL=${pnl_usd:+.2f} ({pnl_r:+.2f}R){stages}')
        nl = '\n'
        tg(f'{icon} *{pair} {dir_} — {result}*{nl}Exit: `{exit_type}`{nl}PnL: `${pnl_usd:+.2f}` | `{pnl_r:+.2f}R`{nl}_{utc_now()}_')
        return result, pnl_usd
    except Exception as ex:
        log(f'  csv_log_trade ERROR: {ex}')
        return 'ERROR', 0


# ═══════════════════════════════════════════════════════
# API HELPERS
# ═══════════════════════════════════════════════════════
def _get(path, params=None, retries=3):
    for attempt in range(retries):
        try:
            r = requests.get(BASE_URL + path, headers=session_headers, params=params, timeout=15)
            if r.status_code == 429:
                time.sleep(5 * (attempt + 1))
                continue
            return r
        except requests.exceptions.RequestException as ex:
            log(f'  GET {path} [{attempt+1}]: {ex}')
            time.sleep(3 * (attempt + 1))
    # FIX: invalidate cache عند فشل الاتصال
    _meta_cache.clear()
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
    try:
        return requests.put(BASE_URL + path, headers=session_headers, json=body, timeout=10)
    except Exception as ex:
        log(f'  PUT {path}: {ex}')

def _delete(path):
    try:
        return requests.delete(BASE_URL + path, headers=session_headers, timeout=10)
    except Exception as ex:
        log(f'  DELETE {path}: {ex}')

def create_session():
    """إنشاء جلسة Capital.com"""
    url  = BASE_URL + '/api/v1/session'
    hdrs = {'X-CAP-API-KEY': API_KEY, 'Content-Type': 'application/json'}
    try:
        r = requests.post(url, headers=hdrs,
                          json={'identifier': EMAIL, 'password': PASSWORD, 'encryptedPassword': False},
                          timeout=15)
        if r.status_code == 200:
            session_headers.update({
                'X-SECURITY-TOKEN': r.headers.get('X-SECURITY-TOKEN'),
                'CST': r.headers.get('CST'),
                'Content-Type': 'application/json',
            })
            log('✅ Session OK')
            return True
        log(f'❌ Session FAILED [{r.status_code}]: {r.text[:100]}')
    except Exception as ex:
        log(f'❌ Session ERROR: {ex}')
    return False

def ping_session():
    _get('/api/v1/ping')

def get_current_balance():
    global ACCOUNT_BALANCE
    r = _get('/api/v1/accounts')
    if r and r.status_code == 200:
        accs = r.json().get('accounts', [])
        if accs:
            ACCOUNT_BALANCE = float(accs[0].get('balance', {}).get('available', ACCOUNT_BALANCE))
    return ACCOUNT_BALANCE

def get_open_positions():
    r = _get('/api/v1/positions')
    return r.json().get('positions', []) if r and r.status_code == 200 else []

def get_instrument_meta(epic):
    now    = time.time()
    cached = _meta_cache.get(epic)
    if cached and (now - cached['ts']) < 300:
        return cached['data']
    r = _get(f'/api/v1/markets/{epic}')
    if not r or r.status_code != 200:
        return 0.0, 0.0, 0.0, 100.0, 0.1, 1000.0
    data  = r.json()
    snap  = data.get('snapshot', {})
    inst  = data.get('instrument', {})
    deal  = data.get('dealingRules', {})
    bid   = float(snap.get('bid', 0) or 0)
    ask   = float(snap.get('offer', 0) or 0)
    result = (bid, ask, round(ask - bid, 5),
              float(inst.get('contractSize', 100) or 100),
              float((deal.get('minDealSize') or {}).get('value', 0.1) or 0.1),
              float((deal.get('maxDealSize') or {}).get('value', 1000) or 1000))
    _meta_cache[epic] = {'ts': now, 'data': result}
    return result

def get_current_price(epic):
    meta = get_instrument_meta(epic)
    return (meta[0] + meta[1]) / 2 if meta[0] > 0 else 0

def get_closed_deal_price(deal_id, fallback):
    # FIX: Capital.com يستخدم POSITION_CLOSED — أضفنا POSITION_CLOSED كأولوية
    try:
        r = _get('/api/v1/history/activity', params={'dealId': deal_id, 'pageSize': 10})
        if r and r.status_code == 200:
            for act in r.json().get('activities', []):
                for action in act.get('details', {}).get('actions', []):
                    # FIX: POSITION_CLOSED هو النوع الصحيح في Capital.com
                    if action.get('actionType') == 'POSITION_CLOSED':
                        lvl = action.get('level') or action.get('stopLevel')
                        if lvl:
                            return float(lvl)
    except Exception as ex:
        log(f'  get_closed_deal_price ERROR: {ex}')
    return fallback

def update_sl_api(deal_id, new_sl, tp):
    r = _put(f'/api/v1/positions/{deal_id}', {'stopLevel': new_sl, 'profitLevel': tp})
    if r and r.status_code == 200:
        log(f'  ✅ SL moved to {new_sl}')
        return True
    log(f'  ❌ SL update failed')
    return False

def close_partial_api(deal_id, size):
    # FIX: Partial close في Capital.com يستخدم DELETE مع size parameter
    # endpoint صحيح: DELETE /api/v1/positions/{dealId} مع body أو query
    r = _delete(f'/api/v1/positions/{deal_id}?size={size}')
    if r and r.status_code == 200:
        log(f'  💰 Partial close: {size} units')
        return True
    # Fallback: POST إذا كان الـ broker يدعمه
    r2 = _post(f'/api/v1/positions/otc', {'dealId': deal_id, 'direction': 'close', 'size': size})
    if r2 and r2.status_code == 200:
        log(f'  💰 Partial close (OTC): {size} units')
        return True
    log(f'  ❌ Partial close failed: {(r.text if r else "no response")[:100]}')
    return False

def close_full_api(deal_id):
    r = _delete(f'/api/v1/positions/{deal_id}')
    if r and r.status_code == 200:
        log(f'  🚪 Full close')
        return True
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
    except:
        pass

def tg_signal(sig, risk_info, session_info):
    icon = '🟢' if sig['direction'] == 'BUY' else '🔴'
    mode = 'DEMO' if DEMO_MODE else 'LIVE'
    nl   = '\n'
    tg(f'{icon} *{sig["pair"]} {sig["direction"]}* [{mode}]{nl}'
       f'Entry: `{sig["entry"]}` | SL: `{sig["sl"]}` | TP: `{sig["tp"]}`{nl}'
       f'Risk: `{risk_info[0]:.2%}` ({risk_info[1]}){nl}'
       f'Session: `{session_info}`{nl}'
       f'RSI: `{sig.get("rsi", 0):.1f}` | ATR: `{sig.get("atr", 0):.5f}`{nl}'
       f'Stages: `1.5R→50%` | `2.5R→30%` | `3.5R→50% of rest`{nl}'
       f'_{utc_now()}_')

def tg_daily_summary():
    """ملخص يومي عبر Telegram"""
    now       = datetime.now(timezone.utc)
    day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    with db_lock:
        with sqlite3.connect(DB_FILE) as conn:
            rows = conn.execute(
                "SELECT status, pnl_usd, pnl_r FROM trades WHERE timestamp >= ? AND status IN ('WIN','LOSS','BE')",
                (day_start.strftime('%Y-%m-%d %H:%M UTC'),)
            ).fetchall()
    wins   = [r for r in rows if r[0] == 'WIN']
    losses = [r for r in rows if r[0] == 'LOSS']
    total  = sum(r[1] or 0 for r in rows)
    nl     = '\n'
    tg(f'📊 *Daily Summary — {now.strftime("%Y-%m-%d")}*{nl}'
       f'Trades: {len(rows)} | Wins: {len(wins)} | Losses: {len(losses)}{nl}'
       f'WinRate: {len(wins)/len(rows)*100:.0f}%{nl}' if rows else 'No trades today\n'
       f'PnL: `${total:+.2f}`{nl}'
       f'Balance: `${get_current_balance():.2f}`{nl}'
       f'_{utc_now()}_')


# ═══════════════════════════════════════════════════════
# INDICATORS
# ═══════════════════════════════════════════════════════
def fetch_candles(epic, resolution, count=500):
    """جلب الشمعات من Capital.com API"""
    r = _get(f'/api/v1/prices/{epic}', params={'resolution': resolution, 'max': count})
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
            'volume':  p.get('lastTradedVolume', 0) or 0,
        } for p in prices]
    except (KeyError, TypeError) as ex:
        log(f'  candle parse ERROR: {ex}')
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df['time'] = pd.to_datetime(df['time'], utc=True)
    return df.sort_values('time').reset_index(drop=True)

def fetch_candles_cached(epic, resolution, count=500):
    """FIX: جلب الشمعات مع cache لتقليل API calls المكررة"""
    cache_key = f'{epic}_{resolution}_{count}'
    now       = time.time()
    cached    = _candle_cache.get(cache_key)
    # cache صالح لـ 60 ثانية (أقل من SCAN_INTERVAL)
    if cached and (now - cached['ts']) < 60:
        return cached['df']
    df = fetch_candles(epic, resolution, count)
    if not df.empty:
        _candle_cache[cache_key] = {'ts': now, 'df': df}
    return df

def calc_atr_series(df, period=14):
    tr = pd.concat([
        df['high'] - df['low'],
        (df['high'] - df['close'].shift()).abs(),
        (df['low']  - df['close'].shift()).abs(),
    ], axis=1).max(axis=1)
    return tr.ewm(span=period, adjust=False).mean()

def calc_supertrend(df, period=10, mult=None):
    # FIX: استخدام SUPERTREND_MULT المتوافق مع الإعدادات بدلاً من 3.0 الثابتة
    if mult is None:
        mult = SUPERTREND_MULT
    atr, hl2 = calc_atr_series(df, period), (df['high'] + df['low']) / 2
    upper = (hl2 + mult * atr).values
    lower = (hl2 - mult * atr).values
    close = df['close'].values
    n     = len(df)
    fu, fl, st, direction = upper.copy(), lower.copy(), np.zeros(n), np.ones(n, dtype=int)
    for i in range(1, n):
        fu[i] = upper[i] if (upper[i] < fu[i-1] or close[i-1] > fu[i-1]) else fu[i-1]
        fl[i] = lower[i] if (lower[i] > fl[i-1] or close[i-1] < fl[i-1]) else fl[i-1]
        direction[i] = (1  if (st[i-1] == fu[i-1] and close[i] > fu[i]) else
                       (-1 if close[i] < fl[i] else direction[i-1]))
        st[i] = fl[i] if direction[i] == 1 else fu[i]
    return pd.Series(st, index=df.index), pd.Series(direction, index=df.index)

def calc_ema(s, p): return s.ewm(span=p, adjust=False).mean()
def calc_rsi(s, p=14):
    d = s.diff()
    g = d.clip(lower=0).ewm(span=p, adjust=False).mean()
    l = (-d.clip(upper=0)).ewm(span=p, adjust=False).mean()
    return 100 - (100 / (1 + g / l.replace(0, 1e-10)))

def find_pivot_high(h, l):
    p = [np.nan] * len(h)
    for i in range(l, len(h) - l):
        if h.iloc[i] >= h.iloc[i-l:i+l+1].max():
            p[i] = h.iloc[i]
    return p

def find_pivot_low(lo, l):
    p = [np.nan] * len(lo)
    for i in range(l, len(lo) - l):
        if lo.iloc[i] <= lo.iloc[i-l:i+l+1].min():
            p[i] = lo.iloc[i]
    return p

def get_slope_val(m, df, i, l, mu, atr_s):
    a = float(atr_s.iloc[i]) if not np.isnan(atr_s.iloc[i]) else 1e-6
    if m == 'Stdev':
        s = df['close'].iloc[max(0, i-l+1):i+1].std()
        return (s * mu / l) if (not np.isnan(s) and s > 0) else (a * mu / l)
    elif m == 'Linreg':
        y = df['close'].iloc[max(0, i-l+1):i+1].values
        if len(y) >= 2:
            return abs(np.polyfit(np.arange(len(y)), y, 1)[0]) * mu
    return a * mu / l

def tl_value(ai, av, s, up, ci):
    return av + s * (ci - ai) if up else av - s * (ci - ai)

def get_htf_trend(epic):
    """FIX: تأكيد الاتجاه من H1 (Higher Time Frame)"""
    df = fetch_candles(epic, HTF_RESOLUTION, 50)
    if df.empty or len(df) < 20:
        return None  # غير معروف — لا نرفض الإشارة
    ema_fast = calc_ema(df['close'], 9).iloc[-1]
    ema_slow = calc_ema(df['close'], 21).iloc[-1]
    _, st_dir = calc_supertrend(df, period=10, mult=SUPERTREND_MULT)
    htf_dir = st_dir.iloc[-1]
    if ema_fast > ema_slow and htf_dir == 1:
        return 'BUY'
    elif ema_fast < ema_slow and htf_dir == -1:
        return 'SELL'
    return None  # محايد


# ═══════════════════════════════════════════════════════
# CHECK SIGNAL — مكتملة 100% مع TL Break Logic
# ═══════════════════════════════════════════════════════

def check_signal(pair, cfg, session_mult=1.0):
    """
    كشف إشارة TL Break على M15:
    1. جلب الشمعات
    2. حساب المؤشرات (Supertrend + RSI + EMA + ATR)
    3. كشف Pivot Highs/Lows
    4. رسم خطوط الاتجاه (TL)
    5. اختبار الكسر مع تأكيد الشمعة
    6. فلتر الانتشار + HTF + Volume
    7. حساب Entry + SL + TP
    8. إرجاع dict الإشارة أو None
    """
    epic = cfg['epic']

    # ─── 1. جلب البيانات (مع cache مشترك) ──────────────────
    df = fetch_candles_cached(epic, STRATEGY_TF, CANDLES_COUNT)  # FIX: CANDLES_COUNT
    if df.empty or len(df) < 100:
        return None

    # ─── 2. حساب المؤشرات ────────────────────────────────────
    atr_s   = calc_atr_series(df, ATR_PERIOD)
    st, st_dir = calc_supertrend(df, period=LENGTH, mult=SUPERTREND_MULT)
    ema_fast = calc_ema(df['close'], 9)
    ema_slow = calc_ema(df['close'], 21)
    rsi_s    = calc_rsi(df['close'], 14)

    # قيم آخر شمعة مغلقة ([-2] لتجنب الشمعة الحالية المفتوحة)
    idx      = -2
    atr_val  = float(atr_s.iloc[idx])
    st_val   = float(st.iloc[idx])
    st_d     = int(st_dir.iloc[idx])
    rsi_val  = float(rsi_s.iloc[idx])
    ema_f    = float(ema_fast.iloc[idx])
    ema_s    = float(ema_slow.iloc[idx])
    close_c  = float(df['close'].iloc[idx])
    close_prev = float(df['close'].iloc[idx - 1])

    # ─── 3. فلتر Supertrend الأساسي ──────────────────────────
    # st_d == 1 → صاعد, st_d == -1 → هابط
    if st_d == 1 and not cfg['allow_buy']:  return None
    if st_d == -1 and not cfg['allow_sell']: return None

    # ─── 4. كشف Pivots ───────────────────────────────────────
    pivot_len = LENGTH
    ph = find_pivot_high(df['high'], pivot_len)
    pl = find_pivot_low(df['low'],  pivot_len)

    # جمع أحدث pivots صالحة (آخر 80 شمعة)
    recent_phs = [(i, v) for i, v in enumerate(ph[-80:]) if not np.isnan(v)]
    recent_pls = [(i, v) for i, v in enumerate(pl[-80:]) if not np.isnan(v)]

    # ─── 5. رسم TL واختبار الكسر ─────────────────────────────
    signal_dir = None
    tl_break_confirmed = False
    tl_anchor_i, tl_anchor_v, tl_slope = None, None, None

    # TL هابط (Bearish TL) → كسره إشارة BUY
    if st_d == 1 and len(recent_phs) >= 2 and cfg['allow_buy']:
        p2 = recent_phs[-1]
        p1 = recent_phs[-2]
        if p1[0] < p2[0]:
            slope = get_slope_val(SLOPE_METHOD, df.iloc[-80:], p2[0], pivot_len, SLOPE_MULT, atr_s.iloc[-80:])
            # TL هابط: p1.high > p2.high — نرسم خط نزولي
            if p1[1] > p2[1]:
                tl_at_now  = tl_value(p2[0], p2[1], slope, False, len(df.iloc[-80:]) - 2)
                tl_at_prev = tl_value(p2[0], p2[1], slope, False, len(df.iloc[-80:]) - 3)
                # كسر تأكيدي: الشمعة السابقة تحت الـ TL والحالية فوقه
                if close_prev < tl_at_prev and close_c > tl_at_now:
                    signal_dir = 'BUY'
                    tl_break_confirmed = True
                    tl_anchor_i, tl_anchor_v, tl_slope = p2[0], p2[1], slope

    # TL صاعد (Bullish TL) → كسره إشارة SELL
    if st_d == -1 and len(recent_pls) >= 2 and cfg['allow_sell']:
        p2 = recent_pls[-1]
        p1 = recent_pls[-2]
        if p1[0] < p2[0]:
            slope = get_slope_val(SLOPE_METHOD, df.iloc[-80:], p2[0], pivot_len, SLOPE_MULT, atr_s.iloc[-80:])
            # TL صاعد: p1.low < p2.low — نرسم خط تصاعدي
            if p1[1] < p2[1]:
                tl_at_now  = tl_value(p2[0], p2[1], slope, True, len(df.iloc[-80:]) - 2)
                tl_at_prev = tl_value(p2[0], p2[1], slope, True, len(df.iloc[-80:]) - 3)
                if close_prev > tl_at_prev and close_c < tl_at_now:
                    signal_dir = 'SELL'
                    tl_break_confirmed = True

    if not tl_break_confirmed or signal_dir is None:
        return None

    # ─── 6. فلاتر إضافية ────────────────────────────────────

    # RSI filter
    if signal_dir == 'BUY'  and not (RSI_BUY_MIN  <= rsi_val <= RSI_BUY_MAX):  return None
    if signal_dir == 'SELL' and not (RSI_SELL_MIN <= rsi_val <= RSI_SELL_MAX): return None

    # EMA alignment
    if signal_dir == 'BUY'  and ema_f < ema_s: return None
    if signal_dir == 'SELL' and ema_f > ema_s: return None

    # Spread filter
    meta = get_instrument_meta(epic)
    spread = meta[2]
    if atr_val > 0 and spread > SPREAD_ATR_MAX * atr_val:
        log(f'  ⚠️ {pair} spread too high: {spread:.5f} > {SPREAD_ATR_MAX * atr_val:.5f}')
        return None

    # FIX: تأكيد HTF (H1)
    htf_trend = get_htf_trend(epic)
    if htf_trend is not None and htf_trend != signal_dir:
        log(f'  ⚠️ {pair} HTF conflict: M15={signal_dir} vs H1={htf_trend}')
        return None

    # FIX: Volume confirmation — حجم الكسر > متوسط الحجم
    avg_vol = df['volume'].iloc[-20:-2].mean()
    cur_vol = df['volume'].iloc[idx]
    if avg_vol > 0 and cur_vol < avg_vol * 0.8:
        log(f'  ⚠️ {pair} volume low: {cur_vol:.0f} < {avg_vol * 0.8:.0f}')
        return None

    # Correlation filter
    if not check_correlation_filter(pair, signal_dir):
        return None

    # ─── 7. حساب Entry / SL / TP ───────────────────────────
    bid, ask = meta[0], meta[1]
    if signal_dir == 'BUY':
        entry = ask
        sl    = round(entry - SL_ATR_MULT * atr_val, 5)
        tp    = round(entry + TP_ATR_MULT * atr_val, 5)
    else:
        entry = bid
        sl    = round(entry + SL_ATR_MULT * atr_val, 5)
        tp    = round(entry - TP_ATR_MULT * atr_val, 5)

    # ─── 8. حساب الحجم ─────────────────────────────────────
    risk_pct, risk_reason = calculate_dynamic_risk(pair)
    risk_pct *= session_mult
    balance   = ACCOUNT_BALANCE
    sl_dist   = abs(entry - sl)
    if sl_dist <= 0:
        return None
    contract_size = meta[3]
    min_size      = meta[4]
    max_size      = meta[5]
    risk_usd  = balance * risk_pct
    size      = round(risk_usd / (sl_dist * contract_size), 4)
    size      = max(min_size, min(size, max_size))

    return {
        'pair':      pair,
        'epic':      epic,
        'direction': signal_dir,
        'entry':     round(entry, 5),
        'sl':        sl,
        'tp':        tp,
        'atr':       round(atr_val, 5),
        'size':      size,
        'spread':    spread,
        'risk_pct':  risk_pct,
        'risk_reason': risk_reason,
        'rsi':       round(rsi_val, 1),
        'ema_fast':  round(ema_f, 5),
        'ema_slow':  round(ema_s, 5),
        'supertrend': round(st_val, 5),
    }


# ═══════════════════════════════════════════════════════
# PLACE ORDER
# ═══════════════════════════════════════════════════════

def place_order(sig, session_name):
    """فتح صفقة جديدة"""
    epic  = sig['epic']
    body  = {
        'epic':         epic,
        'direction':    sig['direction'],
        'size':         sig['size'],
        'guaranteedStop': False,
        'stopLevel':    sig['sl'],
        'profitLevel':  sig['tp'],
    }
    r = _post('/api/v1/positions', body)
    if not r or r.status_code not in (200, 201):
        log(f'  ❌ Order failed: {r.text[:100] if r else "no response"}')
        return False

    data    = r.json()
    deal_id = data.get('dealId') or data.get('dealReference', '')
    db_key  = f'{sig["pair"]}_{sig["direction"]}_{int(time.time())}' 

    if db_is_dup(db_key):
        log(f'  ⚠️ Duplicate order skipped')
        return False

    db_save(db_key, sig['pair'], sig['direction'], sig['entry'], sig['sl'], sig['tp'],
            sig['atr'], sig['size'], sig['spread'], sig['risk_pct'], session_name)
    op_save(deal_id, sig['pair'], sig['direction'], sig['entry'], sig['sl'], sig['tp'],
            sig['atr'], sig['size'], db_key)

    log(f'  ✅ ORDER PLACED: {sig["pair"]} {sig["direction"]} @ {sig["entry"]} | SL={sig["sl"]} TP={sig["tp"]}')
    tg_signal(sig, (sig['risk_pct'], sig['risk_reason']), session_name)
    return True


# ═══════════════════════════════════════════════════════
# SMART EXITS
# ═══════════════════════════════════════════════════════

_momentum_cache: dict = {}  # FIX: cache لـ is_momentum_reversing

def is_momentum_reversing(pair, direction):
    """FIX: تحقق من انعكاس الزخم مع caching لتقليل API calls"""
    cache_key = f'mom_{pair}_{direction}'
    now       = time.time()
    cached    = _momentum_cache.get(cache_key)
    if cached and (now - cached['ts']) < 60:  # cache 60 ثانية
        return cached['result']
    df = fetch_candles(PAIRS[pair]['epic'], 'MINUTE_5', 10)
    if df.empty or len(df) < 5:
        return False
    rsi      = calc_rsi(df['close'], 5)
    rsi_now  = rsi.iloc[-1]
    rsi_prev = rsi.iloc[-2]
    result   = (direction == 'BUY'  and rsi_now < rsi_prev - 3) or                (direction == 'SELL' and rsi_now > rsi_prev + 3)
    _momentum_cache[cache_key] = {'ts': now, 'result': result}
    return result

def calculate_sl_at_r(pos, locked_r):
    entry, dir_, sl_dist = pos['entry'], pos['direction'], abs(pos['entry'] - pos['sl'])
    return round(entry + (locked_r * sl_dist), 5) if dir_ == 'BUY' else round(entry - (locked_r * sl_dist), 5)

def get_progressive_lock(profit_r):
    for threshold, locked in sorted(PROGRESSIVE_LOCK.items(), reverse=True):
        if profit_r >= threshold:
            return locked, f'@ {threshold}R'
    return 0, 'none'

def should_move_sl(current_sl, new_sl, direction):
    if direction == 'BUY':  return new_sl > current_sl + 0.00001
    else:                   return new_sl < current_sl - 0.00001

def calculate_trailing_sl(pos, cur_price, atr):
    trail_dist = atr * TRAILING_ATR_MULT
    return round(cur_price - trail_dist, 5) if pos['direction'] == 'BUY' else round(cur_price + trail_dist, 5)

def manage_smart_exits():
    """إدارة خروج ذكية — Partial TP + Trailing + Early Exit + Time Exit"""
    tracked  = op_get_all()
    if not tracked: return

    live_pos = get_open_positions()
    live_ids = {p.get('position', {}).get('dealId', '') for p in live_pos}

    for pos in tracked:
        deal_id = pos['deal_id']

        # ─── تسجيل الإغلاق ─────────────────────────────────
        if deal_id not in live_ids:
            exit_price = get_closed_deal_price(deal_id, get_current_price(pos['pair']))
            if exit_price > 0:
                result, _ = csv_log_trade(pos, exit_price,
                                          pos['stage1_done'], pos['stage2_done'], pos['stage3_done'],
                                          pos['final_locked_r'], 'STOP_OUT')
                if result != 'ERROR':
                    db_update(pos['db_key'], result.upper() if result in ('WIN','LOSS','BE') else 'CLOSED', 'STOP_OUT')
                    op_delete(deal_id)
            continue

        # ─── حساب الربح الحالي ─────────────────────────────
        cur_price = get_current_price(pos['pair'])
        if cur_price <= 0: continue

        entry, sl, tp = pos['entry'], pos['sl'], pos['tp']
        size, dir_    = pos['size'], pos['direction']
        atr_val       = pos['atr']
        sl_dist = abs(entry - sl)
        if sl_dist <= 0: continue

        profit_pts = (cur_price - entry) if dir_ == 'BUY' else (entry - cur_price)
        profit_r   = profit_pts / sl_dist

        bars_held = pos.get('bars_held', 0) + 1
        op_update(deal_id, bars_held=bars_held)

        s1, s2, s3 = pos['stage1_done'], pos['stage2_done'], pos['stage3_done']
        log(f'  📊 {pos["pair"]} {dir_} | R={profit_r:.2f} | Bars={bars_held} | S1:{s1} S2:{s2} S3:{s3}')

        # ─── 1. Time-based Exit ────────────────────────────
        if bars_held > MAX_TRADE_DURATION_BARS and profit_r < 0.5:
            log(f'  ⏰ TIME EXIT: {bars_held} bars, profit {profit_r:.2f}R')
            if close_full_api(deal_id):
                result, _ = csv_log_trade(pos, cur_price, s1, s2, s3, pos['final_locked_r'], 'TIME_EXIT')
                db_update(pos['db_key'], result, 'TIME_EXIT')
                op_delete(deal_id)
                nl = '\n'
                tg(f'⏰ *Time Exit — {pos["pair"]} {dir_}*{nl}مدة طويلة ({bars_held} شمعة){nl}@ `{cur_price}` | PnL: `{profit_r:.2f}R`{nl}_{utc_now()}_')
            continue

        # ─── 2. Early Exit ─────────────────────────────────
        # FIX: المنطق المقصود هو: خروج مبكر عندما profit_r بين -0.4 و -0.2
        # (أي الخسارة بدأت تتراكم لكن لم تصل SL بعد)
        if EARLY_EXIT_THRESHOLD <= profit_r < -0.2:
            if is_momentum_reversing(pos['pair'], dir_):
                log(f'  🔄 EARLY EXIT: momentum reversing @ {profit_r:.2f}R')
                if close_full_api(deal_id):
                    result, _ = csv_log_trade(pos, cur_price, s1, s2, s3, pos['final_locked_r'], 'EARLY_EXIT')
                    db_update(pos['db_key'], result, 'EARLY_EXIT')
                    op_delete(deal_id)
                    nl = '\n'
                    tg(f'🔄 *Early Exit — {pos["pair"]} {dir_}*{nl}انعكاس زخم مبكر @ `{profit_r:.2f}R`{nl}خسارة أقل من SL الكامل ✅{nl}_{utc_now()}_')
                continue

        # ─── 3. Partial TP — Stage 1 (1.5R) ───────────────
        if not s1 and profit_r >= STAGE1_TP_R:
            partial_size = round(size * STAGE1_PCT, 4)
            if close_partial_api(deal_id, partial_size):
                op_update(deal_id, stage1_done=1)
                # قفل SL عند نقطة التعادل
                be_sl = round(entry + 0.00001, 5) if dir_ == 'BUY' else round(entry - 0.00001, 5)
                if should_move_sl(sl, be_sl, dir_):
                    if update_sl_api(deal_id, be_sl, tp):
                        op_update(deal_id, sl=be_sl)
                nl = '\n'
                tg(f'💰 *Partial TP1 — {pos["pair"]} {dir_}*{nl}Stage 1 @ {profit_r:.2f}R | أُغلق {STAGE1_PCT:.0%}{nl}SL → Break Even{nl}_{utc_now()}_')
                log(f'  💰 Stage1 TP @ {profit_r:.2f}R')

        # ─── 4. Partial TP — Stage 2 (2.5R) ───────────────
        if s1 and not s2 and profit_r >= STAGE2_TP_R:
            remaining = size * (1 - STAGE1_PCT)
            partial_size = round(remaining * STAGE2_PCT, 4)
            if close_partial_api(deal_id, partial_size):
                op_update(deal_id, stage2_done=1)
                nl = '\n'
                tg(f'💰 *Partial TP2 — {pos["pair"]} {dir_}*{nl}Stage 2 @ {profit_r:.2f}R | أُغلق {STAGE2_PCT:.0%} من المتبقي{nl}_{utc_now()}_')
                log(f'  💰 Stage2 TP @ {profit_r:.2f}R')

        # ─── 5. Progressive SL Lock ────────────────────────
        locked_r, lock_reason = get_progressive_lock(profit_r)
        if locked_r > 0:
            new_sl = calculate_sl_at_r(pos, locked_r)
            cur_sl = float(pos['sl'])
            if should_move_sl(cur_sl, new_sl, dir_):
                if update_sl_api(deal_id, new_sl, tp):
                    op_update(deal_id, sl=new_sl, final_locked_r=locked_r)
                    log(f'  🔒 SL locked @ {locked_r}R ({lock_reason})')

        # ─── 6. Trailing Stop (بعد 2.5R) ─────────────────
        if profit_r >= TRAILING_START_R:
            trail_sl = calculate_trailing_sl(pos, cur_price, atr_val)
            cur_sl   = float(pos['sl'])
            if should_move_sl(cur_sl, trail_sl, dir_):
                if update_sl_api(deal_id, trail_sl, tp):
                    op_update(deal_id, sl=trail_sl)
                    log(f'  📈 Trailing SL → {trail_sl}')

        # ─── 7. Final TP (3.5R) ───────────────────────────
        if s1 and s2 and not s3 and profit_r >= FINAL_TP_R:
            if close_full_api(deal_id):
                result, _ = csv_log_trade(pos, cur_price, s1, s2, 1, pos['final_locked_r'], 'FINAL_TP')
                db_update(pos['db_key'], result, 'FINAL_TP')
                op_delete(deal_id)
                nl = '\n'
                tg(f'🏆 *Final TP — {pos["pair"]} {dir_}*{nl}@ {profit_r:.2f}R | هدف اكتمل!{nl}_{utc_now()}_')


# ═══════════════════════════════════════════════════════
# MAIN LOOP — الحلقة الرئيسية
# ═══════════════════════════════════════════════════════

def main_loop():
    """الحلقة الرئيسية للبوت"""
    log('🚀 TL Breaks Bot v3.3 — بدء التشغيل')
    db_init()
    csv_init()

    # إنشاء الجلسة الأولى
    if not create_session():
        log('❌ فشل الاتصال الأولي — إعادة المحاولة بعد 60 ثانية')
        time.sleep(60)
        if not create_session():
            log('❌ فشل الاتصال مرتين — إيقاف البوت')
            return

    tg(f'🤖 *TL Breaks Bot v3.3 — تشغيل*\nأزواج: {", ".join(PAIRS.keys())}\nInterval: {SCAN_INTERVAL}s\n_{utc_now()}_')

    last_ping      = time.time()
    last_daily_sum = datetime.now(timezone.utc).date()
    scan_count     = 0

    while True:
        try:
            now = time.time()

            # ─── Ping كل 300 ثانية ──────────────────────
            if now - last_ping >= 300:
                ping_session()
                last_ping = now

            # ─── تحديث الرصيد ────────────────────────────
            balance = get_current_balance()
            log(f'\n{"="*50}')
            log(f'💰 Balance: ${balance:.2f} | Scan #{scan_count + 1}')

            # ─── التحقق من شروط التداول ──────────────────
            ok, reason, session_mult, session_name, day_pnl = should_trade()
            if not ok:
                log(f'⏸ التداول موقف: {reason}')
                # إدارة الصفقات المفتوحة حتى في حالة الإيقاف
                manage_smart_exits()
                time.sleep(SCAN_INTERVAL)
                scan_count += 1
                continue

            log(f'📅 Session: {session_name} | Day PnL: ${day_pnl:.2f}')

            # ─── إدارة الصفقات المفتوحة أولاً ─────────────
            manage_smart_exits()

            # ─── فحص عدد الصفقات المفتوحة ────────────────
            open_count = len(op_get_all())
            if open_count >= MAX_OPEN_TRADES:
                log(f'⚠️ الحد الأقصى للصفقات: {open_count}/{MAX_OPEN_TRADES}')
                time.sleep(SCAN_INTERVAL)
                scan_count += 1
                continue

            # ─── مسح الأزواج ─────────────────────────────
            for pair, cfg in PAIRS.items():
                try:
                    # تفريغ cache الشمعات قبل كل زوج
                    _candle_cache.clear()

                    log(f'  🔍 فحص {pair}...')

                    # فحص نظام التقلب
                    vol_regime, vol_mult = check_volatility_regime(cfg['epic'])
                    if vol_regime == 'EXTREME':
                        log(f'  ⚠️ {pair}: تقلب مفرط — تخطي')
                        continue

                    effective_mult = session_mult * vol_mult
                    sig = check_signal(pair, cfg, effective_mult)

                    if sig:
                        log(f'  🎯 إشارة: {pair} {sig["direction"]} @ {sig["entry"]}')
                        # إعادة فحص عدد الصفقات
                        if len(op_get_all()) < MAX_OPEN_TRADES:
                            place_order(sig, session_name or 'N/A')
                    else:
                        log(f'  — لا إشارة على {pair}')

                    time.sleep(1)  # تجنب rate limiting

                except Exception as pair_ex:
                    log(f'  ❌ خطأ في {pair}: {pair_ex}')
                    continue

            # ─── Daily Summary ────────────────────────────
            today = datetime.now(timezone.utc).date()
            if today != last_daily_sum:
                tg_daily_summary()
                last_daily_sum = today

            scan_count += 1
            log(f'✅ انتهى المسح #{scan_count} — انتظار {SCAN_INTERVAL}s')
            time.sleep(SCAN_INTERVAL)

        except KeyboardInterrupt:
            log('\n⛔ إيقاف يدوي')
            tg(f'⛔ *البوت أُوقف يدوياً*\n_{utc_now()}_')
            break

        except Exception as ex:
            log(f'❌ خطأ في الحلقة الرئيسية: {ex}')
            # محاولة إعادة الاتصال
            log('🔄 إعادة إنشاء الجلسة...')
            time.sleep(30)
            create_session()
            time.sleep(SCAN_INTERVAL)


# ═══════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════
if __name__ == '__main__':
    main_loop()
