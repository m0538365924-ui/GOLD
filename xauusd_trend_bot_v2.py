#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ==========================================================
# multi_pairs_bot.py â€” TL Breaks Bot (v3.3-final)
# âœ… FIX: Ø¬Ù…ÙŠØ¹ Ø§Ù„Ø£Ø®Ø·Ø§Ø¡ Ù…ÙØµÙ„Ø­Ø© + ØªØ­Ø³ÙŠÙ†Ø§Øª Ù…Ø¶Ø§ÙØ©
# ==========================================================

import os, csv, json, time, sqlite3, requests
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timezone, timedelta
from threading import Lock
from dotenv import load_dotenv

load_dotenv()

# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# Ø§Ù„Ù…ØªØºÙŠØ±Ø§Øª Ø§Ù„Ø±Ø¦ÙŠØ³ÙŠØ© â€” Ø¹Ø¯Ù‘Ù„Ù‡Ø§ Ù…Ø¨Ø§Ø´Ø±Ø© Ù‡Ù†Ø§
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
API_KEY    = os.getenv('CAPITAL_API_KEY',  'YOUR_API_KEY')
EMAIL      = os.getenv('CAPITAL_EMAIL',    'your@email.com')
PASSWORD   = os.getenv('CAPITAL_PASSWORD', 'YourPassword')
TG_TOKEN   = os.getenv('TG_TOKEN',         'YOUR_TG_BOT_TOKEN')
TG_CHAT_ID = os.getenv('TG_CHAT_ID',       'YOUR_CHAT_ID')

BASE_URL  = 'https://api-capital.backend-capital.com'
DEMO_MODE = os.getenv('DEMO_MODE', 'false').lower() == 'true'

# Ø§Ù„Ø£Ø²ÙˆØ§Ø¬ Ø§Ù„Ù…Ø¯Ø¹ÙˆÙ…Ø©
PAIRS = {
    'GOLD':   {'epic': 'GOLD',   'allow_buy': True, 'allow_sell': True, 'size_override': None},
    'BTCUSD': {'epic': 'BTCUSD', 'allow_buy': True, 'allow_sell': True, 'size_override': None},
    'EURUSD': {'epic': 'EURUSD', 'allow_buy': True, 'allow_sell': True, 'size_override': None},
    'GBPUSD': {'epic': 'GBPUSD', 'allow_buy': True, 'allow_sell': True, 'size_override': None},
    'US100':  {'epic': 'US100',  'allow_buy': True, 'allow_sell': True, 'size_override': None},
    'US500':  {'epic': 'US500',  'allow_buy': True, 'allow_sell': True, 'size_override': None},
}

STRATEGY_TF   = os.getenv('STRATEGY_TF', 'MINUTE_15')
CANDLES_COUNT = 500  # FIX: ÙƒØ§Ù† Ù…ÙƒØªÙˆØ¨Ø§Ù‹ CANDLES_COUN ÙÙŠ Ø¢Ø®Ø± Ø§Ù„ÙƒÙˆØ¯ Ø§Ù„Ø£ØµÙ„ÙŠ
SCAN_INTERVAL = int(os.getenv('SCAN_INTERVAL', '300'))

# Ø¥Ø¹Ø¯Ø§Ø¯Ø§Øª Ø§Ù„Ø§Ø³ØªØ±Ø§ØªÙŠØ¬ÙŠØ©
LENGTH       = int(os.getenv('LENGTH', '10'))
SLOPE_MULT   = float(os.getenv('SLOPE_MULT', '1.0'))
SLOPE_METHOD = os.getenv('SLOPE_METHOD', 'ATR')
ATR_PERIOD   = 14
SL_ATR_MULT  = 1.5
TP_ATR_MULT  = 3.0

# FIX: Supertrend ÙŠØ³ØªØ®Ø¯Ù… Ù†ÙØ³ Ø¥Ø¹Ø¯Ø§Ø¯Ø§Øª Ø§Ù„ÙƒÙˆØ¯ (mult=SLOPE_MULT Ø¨Ø¯Ù„Ø§Ù‹ Ù…Ù† 3.0 Ø§Ù„Ø«Ø§Ø¨ØªØ©)
SUPERTREND_MULT = float(os.getenv('SUPERTREND_MULT', '1.0'))  # FIX: Ù…ØªÙˆØ§ÙÙ‚ Ù…Ø¹ SLOPE_MULT

# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# Ø¥Ø¹Ø¯Ø§Ø¯Ø§Øª Ø¥Ø¯Ø§Ø±Ø© Ø§Ù„Ù…Ø®Ø§Ø·Ø± Ø§Ù„Ø¯ÙŠÙ†Ø§Ù…ÙŠÙƒÙŠØ©
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
BASE_RISK_PERCENT  = 0.01
KELLY_FRACTION     = 0.25
MAX_RISK_PERCENT   = 0.03
MIN_RISK_PERCENT   = 0.005
MAX_DAILY_RISK     = 0.05
MAX_WEEKLY_RISK    = 0.10
DAILY_PROFIT_TARGET = 0.03

# Ø¥Ø¹Ø¯Ø§Ø¯Ø§Øª Ø§Ù„Ø¬Ù„Ø³Ø§Øª
SESSIONS = {
    'ASIA':       {'start': 0,  'end': 7,  'risk_mult': 0.5, 'name': 'Ø¢Ø³ÙŠØ§ (Ù‡Ø§Ø¯Ø¦)'},
    'LONDON_OPEN':{'start': 7,  'end': 10, 'risk_mult': 1.2, 'name': 'ÙØªØ­ Ù„Ù†Ø¯Ù†'},
    'LONDON_MID': {'start': 10, 'end': 12, 'risk_mult': 1.0, 'name': 'Ù…Ù†ØªØµÙ Ù„Ù†Ø¯Ù†'},
    'LONDON_NY':  {'start': 12, 'end': 16, 'risk_mult': 1.5, 'name': 'ØªØ¯Ø§Ø®Ù„ Ù„Ù†Ø¯Ù†-Ù†ÙŠÙˆÙŠÙˆØ±Ùƒ â­'},
    'NY_PM':      {'start': 16, 'end': 20, 'risk_mult': 0.7, 'name': 'Ø¨Ø¹Ø¯ Ø§Ù„Ø¸Ù‡Ø± Ø§Ù„Ø£Ù…Ø±ÙŠÙƒÙŠ'},
    'QUIET':      {'start': 20, 'end': 24, 'risk_mult': 0.3, 'name': 'Ù‡Ø§Ø¯Ø¦ (ØªØ¬Ù†Ø¨)'},
}

VOLATILITY_THRESHOLDS = {'EXTREME': 2.0, 'HIGH': 1.5, 'LOW': 0.6}

# Ø¥Ø¹Ø¯Ø§Ø¯Ø§Øª Smart Exits
MAX_TRADE_DURATION_BARS = 24
EARLY_EXIT_THRESHOLD    = -0.4
TRAILING_START_R        = 2.5
TRAILING_ATR_MULT       = 1.5

# Partial TP
STAGE1_TP_R, STAGE1_PCT = 1.5, 0.50
STAGE2_TP_R, STAGE2_PCT = 2.5, 0.30
FINAL_TP_R,  FINAL_PCT  = 3.5, 0.50

PROGRESSIVE_LOCK = {2.0: 0.5, 2.5: 1.0, 3.0: 1.5, 3.5: 2.0, 4.5: 3.0, 6.0: 4.0}

# Ø¥Ø¹Ø¯Ø§Ø¯Ø§Øª RSI
RSI_SELL_MIN, RSI_SELL_MAX = 55, 78
RSI_BUY_MIN,  RSI_BUY_MAX  = 22, 45

# FIX: ØªØ­Ø³ÙŠÙ†Ø§Øª v3.3 â€” ÙÙ„Ø§ØªØ± Ø¥Ø¶Ø§ÙÙŠØ©
SPREAD_ATR_MAX    = 0.25
HTF_RESOLUTION    = 'HOUR'         # ØªØ£ÙƒÙŠØ¯ H1 Ù„Ù„Ø¥Ø´Ø§Ø±Ø§Øª M15
CORR_PAIRS        = {frozenset({'EURUSD', 'GBPUSD'})}  # Ø£Ø²ÙˆØ§Ø¬ Ù…ØªØ±Ø§Ø¨Ø·Ø© â€” ØªØ¬Ù†Ø¨ ÙØªØ­ Ù†ÙØ³ Ø§Ù„Ø§ØªØ¬Ø§Ù‡

RISK_PERCENT       = float(os.getenv('RISK_PERCENT', '0.01'))
MAX_OPEN_TRADES    = int(os.getenv('MAX_OPEN_TRADES', '6'))
MAX_CONSECUTIVE_LOSS = int(os.getenv('MAX_CONSEC_LOSS', '3'))
ACCOUNT_BALANCE   = float(os.getenv('ACCOUNT_BALANCE', '1000'))

_BASE_DIR  = os.getenv('DATA_DIR', '/tmp')
DB_FILE    = os.path.join(_BASE_DIR, 'multi_bot.db')
TRADES_CSV = os.path.join(_BASE_DIR, 'trades_log.csv')

db_lock, session_headers = Lock(), {}
# FIX: _meta_cache Ù…Ø¹ invalidation Ø¹Ù†Ø¯ ÙØ´Ù„ Ø§Ù„Ø§ØªØµØ§Ù„
_meta_cache: dict = {}
_candle_cache: dict = {}  # FIX: cache Ù„Ù„Ø´Ù…Ø¹Ø§Øª Ù„ØªÙ‚Ù„ÙŠÙ„ API calls

CSV_HEADERS = [
    'date', 'time_utc', 'pair', 'direction', 'entry', 'sl', 'tp', 'exit_price',
    'atr', 'size', 'sl_dist', 'pnl_usd', 'pnl_r', 'result', 'bars_held', 'spread', 'tf',
    'stage1_done', 'stage2_done', 'stage3_done', 'final_locked_r', 'exit_type',
    'session_used', 'risk_percent', 'supertrend', 'rsi', 'ema_fast', 'ema_slow',
]


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# DATABASE â€” Ù…Ø¹ Migration ÙƒØ§Ù…Ù„
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

def _migrate_database(conn):
    """Ø¥Ø¶Ø§ÙØ© Ø£Ø¹Ù…Ø¯Ø© Ù…ÙÙ‚ÙˆØ¯Ø© ØªÙ„Ù‚Ø§Ø¦ÙŠØ§Ù‹ Ù„Ù„Ø¬Ø¯Ø§ÙˆÙ„ Ø§Ù„Ù‚Ø¯ÙŠÙ…Ø©"""
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
                log(f'  âœ… Migrated: added column {col}')
    except Exception as ex:
        log(f'  âš ï¸ Migration warning: {ex}')

def db_init():
    """ØªÙ‡ÙŠØ¦Ø© Ù‚Ø§Ø¹Ø¯Ø© Ø§Ù„Ø¨ÙŠØ§Ù†Ø§Øª Ù…Ø¹ Ø¬Ù…ÙŠØ¹ Ø§Ù„Ø£Ø¹Ù…Ø¯Ø©"""
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
    """Ø¬Ù„Ø¨ Ø§Ù„ØµÙÙ‚Ø§Øª Ø§Ù„Ø£Ø®ÙŠØ±Ø© Ù…Ø¹ Ø¯Ø¹Ù… DB Ø§Ù„Ù‚Ø¯ÙŠÙ…"""
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
    """ØªØ­Ø¯ÙŠØ« PnL ÙÙŠ DB Ø¨Ø¹Ø¯ Ø§Ù„Ø¥ØºÙ„Ø§Ù‚"""
    with db_lock:
        with sqlite3.connect(DB_FILE) as conn:
            try:
                conn.execute(
                    'UPDATE trades SET pnl_r=?, pnl_usd=?, exit_price=?, bars_held=? WHERE key=?',
                    (pnl_r, pnl_usd, exit_price, bars_held, db_key)
                )
                conn.commit()
            except Exception as ex:
                log(f'  âš ï¸ Could not update PnL: {ex}')

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
    # FIX: Ø§Ø³ØªØ®Ø¯Ø§Ù… parameterized queries Ø¨Ø¯Ù„Ø§Ù‹ Ù…Ù† f-string Ù…Ø¨Ø§Ø´Ø±Ø© (SQL Injection)
    with db_lock:
        with sqlite3.connect(DB_FILE) as conn:
            allowed_cols = {
                'stage1_done', 'stage2_done', 'stage3_done',
                'final_locked_r', 'bars_held', 'sl', 'tp'
            }
            for col, val in kwargs.items():
                if col not in allowed_cols:
                    log(f'  âš ï¸ op_update: Ø¹Ù…ÙˆØ¯ ØºÙŠØ± Ù…Ø³Ù…ÙˆØ­ Ø¨Ù‡: {col}')
                    continue
                # FIX: whitelist + parameterized query Ø¢Ù…Ù†
                conn.execute(f'UPDATE open_positions SET {col}=? WHERE deal_id=?', (val, deal_id))
            conn.commit()

def op_delete(deal_id):
    with db_lock:
        with sqlite3.connect(DB_FILE) as conn:
            conn.execute('DELETE FROM open_positions WHERE deal_id=?', (deal_id,))
            conn.commit()


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# Ø¥Ø¯Ø§Ø±Ø© Ø§Ù„Ù…Ø®Ø§Ø·Ø± Ø§Ù„Ø¯ÙŠÙ†Ø§Ù…ÙŠÙƒÙŠØ© (Kelly + Drawdown)
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

def calculate_pnl_since(since_dt):
    """Ø­Ø³Ø§Ø¨ PnL Ù…Ù†Ø° ØªØ§Ø±ÙŠØ® Ù…Ø¹ÙŠÙ†"""
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
    """Ø§Ù„ØªØ­Ù‚Ù‚ Ù…Ù† Ø­Ø¯ÙˆØ¯ Ø§Ù„Ø®Ø³Ø§Ø±Ø© Ø§Ù„ÙŠÙˆÙ…ÙŠØ© ÙˆØ§Ù„Ø£Ø³Ø¨ÙˆØ¹ÙŠØ©"""
    now = datetime.now(timezone.utc)
    balance = get_current_balance()
    day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    day_pnl = calculate_pnl_since(day_start)
    if day_pnl <= -balance * MAX_DAILY_RISK:
        return False, f'ðŸ›‘ DAILY LIMIT: {day_pnl/balance:.1%} loss', 0.0
    if day_pnl >= balance * DAILY_PROFIT_TARGET:
        return False, f'ðŸ”’ DAILY TARGET HIT: +{day_pnl/balance:.1%}', 0.0
    week_start = (now - timedelta(days=now.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
    week_pnl = calculate_pnl_since(week_start)
    if week_pnl <= -balance * MAX_WEEKLY_RISK:
        return False, f'ðŸ›‘ WEEKLY LIMIT: {week_pnl/balance:.1%} loss', 0.0
    return True, 'OK', day_pnl

def get_pair_stats(pair, lookback=20):
    """Ø¥Ø­ØµØ§Ø¦ÙŠØ§Øª Ø£Ø¯Ø§Ø¡ Ø§Ù„Ø²ÙˆØ¬ Ù„Ù„Ù€ Kelly"""
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
    """Ø­Ø³Ø§Ø¨ Ø§Ù„Ù…Ø®Ø§Ø·Ø±Ø© Ø§Ù„Ø¯ÙŠÙ†Ø§Ù…ÙŠÙƒÙŠØ©"""
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


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# ÙÙ„ØªØ± Ø§Ù„Ø¬Ù„Ø³Ø§Øª + Ø§Ù„ØªÙ‚Ù„Ø¨
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

def get_session_info():
    """Ø§Ù„Ø¬Ù„Ø³Ø© Ø§Ù„Ø­Ø§Ù„ÙŠØ©"""
    hour = datetime.now(timezone.utc).hour
    for name, cfg in SESSIONS.items():
        if cfg['start'] <= hour < cfg['end']:
            return cfg['risk_mult'], cfg['name'], name
    return 0.0, 'Ù…ØºÙ„Ù‚Ø©', 'CLOSED'

def check_volatility_regime(epic):
    """Ù†Ø¸Ø§Ù… Ø§Ù„ØªÙ‚Ù„Ø¨ Ø§Ù„Ø­Ø§Ù„ÙŠ"""
    # FIX: Ø§Ø³ØªØ®Ø¯Ø§Ù… candle cache Ù„ØªØ¬Ù†Ø¨ API call Ù…ÙƒØ±Ø±
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
    """Ø§Ù„ØªØ­Ù‚Ù‚ Ø§Ù„Ù†Ù‡Ø§Ø¦ÙŠ Ù…Ù† Ø§Ù„Ø¬Ø§Ù‡Ø²ÙŠØ©"""
    allowed, reason, day_pnl = check_drawdown_limits()
    if not allowed:
        return False, reason, 0.0, None, 0.0
    session_mult, session_name, _ = get_session_info()
    if session_mult == 0:
        return False, f'â¸ Ø®Ø§Ø±Ø¬ Ø§Ù„Ø¬Ù„Ø³Ø§Øª: {session_name}', 0.0, None, 0.0
    return True, 'OK', session_mult, session_name, day_pnl

def check_correlation_filter(pair, direction):
    """FIX: ØªØ¬Ù†Ø¨ ÙØªØ­ ØµÙÙ‚Ø§Øª Ù…ØªØ¹Ø§ÙƒØ³Ø© Ø¹Ù„Ù‰ Ø£Ø²ÙˆØ§Ø¬ Ù…ØªØ±Ø§Ø¨Ø·Ø©"""
    open_positions = op_get_all()
    for pos in open_positions:
        pair_set = frozenset({pair, pos['pair']})
        if pair_set in CORR_PAIRS and pos['direction'] == direction:
            log(f'  âš ï¸ Correlation filter: {pair} Ù…Ø±ØªØ¨Ø· Ø¨Ù€ {pos["pair"]} Ø¨Ù†ÙØ³ Ø§Ù„Ø§ØªØ¬Ø§Ù‡')
            return False
    return True


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# CSV & LOG HELPERS
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
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
        icon   = 'âœ…' if result == 'WIN' else ('âŒ' if result == 'LOSS' else 'ðŸ”µ')
        stages = f' | S1:{stage1} S2:{stage2} S3:{stage3}' if any([stage1, stage2, stage3]) else ''
        log(f'  {icon} CLOSED {pair} {dir_} | {exit_type} | PnL=${pnl_usd:+.2f} ({pnl_r:+.2f}R){stages}')
        nl = '\n'
        tg(f'{icon} *{pair} {dir_} â€” {result}*{nl}Exit: `{exit_type}`{nl}PnL: `${pnl_usd:+.2f}` | `{pnl_r:+.2f}R`{nl}_{utc_now()}_')
        return result, pnl_usd
    except Exception as ex:
        log(f'  csv_log_trade ERROR: {ex}')
        return 'ERROR', 0


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# API HELPERS
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
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
    # FIX: invalidate cache Ø¹Ù†Ø¯ ÙØ´Ù„ Ø§Ù„Ø§ØªØµØ§Ù„
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
    """Ø¥Ù†Ø´Ø§Ø¡ Ø¬Ù„Ø³Ø© Capital.com"""
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
            log('âœ… Session OK')
            return True
        log(f'âŒ Session FAILED [{r.status_code}]: {r.text[:100]}')
    except Exception as ex:
        log(f'âŒ Session ERROR: {ex}')
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
    # FIX: Capital.com ÙŠØ³ØªØ®Ø¯Ù… POSITION_CLOSED â€” Ø£Ø¶ÙÙ†Ø§ POSITION_CLOSED ÙƒØ£ÙˆÙ„ÙˆÙŠØ©
    try:
        r = _get('/api/v1/history/activity', params={'dealId': deal_id, 'pageSize': 10})
        if r and r.status_code == 200:
            for act in r.json().get('activities', []):
                for action in act.get('details', {}).get('actions', []):
                    # FIX: POSITION_CLOSED Ù‡Ùˆ Ø§Ù„Ù†ÙˆØ¹ Ø§Ù„ØµØ­ÙŠØ­ ÙÙŠ Capital.com
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
        log(f'  âœ… SL moved to {new_sl}')
        return True
    log(f'  âŒ SL update failed')
    return False

def close_partial_api(deal_id, size):
    # FIX: Partial close ÙÙŠ Capital.com ÙŠØ³ØªØ®Ø¯Ù… DELETE Ù…Ø¹ size parameter
    # endpoint ØµØ­ÙŠØ­: DELETE /api/v1/positions/{dealId} Ù…Ø¹ body Ø£Ùˆ query
    r = _delete(f'/api/v1/positions/{deal_id}?size={size}')
    if r and r.status_code == 200:
        log(f'  ðŸ’° Partial close: {size} units')
        return True
    # Fallback: POST Ø¥Ø°Ø§ ÙƒØ§Ù† Ø§Ù„Ù€ broker ÙŠØ¯Ø¹Ù…Ù‡
    r2 = _post(f'/api/v1/positions/otc', {'dealId': deal_id, 'direction': 'close', 'size': size})
    if r2 and r2.status_code == 200:
        log(f'  ðŸ’° Partial close (OTC): {size} units')
        return True
    log(f'  âŒ Partial close failed: {(r.text if r else "no response")[:100]}')
    return False

def close_full_api(deal_id):
    r = _delete(f'/api/v1/positions/{deal_id}')
    if r and r.status_code == 200:
        log(f'  ðŸšª Full close')
        return True
    return False


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# TELEGRAM
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
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
    icon = 'ðŸŸ¢' if sig['direction'] == 'BUY' else 'ðŸ”´'
    mode = 'DEMO' if DEMO_MODE else 'LIVE'
    nl   = '\n'
    tg(f'{icon} *{sig["pair"]} {sig["direction"]}* [{mode}]{nl}'
       f'Entry: `{sig["entry"]}` | SL: `{sig["sl"]}` | TP: `{sig["tp"]}`{nl}'
       f'Risk: `{risk_info[0]:.2%}` ({risk_info[1]}){nl}'
       f'Session: `{session_info}`{nl}'
       f'RSI: `{sig.get("rsi", 0):.1f}` | ATR: `{sig.get("atr", 0):.5f}`{nl}'
       f'Stages: `1.5Râ†’50%` | `2.5Râ†’30%` | `3.5Râ†’50% of rest`{nl}'
       f'_{utc_now()}_')

def tg_daily_summary():
    """Ù…Ù„Ø®Øµ ÙŠÙˆÙ…ÙŠ Ø¹Ø¨Ø± Telegram"""
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
    tg(f'ðŸ“Š *Daily Summary â€” {now.strftime("%Y-%m-%d")}*{nl}'
       f'Trades: {len(rows)} | Wins: {len(wins)} | Losses: {len(losses)}{nl}'
       f'WinRate: {len(wins)/len(rows)*100:.0f}%{nl}' if rows else 'No trades today\n'
       f'PnL: `${total:+.2f}`{nl}'
       f'Balance: `${get_current_balance():.2f}`{nl}'
       f'_{utc_now()}_')


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# INDICATORS
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
def fetch_candles(epic, resolution, count=500):
    """Ø¬Ù„Ø¨ Ø§Ù„Ø´Ù…Ø¹Ø§Øª Ù…Ù† Capital.com API"""
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
    """FIX: Ø¬Ù„Ø¨ Ø§Ù„Ø´Ù…Ø¹Ø§Øª Ù…Ø¹ cache Ù„ØªÙ‚Ù„ÙŠÙ„ API calls Ø§Ù„Ù…ÙƒØ±Ø±Ø©"""
    cache_key = f'{epic}_{resolution}_{count}'
    now       = time.time()
    cached    = _candle_cache.get(cache_key)
    # cache ØµØ§Ù„Ø­ Ù„Ù€ 60 Ø«Ø§Ù†ÙŠØ© (Ø£Ù‚Ù„ Ù…Ù† SCAN_INTERVAL)
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
    # FIX: Ø§Ø³ØªØ®Ø¯Ø§Ù… SUPERTREND_MULT Ø§Ù„Ù…ØªÙˆØ§ÙÙ‚ Ù…Ø¹ Ø§Ù„Ø¥Ø¹Ø¯Ø§Ø¯Ø§Øª Ø¨Ø¯Ù„Ø§Ù‹ Ù…Ù† 3.0 Ø§Ù„Ø«Ø§Ø¨ØªØ©
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
    """FIX: ØªØ£ÙƒÙŠØ¯ Ø§Ù„Ø§ØªØ¬Ø§Ù‡ Ù…Ù† H1 (Higher Time Frame)"""
    df = fetch_candles(epic, HTF_RESOLUTION, 50)
    if df.empty or len(df) < 20:
        return None  # ØºÙŠØ± Ù…Ø¹Ø±ÙˆÙ â€” Ù„Ø§ Ù†Ø±ÙØ¶ Ø§Ù„Ø¥Ø´Ø§Ø±Ø©
    ema_fast = calc_ema(df['close'], 9).iloc[-1]
    ema_slow = calc_ema(df['close'], 21).iloc[-1]
    _, st_dir = calc_supertrend(df, period=10, mult=SUPERTREND_MULT)
    htf_dir = st_dir.iloc[-1]
    if ema_fast > ema_slow and htf_dir == 1:
        return 'BUY'
    elif ema_fast < ema_slow and htf_dir == -1:
        return 'SELL'
    return None  # Ù…Ø­Ø§ÙŠØ¯


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# CHECK SIGNAL â€” Ù…ÙƒØªÙ…Ù„Ø© 100% Ù…Ø¹ TL Break Logic
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

def check_signal(pair, cfg, session_mult=1.0):
    """
    ÙƒØ´Ù Ø¥Ø´Ø§Ø±Ø© TL Break Ø¹Ù„Ù‰ M15:
    1. Ø¬Ù„Ø¨ Ø§Ù„Ø´Ù…Ø¹Ø§Øª
    2. Ø­Ø³Ø§Ø¨ Ø§Ù„Ù…Ø¤Ø´Ø±Ø§Øª (Supertrend + RSI + EMA + ATR)
    3. ÙƒØ´Ù Pivot Highs/Lows
    4. Ø±Ø³Ù… Ø®Ø·ÙˆØ· Ø§Ù„Ø§ØªØ¬Ø§Ù‡ (TL)
    5. Ø§Ø®ØªØ¨Ø§Ø± Ø§Ù„ÙƒØ³Ø± Ù…Ø¹ ØªØ£ÙƒÙŠØ¯ Ø§Ù„Ø´Ù…Ø¹Ø©
    6. ÙÙ„ØªØ± Ø§Ù„Ø§Ù†ØªØ´Ø§Ø± + HTF + Volume
    7. Ø­Ø³Ø§Ø¨ Entry + SL + TP
    8. Ø¥Ø±Ø¬Ø§Ø¹ dict Ø§Ù„Ø¥Ø´Ø§Ø±Ø© Ø£Ùˆ None
    """
    epic = cfg['epic']

    # â”€â”€â”€ 1. Ø¬Ù„Ø¨ Ø§Ù„Ø¨ÙŠØ§Ù†Ø§Øª (Ù…Ø¹ cache Ù…Ø´ØªØ±Ùƒ) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    df = fetch_candles_cached(epic, STRATEGY_TF, CANDLES_COUNT)  # FIX: CANDLES_COUNT
    if df.empty or len(df) < 100:
        return None

    # â”€â”€â”€ 2. Ø­Ø³Ø§Ø¨ Ø§Ù„Ù…Ø¤Ø´Ø±Ø§Øª â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    atr_s   = calc_atr_series(df, ATR_PERIOD)
    st, st_dir = calc_supertrend(df, period=LENGTH, mult=SUPERTREND_MULT)
    ema_fast = calc_ema(df['close'], 9)
    ema_slow = calc_ema(df['close'], 21)
    rsi_s    = calc_rsi(df['close'], 14)

    # Ù‚ÙŠÙ… Ø¢Ø®Ø± Ø´Ù…Ø¹Ø© Ù…ØºÙ„Ù‚Ø© ([-2] Ù„ØªØ¬Ù†Ø¨ Ø§Ù„Ø´Ù…Ø¹Ø© Ø§Ù„Ø­Ø§Ù„ÙŠØ© Ø§Ù„Ù…ÙØªÙˆØ­Ø©)
    idx      = -2
    atr_val  = float(atr_s.iloc[idx])
    st_val   = float(st.iloc[idx])
    st_d     = int(st_dir.iloc[idx])
    rsi_val  = float(rsi_s.iloc[idx])
    ema_f    = float(ema_fast.iloc[idx])
    ema_s    = float(ema_slow.iloc[idx])
    close_c  = float(df['close'].iloc[idx])
    close_prev = float(df['close'].iloc[idx - 1])

    # â”€â”€â”€ 3. ÙÙ„ØªØ± Supertrend Ø§Ù„Ø£Ø³Ø§Ø³ÙŠ â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    # st_d == 1 â†’ ØµØ§Ø¹Ø¯, st_d == -1 â†’ Ù‡Ø§Ø¨Ø·
    if st_d == 1 and not cfg['allow_buy']:  return None
    if st_d == -1 and not cfg['allow_sell']: return None

    # â”€â”€â”€ 4. ÙƒØ´Ù Pivots â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    pivot_len = LENGTH
    ph = find_pivot_high(df['high'], pivot_len)
    pl = find_pivot_low(df['low'],  pivot_len)

    # Ø¬Ù…Ø¹ Ø£Ø­Ø¯Ø« pivots ØµØ§Ù„Ø­Ø© (Ø¢Ø®Ø± 80 Ø´Ù…Ø¹Ø©)
    recent_phs = [(i, v) for i, v in enumerate(ph[-80:]) if not np.isnan(v)]
    recent_pls = [(i, v) for i, v in enumerate(pl[-80:]) if not np.isnan(v)]

    # â”€â”€â”€ 5. Ø±Ø³Ù… TL ÙˆØ§Ø®ØªØ¨Ø§Ø± Ø§Ù„ÙƒØ³Ø± â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    signal_dir = None
    tl_break_confirmed = False
    tl_anchor_i, tl_anchor_v, tl_slope = None, None, None

    # TL Ù‡Ø§Ø¨Ø· (Bearish TL) â†’ ÙƒØ³Ø±Ù‡ Ø¥Ø´Ø§Ø±Ø© BUY
    if st_d == 1 and len(recent_phs) >= 2 and cfg['allow_buy']:
        p2 = recent_phs[-1]
        p1 = recent_phs[-2]
        if p1[0] < p2[0]:
            slope = get_slope_val(SLOPE_METHOD, df.iloc[-80:], p2[0], pivot_len, SLOPE_MULT, atr_s.iloc[-80:])
            # TL Ù‡Ø§Ø¨Ø·: p1.high > p2.high â€” Ù†Ø±Ø³Ù… Ø®Ø· Ù†Ø²ÙˆÙ„ÙŠ
            if p1[1] > p2[1]:
                tl_at_now  = tl_value(p2[0], p2[1], slope, False, len(df.iloc[-80:]) - 2)
                tl_at_prev = tl_value(p2[0], p2[1], slope, False, len(df.iloc[-80:]) - 3)
                # ÙƒØ³Ø± ØªØ£ÙƒÙŠØ¯ÙŠ: Ø§Ù„Ø´Ù…Ø¹Ø© Ø§Ù„Ø³Ø§Ø¨Ù‚Ø© ØªØ­Øª Ø§Ù„Ù€ TL ÙˆØ§Ù„Ø­Ø§Ù„ÙŠØ© ÙÙˆÙ‚Ù‡
                if close_prev < tl_at_prev and close_c > tl_at_now:
                    signal_dir = 'BUY'
                    tl_break_confirmed = True
                    tl_anchor_i, tl_anchor_v, tl_slope = p2[0], p2[1], slope

    # TL ØµØ§Ø¹Ø¯ (Bullish TL) â†’ ÙƒØ³Ø±Ù‡ Ø¥Ø´Ø§Ø±Ø© SELL
    if st_d == -1 and len(recent_pls) >= 2 and cfg['allow_sell']:
        p2 = recent_pls[-1]
        p1 = recent_pls[-2]
        if p1[0] < p2[0]:
            slope = get_slope_val(SLOPE_METHOD, df.iloc[-80:], p2[0], pivot_len, SLOPE_MULT, atr_s.iloc[-80:])
            # TL ØµØ§Ø¹Ø¯: p1.low < p2.low â€” Ù†Ø±Ø³Ù… Ø®Ø· ØªØµØ§Ø¹Ø¯ÙŠ
            if p1[1] < p2[1]:
                tl_at_now  = tl_value(p2[0], p2[1], slope, True, len(df.iloc[-80:]) - 2)
                tl_at_prev = tl_value(p2[0], p2[1], slope, True, len(df.iloc[-80:]) - 3)
                if close_prev > tl_at_prev and close_c < tl_at_now:
                    signal_dir = 'SELL'
                    tl_break_confirmed = True

    if not tl_break_confirmed or signal_dir is None:
        return None

    # â”€â”€â”€ 6. ÙÙ„Ø§ØªØ± Ø¥Ø¶Ø§ÙÙŠØ© â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

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
        log(f'  âš ï¸ {pair} spread too high: {spread:.5f} > {SPREAD_ATR_MAX * atr_val:.5f}')
        return None

    # FIX: ØªØ£ÙƒÙŠØ¯ HTF (H1)
    htf_trend = get_htf_trend(epic)
    if htf_trend is not None and htf_trend != signal_dir:
        log(f'  âš ï¸ {pair} HTF conflict: M15={signal_dir} vs H1={htf_trend}')
        return None

    # FIX: Volume confirmation â€” Ø­Ø¬Ù… Ø§Ù„ÙƒØ³Ø± > Ù…ØªÙˆØ³Ø· Ø§Ù„Ø­Ø¬Ù…
    avg_vol = df['volume'].iloc[-20:-2].mean()
    cur_vol = df['volume'].iloc[idx]
    if avg_vol > 0 and cur_vol < avg_vol * 0.8:
        log(f'  âš ï¸ {pair} volume low: {cur_vol:.0f} < {avg_vol * 0.8:.0f}')
        return None

    # Correlation filter
    if not check_correlation_filter(pair, signal_dir):
        return None

    # â”€â”€â”€ 7. Ø­Ø³Ø§Ø¨ Entry / SL / TP â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    bid, ask = meta[0], meta[1]
    if signal_dir == 'BUY':
        entry = ask
        sl    = round(entry - SL_ATR_MULT * atr_val, 5)
        tp    = round(entry + TP_ATR_MULT * atr_val, 5)
    else:
        entry = bid
        sl    = round(entry + SL_ATR_MULT * atr_val, 5)
        tp    = round(entry - TP_ATR_MULT * atr_val, 5)

    # â”€â”€â”€ 8. Ø­Ø³Ø§Ø¨ Ø§Ù„Ø­Ø¬Ù… â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
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


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# PLACE ORDER
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

def place_order(sig, session_name):
    """ÙØªØ­ ØµÙÙ‚Ø© Ø¬Ø¯ÙŠØ¯Ø©"""
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
        log(f'  âŒ Order failed: {r.text[:100] if r else "no response"}')
        return False

    data    = r.json()
    deal_id = data.get('dealId') or data.get('dealReference', '')
    db_key  = f'{sig["pair"]}_{sig["direction"]}_{int(time.time())}' 

    if db_is_dup(db_key):
        log(f'  âš ï¸ Duplicate order skipped')
        return False

    db_save(db_key, sig['pair'], sig['direction'], sig['entry'], sig['sl'], sig['tp'],
            sig['atr'], sig['size'], sig['spread'], sig['risk_pct'], session_name)
    op_save(deal_id, sig['pair'], sig['direction'], sig['entry'], sig['sl'], sig['tp'],
            sig['atr'], sig['size'], db_key)

    log(f'  âœ… ORDER PLACED: {sig["pair"]} {sig["direction"]} @ {sig["entry"]} | SL={sig["sl"]} TP={sig["tp"]}')
    tg_signal(sig, (sig['risk_pct'], sig['risk_reason']), session_name)
    return True


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# SMART EXITS
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

_momentum_cache: dict = {}  # FIX: cache Ù„Ù€ is_momentum_reversing

def is_momentum_reversing(pair, direction):
    """FIX: ØªØ­Ù‚Ù‚ Ù…Ù† Ø§Ù†Ø¹ÙƒØ§Ø³ Ø§Ù„Ø²Ø®Ù… Ù…Ø¹ caching Ù„ØªÙ‚Ù„ÙŠÙ„ API calls"""
    cache_key = f'mom_{pair}_{direction}'
    now       = time.time()
    cached    = _momentum_cache.get(cache_key)
    if cached and (now - cached['ts']) < 60:  # cache 60 Ø«Ø§Ù†ÙŠØ©
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
    """Ø¥Ø¯Ø§Ø±Ø© Ø®Ø±ÙˆØ¬ Ø°ÙƒÙŠØ© â€” Partial TP + Trailing + Early Exit + Time Exit"""
    tracked  = op_get_all()
    if not tracked: return

    live_pos = get_open_positions()
    live_ids = {p.get('position', {}).get('dealId', '') for p in live_pos}

    for pos in tracked:
        deal_id = pos['deal_id']

        # â”€â”€â”€ ØªØ³Ø¬ÙŠÙ„ Ø§Ù„Ø¥ØºÙ„Ø§Ù‚ â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
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

        # â”€â”€â”€ Ø­Ø³Ø§Ø¨ Ø§Ù„Ø±Ø¨Ø­ Ø§Ù„Ø­Ø§Ù„ÙŠ â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
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
        log(f'  ðŸ“Š {pos["pair"]} {dir_} | R={profit_r:.2f} | Bars={bars_held} | S1:{s1} S2:{s2} S3:{s3}')

        # â”€â”€â”€ 1. Time-based Exit â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        if bars_held > MAX_TRADE_DURATION_BARS and profit_r < 0.5:
            log(f'  â° TIME EXIT: {bars_held} bars, profit {profit_r:.2f}R')
            if close_full_api(deal_id):
                result, _ = csv_log_trade(pos, cur_price, s1, s2, s3, pos['final_locked_r'], 'TIME_EXIT')
                db_update(pos['db_key'], result, 'TIME_EXIT')
                op_delete(deal_id)
                nl = '\n'
                tg(f'â° *Time Exit â€” {pos["pair"]} {dir_}*{nl}Ù…Ø¯Ø© Ø·ÙˆÙŠÙ„Ø© ({bars_held} Ø´Ù…Ø¹Ø©){nl}@ `{cur_price}` | PnL: `{profit_r:.2f}R`{nl}_{utc_now()}_')
            continue

        # â”€â”€â”€ 2. Early Exit â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        # FIX: Ø§Ù„Ù…Ù†Ø·Ù‚ Ø§Ù„Ù…Ù‚ØµÙˆØ¯ Ù‡Ùˆ: Ø®Ø±ÙˆØ¬ Ù…Ø¨ÙƒØ± Ø¹Ù†Ø¯Ù…Ø§ profit_r Ø¨ÙŠÙ† -0.4 Ùˆ -0.2
        # (Ø£ÙŠ Ø§Ù„Ø®Ø³Ø§Ø±Ø© Ø¨Ø¯Ø£Øª ØªØªØ±Ø§ÙƒÙ… Ù„ÙƒÙ† Ù„Ù… ØªØµÙ„ SL Ø¨Ø¹Ø¯)
        if EARLY_EXIT_THRESHOLD <= profit_r < -0.2:
            if is_momentum_reversing(pos['pair'], dir_):
                log(f'  ðŸ”„ EARLY EXIT: momentum reversing @ {profit_r:.2f}R')
                if close_full_api(deal_id):
                    result, _ = csv_log_trade(pos, cur_price, s1, s2, s3, pos['final_locked_r'], 'EARLY_EXIT')
                    db_update(pos['db_key'], result, 'EARLY_EXIT')
                    op_delete(deal_id)
                    nl = '\n'
                    tg(f'ðŸ”„ *Early Exit â€” {pos["pair"]} {dir_}*{nl}Ø§Ù†Ø¹ÙƒØ§Ø³ Ø²Ø®Ù… Ù…Ø¨ÙƒØ± @ `{profit_r:.2f}R`{nl}Ø®Ø³Ø§Ø±Ø© Ø£Ù‚Ù„ Ù…Ù† SL Ø§Ù„ÙƒØ§Ù…Ù„ âœ…{nl}_{utc_now()}_')
                continue

        # â”€â”€â”€ 3. Partial TP â€” Stage 1 (1.5R) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        if not s1 and profit_r >= STAGE1_TP_R:
            partial_size = round(size * STAGE1_PCT, 4)
            if close_partial_api(deal_id, partial_size):
                op_update(deal_id, stage1_done=1)
                # Ù‚ÙÙ„ SL Ø¹Ù†Ø¯ Ù†Ù‚Ø·Ø© Ø§Ù„ØªØ¹Ø§Ø¯Ù„
                be_sl = round(entry + 0.00001, 5) if dir_ == 'BUY' else round(entry - 0.00001, 5)
                if should_move_sl(sl, be_sl, dir_):
                    if update_sl_api(deal_id, be_sl, tp):
                        op_update(deal_id, sl=be_sl)
                nl = '\n'
                tg(f'ðŸ’° *Partial TP1 â€” {pos["pair"]} {dir_}*{nl}Stage 1 @ {profit_r:.2f}R | Ø£ÙØºÙ„Ù‚ {STAGE1_PCT:.0%}{nl}SL â†’ Break Even{nl}_{utc_now()}_')
                log(f'  ðŸ’° Stage1 TP @ {profit_r:.2f}R')

        # â”€â”€â”€ 4. Partial TP â€” Stage 2 (2.5R) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        if s1 and not s2 and profit_r >= STAGE2_TP_R:
            remaining = size * (1 - STAGE1_PCT)
            partial_size = round(remaining * STAGE2_PCT, 4)
            if close_partial_api(deal_id, partial_size):
                op_update(deal_id, stage2_done=1)
                nl = '\n'
                tg(f'ðŸ’° *Partial TP2 â€” {pos["pair"]} {dir_}*{nl}Stage 2 @ {profit_r:.2f}R | Ø£ÙØºÙ„Ù‚ {STAGE2_PCT:.0%} Ù…Ù† Ø§Ù„Ù…ØªØ¨Ù‚ÙŠ{nl}_{utc_now()}_')
                log(f'  ðŸ’° Stage2 TP @ {profit_r:.2f}R')

        # â”€â”€â”€ 5. Progressive SL Lock â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        locked_r, lock_reason = get_progressive_lock(profit_r)
        if locked_r > 0:
            new_sl = calculate_sl_at_r(pos, locked_r)
            cur_sl = float(pos['sl'])
            if should_move_sl(cur_sl, new_sl, dir_):
                if update_sl_api(deal_id, new_sl, tp):
                    op_update(deal_id, sl=new_sl, final_locked_r=locked_r)
                    log(f'  ðŸ”’ SL locked @ {locked_r}R ({lock_reason})')

        # â”€â”€â”€ 6. Trailing Stop (Ø¨Ø¹Ø¯ 2.5R) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        if profit_r >= TRAILING_START_R:
            trail_sl = calculate_trailing_sl(pos, cur_price, atr_val)
            cur_sl   = float(pos['sl'])
            if should_move_sl(cur_sl, trail_sl, dir_):
                if update_sl_api(deal_id, trail_sl, tp):
                    op_update(deal_id, sl=trail_sl)
                    log(f'  ðŸ“ˆ Trailing SL â†’ {trail_sl}')

        # â”€â”€â”€ 7. Final TP (3.5R) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        if s1 and s2 and not s3 and profit_r >= FINAL_TP_R:
            if close_full_api(deal_id):
                result, _ = csv_log_trade(pos, cur_price, s1, s2, 1, pos['final_locked_r'], 'FINAL_TP')
                db_update(pos['db_key'], result, 'FINAL_TP')
                op_delete(deal_id)
                nl = '\n'
                tg(f'ðŸ† *Final TP â€” {pos["pair"]} {dir_}*{nl}@ {profit_r:.2f}R | Ù‡Ø¯Ù Ø§ÙƒØªÙ…Ù„!{nl}_{utc_now()}_')


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# MAIN LOOP â€” Ø§Ù„Ø­Ù„Ù‚Ø© Ø§Ù„Ø±Ø¦ÙŠØ³ÙŠØ©
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

def main_loop():
    """Ø§Ù„Ø­Ù„Ù‚Ø© Ø§Ù„Ø±Ø¦ÙŠØ³ÙŠØ© Ù„Ù„Ø¨ÙˆØª"""
    log('ðŸš€ TL Breaks Bot v3.3 â€” Ø¨Ø¯Ø¡ Ø§Ù„ØªØ´ØºÙŠÙ„')
    db_init()
    csv_init()

    # Ø¥Ù†Ø´Ø§Ø¡ Ø§Ù„Ø¬Ù„Ø³Ø© Ø§Ù„Ø£ÙˆÙ„Ù‰
    if not create_session():
        log('âŒ ÙØ´Ù„ Ø§Ù„Ø§ØªØµØ§Ù„ Ø§Ù„Ø£ÙˆÙ„ÙŠ â€” Ø¥Ø¹Ø§Ø¯Ø© Ø§Ù„Ù…Ø­Ø§ÙˆÙ„Ø© Ø¨Ø¹Ø¯ 60 Ø«Ø§Ù†ÙŠØ©')
        time.sleep(60)
        if not create_session():
            log('âŒ ÙØ´Ù„ Ø§Ù„Ø§ØªØµØ§Ù„ Ù…Ø±ØªÙŠÙ† â€” Ø¥ÙŠÙ‚Ø§Ù Ø§Ù„Ø¨ÙˆØª')
            return

    tg(f'ðŸ¤– *TL Breaks Bot v3.3 â€” ØªØ´ØºÙŠÙ„*\nØ£Ø²ÙˆØ§Ø¬: {", ".join(PAIRS.keys())}\nInterval: {SCAN_INTERVAL}s\n_{utc_now()}_')

    last_ping      = time.time()
    last_daily_sum = datetime.now(timezone.utc).date()
    scan_count     = 0

    while True:
        try:
            now = time.time()

            # â”€â”€â”€ Ping ÙƒÙ„ 300 Ø«Ø§Ù†ÙŠØ© â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
            if now - last_ping >= 300:
                ping_session()
                last_ping = now

            # â”€â”€â”€ ØªØ­Ø¯ÙŠØ« Ø§Ù„Ø±ØµÙŠØ¯ â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
            balance = get_current_balance()
            log(f'\n{"="*50}')
            log(f'ðŸ’° Balance: ${balance:.2f} | Scan #{scan_count + 1}')

            # â”€â”€â”€ Ø§Ù„ØªØ­Ù‚Ù‚ Ù…Ù† Ø´Ø±ÙˆØ· Ø§Ù„ØªØ¯Ø§ÙˆÙ„ â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
            ok, reason, session_mult, session_name, day_pnl = should_trade()
            if not ok:
                log(f'â¸ Ø§Ù„ØªØ¯Ø§ÙˆÙ„ Ù…ÙˆÙ‚Ù: {reason}')
                # Ø¥Ø¯Ø§Ø±Ø© Ø§Ù„ØµÙÙ‚Ø§Øª Ø§Ù„Ù…ÙØªÙˆØ­Ø© Ø­ØªÙ‰ ÙÙŠ Ø­Ø§Ù„Ø© Ø§Ù„Ø¥ÙŠÙ‚Ø§Ù
                manage_smart_exits()
                time.sleep(SCAN_INTERVAL)
                scan_count += 1
                continue

            log(f'ðŸ“… Session: {session_name} | Day PnL: ${day_pnl:.2f}')

            # â”€â”€â”€ Ø¥Ø¯Ø§Ø±Ø© Ø§Ù„ØµÙÙ‚Ø§Øª Ø§Ù„Ù…ÙØªÙˆØ­Ø© Ø£ÙˆÙ„Ø§Ù‹ â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
            manage_smart_exits()

            # â”€â”€â”€ ÙØ­Øµ Ø¹Ø¯Ø¯ Ø§Ù„ØµÙÙ‚Ø§Øª Ø§Ù„Ù…ÙØªÙˆØ­Ø© â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
            open_count = len(op_get_all())
            if open_count >= MAX_OPEN_TRADES:
                log(f'âš ï¸ Ø§Ù„Ø­Ø¯ Ø§Ù„Ø£Ù‚ØµÙ‰ Ù„Ù„ØµÙÙ‚Ø§Øª: {open_count}/{MAX_OPEN_TRADES}')
                time.sleep(SCAN_INTERVAL)
                scan_count += 1
                continue

            # â”€â”€â”€ Ù…Ø³Ø­ Ø§Ù„Ø£Ø²ÙˆØ§Ø¬ â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
            for pair, cfg in PAIRS.items():
                try:
                    # ØªÙØ±ÙŠØº cache Ø§Ù„Ø´Ù…Ø¹Ø§Øª Ù‚Ø¨Ù„ ÙƒÙ„ Ø²ÙˆØ¬
                    _candle_cache.clear()

                    log(f'  ðŸ” ÙØ­Øµ {pair}...')

                    # ÙØ­Øµ Ù†Ø¸Ø§Ù… Ø§Ù„ØªÙ‚Ù„Ø¨
                    vol_regime, vol_mult = check_volatility_regime(cfg['epic'])
                    if vol_regime == 'EXTREME':
                        log(f'  âš ï¸ {pair}: ØªÙ‚Ù„Ø¨ Ù…ÙØ±Ø· â€” ØªØ®Ø·ÙŠ')
                        continue

                    effective_mult = session_mult * vol_mult
                    sig = check_signal(pair, cfg, effective_mult)

                    if sig:
                        log(f'  ðŸŽ¯ Ø¥Ø´Ø§Ø±Ø©: {pair} {sig["direction"]} @ {sig["entry"]}')
                        # Ø¥Ø¹Ø§Ø¯Ø© ÙØ­Øµ Ø¹Ø¯Ø¯ Ø§Ù„ØµÙÙ‚Ø§Øª
                        if len(op_get_all()) < MAX_OPEN_TRADES:
                            place_order(sig, session_name or 'N/A')
                    else:
                        log(f'  â€” Ù„Ø§ Ø¥Ø´Ø§Ø±Ø© Ø¹Ù„Ù‰ {pair}')

                    time.sleep(1)  # ØªØ¬Ù†Ø¨ rate limiting

                except Exception as pair_ex:
                    log(f'  âŒ Ø®Ø·Ø£ ÙÙŠ {pair}: {pair_ex}')
                    continue

            # â”€â”€â”€ Daily Summary â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
            today = datetime.now(timezone.utc).date()
            if today != last_daily_sum:
                tg_daily_summary()
                last_daily_sum = today

            scan_count += 1
            log(f'âœ… Ø§Ù†ØªÙ‡Ù‰ Ø§Ù„Ù…Ø³Ø­ #{scan_count} â€” Ø§Ù†ØªØ¸Ø§Ø± {SCAN_INTERVAL}s')
            time.sleep(SCAN_INTERVAL)

        except KeyboardInterrupt:
            log('\nâ›” Ø¥ÙŠÙ‚Ø§Ù ÙŠØ¯ÙˆÙŠ')
            tg(f'â›” *Ø§Ù„Ø¨ÙˆØª Ø£ÙÙˆÙ‚Ù ÙŠØ¯ÙˆÙŠØ§Ù‹*\n_{utc_now()}_')
            break

        except Exception as ex:
            log(f'âŒ Ø®Ø·Ø£ ÙÙŠ Ø§Ù„Ø­Ù„Ù‚Ø© Ø§Ù„Ø±Ø¦ÙŠØ³ÙŠØ©: {ex}')
            # Ù…Ø­Ø§ÙˆÙ„Ø© Ø¥Ø¹Ø§Ø¯Ø© Ø§Ù„Ø§ØªØµØ§Ù„
            log('ðŸ”„ Ø¥Ø¹Ø§Ø¯Ø© Ø¥Ù†Ø´Ø§Ø¡ Ø§Ù„Ø¬Ù„Ø³Ø©...')
            time.sleep(30)
            create_session()
            time.sleep(SCAN_INTERVAL)


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# ENTRY POINT
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
if __name__ == '__main__':
    main_loop()    'GBPUSD': {'epic': 'GBPUSD', 'allow_buy': True, 'allow_sell': True, 'size_override': None},
    'US100': {'epic': 'US100', 'allow_buy': True, 'allow_sell': True, 'size_override': None},
    'US500': {'epic': 'US500', 'allow_buy': True, 'allow_sell': True, 'size_override': None},


STRATEGY_TF   = os.getenv('STRATEGY_TF', 'MINUTE_15')
CANDLES_COUNT = 500
SCAN_INTERVAL = int(os.getenv('SCAN_INTERVAL', '300'))

LENGTH, SLOPE_MULT, SLOPE_METHOD = int(os.getenv('LENGTH', '10')), float(os.getenv('SLOPE_MULT', '1.0')), os.getenv('SLOPE_METHOD', 'ATR')
ATR_PERIOD, SL_ATR_MULT, TP_ATR_MULT = 14, 1.5, 3.0

# ═══════════════════════════════════════════════════════
# ✅ v3.2: إعدادات جديدة
# ═══════════════════════════════════════════════════════

# --- Dynamic Risk ---
BASE_RISK_PERCENT = 0.01      # 1% افتراضي
KELLY_FRACTION = 0.25       # ربع Kelly (محافظ)
MAX_RISK_PERCENT = 0.03     # 3% سقف المخاطرة
MIN_RISK_PERCENT = 0.005    # 0.5% أدنى المخاطرة

MAX_DAILY_RISK = 0.05       # 5% خسارة يومية كحد أقصى
MAX_WEEKLY_RISK = 0.10      # 10% خسارة أسبوعية
DAILY_PROFIT_TARGET = 0.03  # 3% هدف يومي (قفل الأرباح)

# --- Smart Session ---
SESSIONS = {
    'ASIA': {'start': 0, 'end': 7, 'risk_mult': 0.5, 'name': 'آسيا (هادئ)'},
    'LONDON_OPEN': {'start': 7, 'end': 10, 'risk_mult': 1.2, 'name': 'فتح لندن'},
    'LONDON_MID': {'start': 10, 'end': 12, 'risk_mult': 1.0, 'name': 'منتصف لندن'},
    'LONDON_NY': {'start': 12, 'end': 16, 'risk_mult': 1.5, 'name': 'تداخل لندن-نيويورك ⭐'},
    'NY_PM': {'start': 16, 'end': 20, 'risk_mult': 0.7, 'name': 'بعد الظهر الأمريكي'},
    'QUIET': {'start': 20, 'end': 24, 'risk_mult': 0.3, 'name': 'هادئ (تجنب)'},
}

VOLATILITY_THRESHOLDS = {
    'EXTREME': 2.0,   # تقلب مفرط - توقف
    'HIGH': 1.5,      # تقلب عالٍ - تقليل
    'LOW': 0.6,       # تقلب منخفض - تقليل
}

# --- Smart Exits ---
MAX_TRADE_DURATION_BARS = 24      # 6 ساعات في M15
EARLY_EXIT_THRESHOLD = -0.4       # خروج مبكر عند -0.4R
TRAILING_START_R = 2.5            # بدء Trailing عند 2.5R
TRAILING_ATR_MULT = 1.5             # مسافة Trailing

# --- Partial TP ---
STAGE1_TP_R, STAGE1_PCT = 1.5, 0.50
STAGE2_TP_R, STAGE2_PCT = 2.5, 0.30
FINAL_TP_R, FINAL_PCT = 3.5, 0.50

PROGRESSIVE_LOCK = {2.0: 0.5, 2.5: 1.0, 3.0: 1.5, 3.5: 2.0, 4.5: 3.0, 6.0: 4.0}

# --- RSI ---
RSI_SELL_MIN, RSI_SELL_MAX = 55, 78
RSI_BUY_MIN, RSI_BUY_MAX = 22, 45

SPREAD_ATR_MAX = 0.25

RISK_PERCENT = float(os.getenv('RISK_PERCENT', '0.01'))
MAX_OPEN_TRADES = int(os.getenv('MAX_OPEN_TRADES', '6'))
MAX_CONSECUTIVE_LOSS = int(os.getenv('MAX_CONSEC_LOSS', '3'))
ACCOUNT_BALANCE = float(os.getenv('ACCOUNT_BALANCE', '1000'))

_BASE_DIR = os.getenv('DATA_DIR', '/tmp')
DB_FILE = os.path.join(_BASE_DIR, 'multi_bot.db')
TRADES_CSV = os.path.join(_BASE_DIR, 'trades_log.csv')

db_lock, session_headers, _meta_cache = Lock(), {}, {}

CSV_HEADERS = [
    'date', 'time_utc', 'pair', 'direction', 'entry', 'sl', 'tp', 'exit_price',
    'atr', 'size', 'sl_dist', 'pnl_usd', 'pnl_r', 'result', 'bars_held', 'spread', 'tf',
    'stage1_done', 'stage2_done', 'stage3_done', 'final_locked_r', 'exit_type',
    'session_used', 'risk_percent', 'supertrend', 'rsi', 'ema_fast', 'ema_slow',
]


# ═══════════════════════════════════════════════════════
# ✅ FIXED: DATABASE WITH MIGRATION
# ═══════════════════════════════════════════════════════

def _migrate_database(conn):
    """إضافة أعمدة مفقودة تلقائياً"""
    try:
        cols = conn.execute("PRAGMA table_info(trades)").fetchall()
        col_names = {c[1] for c in cols}
        
        new_cols = {
            'pnl_r': 'REAL DEFAULT 0',
            'pnl_usd': 'REAL DEFAULT 0', 
            'exit_price': 'REAL',
            'bars_held': 'INTEGER DEFAULT 0'
        }
        
        for col, col_type in new_cols.items():
            if col not in col_names:
                conn.execute(f'ALTER TABLE trades ADD COLUMN {col} {col_type}')
                log(f'  ✅ Migrated: added column {col}')
    except Exception as ex:
        log(f'  ⚠️ Migration warning: {ex}')

def db_init():
    with sqlite3.connect(DB_FILE) as conn:
        # إنشاء الجدول مع جميع الأعمدة (للجداول الجديدة)
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
        
        # ✅ migration للجداول القديمة
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
            except sqlite3.IntegrityError: pass

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
    """✅ FIXED: جلب الصفقات مع التعامل مع البيانات القديمة/الجديدة"""
    with db_lock:
        with sqlite3.connect(DB_FILE) as conn:
            # التحقق من الأعمدة المتاحة
            cols = {c[1] for c in conn.execute("PRAGMA table_info(trades)").fetchall()}
            
            # ✅ إذا كان pnl_r غير موجود، نستخدم طريقة بديلة
            if 'pnl_r' not in cols:
                log(f'  ⚠️ pnl_r not found, using fallback calculation')
                query = """SELECT pair, direction, status, entry, sl, tp, timestamp 
                          FROM trades WHERE status IN ('WIN','LOSS')"""
                params = []
                if pair:
                    query += " AND pair=?"
                    params.append(pair)
                query += " ORDER BY id DESC LIMIT ?"
                params.append(limit)
                
                rows = conn.execute(query, params).fetchall()
                
                # ✅ حساب pnl_r تقريبياً من البيانات المتاحة
                result = []
                for r in rows:
                    pair_name, direction, status, entry, sl, tp, timestamp = r
                    # تقدير PnL بناءً على النتيجة
                    if status == 'WIN':
                        # ربح تقريبي: 2R للفائزين (متوسط)
                        pnl_r = 2.0
                    else:
                        # خسارة: -1R
                        pnl_r = -1.0
                    result.append((pair_name, direction, status, pnl_r, timestamp))
                return result
            
            # ✅ الطريقة العادية (إذا كان العمود موجود)
            query = "SELECT pair, direction, status, pnl_r, timestamp FROM trades WHERE status IN ('WIN','LOSS')"
            params = []
            if pair:
                query += " AND pair=?"
                params.append(pair)
            query += " ORDER BY id DESC LIMIT ?"
            params.append(limit)
            
            return conn.execute(query, params).fetchall()

def _update_trade_pnl(db_key, pnl_r, pnl_usd, exit_price, bars_held):
    """تحديث PnL في قاعدة البيانات عند الإغلاق"""
    with db_lock:
        with sqlite3.connect(DB_FILE) as conn:
            try:
                # ✅ التحقق من وجود الأعمدة أولاً
                cols = {c[1] for c in conn.execute("PRAGMA table_info(trades)").fetchall()}
                
                if 'pnl_r' in cols:
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
# ✅ v3.2: DYNAMIC RISK MANAGEMENT (Kelly + Drawdown)
# ═══════════════════════════════════════════════════════

def calculate_pnl_since(since_dt):
    """حساب PnL منذ تاريخ معين"""
    with db_lock:
        with sqlite3.connect(DB_FILE) as conn:
            # ✅ التحقق من الأعمدة المتاحة
            cols = {c[1] for c in conn.execute("PRAGMA table_info(trades)").fetchall()}
            
            if 'pnl_r' not in cols:
                # طريقة بديلة: حساب تقريبي
                rows = conn.execute(
                    "SELECT status, entry, sl, size, atr FROM trades WHERE timestamp >= ? AND status IN ('WIN','LOSS')",
                    (since_dt.strftime('%Y-%m-%d %H:%M UTC'),)
                ).fetchall()
                
                total_pnl = 0.0
                for status, entry, sl, size, atr in rows:
                    sl_dist = abs(entry - sl) if entry and sl else (atr * SL_ATR_MULT if atr else 0.01)
                    pnl = sl_dist * size * 100
                    total_pnl += pnl if status == 'WIN' else -pnl
                return total_pnl
            
            # الطريقة العادية
            rows = conn.execute(
                "SELECT pnl_r, sl, size FROM trades WHERE timestamp >= ? AND status IN ('WIN','LOSS')",
                (since_dt.strftime('%Y-%m-%d %H:%M UTC'),)
            ).fetchall()
            
            total_pnl = 0.0
            for pnl_r, sl, size in rows:
                if pnl_r and sl:
                    total_pnl += pnl_r * abs(sl) * size * 100
            return total_pnl

def check_drawdown_limits():
    """التحقق من حدود الخسارة اليومية والأسبوعية"""
    now = datetime.now(timezone.utc)
    balance = get_current_balance()
    
    # يومي
    day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    day_pnl = calculate_pnl_since(day_start)
    
    if day_pnl <= -balance * MAX_DAILY_RISK:
        return False, f'🛑 DAILY LIMIT: {day_pnl/balance:.1%} loss', 0.0
    
    if day_pnl >= balance * DAILY_PROFIT_TARGET:
        return False, f'🔒 DAILY TARGET HIT: +{day_pnl/balance:.1%}', 0.0
    
    # أسبوعي
    week_start = (now - timedelta(days=now.weekday())).replace(hour=0, minute=0, second=0)
    week_pnl = calculate_pnl_since(week_start)
    
    if week_pnl <= -balance * MAX_WEEKLY_RISK:
        return False, f'🛑 WEEKLY LIMIT: {week_pnl/balance:.1%} loss', 0.0
    
    return True, 'OK', day_pnl

def get_pair_stats(pair, lookback=20):
    """إحصائيات أداء الزوج"""
    trades = db_get_recent_trades(pair, lookback)
    if len(trades) < 5:
        return None
    
    wins = [t for t in trades if t[2] == 'WIN']
    losses = [t for t in trades if t[2] == 'LOSS']
    
    win_rate = len(wins) / len(trades)
    avg_win_r = np.mean([t[3] for t in wins]) if wins else 0
    avg_loss_r = abs(np.mean([t[3] for t in losses])) if losses else 1
    
    # حساب Kelly
    if avg_loss_r == 0: avg_loss_r = 1
    kelly = (win_rate * (avg_win_r/avg_loss_r) - (1 - win_rate)) / (avg_win_r/avg_loss_r) if avg_win_r > 0 else 0
    kelly = max(0, min(kelly, 0.10))
    
    # سلسلة متتالية
    consecutive_wins = consecutive_losses = 0
    cur_type = None
    cur_consec = 0
    
    for t in sorted(trades, key=lambda x: x[4]):
        if t[2] == 'WIN':
            if cur_type == 'WIN':
                cur_consec += 1
            else:
                cur_consec = 1
                cur_type = 'WIN'
            consecutive_wins = max(consecutive_wins, cur_consec)
        else:
            if cur_type == 'LOSS':
                cur_consec += 1
            else:
                cur_consec = 1
                cur_type = 'LOSS'
            consecutive_losses = max(consecutive_losses, cur_consec)
    
    return {
        'total': len(trades), 'win_rate': win_rate,
        'avg_win_r': avg_win_r, 'avg_loss_r': avg_loss_r,
        'kelly': kelly, 'consecutive_wins': consecutive_wins,
        'consecutive_losses': consecutive_losses,
        'profit_factor': (len(wins) * avg_win_r) / (len(losses) * avg_loss_r) if losses and avg_loss_r > 0 else float('inf')
    }

def calculate_dynamic_risk(pair, base_risk=BASE_RISK_PERCENT):
    """حساب المخاطرة الديناميكية"""
    stats = get_pair_stats(pair)
    
    if not stats:
        return base_risk, 'default (no stats)'
    
    risk = base_risk
    
    # تطبيق Kelly (ربع فقط)
    kelly_risk = stats['kelly'] * KELLY_FRACTION
    if stats['win_rate'] > 0.5 and stats['profit_factor'] > 1.5:
        risk = max(risk, min(kelly_risk, MAX_RISK_PERCENT))
        reason = f'kelly:{kelly_risk:.2%}'
    else:
        reason = 'base'
    
    # تقليل في سلسلة خسائر
    if stats['consecutive_losses'] >= 3:
        risk *= 0.4
        reason += ' | 3+ losses (40%)'
    elif stats['consecutive_losses'] == 2:
        risk *= 0.6
        reason += ' | 2 losses (60%)'
    
    # تقليل في Win Rate منخفض
    if stats['win_rate'] < 0.35:
        risk *= 0.7
        reason += ' | low WR (70%)'
    
    # زيادة محدودة في سلسلة أرباح
    if stats['consecutive_wins'] >= 3 and stats['win_rate'] > 0.6:
        risk = min(risk * 1.3, MAX_RISK_PERCENT)
        reason += ' | hot streak (+30%)'
    
    return max(MIN_RISK_PERCENT, min(risk, MAX_RISK_PERCENT)), reason


# ═══════════════════════════════════════════════════════
# ✅ v3.2: SMART SESSION FILTER
# ═══════════════════════════════════════════════════════

def get_session_info():
    """احصل على معلومات الجلسة الحالية"""
    hour = datetime.now(timezone.utc).hour
    
    for session_name, config in SESSIONS.items():
        if config['start'] <= hour < config['end']:
            return config['risk_mult'], config['name'], session_name
    
    return 0.0, 'مغلقة', 'CLOSED'

def check_volatility_regime(epic):
    """تحقق من نظام التقلب"""
    df = fetch_candles(epic, STRATEGY_TF, 100)
    if df.empty or len(df) < 50:
        return 'UNKNOWN', 1.0
    
    atr_current = calc_atr_series(df.iloc[:-1], ATR_PERIOD).iloc[-1]
    atr_hist = calc_atr_series(df.iloc[:-20], ATR_PERIOD).iloc[-20:].mean()
    
    ratio = atr_current / atr_hist if atr_hist > 0 else 1
    
    if ratio > VOLATILITY_THRESHOLDS['EXTREME']:
        return 'EXTREME', 0.0  # توقف
    elif ratio > VOLATILITY_THRESHOLDS['HIGH']:
        return 'HIGH', 0.7     # تقليل
    elif ratio < VOLATILITY_THRESHOLDS['LOW']:
        return 'LOW', 0.6      # تقليل
    else:
        return 'NORMAL', 1.0

def should_trade():
    """التحقق النهائي من الجاهزية للتداول"""
    # 1. حدود الخسارة
    allowed, reason, day_pnl = check_drawdown_limits()
    if not allowed:
        return False, reason, 0.0, None, 0.0
    
    # 2. الجلسة
    session_mult, session_name, session_code = get_session_info()
    if session_mult == 0:
        return False, f'⏸ خارج الجلسات: {session_name}', 0.0, None, 0.0
    
    return True, 'OK', session_mult, session_name, day_pnl


# ═══════════════════════════════════════════════════════
# CSV & API HELPERS
# ═══════════════════════════════════════════════════════
def utc_now(): return datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
def log(msg): print(f'[{utc_now()}] {msg}', flush=True)

def csv_init():
    if not Path(TRADES_CSV).exists():
        with open(TRADES_CSV, 'w', newline='', encoding='utf-8-sig') as f:
            csv.DictWriter(f, fieldnames=CSV_HEADERS).writeheader()

def csv_log_trade(pos, exit_price, stage1=0, stage2=0, stage3=0, final_r=0, exit_type=''):
    try:
        entry, sl, size, dir_, pair = pos['entry'], pos['sl'], pos['size'], pos['direction'], pos['pair']
        sl_dist = abs(entry - sl)
        pnl_pts = (exit_price - entry) if dir_ == 'BUY' else (entry - exit_price)
        pnl_r = round(pnl_pts / sl_dist, 2) if sl_dist > 0 else 0
        result = 'WIN' if pnl_pts > 0 else ('LOSS' if pnl_pts < 0 else 'BE')
        
        meta = _meta_cache.get(pair, {}).get('data')
        pnl_usd = round(pnl_pts * size * (meta[3] if meta else 1), 2)
        
        # ✅ تحديث قاعدة البيانات أيضاً
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

        icon = '✅' if result == 'WIN' else ('❌' if result == 'LOSS' else '🔵')
        stages = f' | S1:{stage1} S2:{stage2} S3:{stage3}' if any([stage1,stage2,stage3]) else ''
        log(f'  {icon} CLOSED {pair} {dir_} | {exit_type} | PnL=${pnl_usd:+.2f} ({pnl_r:+.2f}R){stages}')
        
        nl = '\n'
        tg(f'{icon} *{pair} {dir_} — {result}*{nl}Exit: `{exit_type}`{nl}PnL: `${pnl_usd:+.2f}` | `{pnl_r:+.2f}R`{nl}_{utc_now()}_')
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

def _delete(path):
    try: return requests.delete(BASE_URL + path, headers=session_headers, timeout=10)
    except Exception as ex: log(f'  DELETE {path}: {ex}')

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

def get_current_balance():
    global ACCOUNT_BALANCE
    r = _get('/api/v1/accounts')
    if r and r.status_code == 200:
        accs = r.json().get('accounts', [])
        if accs: ACCOUNT_BALANCE = float(accs[0].get('balance', {}).get('available', ACCOUNT_BALANCE))
    return ACCOUNT_BALANCE

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

def close_full_api(deal_id):
    r = _delete(f'/api/v1/positions/{deal_id}')
    if r and r.status_code == 200:
        log(f'  🚪 Full close'); return True
    return False


# ═══════════════════════════════════════════════════════
# TELEGRAM
# ═══════════════════════════════════════════════════════
def tg(text):
    if not TG_TOKEN: return
    try:
        requests.post(f'https://api.telegram.org/bot{TG_TOKEN}/sendMessage',
                      data={'chat_id': TG_CHAT_ID, 'text': text, 'parse_mode': 'Markdown'}, timeout=10)
    except: pass

def tg_signal(sig, filters_info, risk_info, session_info):
    icon, mode = ('🟢' if sig['direction'] == 'BUY' else '🔴'), ('DEMO' if DEMO_MODE else 'LIVE')
    entry_type = 'قاع📉' if sig['direction'] == 'BUY' else 'قمة📈'
    nl = '\n'
    tg(f'{icon} *{sig["pair"]} {sig["direction"]}* [{mode}] {entry_type}{nl}'
       f'Entry: `{sig["entry"]}` | SL: `{sig["sl"]}` | TP: `{sig["tp"]}`{nl}'
       f'Risk: `{risk_info[0]:.2%}` ({risk_info[1]}){nl}'
       f'Session: `{session_info}`{nl}'
       f'Stages: `1.5R→50%` | `2.5R→30%` | `3.5R→50% of rest`{nl}'
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
# ✅ v3.2: SMART EXITS
# ═══════════════════════════════════════════════════════

def is_momentum_reversing(pair, direction):
    """تحقق من انعكاس الزخم في M5"""
    df = fetch_candles(PAIRS[pair]['epic'], 'MINUTE_5', 10)
    if df.empty or len(df) < 5: return False
    
    rsi = calc_rsi(df['close'], 5)
    if len(rsi) < 3: return False
    
    rsi_now, rsi_prev = rsi.iloc[-1], rsi.iloc[-2]
    
    if direction == 'BUY' and rsi_now < rsi_prev - 3: return True
    if direction == 'SELL' and rsi_now > rsi_prev + 3: return True
    return False

def calculate_sl_at_r(pos, locked_r):
    entry, dir_, sl_dist = pos['entry'], pos['direction'], abs(pos['entry'] - pos['sl'])
    if dir_ == 'BUY': return round(entry + (locked_r * sl_dist), 5)
    else: return round(entry - (locked_r * sl_dist), 5)

def get_progressive_lock(profit_r):
    for threshold, locked in sorted(PROGRESSIVE_LOCK.items(), reverse=True):
        if profit_r >= threshold: return locked, f'@ {threshold}R'
    return 0, 'none'

def should_move_sl(current_sl, new_sl, direction):
    if direction == 'BUY': return new_sl > current_sl + 0.00001
    else: return new_sl < current_sl - 0.00001

def calculate_trailing_sl(pos, cur_price, atr):
    dir_ = pos['direction']
    trail_dist = atr * TRAILING_ATR_MULT
    if dir_ == 'BUY': return round(cur_price - trail_dist, 5)
    else: return round(cur_price + trail_dist, 5)

def manage_smart_exits():
    """إدارة خروج ذكية شاملة"""
    tracked = op_get_all()
    if not tracked: return
    
    live_pos = get_open_positions()
    live_ids = {p.get('position', {}).get('dealId', '') for p in live_pos}

    for pos in tracked:
        deal_id = pos['deal_id']
        
        # ✅ تسجيل الإغلاق الكامل
        if deal_id not in live_ids:
            exit_price = get_closed_deal_price(deal_id, get_current_price(pos['pair']))
            if exit_price > 0:
                result, _ = csv_log_trade(pos, exit_price, pos['stage1_done'], 
                                          pos['stage2_done'], pos['stage3_done'],
                                          pos['final_locked_r'], 'STOP_OUT')
                if result != 'ERROR':
                    db_update(pos['db_key'], result.upper() if result in ('WIN','LOSS','BE') else 'CLOSED', 'STOP_OUT')
                    op_delete(deal_id)
            continue

        # ✅ حساب الربح والمدة
        cur_price = get_current_price(pos['pair'])
        if cur_price <= 0: continue
        
        entry, sl, tp, size, dir_ = pos['entry'], pos['sl'], pos['tp'], pos['size'], pos['direction']
        sl_dist = abs(entry - sl)
        if sl_dist <= 0: continue
        
        profit_pts = (cur_price - entry) if dir_ == 'BUY' else (entry - cur_price)
        profit_r = profit_pts / sl_dist
        
        # ✅ تحديث عدد الشمعات
        bars_held = pos.get('bars_held', 0) + 1
        op_update(deal_id, bars_held=bars_held)
        
        s1, s2, s3 = pos['stage1_done'], pos['stage2_done'], pos['stage3_done']
        log(f'  📊 {pos["pair"]} {dir_} | R={profit_r:.2f} | Bars={bars_held} | S1:{s1} S2:{s2} S3:{s3}')

        # ═══════════════════════════════════════════════════════
        # 1. SMART EXIT: خروج زمني (Time-based Exit)
        # ═══════════════════════════════════════════════════════
        if bars_held > MAX_TRADE_DURATION_BARS and profit_r < 0.5:
            log(f'  ⏰ TIME EXIT: {bars_held} bars, profit {profit_r:.2f}R')
            if close_full_api(deal_id):
                result, _ = csv_log_trade(pos, cur_price, s1, s2, s3, pos['final_locked_r'], 'TIME_EXIT')
                db_update(pos['db_key'], result, 'TIME_EXIT')
                op_delete(deal_id)
                
                nl = '\n'
                tg(f'⏰ *Time Exit — {pos["pair"]} {dir_}*{nl}'
                   f'مدة طويلة ({bars_held} شمعة) بدون ربح حقيقي{nl}'
                   f'أُغلقت @ `{cur_price}` | PnL: `{profit_r:.2f}R`{nl}'
                   f'_{utc_now()}_')
            continue

        # ═══════════════════════════════════════════════════════
        # 2. SMART EXIT: خروج مبكر (Early Exit)
        # ═══════════════════════════════════════════════════════
        if EARLY_EXIT_THRESHOLD < profit_r < -0.2:  # بين -0.4 و -0.2
            if is_momentum_reversing(pos['pair'], dir_):
                log(f'  🔄 EARLY EXIT: momentum reversing @ {profit_r:.2f}R')
                if close_full_api(deal_id):
                    result, _ = csv_log_trade(pos, cur_price, s1, s2, s3, pos['final_locked_r'], 'EARLY_EXIT')
                    db_update(pos['db_key'], result, 'EARLY_EXIT')
                    op_delete(deal_id)
                    
                    nl = '\n'
                    tg(f'🔄 *Early Exit — {pos["pair"]} {dir_}*{nl}'
                       f'انعكاس زخم مبكر @ `{profit_r:.2f}R`{nl}'
                       f'خسارة أقل من SL الكامل ✅{nl}'
                       f'_{utc_now()}_')
                continue

        # ═══════════════════════════════════════════════════════
        # 3. Partial TP المتدرج
        # ═══════════════════════════════════════════════════════
        if not s1 and profit_r >= STAGE1_TP_R:
            partial_size = round(size * STAGE1_PCT, 2)
            _, _, _, cs, min_sz, _ = get_instrument_meta(pos['pair'])
            if partial_size >= min_sz and close_partial_api(deal_id, partial_size):
                op_update(deal_id, stage1_done=1)
                nl = '\n'
                tg(f'💰 *Stage 1 — {pos["pair"]} {dir_}*{nl}'
                   f'50% @ `{profit_r:.2f}R` | SL مفتوح 🔓{nl}'
                   f'_{utc_now()}_')

        elif s1 and not s2 and profit_r >= STAGE2_TP_R:
            remaining = size * (1 - STAGE1_PCT)
            stage2_size = round(remaining * (STAGE2_PCT / (1 - STAGE1_PCT)), 2)
            _, _, _, cs, min_sz, _ = get_instrument_meta(pos['pair'])
            if stage2_size >= min_sz and close_partial_api(deal_id, stage2_size):
                new_sl = calculate_sl_at_r(pos, 0.5)
                if new_sl and update_sl_api(deal_id, new_sl, tp):
                    op_update(deal_id, stage2_done=1, final_locked_r=0.5, sl=new_sl)
                    nl = '\n'
                    tg(f'💰💰 *Stage 2 — {pos["pair"]} {dir_}*{nl}'
                       f'+30% | SL → `+0.5R` 🔒{nl}'
                       f'_{utc_now()}_')

        elif s2 and not s3 and profit_r >= FINAL_TP_R:
            remaining = size * (1 - STAGE1_PCT) * (1 - STAGE2_PCT / (1 - STAGE1_PCT))
            final_size = round(remaining * FINAL_PCT, 2)
            _, _, _, cs, min_sz, _ = get_instrument_meta(pos['pair'])
            if final_size >= min_sz and close_partial_api(deal_id, final_size):
                new_sl = calculate_sl_at_r(pos, 2.0)
                if new_sl and update_sl_api(deal_id, new_sl, tp):
                    op_update(deal_id, stage3_done=1, final_locked_r=2.0, sl=new_sl)

        # ═══════════════════════════════════════════════════════
        # 4. Progressive Lock للباقي
        # ═══════════════════════════════════════════════════════
        elif s2:
            new_locked_r, _ = get_progressive_lock(profit_r)
            if new_locked_r > pos['final_locked_r']:
                new_sl = calculate_sl_at_r(pos, new_locked_r)
                if new_sl and should_move_sl(pos['sl'], new_sl, dir_) and update_sl_api(deal_id, new_sl, tp):
                    op_update(deal_id, final_locked_r=new_locked_r, sl=new_sl)
                    tg(f'📈 Progressive: locked {new_locked_r}R')

        # ═══════════════════════════════════════════════════════
        # 5. Trailing للباقي النهائي
        # ═══════════════════════════════════════════════════════
        elif (s3 or profit_r >= TRAILING_START_R) and profit_r > pos.get('last_trail_r', 0) + 0.5:
            df = fetch_candles(PAIRS[pos['pair']]['epic'], STRATEGY_TF, 50)
            if not df.empty:
                atr = float(calc_atr_series(df.iloc[:-1], ATR_PERIOD).iloc[-1])
                if atr > 0:
                    trail_sl = calculate_trailing_sl(pos, cur_price, atr)
                    if should_move_sl(pos['sl'], trail_sl, dir_) and update_sl_api(deal_id, trail_sl, tp):
                        op_update(deal_id, sl=trail_sl, last_trail_r=profit_r)
                        log(f'  🏃 Trailing: SL → {trail_sl}')


# ═══════════════════════════════════════════════════════
# SIGNAL DETECTION
# ═══════════════════════════════════════════════════════
def check_signal(pair_name, config, session_mult, risk_mult):
    epic, allow_buy, allow_sell = config['epic'], config['allow_buy'], config['allow_sell']
    if not allow_buy and not allow_sell: return None

    # ✅ فلتر التقلب
    vol_regime, vol_mult = check_volatility_regime(epic)
    if vol_regime == 'EXTREME':
        log(f'  {pair_name}: ⛔ تقلب مفرط - توقف')
        return None
    
    final_risk_mult = session_mult * vol_mult * risk_mult
    if final_risk_mult < 0.3:
        log(f'  {pair_name}: ⏸ مخاطرة منخفضة جداً ({final_risk_mult:.1%})')
        return None

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
                filters = [csd == -1, cef < ces, cet < cem < cesl, RSI_SELL_MIN < cr < RSI_SELL_MAX]
                if all(filters):
                    signal, entry = 'SELL', bid
                    log(f'  {pair_name}: 🔴 SELL @ قمة | RSI={cr:.1f} | RiskMult={final_risk_mult:.2f}')

    # ✅ BUY: كسر ترند سفلي (قاع)
    if allow_buy and not signal and lower_tl:
        ai, av, sv = lower_tl
        if ai < li - 1:
            l_last, l_prev = tl_value(ai, av, sv, True, li), tl_value(ai, av, sv, True, li - 1)
            pc = float(df_c['close'].iloc[li - 1])
            if lc < l_last and pc < l_prev:
                filters = [csd == 1, cef > ces, cet > cem > cesl, RSI_BUY_MIN < cr < RSI_BUY_MAX]
                if all(filters):
                    signal, entry = 'BUY', ask
                    log(f'  {pair_name}: 🟢 BUY @ قاع | RSI={cr:.1f} | RiskMult={final_risk_mult:.2f}')

    if not signal: return None

    if signal == 'SELL':
        sl, tp = round(entry + SL_ATR_MULT * la + sp, 5), round(entry - TP_ATR_MULT * la, 5)
    else:
        sl, tp = round(entry - SL_ATR_MULT * la - sp, 5), round(entry + TP_ATR_MULT * la, 5)

    sld = abs(entry - sl)
    if sld < la * 0.1: return None

    # ✅ حساب الحجم بالمخاطرة الديناميكية
    dynamic_risk, risk_reason = calculate_dynamic_risk(pair_name, BASE_RISK_PERCENT)
    final_risk = dynamic_risk * final_risk_mult
    
    sz = config.get('size_override')
    if sz:
        size = max(min(float(sz), max_sz), min_sz)
    else:
        risk_usd = get_current_balance() * final_risk
        size = max(min(round(risk_usd / (sld * cs), 2), max_sz), min_sz)

    return {
        'pair': pair_name, 'epic': epic, 'direction': signal, 'entry': round(entry, 5),
        'sl': sl, 'tp': tp, 'atr': round(la, 5), 'size': size, 'spread': round(sp, 5),
        'risk_percent': final_risk, 'risk_reason': risk_reason,
        'filters_info': {'rsi': cr, 'st_dir': csd, 'vol_regime': vol_regime}
    }


# ═══════════════════════════════════════════════════════
# EXECUTE & SCAN
# ═══════════════════════════════════════════════════════
def execute_order(sig):
    body = {'epic': sig['epic'], 'direction': sig['direction'], 'size': sig['size'],
            'guaranteedStop': False, 'trailingStop': False, 'stopLevel': sig['sl'], 'profitLevel': sig['tp']}
    log(f'  📤 {sig["pair"]} {sig["direction"]} | entry≈{sig["entry"]} SL={sig["sl"]} TP={sig["tp"]} Risk={sig["risk_percent"]:.2%}')
    
    r = _post('/api/v1/positions', body)
    if not r:
        tg(f'❌ {sig["pair"]} {sig["direction"]}: no response'); return 'ERROR', 'no response'
    
    data = r.json()
    if r.status_code == 200:
        ref = data.get('dealReference', 'N/A')
        time.sleep(2)
        rc = _get(f'/api/v1/confirms/{ref}')
        if rc and rc.status_code == 200:
            c = rc.json()
            status, deal_id = c.get('dealStatus', 'UNKNOWN'), c.get('dealId', ref)
            if status in ('ACCEPTED', 'SUCCESS'):
                db_key = f'{sig["pair"]}_{datetime.now(timezone.utc).strftime("%Y-%m-%d_%H%M")}'
                op_save(deal_id, sig['pair'], sig['direction'], sig['entry'], sig['sl'], sig['tp'], sig['atr'], sig['size'], db_key)
            return status, ref
        return 'UNKNOWN', ref
    return 'FAILED', data.get('errorCode')

def run_scan():
    now = datetime.now(timezone.utc)
    
    # ✅ Smart Session & Drawdown Check
    can_trade, reason, session_mult, session_name, day_pnl = should_trade()
    if not can_trade:
        log(f'⏸ {reason}')
        if 'LIMIT' in reason or 'TARGET' in reason:
            tg(f'🛑 {reason}')
        return

    log('─' * 60)
    log(f'🔍 Scan v3.2-fixed | {session_name} | RiskMult:{session_mult:.1f}x | DayPnL:{day_pnl/ACCOUNT_BALANCE:+.1%}')
    log('─' * 60)

    get_current_balance()
    manage_smart_exits()

    open_pos = get_open_positions()
    log(f'  مفتوحة: {len(open_pos)} / {MAX_OPEN_TRADES}')
    if len(open_pos) >= MAX_OPEN_TRADES:
        log('  ⏸ الحد الأقصى'); return

    ts_key = now.strftime('%Y-%m-%d_%H%M')
    
    for pair_name, config in PAIRS.items():
        if len(open_pos) >= MAX_OPEN_TRADES: break
        
        # ✅ Dynamic Risk لكل زوج
        risk_pct, risk_reason = calculate_dynamic_risk(pair_name, BASE_RISK_PERCENT)
        log(f'  {pair_name}: Risk={risk_pct:.2%} ({risk_reason})')
        
        if db_consec_losses(pair_name) >= MAX_CONSECUTIVE_LOSS:
            log(f'  {pair_name}: تخطّي بسبب خسائر متتالية'); continue
        
        key = f'{pair_name}_{ts_key}'
        if db_is_dup(key): log(f'  {pair_name}: مكرر'); continue
        
        log(f'  {pair_name}: فحص...')
        sig = check_signal(pair_name, config, session_mult, risk_pct / BASE_RISK_PERCENT)
        if not sig: log(f'  {pair_name}: لا إشارة'); continue
        
        db_save(key, pair_name, sig['direction'], sig['entry'], sig['sl'], sig['tp'], 
                sig['atr'], sig['size'], sig['spread'], sig['risk_percent'], session_name)
        tg_signal(sig, sig['filters_info'], (sig['risk_percent'], sig['risk_reason']), session_name)
        
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
    mode = 'DEMO' if DEMO_MODE else 'LIVE'

    print('=' * 70, flush=True)
    print(f'  Multi-Pairs Bot v3.2-fixed — Database + Smart Features', flush=True)
    print(f'  ✅ Fixed: pnl_r column with auto-migration', flush=True)
    print(f'  ✅ Dynamic Risk: Kelly {KELLY_FRACTION:.0%} | DailyMax {MAX_DAILY_RISK:.0%}', flush=True)
    print(f'  ✅ Smart Session: London/NY 1.5x | Asia 0.5x | Quiet 0.3x', flush=True)
    print(f'  ✅ Smart Exits: Time({MAX_TRADE_DURATION_BARS}bars) | Early({EARLY_EXIT_THRESHOLD}R)', flush=True)
    print('=' * 70, flush=True)

    nl = '\n'
    tg(f'🚀 *Bot v3.2-fixed* [{mode}]{nl}'
       f'✅ Database: auto-migration enabled{nl}'
       f'📊 Dynamic Risk: Kelly `{KELLY_FRACTION:.0%}` | Daily `{MAX_DAILY_RISK:.0%}`{nl}'
       f'⏰ Smart Session: London/NY `1.5x`{nl}'
       f'🚪 Smart Exits: Time `{MAX_TRADE_DURATION_BARS}` | Early `{EARLY_EXIT_THRESHOLD}R`{nl}'
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
