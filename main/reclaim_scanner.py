"""
ROOT/main/reclaim_scanner.py

Scans NIFTY50 stocks for VWAP reclaim setups
Runs at XX:16:00 (1 min after hourly candle close)
NO NIFTY FILTER - Just scans all stocks (filter happens at entry time)
Outputs: reclaim_watchlist.json

UPDATED: Now uses whitelisted_symbols.csv ONLY (no fallback)
This ensures only profitable stocks are ever scanned
"""

import sys
import json
from pathlib import Path
from datetime import datetime, timedelta, time as dt_time
import time
import pytz

ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT))

from kite_client import get_kite_client
from telegram_notifier import notify_reclaims_found


# ============ CONFIG ============
# ONLY use whitelisted_symbols.csv (no fallback to bad stocks)
WHITELIST_CSV = ROOT / "whitelisted_symbols.csv"
INSTRUMENTS_JSON = ROOT / "instruments_nse.json"
WATCHLIST_OUTPUT = ROOT / "main" / "reclaim_watchlist.json"


def load_symbols_to_scan():
    """
    Load symbols to scan from whitelist ONLY
    Exits if whitelist not found (safety check)
    """
    if not WHITELIST_CSV.exists():
        raise FileNotFoundError(
            f"\n{'!'*60}\n"
            f"ERROR: whitelisted_symbols.csv not found!\n"
            f"Expected location: {WHITELIST_CSV}\n"
            f"Cannot scan without whitelist (would include underperformers)\n"
            f"{'!'*60}\n"
        )
    
    print(f"[CONFIG] Using whitelist: {WHITELIST_CSV.name}")
    with open(WHITELIST_CSV) as f:
        symbols = [line.strip() for line in f if line.strip()]
    
    print(f"[CONFIG] Loaded {len(symbols)} whitelisted stocks\n")
    return symbols


def load_instrument_tokens():
    """Load NSE instrument tokens"""
    with open(INSTRUMENTS_JSON) as f:
        instruments = json.load(f)
    return {i["tradingsymbol"]: i["instrument_token"] for i in instruments}


def calculate_session_vwap(candles):
    """
    Calculate cumulative VWAP from completed session candles
    
    Args:
        candles: List of completed candles to include in VWAP calculation
    
    Returns: vwap_value
    """
    if not candles:
        return None
    
    # VWAP calculation
    cum_tpv = 0  # Typical Price × Volume
    cum_vol = 0
    
    for c in candles:
        tp = (c['high'] + c['low'] + c['close']) / 3
        cum_tpv += tp * c['volume']
        cum_vol += c['volume']
    
    vwap = cum_tpv / cum_vol if cum_vol > 0 else None
    
    return vwap


def get_volume_sma50(all_candles, current_candle_index):
    """
    Calculate volume SMA50 from already-fetched candles
    Uses 50 hourly candles ending BEFORE current_candle_index (not including it)
    """
    # Need at least 50 candles before current_candle_index
    if current_candle_index < 50:
        return None
    
    # Take 50 candles ending BEFORE the candle we're checking
    start_idx = current_candle_index - 50
    end_idx = current_candle_index  # Exclusive, so this is correct
    
    volume_candles = all_candles[start_idx:end_idx]
    
    if len(volume_candles) < 50:
        return None
    
    volumes = [c['volume'] for c in volume_candles]
    return sum(volumes) / 50


def check_reclaim(candle, vwap, vol_sma50):
    """
    Check if candle is a VWAP reclaim
    Returns: (is_reclaim, reclaim_high, reclaim_low)
    """
    if vwap is None or vol_sma50 is None:
        return False, None, None
    
    open_below = candle['open'] < vwap
    close_above = candle['close'] > vwap
    volume_ok = candle['volume'] > 1.5 * vol_sma50
    
    is_reclaim = open_below and close_above and volume_ok
    
    return is_reclaim, candle['high'] if is_reclaim else None, candle['low'] if is_reclaim else None


def scan_stocks():
    """
    Main scanner logic
    Scans whitelisted stocks ONLY for reclaims
    NO NIFTY FILTER - Filter check happens at entry time
    """
    # Use current datetime with IST timezone
    ist = pytz.timezone('Asia/Kolkata')
    scan_date = datetime.now(ist)
    current_time = scan_date.time()
    
    print(f"[SCAN] Running at {scan_date.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # VALIDATION: Check if within valid scanner hours
    market_start = dt_time(9, 0)   # 9:00 AM
    scanner_end = dt_time(15, 20)  # 3:20 PM (last valid scan at 3:16)
    
    if not (market_start <= current_time <= scanner_end):
        print(f"\n{'!'*60}")
        print(f"[WARNING] Scanner running outside valid hours")
        print(f"[WARNING] Current time: {scan_date.strftime('%H:%M:%S')}")
        print(f"[WARNING] Valid hours: 9:00 AM - 3:20 PM")
        print(f"[WARNING] Results may include stale reclaims")
        print(f"{'!'*60}\n")
    
    # Load whitelisted symbols ONLY (exits if file missing)
    symbols = load_symbols_to_scan()
    
    print(f"[SCANNER] Scanning {len(symbols)} whitelisted stocks...\n")
    
    kite = get_kite_client()
    tokens = load_instrument_tokens()
    
    watchlist = {}
    
    # Fetch enough historical data for volume SMA50 (need ~60+ candles)
    from_date = scan_date - timedelta(days=20)
    to_date = scan_date
    
    for symbol in symbols:
        token = tokens.get(symbol)
        if not token:
            print(f"[SKIP] {symbol} - token not found")
            continue
        
        try:
            time.sleep(0.35)  # Rate limit
            
            # Fetch hourly candles (includes today + history for volume SMA50)
            all_candles = kite.historical_data(
                instrument_token=token,
                from_date=from_date,
                to_date=to_date,
                interval="60minute"
            )
            
            if len(all_candles) < 52:  # Need enough for SMA50 + current data
                continue
            
            # Find today's candles (from 9:15 AM onwards)
            today_start = scan_date.replace(hour=9, minute=15, second=0, microsecond=0)
            today_candles = [c for c in all_candles if c['date'] >= today_start]
            
            # Need at least 2 today's candles: 1 completed + 1 current incomplete
            if len(today_candles) < 2:
                continue
            
            # Find the candle we're checking (second-to-last in today's candles)
            check_candle = today_candles[-2]
            
            # Calculate VWAP from completed candles BEFORE the candle being checked
            # For first candle (10:16 checking 9:15-10:15): vwap_candles = [] (empty)
            # For subsequent candles: vwap_candles = all completed candles before checking candle
            vwap_candles = today_candles[:-2]  # Exclude checking candle and current incomplete
            
            # Calculate VWAP
            if len(vwap_candles) == 0:
                # First candle case: VWAP = the candle's own typical price
                # For reclaim check: open < TP < close means bullish candle with specific geometry
                vwap = (check_candle['high'] + check_candle['low'] + check_candle['close']) / 3
            else:
                # Subsequent candles: cumulative VWAP from prior completed candles
                vwap = calculate_session_vwap(vwap_candles)
            
            # VALIDATION: Check candle freshness (should be recent)
            candle_age_minutes = (scan_date - check_candle['date']).total_seconds() / 60
            
            # Candle should be less than 90 minutes old (allows for some delay)
            if candle_age_minutes > 120:
                print(f"[SKIP] {symbol} - Candle too old ({candle_age_minutes:.0f} min ago at {check_candle['date'].strftime('%H:%M')})")
                continue
            
            # Find this candle's index in all_candles for volume SMA50
            check_candle_index = None
            for i, c in enumerate(all_candles):
                if c['date'] == check_candle['date']:
                    check_candle_index = i
                    break
            
            if check_candle_index is None:
                continue
            
            # Get volume SMA50 using data up to (but not including) the candle we're checking
            vol_sma50 = get_volume_sma50(all_candles, check_candle_index)
            
            # Check reclaim
            is_reclaim, reclaim_high, reclaim_low = check_reclaim(check_candle, vwap, vol_sma50)
            
            if is_reclaim:
                watchlist[symbol] = {
                    "reclaim_high": reclaim_high,
                    "reclaim_low": reclaim_low,
                    "timestamp": check_candle['date'].isoformat(),
                    "vwap": vwap,
                    "candle_age_minutes": round(candle_age_minutes, 1)
                }
                print(f"[RECLAIM] {symbol} | High: {reclaim_high:.2f}, Low: {reclaim_low:.2f} | VWAP: {vwap:.2f} | Candle: {check_candle['date'].strftime('%H:%M')}")
        
        except Exception as e:
            print(f"[ERROR] {symbol}: {e}")
            continue
    
    return watchlist


def save_watchlist(watchlist):
    """Save watchlist to JSON"""
    WATCHLIST_OUTPUT.parent.mkdir(exist_ok=True)
    with open(WATCHLIST_OUTPUT, 'w') as f:
        json.dump(watchlist, f, indent=2)
    print(f"\n[SAVED] {len(watchlist)} stocks → {WATCHLIST_OUTPUT}")
    
    # Send Telegram notification
    ist = pytz.timezone('Asia/Kolkata')
    stocks = list(watchlist.keys())
    notify_reclaims_found(len(watchlist), stocks, datetime.now(ist).strftime('%H:%M'))


if __name__ == "__main__":
    watchlist = scan_stocks()
    save_watchlist(watchlist)