"""
ROOT/main/reclaim_scanner.py

Scans NIFTY50 stocks for VWAP reclaim setups
Runs at XX:16:00 (16 min after hourly candle close)
Outputs: reclaim_watchlist.json
"""

import sys
import json
from pathlib import Path
from datetime import datetime, timedelta
import time
import pytz

ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT))

from kite_client import get_kite_client
from filter import is_trading_enabled


# ============ CONFIG ============
NIFTY50_CSV = ROOT / "nifty50_symbols.csv"
INSTRUMENTS_JSON = ROOT / "instruments_nse.json"
WATCHLIST_OUTPUT = ROOT / "main" / "reclaim_watchlist.json"


def load_nifty50_symbols():
    """Load NIFTY50 symbols from CSV"""
    with open(NIFTY50_CSV) as f:
        return [line.strip() for line in f if line.strip()]


def load_instrument_tokens():
    """Load NSE instrument tokens"""
    with open(INSTRUMENTS_JSON) as f:
        instruments = json.load(f)
    return {i["tradingsymbol"]: i["instrument_token"] for i in instruments}


def calculate_session_vwap(candles):
    """
    Calculate cumulative VWAP from session start
    At XX:16, last candle (candles[-1]) is incomplete current hour
    Use candles[:-1] which are all completed candles up to previous hour
    Returns: vwap_value
    """
    if len(candles) < 2:
        return None
    
    # Use only completed candles (exclude last incomplete one)
    completed_candles = candles[:-1]
    
    # VWAP calculation
    cum_tpv = 0  # Typical Price × Volume
    cum_vol = 0
    
    for c in completed_candles:
        tp = (c['high'] + c['low'] + c['close']) / 3
        cum_tpv += tp * c['volume']
        cum_vol += c['volume']
    
    vwap = cum_tpv / cum_vol if cum_vol > 0 else None
    
    return vwap


def get_volume_sma50(all_candles, current_candle_index):
    """
    Calculate volume SMA50 from already-fetched candles
    Uses 50 hourly candles ending at current_candle_index
    """
    # Need at least 50 candles before current_candle_index
    if current_candle_index < 50:
        return None
    
    # Take 50 candles ending at the candle we're checking
    start_idx = current_candle_index - 50
    end_idx = current_candle_index
    
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
    """
    # Check trend gate
    if not is_trading_enabled():
        print("[FILTER] Trend gate OFF - no scanning")
        return {}
    
    print("[FILTER] Trend gate ON - scanning...")
    
    kite = get_kite_client()
    symbols = load_nifty50_symbols()
    tokens = load_instrument_tokens()
    
    watchlist = {}
    
    # Use current datetime with IST timezone
    ist = pytz.timezone('Asia/Kolkata')
    scan_date = datetime.now(ist)
    print(f"[SCAN] Running at {scan_date.strftime('%Y-%m-%d %H:%M:%S')}")
    
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
            
            # Need at least 3 today's candles: 2 completed + 1 current incomplete
            if len(today_candles) < 3:
                continue
            
            # Calculate VWAP from today's completed candles only
            vwap = calculate_session_vwap(today_candles)
            
            # Find the candle we're checking (second-to-last in today's candles)
            check_candle = today_candles[-2]
            
            # Find this candle's index in all_candles for volume SMA50
            check_candle_index = None
            for i, c in enumerate(all_candles):
                if c['date'] == check_candle['date']:
                    check_candle_index = i
                    break
            
            if check_candle_index is None:
                continue
            
            # Get volume SMA50 using data up to the candle we're checking
            vol_sma50 = get_volume_sma50(all_candles, check_candle_index)
            
            # Check reclaim
            is_reclaim, reclaim_high, reclaim_low = check_reclaim(check_candle, vwap, vol_sma50)
            
            if is_reclaim:
                watchlist[symbol] = {
                    "reclaim_high": reclaim_high,
                    "reclaim_low": reclaim_low,
                    "timestamp": check_candle['date'].isoformat(),
                    "vwap": vwap
                }
                print(f"[RECLAIM] {symbol} @ {reclaim_high:.2f} (Low: {reclaim_low:.2f}, VWAP: {vwap:.2f})")
        
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


if __name__ == "__main__":
    watchlist = scan_stocks()
    save_watchlist(watchlist)