"""
ROOT/main/entry_checker.py

Checks if watchlist stocks trading above reclaim high
Runs at XX:15:00 (right when hourly candle closes)
STEP 1: Check NIFTY candle close > SMA50
STEP 2: If pass, check stock LTP > reclaim high
Outputs: entry_signals.json

UPDATED: Adds NIFTY filter data to signals for logging
"""

import sys
import json
from pathlib import Path
from datetime import datetime, timedelta
import pytz

ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT))

from kite_client import get_kite_client
from telegram_notifier import notify_entry_signals, notify_nifty_filter


# ============ CONFIG ============
WATCHLIST_INPUT = ROOT / "main" / "reclaim_watchlist.json"
SIGNALS_OUTPUT = ROOT / "main" / "entry_signals.json"
NIFTY_TOKEN = 256265  # NSE:NIFTY 50


def load_watchlist():
    """Load reclaim watchlist"""
    if not WATCHLIST_INPUT.exists():
        print("[ERROR] Watchlist not found")
        return {}
    
    with open(WATCHLIST_INPUT) as f:
        return json.load(f)


def check_nifty_filter():
    """
    STEP 1: Check if NIFTY's last completed candle closed above SMA50
    At 11:15, checks if 10:15-11:15 candle close > SMA50
    Returns: (passed, nifty_close, sma50)
    """
    kite = get_kite_client()
    ist = pytz.timezone('Asia/Kolkata')
    
    print(f"[STEP 1/2] Checking NIFTY Filter (Candle Close vs SMA50)...\n")
    
    try:
        # Fetch NIFTY hourly candles
        to_date = datetime.now(ist)
        from_date = to_date - timedelta(days=20)  # ~60 hourly candles
        
        candles = kite.historical_data(
            instrument_token=NIFTY_TOKEN,
            from_date=from_date,
            to_date=to_date,
            interval="60minute"
        )
        
        if len(candles) < 52:
            print(f"[ERROR] Insufficient NIFTY data: {len(candles)} candles")
            return False, None, None
        
        # Get last completed candle's close
        last_candle_close = candles[-1]['close']
        
        # Calculate SMA50 from 50 candles BEFORE the last candle
        sma50_candles = [c['close'] for c in candles[-52:-2]]
        
        if len(sma50_candles) < 50:
            print(f"[ERROR] Insufficient candles for SMA50: {len(sma50_candles)}")
            return False, None, None
        
        sma50 = sum(sma50_candles) / 50
        
        # Check if last candle closed above SMA50
        passed = last_candle_close > sma50
        
        status = "✅ PASSED" if passed else "❌ FAILED"
        print(f"[NIFTY FILTER] {status}")
        print(f"  Candle Close: ₹{last_candle_close:.2f}")
        print(f"  SMA50: ₹{sma50:.2f}")
        print()
        
        # Send Telegram notification
        notify_nifty_filter(passed, last_candle_close, sma50, datetime.now(ist).strftime('%H:%M'))
        
        return passed, last_candle_close, sma50
        
    except Exception as e:
        print(f"[ERROR] Failed to check NIFTY filter: {e}")
        return False, None, None


def check_entries():
    """
    Main entry check logic
    STEP 1: Check NIFTY filter
    STEP 2: If pass, check stock LTP > reclaim high
    """
    ist = pytz.timezone('Asia/Kolkata')
    now = datetime.now(ist)
    
    print(f"\n{'='*60}")
    print(f"[ENTRY CHECK] Starting at {now.strftime('%H:%M:%S')}")
    print(f"{'='*60}\n")
    
    # STEP 1: Check NIFTY filter
    nifty_passed, nifty_close, sma50 = check_nifty_filter()
    
    if not nifty_passed:
        print(f"{'─'*60}")
        print(f"[BLOCKED] NIFTY filter failed - No entries allowed")
        print(f"{'─'*60}\n")
        return {}, nifty_close, sma50  # Return NIFTY data even if failed
    
    print(f"{'─'*60}")
    print(f"[NIFTY FILTER PASSED] Proceeding with stock checks")
    print(f"{'─'*60}\n")
    
    # STEP 2: Check stocks
    print(f"[STEP 2/2] Checking Watchlist Stocks (LTP vs Reclaim High)...\n")
    
    # Load watchlist
    watchlist = load_watchlist()
    
    if not watchlist:
        print("[INFO] Watchlist empty - no stocks to check")
        return {}, nifty_close, sma50
    
    print(f"[CHECK] Checking {len(watchlist)} stocks\n")
    
    kite = get_kite_client()
    
    entry_signals = {}
    
    # Prepare instrument list for bulk quote
    instruments = [f"NSE:{symbol}" for symbol in watchlist.keys()]
    
    try:
        # Get live quotes for all watchlist stocks at once
        quotes = kite.quote(instruments)
        
        for symbol, data in watchlist.items():
            instrument_key = f"NSE:{symbol}"
            
            if instrument_key not in quotes:
                print(f"  ⚠️  [SKIP] {symbol} - no quote data")
                continue
            
            reclaim_high = data["reclaim_high"]
            reclaim_low = data["reclaim_low"]
            current_price = quotes[instrument_key]['last_price']
            
            # Check if current price > reclaim high
            if current_price > reclaim_high:
                entry_signals[symbol] = {
                    "entry_price": current_price,
                    "reclaim_high": reclaim_high,
                    "reclaim_low": reclaim_low,
                    "timestamp": now.isoformat(),
                    # ADD NIFTY data for order_manager logging
                    "nifty_close": nifty_close,
                    "nifty_sma50": sma50
                }
                print(f"  ✅ [ENTRY SIGNAL] {symbol} @ ₹{current_price:.2f} (reclaim high: ₹{reclaim_high:.2f})")
            else:
                print(f"  ❌ [NO ENTRY] {symbol} LTP ₹{current_price:.2f} <= reclaim high ₹{reclaim_high:.2f}")
    
    except Exception as e:
        print(f"\n[ERROR] Failed to get quotes: {e}")
    
    return entry_signals, nifty_close, sma50


def save_signals(signals):
    """Save entry signals to JSON"""
    SIGNALS_OUTPUT.parent.mkdir(exist_ok=True)
    with open(SIGNALS_OUTPUT, 'w') as f:
        json.dump(signals, f, indent=2)
    
    # Send Telegram notification
    notify_entry_signals(signals)
    
    print(f"\n{'='*60}")
    print(f"[RESULT] {len(signals)} entry signals generated")
    print(f"[SAVED] → {SIGNALS_OUTPUT}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    signals, nifty_close, sma50 = check_entries()
    save_signals(signals)