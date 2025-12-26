"""
ROOT/main/entry_checker.py

Checks if watchlist stocks close above reclaim high
Runs at XX:14:58 (2 sec before hourly candle close)
First checks NIFTY LTP > SMA50, then checks stock LTP > reclaim high
Outputs: entry_signals.json
"""

import sys
import json
from pathlib import Path
from datetime import datetime
import pytz

ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT))

from kite_client import get_kite_client
from filter import is_trading_enabled


# ============ CONFIG ============
WATCHLIST_INPUT = ROOT / "main" / "reclaim_watchlist.json"
SIGNALS_OUTPUT = ROOT / "main" / "entry_signals.json"


def load_watchlist():
    """Load reclaim watchlist"""
    if not WATCHLIST_INPUT.exists():
        print("[ERROR] Watchlist not found")
        return {}
    
    with open(WATCHLIST_INPUT) as f:
        return json.load(f)


def check_entries():
    """
    STEP 1: Check NIFTY LTP > SMA50 (trend gate)
    STEP 2: If pass, check stock LTP > reclaim high
    Uses LTP (real-time price) at XX:14:58
    """
    ist = pytz.timezone('Asia/Kolkata')
    now = datetime.now(ist)
    
    print(f"\n{'='*60}")
    print(f"[ENTRY CHECK] Starting at {now.strftime('%H:%M:%S')}")
    print(f"{'='*60}\n")
    
    # STEP 1: Check NIFTY trend gate FIRST
    print(f"[STEP 1/2] Checking NIFTY Trend Gate (LTP vs SMA50)...")
    
    if not is_trading_enabled():
        print("\n❌ [BLOCKED] NIFTY trend gate FAILED - No entries allowed")
        print(f"{'='*60}\n")
        return {}
    
    print("\n✅ [PASSED] NIFTY trend gate - Proceeding with stock checks\n")
    
    # STEP 2: Check watchlist stocks
    print(f"[STEP 2/2] Checking Watchlist Stocks (LTP vs Reclaim High)...")
    
    watchlist = load_watchlist()
    
    if not watchlist:
        print("[INFO] Watchlist empty - no stocks to check")
        return {}
    
    print(f"[CHECK] Checking {len(watchlist)} stocks\n")
    
    kite = get_kite_client()
    
    entry_signals = {}
    
    # Prepare instrument list for bulk quote (single API call)
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
            current_price = quotes[instrument_key]['last_price']
            
            # Check if current price > reclaim high
            if current_price > reclaim_high:
                entry_signals[symbol] = {
                    "entry_price": current_price,
                    "reclaim_high": reclaim_high,
                    "timestamp": now.isoformat()
                }
                print(f"  ✅ [ENTRY SIGNAL] {symbol} @ ₹{current_price:.2f} (reclaim: ₹{reclaim_high:.2f})")
            else:
                print(f"  ❌ [NO ENTRY] {symbol} LTP ₹{current_price:.2f} <= reclaim ₹{reclaim_high:.2f}")
    
    except Exception as e:
        print(f"\n[ERROR] Failed to get quotes: {e}")
    
    return entry_signals


def save_signals(signals):
    """Save entry signals to JSON"""
    SIGNALS_OUTPUT.parent.mkdir(exist_ok=True)
    with open(SIGNALS_OUTPUT, 'w') as f:
        json.dump(signals, f, indent=2)
    
    print(f"\n{'='*60}")
    print(f"[RESULT] {len(signals)} entry signals generated")
    print(f"[SAVED] → {SIGNALS_OUTPUT}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    signals = check_entries()
    save_signals(signals)