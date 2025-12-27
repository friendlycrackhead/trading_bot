"""
NIFTY Hourly SMA50 Filter - MATCHES BACKTEST LOGIC
Checks if PREVIOUS NIFTY hourly candle closed above SMA50
No live LTP checks - uses completed candle data only
"""
import sys
import json
from pathlib import Path
from datetime import datetime, timedelta
import pytz

# Add ROOT to path to import kite_client
ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT))

from kite_client import get_kite_client
from telegram_notifier import notify_nifty_filter


# ============ CONFIG ============
SMA_CACHE_FILE = ROOT / "main" / "nifty_sma50_cache.json"
CACHE_VALIDITY_SECONDS = 3600  # 1 hour (refresh with scanner)


def load_sma_cache():
    """Load cached NIFTY close and SMA50 if valid"""
    if not SMA_CACHE_FILE.exists():
        return None, None
    
    try:
        with open(SMA_CACHE_FILE) as f:
            cache = json.load(f)
        
        # Check if cache is still valid (< 1 hour old)
        cache_time = datetime.fromisoformat(cache['timestamp'])
        ist = pytz.timezone('Asia/Kolkata')
        now = datetime.now(ist)
        
        age_seconds = (now - cache_time).total_seconds()
        
        if age_seconds < CACHE_VALIDITY_SECONDS:
            return cache['last_close'], cache['sma50']
        else:
            return None, None
    except:
        return None, None


def save_sma_cache(last_close, sma50):
    """Save NIFTY last candle close and SMA50 to cache"""
    ist = pytz.timezone('Asia/Kolkata')
    cache = {
        'last_close': last_close,
        'sma50': sma50,
        'timestamp': datetime.now(ist).isoformat()
    }
    
    SMA_CACHE_FILE.parent.mkdir(exist_ok=True)
    with open(SMA_CACHE_FILE, 'w') as f:
        json.dump(cache, f, indent=2)


def fetch_and_calculate_sma50():
    """
    Fetch historical data and calculate SMA50
    Returns: (last_candle_close, sma50)
    Uses PREVIOUS candle's close and SMA50 from candles BEFORE that
    """
    kite = get_kite_client()
    
    # NIFTY 50 instrument token
    NIFTY_TOKEN = 256265  # NSE:NIFTY 50
    
    # Use IST timezone
    ist = pytz.timezone('Asia/Kolkata')
    to_date = datetime.now(ist)
    from_date = to_date - timedelta(days=20)  # ~20 days to ensure 60+ hourly candles
    
    try:
        # Fetch historical candles
        candles = kite.historical_data(
            instrument_token=NIFTY_TOKEN,
            from_date=from_date,
            to_date=to_date,
            interval="60minute"
        )
    except Exception as e:
        print(f"[ERROR] Failed to fetch NIFTY hourly data: {e}")
        return None, None
    
    if len(candles) < 52:
        print(f"[ERROR] Insufficient data for SMA50: {len(candles)} candles")
        return None, None
    
    # Get last completed candle's close
    last_candle_close = candles[-1]['close']
    
    # Calculate SMA50 using 50 candles BEFORE the last candle
    # candles[-1] = most recent completed candle (this is what we're checking)
    # candles[-52:-2] = 50 candles BEFORE the last candle (for SMA50)
    last_50_closes = [c['close'] for c in candles[-52:-2]]
    
    if len(last_50_closes) < 50:
        print(f"[ERROR] Insufficient candles for SMA50: {len(last_50_closes)}")
        return None, None
    
    sma50 = sum(last_50_closes) / 50
    
    return last_candle_close, sma50


def is_trading_enabled() -> bool:
    """
    Returns True if NIFTY's PREVIOUS candle closed above SMA50, else False
    Uses cached values if available (< 1 hour old), otherwise calculates fresh
    Matches backtest logic: previous candle close vs SMA50 from candles before it
    """
    # Try to load cached values
    last_close, sma50 = load_sma_cache()
    
    if last_close is None or sma50 is None:
        # Cache miss or expired - calculate fresh
        last_close, sma50 = fetch_and_calculate_sma50()
        
        if last_close is None or sma50 is None:
            print("[FILTER] Failed to calculate NIFTY filter - blocking trades")
            return False
        
        # Save to cache (silent)
        save_sma_cache(last_close, sma50)
    
    # Check: did previous candle close above SMA50?
    enabled = last_close > sma50
    
    status = "ON" if enabled else "OFF"
    print(f"[NIFTY FILTER] Trading: {status} | Close: {last_close:.2f} | SMA50: {sma50:.2f}")
    
    return enabled


def update_sma_cache():
    """
    Explicitly update NIFTY filter cache
    Called hourly with scanner to keep cache fresh
    """
    last_close, sma50 = fetch_and_calculate_sma50()
    
    if last_close is not None and sma50 is not None:
        save_sma_cache(last_close, sma50)
        
        status = "ON" if last_close > sma50 else "OFF"
        print(f"[NIFTY FILTER] Trading: {status} | Close: {last_close:.2f} | SMA50: {sma50:.2f}")
        
        ist = pytz.timezone('Asia/Kolkata')
        notify_nifty_filter(last_close > sma50, last_close, sma50, datetime.now(ist).strftime('%H:%M'))
        
        return True
    else:
        print(f"[NIFTY FILTER] ✗ Failed to update filter")
        return False


if __name__ == "__main__":
    # Test - force cache update
    print("Testing NIFTY filter...\n")
    update_sma_cache()
    print()
    
    result = is_trading_enabled()
    print(f"\nTrading Enabled: {result}")