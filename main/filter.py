"""
NIFTY Hourly SMA50 Filter - OPTIMIZED WITH CACHING
Uses NIFTY's current LTP (live price) and compares against cached SMA50
SMA50 cache updated every 5 minutes to reduce latency at entry time
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


# ============ CONFIG ============
SMA_CACHE_FILE = ROOT / "main" / "nifty_sma50_cache.json"
CACHE_VALIDITY_SECONDS = 300  # 5 minutes


def load_sma_cache():
    """Load cached SMA50 if valid"""
    if not SMA_CACHE_FILE.exists():
        return None
    
    try:
        with open(SMA_CACHE_FILE) as f:
            cache = json.load(f)
        
        # Check if cache is still valid (< 5 minutes old)
        cache_time = datetime.fromisoformat(cache['timestamp'])
        ist = pytz.timezone('Asia/Kolkata')
        now = datetime.now(ist)
        
        age_seconds = (now - cache_time).total_seconds()
        
        if age_seconds < CACHE_VALIDITY_SECONDS:
            return cache['sma50']
        else:
            return None
    except:
        return None


def save_sma_cache(sma50):
    """Save SMA50 to cache"""
    ist = pytz.timezone('Asia/Kolkata')
    cache = {
        'sma50': sma50,
        'timestamp': datetime.now(ist).isoformat()
    }
    
    SMA_CACHE_FILE.parent.mkdir(exist_ok=True)
    with open(SMA_CACHE_FILE, 'w') as f:
        json.dump(cache, f, indent=2)


def fetch_and_calculate_sma50():
    """Fetch historical data and calculate SMA50"""
    kite = get_kite_client()
    
    # NIFTY 50 instrument token
    NIFTY_TOKEN = 256265  # NSE:NIFTY 50
    
    # Use IST timezone
    ist = pytz.timezone('Asia/Kolkata')
    to_date = datetime.now(ist)
    from_date = to_date - timedelta(days=20)  # ~20 days to ensure 60+ hourly candles
    
    try:
        # Fetch historical candles for SMA50 calculation
        candles = kite.historical_data(
            instrument_token=NIFTY_TOKEN,
            from_date=from_date,
            to_date=to_date,
            interval="60minute"
        )
    except Exception as e:
        print(f"[ERROR] Failed to fetch NIFTY hourly data: {e}")
        return None
    
    if len(candles) < 50:
        print(f"[ERROR] Insufficient data for SMA50: {len(candles)} candles")
        return None
    
    # Calculate SMA50 using last 50 completed candles
    # candles[-1] = current forming candle (incomplete)
    # candles[-51:-1] = last 50 completed candles
    last_50_closes = [c['close'] for c in candles[-51:-1]]
    sma50 = sum(last_50_closes) / 50
    
    return sma50


def is_trading_enabled() -> bool:
    """
    Returns True if NIFTY current LTP > SMA50, else False
    Uses cached SMA50 if available (< 5 min old), otherwise calculates fresh
    At XX:14:58, gets NIFTY's live price and compares against SMA50
    """
    kite = get_kite_client()
    
    # Try to load cached SMA50
    sma50 = load_sma_cache()
    
    if sma50 is None:
        # Cache miss or expired - calculate fresh
        print("[CACHE] SMA50 cache miss/expired - calculating fresh...")
        sma50 = fetch_and_calculate_sma50()
        
        if sma50 is None:
            return False
        
        # Save to cache
        save_sma_cache(sma50)
        print(f"[CACHE] SMA50 cached: {sma50:.2f}")
    else:
        print(f"[CACHE] Using cached SMA50: {sma50:.2f}")
    
    # Get NIFTY's current LTP (live price at XX:14:58)
    try:
        quote = kite.quote("NSE:NIFTY 50")
        nifty_ltp = quote["NSE:NIFTY 50"]['last_price']
    except Exception as e:
        print(f"[ERROR] Failed to fetch NIFTY LTP: {e}")
        return False
    
    enabled = nifty_ltp > sma50
    
    print(f"[FILTER] NIFTY LTP: {nifty_ltp:.2f} | SMA50: {sma50:.2f} | Trading: {enabled}")
    
    return enabled


def update_sma_cache():
    """
    Explicitly update SMA50 cache
    Called periodically (e.g., every 5 minutes) to keep cache fresh
    """
    print("[CACHE UPDATE] Refreshing NIFTY SMA50 cache...")
    
    sma50 = fetch_and_calculate_sma50()
    
    if sma50 is not None:
        save_sma_cache(sma50)
        print(f"[CACHE UPDATE] ✓ SMA50 updated: {sma50:.2f}")
        return True
    else:
        print(f"[CACHE UPDATE] ✗ Failed to update SMA50")
        return False


if __name__ == "__main__":
    # Test - force cache update
    print("Testing filter with cache update...\n")
    update_sma_cache()
    print()
    
    result = is_trading_enabled()
    print(f"\nTrading Enabled: {result}")