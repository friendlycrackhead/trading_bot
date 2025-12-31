"""
ROOT/main/position_monitor.py

Monitors open CNC holdings for SL/TP triggers
Runs every 1 second in main loop
Calls order_manager to place exit orders when SL or TP hit

UPDATED: Uses log_manager for trade exit logging with bars_held tracking
FIXED: Bars held calculation only counts market hours (not 24/7)
"""

import sys
import json
from pathlib import Path
from datetime import datetime, timedelta, time as dt_time
import pytz

ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT))

from kite_client import get_kite_client
from order_manager import place_exit_order
from log_manager import log_trade_exit
from telegram_notifier import notify_position_exit


# ============ CONFIG ============
POSITIONS_CACHE = ROOT / "main" / "open_positions.json"


def load_positions_cache():
    """Load cached open positions (stores entry/SL/TP data)"""
    if not POSITIONS_CACHE.exists():
        return {}
    
    with open(POSITIONS_CACHE) as f:
        return json.load(f)


def save_positions_cache(positions):
    """Save open positions cache"""
    POSITIONS_CACHE.parent.mkdir(exist_ok=True)
    with open(POSITIONS_CACHE, 'w') as f:
        json.dump(positions, f, indent=2)


def calculate_bars_held(entry_timestamp):
    """
    Calculate number of hourly bars held during market hours ONLY
    Market hours: 9:15 AM - 3:30 PM (6 hourly candles max per day)
    
    This correctly handles:
    - Intraday trades (same day entry/exit)
    - Overnight holds (across multiple days)
    - Weekend holds (skips Sat/Sun)
    
    Args:
        entry_timestamp: ISO timestamp of entry
    
    Returns: bars_held (integer) - only counts market hours
    
    Example:
        Entry Monday 2:00 PM → Exit Tuesday 11:00 AM
        Monday: 2:00-3:30 = 1.5 hrs → 2 bars
        Tuesday: 9:15-11:00 = 1.75 hrs → 2 bars
        Total: 4 bars (not 21!)
    """
    ist = pytz.timezone('Asia/Kolkata')
    
    entry_time = datetime.fromisoformat(entry_timestamp)
    exit_time = datetime.now(ist)
    
    # Market hours: 9:15 AM - 3:30 PM
    market_start = dt_time(9, 15)
    market_end = dt_time(15, 30)
    
    bars = 0
    current_date = entry_time.date()
    end_date = exit_time.date()
    
    # Loop through each day from entry to exit
    while current_date <= end_date:
        # Skip weekends (Saturday=5, Sunday=6)
        if current_date.weekday() >= 5:
            current_date += timedelta(days=1)
            continue
        day_start = datetime.combine(current_date, market_start, tzinfo=ist)
        day_end = datetime.combine(current_date, market_end, tzinfo=ist)
        
        # Get actual start/end times for this day
        # (might be entry time on first day, exit time on last day)
        actual_start = max(entry_time, day_start)
        actual_end = min(exit_time, day_end)
        
        # Only count if this day had any market time
        if actual_start < actual_end:
            # Calculate hours in market this day
            market_seconds = (actual_end - actual_start).total_seconds()
            market_hours = market_seconds / 3600
            
            # Round to nearest hour (1 hour = 1 bar)
            # Minimum 1 bar if any time in market
            day_bars = max(1, round(market_hours))
            bars += day_bars
        
        # Move to next day
        current_date += timedelta(days=1)
    
    return bars


def monitor_positions():
    """
    Check open CNC holdings/positions against SL/TP
    Cross-references cache (entry/SL/TP data) with actual holdings AND positions from Kite
    Calls order_manager to place exit orders
    
    IMPORTANT: Checks both holdings (T+1) and positions (intraday) because:
    - If bought and still holding end of day → goes to holdings
    - If bought and sold same day → stays in positions, never reaches holdings
    """
    cache = load_positions_cache()
    
    if not cache:
        return  # No positions to monitor
    
    kite = get_kite_client()
    
    # Get actual holdings from account (T+1 positions)
    try:
        holdings = kite.holdings()
    except Exception as e:
        print(f"[ERROR] Failed to fetch holdings: {e}")
        return
    
    # Get positions (intraday trades)
    try:
        positions = kite.positions()
        # positions() returns dict with 'day' and 'net' keys
        # 'day' = intraday positions
        # 'net' = combined (day + overnight)
        day_positions = positions.get('day', [])
    except Exception as e:
        print(f"[ERROR] Failed to fetch positions: {e}")
        return
    
    # Create dict of current holdings/positions (symbol: quantity)
    current_holdings = {}
    
    # 1. Add from holdings (T+1)
    for h in holdings:
        symbol = h['tradingsymbol']
        quantity = h['quantity']
        
        # Only track long positions (quantity > 0)
        if quantity > 0:
            current_holdings[symbol] = quantity
    
    # 2. Add from positions (intraday) - these override holdings if present
    for p in day_positions:
        symbol = p['tradingsymbol']
        quantity = p['quantity']
        
        # Only track long positions (quantity > 0)
        if quantity > 0:
            # If already in holdings, this is the more current value
            current_holdings[symbol] = quantity
    
    # Get live quotes for positions in cache
    instruments = [f"NSE:{symbol}" for symbol in cache.keys()]
    
    try:
        quotes = kite.quote(instruments)
    except Exception as e:
        print(f"[ERROR] Failed to get quotes: {e}")
        return
    
    positions_to_remove = []
    
    for symbol, pos_data in cache.items():
        # Check if we still hold this stock (in either holdings or positions)
        if symbol not in current_holdings:
            print(f"\n[POSITION CLOSED] {symbol} - No longer in holdings (manual exit or already processed)")
            positions_to_remove.append(symbol)
            continue
        
        # Verify quantity matches (update cache if mismatch)
        actual_quantity = current_holdings[symbol]
        cached_quantity = pos_data['quantity']
        
        if actual_quantity != cached_quantity:
            print(f"\n[WARNING] {symbol} quantity mismatch - Cache: {cached_quantity}, Actual: {actual_quantity}")
            print(f"[WARNING] Updating cache to actual quantity: {actual_quantity}")
            # Update cache immediately to keep R-tracking accurate
            pos_data['quantity'] = actual_quantity
            cache[symbol] = pos_data
            save_positions_cache(cache)
        
        # Get live price
        instrument_key = f"NSE:{symbol}"
        
        if instrument_key not in quotes:
            print(f"[WARNING] {symbol} - No quote data available")
            continue
        
        ltp = quotes[instrument_key]['last_price']
        entry_price = pos_data['entry_price']
        stop_loss = pos_data['stop_loss']
        target_price = pos_data['target_price']
        trade_id = pos_data.get('trade_id')
        entry_timestamp = pos_data.get('entry_timestamp')
        
        # Use actual quantity from holdings (cache now updated if there was mismatch)
        quantity = actual_quantity
        
        # Check SL hit
        if ltp <= stop_loss:
            print(f"\n{'!'*60}")
            print(f"[SL HIT] {symbol}")
            print(f"  LTP: ₹{ltp:.2f} <= SL: ₹{stop_loss:.2f}")
            print(f"  Quantity: {quantity}")
            print(f"{'!'*60}")
            
            # Call order_manager to place exit order
            exit_price = place_exit_order(symbol, quantity, "SL")
            
            if exit_price:
                # Calculate bars held (market hours only)
                bars_held = calculate_bars_held(entry_timestamp) if entry_timestamp else 1
                
                # Get exit timestamp
                ist = pytz.timezone('Asia/Kolkata')
                exit_timestamp = datetime.now(ist).isoformat()
                
                # Log trade exit to log_manager
                r_value = log_trade_exit(
                    trade_id=trade_id,
                    symbol=symbol,
                    exit_timestamp=exit_timestamp,
                    exit_price=exit_price,
                    exit_reason="SL",
                    bars_held=bars_held
                )
                
                # Send Telegram notification
                notify_position_exit(symbol, entry_price, exit_price, stop_loss, quantity, r_value, "SL Hit")
                
                positions_to_remove.append(symbol)
        
        # Check TP hit
        elif ltp >= target_price:
            print(f"\n{'!'*60}")
            print(f"[TP HIT] {symbol}")
            print(f"  LTP: ₹{ltp:.2f} >= TP: ₹{target_price:.2f}")
            print(f"  Quantity: {quantity}")
            print(f"{'!'*60}")
            
            # Call order_manager to place exit order
            exit_price = place_exit_order(symbol, quantity, "TP")
            
            if exit_price:
                # Calculate bars held (market hours only)
                bars_held = calculate_bars_held(entry_timestamp) if entry_timestamp else 1
                
                # Get exit timestamp
                ist = pytz.timezone('Asia/Kolkata')
                exit_timestamp = datetime.now(ist).isoformat()
                
                # Log trade exit to log_manager
                r_value = log_trade_exit(
                    trade_id=trade_id,
                    symbol=symbol,
                    exit_timestamp=exit_timestamp,
                    exit_price=exit_price,
                    exit_reason="TP",
                    bars_held=bars_held
                )
                
                # Send Telegram notification
                notify_position_exit(symbol, entry_price, exit_price, stop_loss, quantity, r_value, "TP Hit")
                
                positions_to_remove.append(symbol)
    
    # Remove closed positions from cache
    if positions_to_remove:
        for symbol in positions_to_remove:
            del cache[symbol]
        save_positions_cache(cache)
        print(f"\n[CACHE] Removed {len(positions_to_remove)} closed positions from tracking")


if __name__ == "__main__":
    monitor_positions()