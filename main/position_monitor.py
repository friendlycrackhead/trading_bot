"""
ROOT/main/position_monitor.py

Monitors open CNC holdings for SL/TP triggers
Runs every 1 second in main loop
Calls order_manager to place exit orders when SL or TP hit

UPDATED: Uses log_manager for trade exit logging with bars_held tracking
"""

import sys
import json
from pathlib import Path
from datetime import datetime
import pytz

ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT))

from kite_client import get_kite_client
from order_manager import place_exit_order
from log_manager import log_trade_exit  # NEW: Import from log_manager
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
    Calculate number of hourly bars held
    
    Args:
        entry_timestamp: ISO timestamp of entry
    
    Returns: bars_held (integer)
    """
    ist = pytz.timezone('Asia/Kolkata')
    
    entry_time = datetime.fromisoformat(entry_timestamp)
    exit_time = datetime.now(ist)
    
    # Calculate hours difference (rounded)
    hours_diff = (exit_time - entry_time).total_seconds() / 3600
    bars_held = max(1, round(hours_diff))  # Minimum 1 bar
    
    return bars_held


def monitor_positions():
    """
    Check open CNC holdings against SL/TP
    Cross-references cache (entry/SL/TP data) with actual holdings from Kite
    Calls order_manager to place exit orders
    """
    cache = load_positions_cache()
    
    if not cache:
        return  # No positions to monitor
    
    kite = get_kite_client()
    
    # Get actual holdings from account
    try:
        holdings = kite.holdings()
    except Exception as e:
        print(f"[ERROR] Failed to fetch holdings: {e}")
        return
    
    # Create dict of current holdings (symbol: quantity)
    current_holdings = {}
    for h in holdings:
        symbol = h['tradingsymbol']
        quantity = h['quantity']
        
        # Only track long positions (quantity > 0)
        if quantity > 0:
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
        # Check if we still hold this stock
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
        trade_id = pos_data.get('trade_id')  # Get trade_id from cache
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
                # Calculate bars held
                bars_held = calculate_bars_held(entry_timestamp) if entry_timestamp else 1
                
                # Get exit timestamp
                ist = pytz.timezone('Asia/Kolkata')
                exit_timestamp = datetime.now(ist).isoformat()
                
                # Log trade exit to log_manager (NEW)
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
                # Calculate bars held
                bars_held = calculate_bars_held(entry_timestamp) if entry_timestamp else 1
                
                # Get exit timestamp
                ist = pytz.timezone('Asia/Kolkata')
                exit_timestamp = datetime.now(ist).isoformat()
                
                # Log trade exit to log_manager (NEW)
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