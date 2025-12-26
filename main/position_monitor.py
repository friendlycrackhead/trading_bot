"""
ROOT/main/position_monitor.py

Monitors open CNC holdings for SL/TP triggers
Runs every 1 second in main loop
Places exit orders when SL or TP hit
"""

import sys
import json
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT))

from kite_client import get_kite_client
from risk_manager import log_trade_exit


# ============ CONFIG ============
POSITIONS_CACHE = ROOT / "main" / "open_positions.json"
TP_MULTIPLIER = 3.0  # 3R target


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


def add_position(symbol, entry_price, stop_loss, quantity):
    """
    Add new position to cache when entry executed
    Called by order_manager
    """
    cache = load_positions_cache()
    
    # Calculate TP = entry + 3R
    risk_per_share = entry_price - stop_loss
    target_price = entry_price + (TP_MULTIPLIER * risk_per_share)
    
    cache[symbol] = {
        "entry_price": entry_price,
        "stop_loss": stop_loss,
        "target_price": target_price,
        "quantity": quantity,
        "entry_timestamp": datetime.now().isoformat()
    }
    
    save_positions_cache(cache)
    print(f"[MONITOR] Added {symbol} - Entry: ₹{entry_price:.2f}, SL: ₹{stop_loss:.2f}, TP: ₹{target_price:.2f}")


def monitor_positions():
    """
    Check open CNC holdings against SL/TP
    Cross-references cache (entry/SL/TP data) with actual holdings from Kite
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
        
        # Verify quantity matches (warn if mismatch but continue)
        actual_quantity = current_holdings[symbol]
        cached_quantity = pos_data['quantity']
        
        if actual_quantity != cached_quantity:
            print(f"\n[WARNING] {symbol} quantity mismatch - Cache: {cached_quantity}, Actual: {actual_quantity}")
            print(f"[WARNING] Using actual quantity: {actual_quantity}")
        
        # Get live price
        instrument_key = f"NSE:{symbol}"
        
        if instrument_key not in quotes:
            print(f"[WARNING] {symbol} - No quote data available")
            continue
        
        ltp = quotes[instrument_key]['last_price']
        entry_price = pos_data['entry_price']
        stop_loss = pos_data['stop_loss']
        target_price = pos_data['target_price']
        
        # Use actual quantity from holdings (not cache)
        quantity = actual_quantity
        
        # Check SL hit
        if ltp <= stop_loss:
            print(f"\n{'!'*60}")
            print(f"[SL HIT] {symbol}")
            print(f"  LTP: ₹{ltp:.2f} <= SL: ₹{stop_loss:.2f}")
            print(f"  Quantity: {quantity}")
            print(f"{'!'*60}")
            
            exit_price = place_exit_order(kite, symbol, quantity, "SL")
            
            if exit_price:
                log_trade_exit(symbol, entry_price, exit_price, stop_loss, quantity)
                positions_to_remove.append(symbol)
        
        # Check TP hit
        elif ltp >= target_price:
            print(f"\n{'!'*60}")
            print(f"[TP HIT] {symbol}")
            print(f"  LTP: ₹{ltp:.2f} >= TP: ₹{target_price:.2f}")
            print(f"  Quantity: {quantity}")
            print(f"{'!'*60}")
            
            exit_price = place_exit_order(kite, symbol, quantity, "TP")
            
            if exit_price:
                log_trade_exit(symbol, entry_price, exit_price, stop_loss, quantity)
                positions_to_remove.append(symbol)
    
    # Remove closed positions from cache
    if positions_to_remove:
        for symbol in positions_to_remove:
            del cache[symbol]
        save_positions_cache(cache)
        print(f"\n[CACHE] Removed {len(positions_to_remove)} closed positions from tracking")


def place_exit_order(kite, symbol, quantity, reason):
    """
    Place market sell order to exit CNC holding
    Returns: executed price or None
    """
    try:
        order_id = kite.place_order(
            variety=kite.VARIETY_REGULAR,
            exchange=kite.EXCHANGE_NSE,
            tradingsymbol=symbol,
            transaction_type=kite.TRANSACTION_TYPE_SELL,
            quantity=quantity,
            product=kite.PRODUCT_CNC,
            order_type=kite.ORDER_TYPE_MARKET
        )
        
        print(f"[EXIT ORDER] {reason} - {symbol} x{quantity} - Order ID: {order_id}")
        
        # Get execution price (use LTP as approximation)
        quote = kite.quote(f"NSE:{symbol}")
        exit_price = quote[f"NSE:{symbol}"]['last_price']
        
        print(f"[EXIT PRICE] Estimated exit: ₹{exit_price:.2f}")
        
        return exit_price
        
    except Exception as e:
        print(f"[ERROR] Exit order failed for {symbol}: {e}")
        return None


if __name__ == "__main__":
    monitor_positions()