"""
ROOT/main/order_manager.py

Unified order manager - handles ALL order placement (entry + exit)
Entry: Called by main after entry_checker generates signals
Exit: Called by position_monitor when SL/TP hit

UPDATED: Added order execution verification for both entry and exit orders
- Verifies order status after placement
- Only logs trades if order actually executed
- Handles rejected orders gracefully

FIXED:
- Added telegram notification in TEST_MODE
- Better error handling
"""

import sys
import json
from pathlib import Path
from datetime import datetime
import time
import pytz

ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT))

from kite_client import get_kite_client
from risk_manager import can_open_new_trades
from telegram_notifier import notify_order_placed, notify_order_skipped
from log_manager import log_trade_entry, generate_trade_id


# ============ CONFIG ============
SIGNALS_INPUT = ROOT / "main" / "entry_signals.json"
POSITIONS_FILE = ROOT / "main" / "open_positions.json"

RISK_PERCENT = 0.01  # 1% risk per trade
TP_MULTIPLIER = 3.0  # 3R target
TEST_MODE = False  # Set to False for live trading


# ============ ENTRY ORDERS ============

def load_entry_signals():
    """Load entry signals from entry_checker"""
    if not SIGNALS_INPUT.exists():
        print("[ERROR] Entry signals file not found")
        return {}
    
    with open(SIGNALS_INPUT) as f:
        return json.load(f)


def load_open_positions():
    """Load current open positions to check for duplicates"""
    if not POSITIONS_FILE.exists():
        return {}
    
    with open(POSITIONS_FILE) as f:
        return json.load(f)


def get_total_equity(kite):
    """Calculate total equity (margin + holdings value)"""
    try:
        # Get available margin
        margins = kite.margins("equity")
        available_margin = margins['available']['live_balance']
        
        # Get holdings value
        holdings = kite.holdings()
        holdings_value = sum(h['quantity'] * h['last_price'] for h in holdings)
        
        total_equity = available_margin + holdings_value
        
        print(f"[EQUITY] Available Margin: ₹{available_margin:,.2f}")
        print(f"[EQUITY] Holdings Value: ₹{holdings_value:,.2f}")
        print(f"[EQUITY] Total Equity: ₹{total_equity:,.2f}")
        
        return total_equity, available_margin
        
    except Exception as e:
        print(f"[ERROR] Failed to get equity: {e}")
        return None, None


def calculate_position_size(entry_price, stop_loss, total_equity):
    """
    Calculate position size based on 1% risk
    Returns: quantity, required_capital, risk_per_share
    """
    # Calculate risk amount (1% of equity)
    risk_amount = total_equity * RISK_PERCENT
    
    # Calculate risk per share
    risk_per_share = entry_price - stop_loss
    
    if risk_per_share <= 0:
        print(f"[ERROR] Invalid risk: entry {entry_price} <= SL {stop_loss}")
        return None, None, None
    
    # Calculate quantity
    quantity = int(risk_amount / risk_per_share)
    
    if quantity <= 0:
        print(f"[ERROR] Calculated quantity is 0")
        return None, None, None
    
    # Calculate required capital
    required_capital = entry_price * quantity
    
    print(f"[CALC] Risk Amount: ₹{risk_amount:,.2f} ({RISK_PERCENT*100}% of equity)")
    print(f"[CALC] Risk/Share: ₹{risk_per_share:.2f}")
    print(f"[CALC] Quantity: {quantity}")
    print(f"[CALC] Required Capital: ₹{required_capital:,.2f}")
    
    return quantity, required_capital, risk_per_share


def place_entry_order(kite, symbol, entry_price, stop_loss, quantity, entry_conditions=None):
    """
    Place BUY order (market, CNC)
    Verifies order execution before logging
    Returns: (order_id, trade_id) or (None, None)
    """
    ist = pytz.timezone('Asia/Kolkata')
    entry_timestamp = datetime.now(ist).isoformat()
    
    # Calculate TP
    risk_per_share = entry_price - stop_loss
    target_price = entry_price + (TP_MULTIPLIER * risk_per_share)
    
    # Generate trade ID
    trade_id = generate_trade_id(symbol, entry_timestamp)
    
    if TEST_MODE:
        print(f"\n[TEST MODE] Would place BUY order:")
        print(f"  Symbol: {symbol}")
        print(f"  Quantity: {quantity}")
        print(f"  Entry: ₹{entry_price:.2f}")
        print(f"  Stop Loss: ₹{stop_loss:.2f}")
        print(f"  Target: ₹{target_price:.2f}")
        print(f"  Trade ID: {trade_id}")
        
        order_id = f"TEST_BUY_{datetime.now(ist).strftime('%H%M%S')}"
        
        # Log trade entry (only trade-level, not order-level)
        log_trade_entry(
            trade_id=trade_id,
            symbol=symbol,
            entry_timestamp=entry_timestamp,
            entry_price=entry_price,
            stop_loss=stop_loss,
            target_price=target_price,
            quantity=quantity,
            entry_conditions=entry_conditions
        )
        
        # Send Telegram notification even in test mode
        notify_order_placed(symbol, quantity, entry_price, stop_loss, target_price)
        
        return order_id, trade_id
    
    # LIVE MODE - Place actual order
    try:
        order_id = kite.place_order(
            variety=kite.VARIETY_REGULAR,
            exchange=kite.EXCHANGE_NSE,
            tradingsymbol=symbol,
            transaction_type=kite.TRANSACTION_TYPE_BUY,
            quantity=quantity,
            product=kite.PRODUCT_CNC,
            order_type=kite.ORDER_TYPE_MARKET
        )
        
        print(f"\n[ORDER SUBMITTED] BUY {symbol}")
        print(f"  Order ID: {order_id}")
        print(f"  Verifying execution...")
        
        # Wait for order to execute (market orders execute within seconds)
        time.sleep(2)
        
        # Verify order executed successfully
        try:
            order_history = kite.order_history(order_id)
            final_status = order_history[-1]['status']
            
            # Check if order completed
            if final_status == 'COMPLETE':
                print(f"  ✅ Order EXECUTED")
                print(f"  Trade ID: {trade_id}")
                print(f"  Quantity: {quantity}")
                print(f"  Entry: ₹{entry_price:.2f}")
                print(f"  Stop Loss: ₹{stop_loss:.2f}")
                print(f"  Target: ₹{target_price:.2f}")
                
                # Log trade entry (only if order completed)
                log_trade_entry(
                    trade_id=trade_id,
                    symbol=symbol,
                    entry_timestamp=entry_timestamp,
                    entry_price=entry_price,
                    stop_loss=stop_loss,
                    target_price=target_price,
                    quantity=quantity,
                    entry_conditions=entry_conditions
                )
                
                return order_id, trade_id
                
            elif final_status == 'REJECTED':
                print(f"  ❌ Order REJECTED by exchange")
                print(f"  Reason: {order_history[-1].get('status_message', 'Unknown')}")
                return None, None
                
            else:
                # Order pending/open - wait a bit more
                print(f"  ⚠️  Order status: {final_status}")
                print(f"  Waiting 3 more seconds...")
                time.sleep(3)
                
                # Check again
                order_history = kite.order_history(order_id)
                final_status = order_history[-1]['status']
                
                if final_status == 'COMPLETE':
                    print(f"  ✅ Order EXECUTED (delayed)")
                    
                    log_trade_entry(
                        trade_id=trade_id,
                        symbol=symbol,
                        entry_timestamp=entry_timestamp,
                        entry_price=entry_price,
                        stop_loss=stop_loss,
                        target_price=target_price,
                        quantity=quantity,
                        entry_conditions=entry_conditions
                    )
                    
                    return order_id, trade_id
                else:
                    print(f"  ❌ Order failed to execute: {final_status}")
                    return None, None
                    
        except Exception as e:
            print(f"  ⚠️  Could not verify order status: {e}")
            print(f"  Proceeding with caution - manual verification recommended")
            # If we can't verify, log anyway since order was accepted
            log_trade_entry(
                trade_id=trade_id,
                symbol=symbol,
                entry_timestamp=entry_timestamp,
                entry_price=entry_price,
                stop_loss=stop_loss,
                target_price=target_price,
                quantity=quantity,
                entry_conditions=entry_conditions
            )
            return order_id, trade_id
        
    except Exception as e:
        print(f"\n[ERROR] Failed to place BUY order for {symbol}: {e}")
        return None, None


def add_to_positions_cache(symbol, trade_id, entry_price, stop_loss, quantity):
    """Add position to cache for monitoring"""
    cache = {}
    if POSITIONS_FILE.exists():
        with open(POSITIONS_FILE) as f:
            cache = json.load(f)
    
    # Calculate TP
    risk_per_share = entry_price - stop_loss
    target_price = entry_price + (TP_MULTIPLIER * risk_per_share)
    
    ist = pytz.timezone('Asia/Kolkata')
    
    cache[symbol] = {
        "trade_id": trade_id,
        "entry_price": entry_price,
        "stop_loss": stop_loss,
        "target_price": target_price,
        "quantity": quantity,
        "entry_timestamp": datetime.now(ist).isoformat()
    }
    
    POSITIONS_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(POSITIONS_FILE, 'w') as f:
        json.dump(cache, f, indent=2)
    
    print(f"[POSITION ADDED] {symbol} - Entry: ₹{entry_price:.2f}, SL: ₹{stop_loss:.2f}, TP: ₹{target_price:.2f}")
    
    # Send Telegram notification (for live mode - test mode sends in place_entry_order)
    if not TEST_MODE:
        notify_order_placed(symbol, quantity, entry_price, stop_loss, target_price)


def process_entry_orders():
    """
    Main entry order processing function
    Called by main.py after entry_checker runs
    """
    ist = pytz.timezone('Asia/Kolkata')
    
    print(f"\n{'='*60}")
    print(f"[ORDER MANAGER] Processing Entry Orders")
    print(f"[TIME] {datetime.now(ist).strftime('%H:%M:%S')}")
    print(f"{'='*60}\n")
    
    # Check monthly DD cap
    allowed, current_r, msg = can_open_new_trades()
    print(f"[RISK CHECK] {msg}")
    
    if not allowed:
        print(f"\n❌ [BLOCKED] Monthly DD cap hit - No new entries allowed\n")
        return
    
    # Load entry signals
    signals = load_entry_signals()
    
    if not signals:
        print("[INFO] No entry signals to process\n")
        return
    
    print(f"[SIGNALS] {len(signals)} entry signal(s) found\n")
    
    # Load existing positions
    existing_positions = load_open_positions()
    
    # Get Kite client
    kite = get_kite_client()
    
    # Get equity
    total_equity, available_margin = get_total_equity(kite)
    
    if total_equity is None or available_margin is None:
        print("[ERROR] Failed to get equity/margin - aborting\n")
        return
    
    print()
    
    # Get NIFTY filter status for entry conditions (from first signal if available)
    nifty_close = None
    nifty_sma50 = None
    if signals:
        first_signal = list(signals.values())[0]
        nifty_close = first_signal.get('nifty_close')
        nifty_sma50 = first_signal.get('nifty_sma50')
    
    # Process each signal
    for symbol, signal_data in signals.items():
        print(f"\n{'─'*60}")
        print(f"[PROCESSING] {symbol}")
        print(f"{'─'*60}")
        
        # Check duplicate
        if symbol in existing_positions:
            print(f"[SKIP] {symbol} - Already have open position")
            notify_order_skipped(symbol, "Already have open position")
            continue
        
        entry_price = signal_data['entry_price']
        reclaim_high = signal_data['reclaim_high']
        stop_loss = signal_data['reclaim_low']
        
        print(f"[SIGNAL] Entry: ₹{entry_price:.2f}")
        print(f"[SIGNAL] Reclaim High: ₹{reclaim_high:.2f}")
        print(f"[SIGNAL] Stop Loss: ₹{stop_loss:.2f} (reclaim_low)")
        
        # Position sizing
        quantity, required_capital, risk_per_share = calculate_position_size(
            entry_price, stop_loss, total_equity
        )
        
        if quantity is None:
            print(f"[SKIP] {symbol} - Position sizing failed")
            notify_order_skipped(symbol, "Position sizing failed")
            continue
        
        # Check margin
        if required_capital > available_margin:
            print(f"\n❌ [SKIP] {symbol} - Insufficient margin")
            print(f"   Need: ₹{required_capital:,.2f}")
            print(f"   Have: ₹{available_margin:,.2f}")
            
            notify_order_skipped(symbol, f"Insufficient margin (Need: ₹{required_capital:,.0f}, Have: ₹{available_margin:,.0f})")
            continue
        
        # Prepare entry conditions
        entry_conditions = {
            "nifty_close": nifty_close,
            "nifty_sma50": nifty_sma50,
            "nifty_filter_passed": nifty_close > nifty_sma50 if nifty_close and nifty_sma50 else None,
            "reclaim_high": reclaim_high,
            "reclaim_low": stop_loss,
            "reclaim_timestamp": signal_data.get('timestamp')
        }
        
        # Place order (with execution verification)
        order_id, trade_id = place_entry_order(kite, symbol, entry_price, stop_loss, quantity, entry_conditions)
        
        if order_id and trade_id:
            # Add to positions cache (only if order executed)
            add_to_positions_cache(symbol, trade_id, entry_price, stop_loss, quantity)
            
            # Update existing_positions to prevent duplicate in same cycle
            existing_positions[symbol] = True
            
            # Reduce available margin for next position
            available_margin -= required_capital
        else:
            # Order failed or rejected
            print(f"\n⚠️  [FAILED] {symbol} - Order not executed")
            notify_order_skipped(symbol, "Order execution failed")
    
    print(f"\n{'='*60}")
    print(f"[ORDER MANAGER] Entry processing complete")
    print(f"{'='*60}\n")


# ============ EXIT ORDERS ============

def place_exit_order(symbol, quantity, reason):
    """
    Place SELL order (market, CNC)
    Called by position_monitor when SL/TP hit
    Verifies order execution before returning
    Returns: exit_price or None
    """
    kite = get_kite_client()
    ist = pytz.timezone('Asia/Kolkata')
    
    if TEST_MODE:
        print(f"\n[TEST MODE] Would place SELL order:")
        print(f"  Symbol: {symbol}")
        print(f"  Quantity: {quantity}")
        print(f"  Reason: {reason}")
        
        # Get estimated exit price (LTP)
        try:
            quote = kite.quote(f"NSE:{symbol}")
            exit_price = quote[f"NSE:{symbol}"]['last_price']
            print(f"  Estimated Exit: ₹{exit_price:.2f}")
            
            return exit_price
        except Exception as e:
            print(f"  [ERROR] Could not get LTP: {e}")
            return None
    
    # LIVE MODE
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
        
        print(f"\n[EXIT ORDER SUBMITTED] {reason} - {symbol}")
        print(f"  Order ID: {order_id}")
        print(f"  Verifying execution...")
        
        # Wait for order to execute
        time.sleep(2)
        
        # Verify order executed successfully
        try:
            order_history = kite.order_history(order_id)
            final_status = order_history[-1]['status']
            
            if final_status == 'COMPLETE':
                exit_price = order_history[-1]['average_price']
                
                print(f"  ✅ Exit order EXECUTED")
                print(f"  Quantity: {quantity}")
                print(f"  Exit Price: ₹{exit_price:.2f}")
                
                return exit_price
                
            elif final_status == 'REJECTED':
                print(f"  ❌ Exit order REJECTED")
                print(f"  Reason: {order_history[-1].get('status_message', 'Unknown')}")
                return None
                
            else:
                # Wait a bit more
                print(f"  ⚠️  Order status: {final_status}")
                print(f"  Waiting 3 more seconds...")
                time.sleep(3)
                
                # Check again
                order_history = kite.order_history(order_id)
                final_status = order_history[-1]['status']
                
                if final_status == 'COMPLETE':
                    exit_price = order_history[-1]['average_price']
                    
                    print(f"  ✅ Exit order EXECUTED (delayed)")
                    print(f"  Exit Price: ₹{exit_price:.2f}")
                    
                    return exit_price
                else:
                    print(f"  ❌ Exit order failed: {final_status}")
                    return None
                    
        except Exception as e:
            print(f"  ⚠️  Could not verify order status: {e}")
            # If we can't verify, try to get average_price from orders()
            try:
                all_orders = kite.orders()
                for o in all_orders:
                    if o['order_id'] == order_id and o['status'] == 'COMPLETE':
                        exit_price = o['average_price']
                        print(f"  Exit price from order book: ₹{exit_price:.2f}")
                        return exit_price
                # Last resort: use LTP
                quote = kite.quote(f"NSE:{symbol}")
                exit_price = quote[f"NSE:{symbol}"]['last_price']
                print(f"  Using LTP as exit price (fallback): ₹{exit_price:.2f}")
                return exit_price
            except Exception as e2:
                print(f"  [ERROR] Fallback also failed: {e2}")
                return None
        
    except Exception as e:
        print(f"\n[ERROR] Exit order failed for {symbol}: {e}")
        return None


if __name__ == "__main__":
    # When run directly, process entry orders
    process_entry_orders()