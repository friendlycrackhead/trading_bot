"""
ROOT/main/order_manager.py

Unified order manager - handles ALL order placement (entry + exit)
Entry: Called by main after entry_checker generates signals
Exit: Called by position_monitor when SL/TP hit
"""

import sys
import json
from pathlib import Path
from datetime import datetime
import pytz

ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT))

from kite_client import get_kite_client
from risk_manager import can_open_new_trades, log_trade_exit
from telegram_notifier import notify_order_placed, notify_order_skipped


# ============ CONFIG ============
SIGNALS_INPUT = ROOT / "main" / "entry_signals.json"
EXECUTION_LOG = ROOT / "logs" / "orders" / "execution_log.json"
POSITIONS_FILE = ROOT / "main" / "open_positions.json"

RISK_PERCENT = 0.01  # 1% risk per trade
TP_MULTIPLIER = 3.0  # 3R target
TEST_MODE = True  # Set to False for live trading


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


def place_entry_order(kite, symbol, entry_price, stop_loss, quantity):
    """
    Place BUY order (market, CNC)
    Returns: order_id or None
    """
    ist = pytz.timezone('Asia/Kolkata')
    
    if TEST_MODE:
        print(f"\n[TEST MODE] Would place BUY order:")
        print(f"  Symbol: {symbol}")
        print(f"  Quantity: {quantity}")
        print(f"  Entry: ₹{entry_price:.2f}")
        print(f"  Stop Loss: ₹{stop_loss:.2f}")
        
        order_id = f"TEST_BUY_{datetime.now(ist).strftime('%H%M%S')}"
        
        # Log to execution log
        log_execution({
            "symbol": symbol,
            "status": "TEST_EXECUTED",
            "quantity": quantity,
            "entry_price": entry_price,
            "stop_loss": stop_loss,
            "required_capital": entry_price * quantity,
            "order_ids": {"buy": order_id},
            "timestamp": datetime.now(ist).isoformat()
        })
        
        return order_id
    
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
        
        print(f"\n[ORDER PLACED] BUY {symbol}")
        print(f"  Order ID: {order_id}")
        print(f"  Quantity: {quantity}")
        print(f"  Entry: ₹{entry_price:.2f}")
        print(f"  Stop Loss: ₹{stop_loss:.2f}")
        
        # Log to execution log
        log_execution({
            "symbol": symbol,
            "status": "EXECUTED",
            "quantity": quantity,
            "entry_price": entry_price,
            "stop_loss": stop_loss,
            "order_id": order_id,
            "timestamp": datetime.now(ist).isoformat()
        })
        
        return order_id
        
    except Exception as e:
        print(f"\n[ERROR] Failed to place BUY order for {symbol}: {e}")
        
        log_execution({
            "symbol": symbol,
            "status": "FAILED",
            "error": str(e),
            "timestamp": datetime.now(ist).isoformat()
        })
        
        return None


def add_to_positions_cache(symbol, entry_price, stop_loss, quantity):
    """Add position to cache for monitoring"""
    cache = {}
    if POSITIONS_FILE.exists():
        with open(POSITIONS_FILE) as f:
            cache = json.load(f)
    
    # Calculate TP = entry + 3R
    risk_per_share = entry_price - stop_loss
    target_price = entry_price + (TP_MULTIPLIER * risk_per_share)
    
    ist = pytz.timezone('Asia/Kolkata')
    
    cache[symbol] = {
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
    
    # Send Telegram notification
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
    
    # Check monthly DD cap once at start
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
    
    # Load existing positions to check for duplicates
    existing_positions = load_open_positions()
    
    # Get Kite client
    kite = get_kite_client()
    
    # Get equity and margin (API returns current state including deployed capital)
    total_equity, available_margin = get_total_equity(kite)
    
    if total_equity is None or available_margin is None:
        print("[ERROR] Failed to get equity/margin - aborting\n")
        return
    
    print()
    
    # Process each signal
    for symbol, signal_data in signals.items():
        print(f"\n{'─'*60}")
        print(f"[PROCESSING] {symbol}")
        print(f"{'─'*60}")
        
        # Check if already have position in this symbol
        if symbol in existing_positions:
                    print(f"[SKIP] {symbol} - Already have open position")
                    log_execution({
                        "symbol": symbol,
                        "status": "SKIPPED",
                        "reason": "Already have open position",
                        "timestamp": datetime.now(ist).isoformat()
                    })
                    notify_order_skipped(symbol, "Already have open position")
                    continue
        
        entry_price = signal_data['entry_price']
        reclaim_high = signal_data['reclaim_high']
        stop_loss = signal_data['reclaim_low']  # Use actual reclaim_low from signal
        
        print(f"[SIGNAL] Entry: ₹{entry_price:.2f}")
        print(f"[SIGNAL] Reclaim High: ₹{reclaim_high:.2f}")
        print(f"[SIGNAL] Stop Loss: ₹{stop_loss:.2f} (reclaim_low)")
        
        # Calculate position size
        quantity, required_capital, risk_per_share = calculate_position_size(
            entry_price, stop_loss, total_equity
        )
        
        if quantity is None:
            print(f"[SKIP] {symbol} - Position sizing failed")
            continue
        
        # Check if enough margin
        if required_capital > available_margin:
                    print(f"\n❌ [SKIP] {symbol} - Insufficient margin")
                    print(f"   Need: ₹{required_capital:,.2f}")
                    print(f"   Have: ₹{available_margin:,.2f}")
                    
                    log_execution({
                        "symbol": symbol,
                        "status": "SKIPPED",
                        "reason": f"Insufficient margin (need ₹{required_capital:,.2f})",
                        "timestamp": datetime.now(ist).isoformat()
                    })
                    notify_order_skipped(symbol, f"Insufficient margin (Need: ₹{required_capital:,.0f}, Have: ₹{available_margin:,.0f})")
                    continue
        
        # Place order
        order_id = place_entry_order(kite, symbol, entry_price, stop_loss, quantity)
        
        if order_id:
            # Add to positions cache for monitoring
            add_to_positions_cache(symbol, entry_price, stop_loss, quantity)
            
            # Add to existing_positions dict to prevent duplicate in same cycle
            existing_positions[symbol] = True
    
    print(f"\n{'='*60}")
    print(f"[ORDER MANAGER] Entry processing complete")
    print(f"{'='*60}\n")


# ============ EXIT ORDERS ============

def place_exit_order(symbol, quantity, reason):
    """
    Place SELL order (market, CNC)
    Called by position_monitor when SL/TP hit
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
        except:
            return None
    
    # LIVE MODE - Place actual order
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
        
        print(f"\n[EXIT ORDER] {reason} - {symbol}")
        print(f"  Order ID: {order_id}")
        print(f"  Quantity: {quantity}")
        
        # Get execution price (use LTP as approximation)
        quote = kite.quote(f"NSE:{symbol}")
        exit_price = quote[f"NSE:{symbol}"]['last_price']
        
        print(f"  Exit Price: ₹{exit_price:.2f}")
        
        return exit_price
        
    except Exception as e:
        print(f"\n[ERROR] Exit order failed for {symbol}: {e}")
        return None


# ============ LOGGING ============

def log_execution(execution_data):
    """Append execution to log file"""
    EXECUTION_LOG.parent.mkdir(parents=True, exist_ok=True)
    
    log = []
    if EXECUTION_LOG.exists():
        with open(EXECUTION_LOG) as f:
            log = json.load(f)
    
    log.append(execution_data)
    
    with open(EXECUTION_LOG, 'w') as f:
        json.dump(log, f, indent=2)


# ============ MAIN ============

if __name__ == "__main__":
    # When run directly, process entry orders
    process_entry_orders()