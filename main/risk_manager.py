"""
ROOT/main/risk_manager.py

Tracks monthly R and enforces -5R drawdown cap
"""

import json
from pathlib import Path
from datetime import datetime
import pytz


ROOT = Path(__file__).resolve().parent.parent
MONTHLY_LOG = ROOT / "main" / "monthly_pnl.json"

MONTHLY_DD_CAP = -5.0  # -5R monthly stop


def load_monthly_log():
    """Load monthly R log"""
    if not MONTHLY_LOG.exists():
        return []
    
    with open(MONTHLY_LOG) as f:
        return json.load(f)


def save_monthly_log(log):
    """Save monthly R log"""
    MONTHLY_LOG.parent.mkdir(exist_ok=True)
    with open(MONTHLY_LOG, 'w') as f:
        json.dump(log, f, indent=2)


def get_current_month_r():
    """
    Calculate cumulative R for current calendar month
    Returns: (current_r, trade_count)
    """
    log = load_monthly_log()
    
    ist = pytz.timezone('Asia/Kolkata')
    current_month = datetime.now(ist).strftime('%Y-%m')
    
    current_r = 0.0
    trade_count = 0
    
    for trade in log:
        trade_month = trade['exit_timestamp'][:7]  # Extract YYYY-MM
        
        if trade_month == current_month:
            current_r += trade['r_value']
            trade_count += 1
    
    return current_r, trade_count


def can_open_new_trades():
    """
    Check if new trades are allowed based on monthly R
    This check happens BEFORE entry, but we also check AFTER each exit
    Returns: (allowed, current_r, message)
    """
    current_r, trade_count = get_current_month_r()
    
    # Block if already at or below cap
    if current_r <= MONTHLY_DD_CAP:
        return False, current_r, f"Monthly DD cap hit: {current_r:.2f}R (limit: {MONTHLY_DD_CAP}R)"
    
    return True, current_r, f"Trading allowed: {current_r:.2f}R ({trade_count} trades this month)"


def log_trade_exit(symbol, entry_price, exit_price, stop_loss, quantity, exit_timestamp=None):
    """
    Log a closed trade and calculate R value
    Also checks if monthly DD cap is now breached after this exit
    
    Args:
        symbol: Stock symbol
        entry_price: Entry price
        exit_price: Exit price  
        stop_loss: SL price
        quantity: Shares traded
        exit_timestamp: ISO timestamp (defaults to now)
    
    Returns: r_value
    """
    if exit_timestamp is None:
        ist = pytz.timezone('Asia/Kolkata')
        exit_timestamp = datetime.now(ist).isoformat()
    
    # Calculate R
    risk_per_share = entry_price - stop_loss
    pnl_per_share = exit_price - entry_price
    
    if risk_per_share <= 0:
        print(f"[ERROR] Invalid risk calculation for {symbol}")
        return 0
    
    r_value = pnl_per_share / risk_per_share
    
    # Load existing log
    log = load_monthly_log()
    
    # Add trade
    trade_record = {
        "symbol": symbol,
        "entry_price": entry_price,
        "exit_price": exit_price,
        "stop_loss": stop_loss,
        "quantity": quantity,
        "pnl_per_share": pnl_per_share,
        "r_value": round(r_value, 2),
        "exit_timestamp": exit_timestamp
    }
    
    log.append(trade_record)
    
    # Save
    save_monthly_log(log)
    
    print(f"[R-LOG] {symbol}: {r_value:.2f}R (Exit: ₹{exit_price}, Entry: ₹{entry_price}, SL: ₹{stop_loss})")
    
    # Check if monthly cap breached AFTER this exit
    current_r, _ = get_current_month_r()
    
    if current_r <= MONTHLY_DD_CAP:
        print(f"\n{'!'*60}")
        print(f"[ALERT] MONTHLY DD CAP BREACHED: {current_r:.2f}R")
        print(f"[ALERT] No new trades will be allowed this month")
        print(f"[ALERT] Existing positions can still be managed")
        print(f"{'!'*60}\n")
    
    return r_value


def reset_monthly_log_if_new_month():
    """
    Archive old month's data and reset for new month
    Called at start of each trading day
    """
    log = load_monthly_log()
    
    if not log:
        return
    
    ist = pytz.timezone('Asia/Kolkata')
    current_month = datetime.now(ist).strftime('%Y-%m')
    
    # Check if any trades from previous months exist
    archive_needed = False
    for trade in log:
        trade_month = trade['exit_timestamp'][:7]
        if trade_month != current_month:
            archive_needed = True
            break
    
    if archive_needed:
        # Archive old data
        archive_path = ROOT / "main" / f"monthly_pnl_archive_{datetime.now(ist).strftime('%Y%m')}.json"
        
        current_month_trades = [t for t in log if t['exit_timestamp'][:7] == current_month]
        old_trades = [t for t in log if t['exit_timestamp'][:7] != current_month]
        
        # Save archive
        if old_trades:
            with open(archive_path, 'w') as f:
                json.dump(old_trades, f, indent=2)
            print(f"[ARCHIVE] Saved {len(old_trades)} old trades → {archive_path}")
        
        # Keep only current month
        save_monthly_log(current_month_trades)
        print(f"[RESET] Monthly log reset - kept {len(current_month_trades)} current month trades")


if __name__ == "__main__":
    # Test
    reset_monthly_log_if_new_month()
    
    allowed, current_r, msg = can_open_new_trades()
    print(f"\n{msg}")
    print(f"Trading Allowed: {allowed}")