"""
ROOT/main/log_manager.py

MONTHLY-FOCUSED logging system for CNC strategy
Optimized for 3-4 trades per month
"""

import json
from pathlib import Path
from datetime import datetime
import pytz
import shutil


ROOT = Path(__file__).resolve().parent.parent

# ============ LOG PATHS (MONTHLY STRUCTURE) ============
LOGS_ROOT = ROOT / "logs"

# Year-based structure: logs/2025/01_January/
def get_monthly_path():
    """Get current month's log directory"""
    ist = pytz.timezone('Asia/Kolkata')
    now = datetime.now(ist)
    year = now.strftime('%Y')
    month = now.strftime('%m_%B')  # 01_January, 02_February, etc.
    return LOGS_ROOT / year / month


# ============ TRADE ID GENERATION ============

def generate_trade_id(symbol, entry_timestamp):
    """
    Generate unique trade ID
    Format: TR_YYYYMMDD_SYMBOL_HHMMSS
    """
    dt = datetime.fromisoformat(entry_timestamp)
    trade_id = f"TR_{dt.strftime('%Y%m%d')}_{symbol}_{dt.strftime('%H%M%S')}"
    return trade_id


# ============ TRADE LOGGING (MONTHLY) ============

def log_trade_entry(trade_id, symbol, entry_timestamp, entry_price, stop_loss, 
                    target_price, quantity, entry_conditions=None):
    """
    Log trade entry to current month's file
    """
    month_path = get_monthly_path()
    month_path.mkdir(parents=True, exist_ok=True)
    
    trades_file = month_path / "trades.json"
    
    trade_record = {
        "trade_id": trade_id,
        "symbol": symbol,
        "entry_timestamp": entry_timestamp,
        "entry_price": entry_price,
        "stop_loss": stop_loss,
        "target_price": target_price,
        "quantity": quantity,
        "entry_conditions": entry_conditions or {},
        "status": "OPEN",
        # Exit fields (filled later)
        "exit_timestamp": None,
        "exit_price": None,
        "exit_reason": None,
        "bars_held": None,
        "pnl_per_share": None,
        "pnl_total": None,
        "r_value": None
    }
    
    # Load existing
    trades = []
    if trades_file.exists():
        with open(trades_file) as f:
            trades = json.load(f)
    
    # Add new
    trades.append(trade_record)
    
    # Save
    with open(trades_file, 'w') as f:
        json.dump(trades, f, indent=2)
    
    print(f"[TRADE LOG] Entry recorded: {trade_id} → {month_path.name}/trades.json")
    
    return trade_id


def log_trade_exit(trade_id, symbol, exit_timestamp, exit_price, exit_reason, bars_held):
    """
    Update trade record with exit details
    Searches current and previous months if needed
    """
    ist = pytz.timezone('Asia/Kolkata')
    exit_date = datetime.fromisoformat(exit_timestamp)
    
    # Try current month first
    month_path = get_monthly_path()
    trades_file = month_path / "trades.json"
    
    if not trades_file.exists():
        print(f"[ERROR] No trades file found for {month_path.name}")
        return 0
    
    with open(trades_file) as f:
        trades = json.load(f)
    
    # Find the trade
    trade_found = False
    for trade in trades:
        if trade_id and trade.get("trade_id") == trade_id:
            trade_found = True
        elif not trade_id and trade["symbol"] == symbol and trade["status"] == "OPEN":
            trade_found = True
        else:
            continue
        
        # Update with exit details
        entry_price = trade["entry_price"]
        stop_loss = trade["stop_loss"]
        quantity = trade["quantity"]
        
        # Calculate P&L and R
        pnl_per_share = exit_price - entry_price
        pnl_total = pnl_per_share * quantity
        
        risk_per_share = entry_price - stop_loss
        r_value = pnl_per_share / risk_per_share if risk_per_share > 0 else 0
        
        # Update record
        trade["exit_timestamp"] = exit_timestamp
        trade["exit_price"] = exit_price
        trade["exit_reason"] = exit_reason
        trade["bars_held"] = bars_held
        trade["pnl_per_share"] = round(pnl_per_share, 2)
        trade["pnl_total"] = round(pnl_total, 2)
        trade["r_value"] = round(r_value, 2)
        trade["status"] = "CLOSED"
        
        # Save updated trades
        with open(trades_file, 'w') as f:
            json.dump(trades, f, indent=2)
        
        print(f"[TRADE LOG] Exit recorded: {symbol} - {r_value:.2f}R → {month_path.name}/trades.json")
        
        # Update monthly summary
        update_monthly_summary(month_path)
        
        return r_value
    
    if not trade_found:
        print(f"[WARNING] Trade not found for exit: {symbol}")
        return 0


def get_open_trade_by_symbol(symbol):
    """Get open trade record by symbol from current month"""
    month_path = get_monthly_path()
    trades_file = month_path / "trades.json"
    
    if not trades_file.exists():
        return None
    
    with open(trades_file) as f:
        trades = json.load(f)
    
    for trade in trades:
        if trade["symbol"] == symbol and trade["status"] == "OPEN":
            return trade
    
    return None


# ============ MONTHLY SUMMARY ============

def update_monthly_summary(month_path=None):
    """
    Generate/update monthly summary
    Called after each trade exit
    """
    if month_path is None:
        month_path = get_monthly_path()
    
    trades_file = month_path / "trades.json"
    summary_file = month_path / "summary.json"
    
    if not trades_file.exists():
        return
    
    with open(trades_file) as f:
        trades = json.load(f)
    
    # Filter closed trades
    closed_trades = [t for t in trades if t["status"] == "CLOSED"]
    open_trades = [t for t in trades if t["status"] == "OPEN"]
    
    if not closed_trades:
        total_r = 0
        total_pnl = 0
        win_rate = 0
        expectancy = 0
        avg_bars_held = 0
    else:
        total_r = sum(t["r_value"] for t in closed_trades)
        total_pnl = sum(t["pnl_total"] for t in closed_trades)
        wins = len([t for t in closed_trades if t["r_value"] > 0])
        win_rate = (wins / len(closed_trades) * 100) if closed_trades else 0
        expectancy = sum(t["r_value"] for t in closed_trades) / len(closed_trades)
        avg_bars_held = sum(t["bars_held"] for t in closed_trades) / len(closed_trades)
    
    summary = {
        "month": month_path.name,
        "trades_closed": len(closed_trades),
        "trades_open": len(open_trades),
        "total_r": round(total_r, 2),
        "total_pnl": round(total_pnl, 2),
        "win_rate": round(win_rate, 1),
        "expectancy": round(expectancy, 3),
        "avg_bars_held": round(avg_bars_held, 1) if closed_trades else 0,
        "best_trade": max([t["r_value"] for t in closed_trades]) if closed_trades else 0,
        "worst_trade": min([t["r_value"] for t in closed_trades]) if closed_trades else 0,
        "updated": datetime.now(pytz.timezone('Asia/Kolkata')).isoformat()
    }
    
    with open(summary_file, 'w') as f:
        json.dump(summary, f, indent=2)
    
    print(f"\n{'='*60}")
    print(f"📊 MONTHLY SUMMARY - {month_path.name}")
    print(f"{'='*60}")
    print(f"Trades Closed: {len(closed_trades)}")
    print(f"Trades Open: {len(open_trades)}")
    print(f"Total R: {total_r:+.2f}R")
    print(f"Total P&L: ₹{total_pnl:+,.2f}")
    print(f"Win Rate: {win_rate:.1f}%")
    print(f"Expectancy: {expectancy:.3f}R")
    print(f"Avg Bars Held: {avg_bars_held:.1f}")
    print(f"{'='*60}\n")
    
    return summary


def get_current_month_stats():
    """
    Get current month statistics
    Returns: (total_r, trade_count, win_rate, closed_trades)
    """
    month_path = get_monthly_path()
    trades_file = month_path / "trades.json"
    
    if not trades_file.exists():
        return 0.0, 0, 0.0, []
    
    with open(trades_file) as f:
        trades = json.load(f)
    
    closed_trades = [t for t in trades if t["status"] == "CLOSED"]
    
    if not closed_trades:
        return 0.0, 0, 0.0, []
    
    total_r = sum(t["r_value"] for t in closed_trades)
    trade_count = len(closed_trades)
    wins = len([t for t in closed_trades if t["r_value"] > 0])
    win_rate = (wins / trade_count * 100) if trade_count > 0 else 0
    
    return total_r, trade_count, win_rate, closed_trades


# ============ YEAR SUMMARY ============

def generate_year_summary(year=None):
    """
    Generate summary for entire year
    Aggregates all monthly summaries
    """
    ist = pytz.timezone('Asia/Kolkata')
    if year is None:
        year = datetime.now(ist).strftime('%Y')
    
    year_path = LOGS_ROOT / year
    
    if not year_path.exists():
        print(f"[INFO] No data for year {year}")
        return
    
    # Collect all monthly summaries
    year_stats = {
        "year": year,
        "months": {},
        "total_trades": 0,
        "total_r": 0.0,
        "total_pnl": 0.0,
        "overall_win_rate": 0.0,
        "overall_expectancy": 0.0
    }
    
    all_closed_trades = []
    
    for month_dir in sorted(year_path.iterdir()):
        if not month_dir.is_dir():
            continue
        
        trades_file = month_dir / "trades.json"
        if not trades_file.exists():
            continue
        
        with open(trades_file) as f:
            trades = json.load(f)
        
        closed = [t for t in trades if t["status"] == "CLOSED"]
        all_closed_trades.extend(closed)
        
        if closed:
            month_r = sum(t["r_value"] for t in closed)
            month_pnl = sum(t["pnl_total"] for t in closed)
            
            year_stats["months"][month_dir.name] = {
                "trades": len(closed),
                "r": round(month_r, 2),
                "pnl": round(month_pnl, 2)
            }
    
    # Calculate overall stats
    if all_closed_trades:
        year_stats["total_trades"] = len(all_closed_trades)
        year_stats["total_r"] = round(sum(t["r_value"] for t in all_closed_trades), 2)
        year_stats["total_pnl"] = round(sum(t["pnl_total"] for t in all_closed_trades), 2)
        wins = len([t for t in all_closed_trades if t["r_value"] > 0])
        year_stats["overall_win_rate"] = round((wins / len(all_closed_trades) * 100), 1)
        year_stats["overall_expectancy"] = round(year_stats["total_r"] / len(all_closed_trades), 3)
    
    # Save year summary
    year_summary_file = year_path / f"year_{year}_summary.json"
    with open(year_summary_file, 'w') as f:
        json.dump(year_stats, f, indent=2)
    
    print(f"\n{'='*60}")
    print(f"📊 YEAR SUMMARY - {year}")
    print(f"{'='*60}")
    print(f"Total Trades: {year_stats['total_trades']}")
    print(f"Total R: {year_stats['total_r']:+.2f}R")
    print(f"Total P&L: ₹{year_stats['total_pnl']:+,.2f}")
    print(f"Win Rate: {year_stats['overall_win_rate']:.1f}%")
    print(f"Expectancy: {year_stats['overall_expectancy']:.3f}R")
    print(f"\nMonthly Breakdown:")
    for month, stats in year_stats["months"].items():
        print(f"  {month}: {stats['trades']} trades, {stats['r']:+.2f}R, ₹{stats['pnl']:+,.0f}")
    print(f"{'='*60}\n")
    
    return year_stats


if __name__ == "__main__":
    # Test
    print("Testing monthly-focused log_manager...")
    stats = get_current_month_stats()
    print(f"Current Month Stats: {stats}")