"""
ROOT/main/log_manager.py

MONTHLY-FOCUSED logging system for CNC strategy
Optimized for 3-4 trades per month

UPDATED: Added equity tracking via API calls
- Tracks equity before/after each trade
- Monthly start/end equity for return calculations
- Yearly summaries with equity curves
- Cash flow tracking for TWR calculations
"""

import sys
import json
from pathlib import Path
from datetime import datetime, timedelta, date
import pytz

ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT))

from kite_client import get_kite_client


# ============ LOG PATHS (MONTHLY STRUCTURE) ============
LOGS_ROOT = ROOT / "logs"
CASH_FLOWS_FILE = LOGS_ROOT / "cash_flows.json"


def get_monthly_path(date=None):
    """Get month's log directory for given date (or current)"""
    ist = pytz.timezone('Asia/Kolkata')
    if date is None:
        date = datetime.now(ist)
    year = date.strftime('%Y')
    month = date.strftime('%m_%B')  # 01_January, 02_February, etc.
    return LOGS_ROOT / year / month


def get_year_path(year=None):
    """Get year's log directory"""
    ist = pytz.timezone('Asia/Kolkata')
    if year is None:
        year = datetime.now(ist).strftime('%Y')
    return LOGS_ROOT / year


# ============ EQUITY TRACKING ============

def get_current_equity():
    """
    Fetch current equity from Kite API
    Returns: total_equity (margin + holdings value)
    """
    try:
        kite = get_kite_client()
        
        # Get available margin
        margins = kite.margins("equity")
        available_margin = margins['available']['live_balance']
        
        # Get holdings value
        holdings = kite.holdings()
        holdings_value = sum(h['quantity'] * h['last_price'] for h in holdings)
        
        total_equity = available_margin + holdings_value
        
        return round(total_equity, 2)
        
    except Exception as e:
        print(f"[ERROR] Failed to fetch equity: {e}")
        return None


# ============ CASH FLOW TRACKING ============

def load_cash_flows():
    """Load cash flows (withdrawals/deposits)"""
    if not CASH_FLOWS_FILE.exists():
        return []
    
    with open(CASH_FLOWS_FILE) as f:
        return json.load(f)


def add_cash_flow(amount, flow_type, note=""):
    """
    Add a cash flow entry (withdrawal or deposit)
    
    Args:
        amount: Positive number (absolute value)
        flow_type: "withdrawal" or "deposit"
        note: Optional description
    """
    ist = pytz.timezone('Asia/Kolkata')
    now = datetime.now(ist)
    
    cash_flows = load_cash_flows()
    
    entry = {
        "date": now.strftime('%Y-%m-%d'),
        "timestamp": now.isoformat(),
        "type": flow_type,
        "amount": abs(amount),
        "note": note
    }
    
    cash_flows.append(entry)
    
    # Ensure directory exists
    CASH_FLOWS_FILE.parent.mkdir(parents=True, exist_ok=True)
    
    with open(CASH_FLOWS_FILE, 'w') as f:
        json.dump(cash_flows, f, indent=2)
    
    print(f"[CASH FLOW] {flow_type.title()}: ₹{amount:,.2f} - {note}")
    
    return entry


def get_cash_flows_for_period(start_date, end_date):
    """Get cash flows between two dates"""
    cash_flows = load_cash_flows()
    
    period_flows = []
    for cf in cash_flows:
        cf_date = datetime.fromisoformat(cf['timestamp']).date()
        if start_date <= cf_date <= end_date:
            period_flows.append(cf)
    
    return period_flows


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
    Fetches and stores current equity before trade
    """
    month_path = get_monthly_path()
    month_path.mkdir(parents=True, exist_ok=True)
    
    trades_file = month_path / "trades.json"
    
    # Fetch current equity
    equity_before = get_current_equity()
    
    trade_record = {
        "trade_id": trade_id,
        "symbol": symbol,
        "entry_timestamp": entry_timestamp,
        "entry_price": entry_price,
        "stop_loss": stop_loss,
        "target_price": target_price,
        "quantity": quantity,
        "entry_conditions": entry_conditions or {},
        "equity_before_trade": equity_before,
        "status": "OPEN",
        # Exit fields (filled later)
        "exit_timestamp": None,
        "exit_price": None,
        "exit_reason": None,
        "bars_held": None,
        "pnl_per_share": None,
        "pnl_total": None,
        "r_value": None,
        "equity_after_trade": None,
        # Charges (manually filled after trade)
        "charges": None,
        "net_pnl": None
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
    print(f"[TRADE LOG] Equity before trade: ₹{equity_before:,.2f}" if equity_before else "[TRADE LOG] Equity: N/A")
    
    return trade_id


def log_trade_exit(trade_id, symbol, exit_timestamp, exit_price, exit_reason, bars_held):
    """
    Update trade record with exit details
    Fetches and stores equity after trade
    Auto-updates monthly and yearly summaries
    
    FIXED: Now searches previous months if trade not found in current month
    (handles cross-month trades: entered Jan, exited Feb)
    """
    ist = pytz.timezone('Asia/Kolkata')
    
    # Fetch current equity after exit
    equity_after = get_current_equity()
    
    # Search for the trade - try current month first, then previous months
    months_to_search = []
    
    # Current month
    current_month_path = get_monthly_path()
    months_to_search.append(current_month_path)
    
    # Previous month (in case trade spans month boundary)
    now = datetime.now(ist)
    if now.month == 1:
        prev_month = date(now.year - 1, 12, 1)
    else:
        prev_month = date(now.year, now.month - 1, 1)
    prev_month_path = get_monthly_path(datetime.combine(prev_month, datetime.min.time(), tzinfo=ist))
    if prev_month_path.exists() and prev_month_path != current_month_path:
        months_to_search.append(prev_month_path)
    
    # Search each month
    for month_path in months_to_search:
        trades_file = month_path / "trades.json"
        
        if not trades_file.exists():
            continue
        
        with open(trades_file) as f:
            trades = json.load(f)
        
        # Find the trade
        for trade in trades:
            if trade_id and trade.get("trade_id") == trade_id:
                pass  # Found by trade_id
            elif not trade_id and trade["symbol"] == symbol and trade["status"] == "OPEN":
                pass  # Found by symbol
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
            trade["equity_after_trade"] = equity_after
            
            # Save updated trades
            with open(trades_file, 'w') as f:
                json.dump(trades, f, indent=2)
            
            print(f"[TRADE LOG] Exit recorded: {symbol} - {r_value:.2f}R → {month_path.name}/trades.json")
            print(f"[TRADE LOG] Equity after trade: ₹{equity_after:,.2f}" if equity_after else "[TRADE LOG] Equity: N/A")
            
            # Update monthly summary for the month where trade was found
            update_monthly_summary(month_path)
            
            # Update yearly summary
            generate_year_summary()
            
            return r_value
    
    # Trade not found in any month
    print(f"[WARNING] Trade not found for exit: {symbol} (searched {len(months_to_search)} months)")
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


def update_trade_charges(trade_id, charges, month_path=None):
    """
    Manually update charges for a trade (from contract note)
    
    Args:
        trade_id: Trade ID to update (e.g., "TR_20260115_RELIANCE_111500")
        charges: Total charges (brokerage + STT + GST + stamp duty + others)
        month_path: Optional - specify month folder, otherwise searches current month
    
    Usage:
        from log_manager import update_trade_charges
        update_trade_charges("TR_20260115_RELIANCE_111500", 125.50)
    """
    ist = pytz.timezone('Asia/Kolkata')
    
    if month_path is None:
        month_path = get_monthly_path()
    
    trades_file = month_path / "trades.json"
    
    if not trades_file.exists():
        print(f"[ERROR] No trades file found for {month_path.name}")
        return False
    
    with open(trades_file) as f:
        trades = json.load(f)
    
    # Find and update the trade
    trade_found = False
    for trade in trades:
        if trade.get("trade_id") == trade_id:
            trade_found = True
            
            trade["charges"] = round(charges, 2)
            
            # Calculate net P&L if pnl_total exists
            if trade.get("pnl_total") is not None:
                trade["net_pnl"] = round(trade["pnl_total"] - charges, 2)
            
            # Save
            with open(trades_file, 'w') as f:
                json.dump(trades, f, indent=2)
            
            print(f"[CHARGES] Updated {trade_id}")
            print(f"  Gross P&L: ₹{trade.get('pnl_total', 0):,.2f}")
            print(f"  Charges: ₹{charges:,.2f}")
            print(f"  Net P&L: ₹{trade.get('net_pnl', 0):,.2f}")
            
            # Re-generate summaries
            update_monthly_summary(month_path)
            generate_year_summary()
            
            return True
    
    if not trade_found:
        print(f"[ERROR] Trade not found: {trade_id}")
        return False


def list_trades_without_charges(month_path=None):
    """
    List all closed trades that don't have charges filled in
    Useful for identifying which trades need contract note data
    """
    if month_path is None:
        month_path = get_monthly_path()
    
    trades_file = month_path / "trades.json"
    
    if not trades_file.exists():
        print(f"[INFO] No trades file for {month_path.name}")
        return []
    
    with open(trades_file) as f:
        trades = json.load(f)
    
    missing_charges = []
    for trade in trades:
        if trade["status"] == "CLOSED" and trade.get("charges") is None:
            missing_charges.append({
                "trade_id": trade["trade_id"],
                "symbol": trade["symbol"],
                "exit_date": trade.get("exit_timestamp", "")[:10],
                "pnl_total": trade.get("pnl_total", 0)
            })
    
    if missing_charges:
        print(f"\n[CHARGES] {len(missing_charges)} trades missing charges in {month_path.name}:")
        for t in missing_charges:
            print(f"  {t['trade_id']} | {t['symbol']} | {t['exit_date']} | ₹{t['pnl_total']:,.2f}")
    else:
        print(f"[CHARGES] All trades in {month_path.name} have charges filled")
    
    return missing_charges


# ============ MONTHLY SUMMARY ============

def update_monthly_summary(month_path=None):
    """
    Generate/update monthly summary with equity tracking
    Called after each trade exit
    """
    ist = pytz.timezone('Asia/Kolkata')
    
    if month_path is None:
        month_path = get_monthly_path()
    
    trades_file = month_path / "trades.json"
    summary_file = month_path / "summary.json"
    
    if not trades_file.exists():
        return
    
    with open(trades_file) as f:
        trades = json.load(f)
    
    # Filter closed and open trades
    closed_trades = [t for t in trades if t["status"] == "CLOSED"]
    open_trades = [t for t in trades if t["status"] == "OPEN"]
    all_trades = trades
    
    # Sort by timestamp
    all_trades_sorted = sorted(all_trades, key=lambda t: t["entry_timestamp"])
    closed_sorted = sorted(closed_trades, key=lambda t: t["exit_timestamp"])
    
    # Calculate basic stats
    if not closed_trades:
        total_r = 0
        total_pnl = 0
        total_charges = 0
        net_pnl = 0
        win_rate = 0
        expectancy = 0
        avg_bars_held = 0
        wins = 0
        losses = 0
    else:
        total_r = sum(t["r_value"] for t in closed_trades)
        total_pnl = sum(t["pnl_total"] for t in closed_trades)
        total_charges = sum(t.get("charges", 0) or 0 for t in closed_trades)
        net_pnl = total_pnl - total_charges
        wins = len([t for t in closed_trades if t["r_value"] > 0])
        losses = len([t for t in closed_trades if t["r_value"] <= 0])
        win_rate = (wins / len(closed_trades) * 100) if closed_trades else 0
        expectancy = total_r / len(closed_trades)
        avg_bars_held = sum(t["bars_held"] for t in closed_trades) / len(closed_trades)
    
    # Equity tracking
    starting_equity = None
    ending_equity = None
    
    # Starting equity: first trade's equity_before_trade
    if all_trades_sorted:
        starting_equity = all_trades_sorted[0].get("equity_before_trade")
    
    # Ending equity: last closed trade's equity_after_trade, or fetch current
    if closed_sorted:
        ending_equity = closed_sorted[-1].get("equity_after_trade")
    
    if ending_equity is None:
        ending_equity = get_current_equity()
    
    # Calculate returns
    raw_return_pct = None
    if starting_equity and ending_equity and starting_equity > 0:
        raw_return_pct = ((ending_equity - starting_equity) / starting_equity) * 100
    
    # Get cash flows for this month
    month_name = month_path.name  # e.g., "01_January"
    month_num = int(month_name.split('_')[0])
    year = int(month_path.parent.name)
    
    month_start = date(year, month_num, 1)
    if month_num == 12:
        month_end = date(year, 12, 31)
    else:
        month_end = date(year, month_num + 1, 1) - timedelta(days=1)
    
    cash_flows = get_cash_flows_for_period(month_start, month_end)
    total_withdrawals = sum(cf['amount'] for cf in cash_flows if cf['type'] == 'withdrawal')
    total_deposits = sum(cf['amount'] for cf in cash_flows if cf['type'] == 'deposit')
    
    # TWR-adjusted return (simplified - accounts for cash flows)
    adjusted_return_pct = None
    if starting_equity and ending_equity and starting_equity > 0:
        # Adjusted ending = ending + withdrawals - deposits
        adjusted_ending = ending_equity + total_withdrawals - total_deposits
        adjusted_return_pct = ((adjusted_ending - starting_equity) / starting_equity) * 100
    
    # Streaks
    max_consecutive_wins = 0
    max_consecutive_losses = 0
    current_wins = 0
    current_losses = 0
    
    for t in closed_sorted:
        if t["r_value"] > 0:
            current_wins += 1
            current_losses = 0
            max_consecutive_wins = max(max_consecutive_wins, current_wins)
        else:
            current_losses += 1
            current_wins = 0
            max_consecutive_losses = max(max_consecutive_losses, current_losses)
    
    # Build summary
    summary = {
        "month": month_path.name,
        "trades_closed": len(closed_trades),
        "trades_open": len(open_trades),
        "wins": wins,
        "losses": losses,
        "total_r": round(total_r, 2),
        "total_pnl": round(total_pnl, 2),
        "total_charges": round(total_charges, 2),
        "net_pnl": round(net_pnl, 2),
        "win_rate": round(win_rate, 1),
        "expectancy": round(expectancy, 3),
        "avg_bars_held": round(avg_bars_held, 1) if closed_trades else 0,
        "best_trade_r": max([t["r_value"] for t in closed_trades]) if closed_trades else 0,
        "worst_trade_r": min([t["r_value"] for t in closed_trades]) if closed_trades else 0,
        "best_trade_pnl": max([t["pnl_total"] for t in closed_trades]) if closed_trades else 0,
        "worst_trade_pnl": min([t["pnl_total"] for t in closed_trades]) if closed_trades else 0,
        "max_consecutive_wins": max_consecutive_wins,
        "max_consecutive_losses": max_consecutive_losses,
        # Equity tracking
        "starting_equity": starting_equity,
        "ending_equity": ending_equity,
        "withdrawals": total_withdrawals,
        "deposits": total_deposits,
        "raw_return_pct": round(raw_return_pct, 2) if raw_return_pct is not None else None,
        "adjusted_return_pct": round(adjusted_return_pct, 2) if adjusted_return_pct is not None else None,
        "updated": datetime.now(ist).isoformat()
    }
    
    with open(summary_file, 'w') as f:
        json.dump(summary, f, indent=2)
    
    print(f"\n{'='*60}")
    print(f"📊 MONTHLY SUMMARY - {month_path.name}")
    print(f"{'='*60}")
    print(f"Trades: {len(closed_trades)} closed, {len(open_trades)} open")
    print(f"W/L: {wins}/{losses}")
    print(f"Total R: {total_r:+.2f}R")
    print(f"Gross P&L: ₹{total_pnl:+,.2f}")
    if total_charges > 0:
        print(f"Charges: ₹{total_charges:,.2f}")
        print(f"Net P&L: ₹{net_pnl:+,.2f}")
    print(f"Win Rate: {win_rate:.1f}%")
    print(f"Expectancy: {expectancy:.3f}R")
    if starting_equity and ending_equity:
        print(f"Equity: ₹{starting_equity:,.0f} → ₹{ending_equity:,.0f}")
        if adjusted_return_pct is not None:
            print(f"Return: {adjusted_return_pct:+.2f}% (adjusted for cash flows)")
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
    Generate comprehensive yearly summary with equity tracking
    Auto-called after each trade exit
    """
    ist = pytz.timezone('Asia/Kolkata')
    if year is None:
        year = datetime.now(ist).strftime('%Y')
    
    year_path = LOGS_ROOT / year
    
    if not year_path.exists():
        print(f"[INFO] No data for year {year}")
        return None
    
    # Collect all trades from all months
    all_closed_trades = []
    monthly_data = {}
    
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
        
        # Load monthly summary if exists
        summary_file = month_dir / "summary.json"
        if summary_file.exists():
            with open(summary_file) as f:
                monthly_data[month_dir.name] = json.load(f)
    
    # Sort all trades by exit timestamp
    all_closed_trades.sort(key=lambda t: t.get("exit_timestamp", t["entry_timestamp"]))
    
    # Calculate overall stats
    if not all_closed_trades:
        print(f"[INFO] No closed trades for year {year}")
        return None
    
    total_trades = len(all_closed_trades)
    total_r = sum(t["r_value"] for t in all_closed_trades)
    total_pnl = sum(t["pnl_total"] for t in all_closed_trades)
    total_charges = sum(t.get("charges", 0) or 0 for t in all_closed_trades)
    net_pnl = total_pnl - total_charges
    wins = len([t for t in all_closed_trades if t["r_value"] > 0])
    losses = total_trades - wins
    win_rate = (wins / total_trades * 100) if total_trades > 0 else 0
    expectancy = total_r / total_trades if total_trades > 0 else 0
    
    # Equity tracking - from monthly summaries
    starting_equity = None
    ending_equity = None
    
    sorted_months = sorted(monthly_data.keys())
    if sorted_months:
        first_month = monthly_data[sorted_months[0]]
        last_month = monthly_data[sorted_months[-1]]
        starting_equity = first_month.get("starting_equity")
        ending_equity = last_month.get("ending_equity")
    
    if ending_equity is None:
        ending_equity = get_current_equity()
    
    # Calculate returns
    raw_return_pct = None
    if starting_equity and ending_equity and starting_equity > 0:
        raw_return_pct = ((ending_equity - starting_equity) / starting_equity) * 100
    
    # Cash flows for year
    from datetime import date, timedelta
    year_start = date(int(year), 1, 1)
    year_end = date(int(year), 12, 31)
    cash_flows = get_cash_flows_for_period(year_start, year_end)
    total_withdrawals = sum(cf['amount'] for cf in cash_flows if cf['type'] == 'withdrawal')
    total_deposits = sum(cf['amount'] for cf in cash_flows if cf['type'] == 'deposit')
    
    # TWR-adjusted return
    adjusted_return_pct = None
    if starting_equity and ending_equity and starting_equity > 0:
        adjusted_ending = ending_equity + total_withdrawals - total_deposits
        adjusted_return_pct = ((adjusted_ending - starting_equity) / starting_equity) * 100
    
    # Advanced stats
    r_values = [t["r_value"] for t in all_closed_trades]
    pnl_values = [t["pnl_total"] for t in all_closed_trades]
    
    # Payoff ratio
    winning_trades = [t for t in all_closed_trades if t["r_value"] > 0]
    losing_trades = [t for t in all_closed_trades if t["r_value"] <= 0]
    avg_win = sum(t["r_value"] for t in winning_trades) / len(winning_trades) if winning_trades else 0
    avg_loss = abs(sum(t["r_value"] for t in losing_trades) / len(losing_trades)) if losing_trades else 0
    payoff_ratio = avg_win / avg_loss if avg_loss > 0 else 0
    
    # Profit factor
    gross_profit = sum(t["pnl_total"] for t in winning_trades)
    gross_loss = abs(sum(t["pnl_total"] for t in losing_trades))
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else 0
    
    # Gain/Pain ratio
    gain_pain_ratio = gross_profit / gross_loss if gross_loss > 0 else 0
    
    # Streaks
    max_consecutive_wins = 0
    max_consecutive_losses = 0
    current_wins = 0
    current_losses = 0
    
    for t in all_closed_trades:
        if t["r_value"] > 0:
            current_wins += 1
            current_losses = 0
            max_consecutive_wins = max(max_consecutive_wins, current_wins)
        else:
            current_losses += 1
            current_wins = 0
            max_consecutive_losses = max(max_consecutive_losses, current_losses)
    
    # Drawdown calculation (equity-based)
    max_drawdown_pct = 0
    max_drawdown_r = 0
    peak_equity = starting_equity if starting_equity else 0
    peak_r = 0
    cumulative_r = 0
    
    equity_curve = []
    r_curve = []
    
    for t in all_closed_trades:
        # R-based drawdown
        cumulative_r += t["r_value"]
        r_curve.append(cumulative_r)
        if cumulative_r > peak_r:
            peak_r = cumulative_r
        drawdown_r = peak_r - cumulative_r
        max_drawdown_r = max(max_drawdown_r, drawdown_r)
        
        # Equity-based drawdown
        if t.get("equity_after_trade"):
            equity = t["equity_after_trade"]
            equity_curve.append(equity)
            if equity > peak_equity:
                peak_equity = equity
            if peak_equity > 0:
                drawdown_pct = ((peak_equity - equity) / peak_equity) * 100
                max_drawdown_pct = max(max_drawdown_pct, drawdown_pct)
    
    # Best/Worst stats
    best_trade_r = max(r_values) if r_values else 0
    worst_trade_r = min(r_values) if r_values else 0
    best_trade_pnl = max(pnl_values) if pnl_values else 0
    worst_trade_pnl = min(pnl_values) if pnl_values else 0
    
    # Monthly best/worst
    monthly_returns = {m: d.get("adjusted_return_pct") or d.get("raw_return_pct") or 0 
                       for m, d in monthly_data.items()}
    best_month = max(monthly_returns.items(), key=lambda x: x[1]) if monthly_returns else (None, 0)
    worst_month = min(monthly_returns.items(), key=lambda x: x[1]) if monthly_returns else (None, 0)
    
    # Win months
    win_months = len([m for m, r in monthly_returns.items() if r > 0])
    
    # Daily P&L aggregation for best/worst day
    daily_pnl = {}
    for t in all_closed_trades:
        exit_date = t.get("exit_timestamp", "")[:10]  # YYYY-MM-DD
        if exit_date:
            daily_pnl[exit_date] = daily_pnl.get(exit_date, 0) + t["pnl_total"]
    
    best_day = max(daily_pnl.items(), key=lambda x: x[1]) if daily_pnl else (None, 0)
    worst_day = min(daily_pnl.items(), key=lambda x: x[1]) if daily_pnl else (None, 0)
    win_days = len([d for d, pnl in daily_pnl.items() if pnl > 0])
    
    # Time in market
    first_trade_date = all_closed_trades[0].get("entry_timestamp", "")[:10] if all_closed_trades else None
    last_trade_date = all_closed_trades[-1].get("exit_timestamp", "")[:10] if all_closed_trades else None
    
    # Avg bars held
    avg_bars_held = sum(t.get("bars_held", 0) for t in all_closed_trades) / total_trades if total_trades > 0 else 0
    
    # CAGR (if we have dates)
    cagr = None
    if first_trade_date and last_trade_date and starting_equity and ending_equity:
        from datetime import datetime as dt
        start = dt.fromisoformat(first_trade_date)
        end = dt.fromisoformat(last_trade_date)
        years = (end - start).days / 365.25
        if years > 0 and starting_equity > 0:
            adjusted_ending = ending_equity + total_withdrawals - total_deposits
            cagr = ((adjusted_ending / starting_equity) ** (1 / years) - 1) * 100
    
    # Sharpe (trade-based, simplified)
    import statistics
    sharpe = None
    if len(r_values) > 1:
        avg_r = statistics.mean(r_values)
        std_r = statistics.stdev(r_values)
        if std_r > 0:
            sharpe = avg_r / std_r
    
    # Sortino (only downside deviation)
    sortino = None
    negative_r = [r for r in r_values if r < 0]
    if negative_r and len(negative_r) > 1:
        avg_r = statistics.mean(r_values)
        downside_std = statistics.stdev(negative_r)
        if downside_std > 0:
            sortino = avg_r / downside_std
    
    # Calmar ratio
    calmar = None
    if cagr and max_drawdown_pct > 0:
        calmar = cagr / max_drawdown_pct
    
    # Recovery factor
    recovery_factor = None
    if max_drawdown_pct > 0 and adjusted_return_pct:
        recovery_factor = adjusted_return_pct / max_drawdown_pct
    
    # Kelly criterion
    kelly = None
    if payoff_ratio > 0:
        win_prob = win_rate / 100
        loss_prob = 1 - win_prob
        kelly = (win_prob - (loss_prob / payoff_ratio)) * 100
    
    # Expected monthly
    expected_monthly = adjusted_return_pct / len(sorted_months) if sorted_months and adjusted_return_pct else None
    
    # Skew and Kurtosis
    skew = None
    kurtosis = None
    if len(r_values) > 2:
        n = len(r_values)
        mean_r = statistics.mean(r_values)
        std_r = statistics.stdev(r_values)
        if std_r > 0:
            skew = sum((r - mean_r) ** 3 for r in r_values) / (n * std_r ** 3)
            kurtosis = sum((r - mean_r) ** 4 for r in r_values) / (n * std_r ** 4) - 3
    
    # Build summary
    year_stats = {
        "year": year,
        "updated": datetime.now(ist).isoformat(),
        
        # Trade counts
        "total_trades": total_trades,
        "wins": wins,
        "losses": losses,
        "win_rate": round(win_rate, 1),
        
        # R stats
        "total_r": round(total_r, 2),
        "expectancy": round(expectancy, 3),
        "best_trade_r": round(best_trade_r, 2),
        "worst_trade_r": round(worst_trade_r, 2),
        "max_drawdown_r": round(max_drawdown_r, 2),
        
        # P&L stats
        "total_pnl": round(total_pnl, 2),
        "total_charges": round(total_charges, 2),
        "net_pnl": round(net_pnl, 2),
        "best_trade_pnl": round(best_trade_pnl, 2),
        "worst_trade_pnl": round(worst_trade_pnl, 2),
        "gross_profit": round(gross_profit, 2),
        "gross_loss": round(gross_loss, 2),
        
        # Equity
        "starting_equity": starting_equity,
        "ending_equity": ending_equity,
        "withdrawals": total_withdrawals,
        "deposits": total_deposits,
        "raw_return_pct": round(raw_return_pct, 2) if raw_return_pct else None,
        "adjusted_return_pct": round(adjusted_return_pct, 2) if adjusted_return_pct else None,
        "max_drawdown_pct": round(max_drawdown_pct, 2),
        "cagr": round(cagr, 2) if cagr else None,
        
        # Ratios
        "sharpe": round(sharpe, 2) if sharpe else None,
        "sortino": round(sortino, 2) if sortino else None,
        "calmar": round(calmar, 2) if calmar else None,
        "profit_factor": round(profit_factor, 2),
        "payoff_ratio": round(payoff_ratio, 2),
        "gain_pain_ratio": round(gain_pain_ratio, 2),
        "recovery_factor": round(recovery_factor, 2) if recovery_factor else None,
        "kelly_criterion": round(kelly, 2) if kelly else None,
        
        # Distribution
        "skew": round(skew, 2) if skew else None,
        "kurtosis": round(kurtosis, 2) if kurtosis else None,
        
        # Streaks
        "max_consecutive_wins": max_consecutive_wins,
        "max_consecutive_losses": max_consecutive_losses,
        
        # Time
        "avg_bars_held": round(avg_bars_held, 1),
        "first_trade_date": first_trade_date,
        "last_trade_date": last_trade_date,
        "expected_monthly": round(expected_monthly, 2) if expected_monthly else None,
        
        # Best/Worst
        "best_day": {"date": best_day[0], "pnl": round(best_day[1], 2)} if best_day[0] else None,
        "worst_day": {"date": worst_day[0], "pnl": round(worst_day[1], 2)} if worst_day[0] else None,
        "best_month": {"month": best_month[0], "return_pct": round(best_month[1], 2)} if best_month[0] else None,
        "worst_month": {"month": worst_month[0], "return_pct": round(worst_month[1], 2)} if worst_month[0] else None,
        "win_days": win_days,
        "win_months": win_months,
        
        # Monthly breakdown
        "months": {m: {
            "trades": d.get("trades_closed", 0),
            "r": d.get("total_r", 0),
            "pnl": d.get("total_pnl", 0),
            "charges": d.get("total_charges", 0),
            "net_pnl": d.get("net_pnl", 0),
            "return_pct": d.get("adjusted_return_pct") or d.get("raw_return_pct"),
            "starting_equity": d.get("starting_equity"),
            "ending_equity": d.get("ending_equity")
        } for m, d in monthly_data.items()},
        
        # Curves (for plotting)
        "equity_curve": equity_curve,
        "r_curve": r_curve
    }
    
    # Save year summary
    year_summary_file = year_path / f"year_{year}_summary.json"
    with open(year_summary_file, 'w') as f:
        json.dump(year_stats, f, indent=2)
    
    print(f"\n{'='*60}")
    print(f"📊 YEAR SUMMARY - {year}")
    print(f"{'='*60}")
    print(f"Trades: {total_trades} (W:{wins}/L:{losses})")
    print(f"Win Rate: {win_rate:.1f}%")
    print(f"Total R: {total_r:+.2f}R | Expectancy: {expectancy:.3f}R")
    print(f"Gross P&L: ₹{total_pnl:+,.2f}")
    if total_charges > 0:
        print(f"Charges: ₹{total_charges:,.2f}")
        print(f"Net P&L: ₹{net_pnl:+,.2f}")
    if starting_equity and ending_equity:
        print(f"Equity: ₹{starting_equity:,.0f} → ₹{ending_equity:,.0f}")
    if adjusted_return_pct:
        print(f"Return: {adjusted_return_pct:+.2f}% (TWR adjusted)")
    if cagr:
        print(f"CAGR: {cagr:.2f}%")
    print(f"Max DD: {max_drawdown_pct:.2f}% | {max_drawdown_r:.2f}R")
    if sharpe:
        print(f"Sharpe: {sharpe:.2f} | Sortino: {sortino:.2f}" if sortino else f"Sharpe: {sharpe:.2f}")
    print(f"Profit Factor: {profit_factor:.2f} | Payoff: {payoff_ratio:.2f}")
    print(f"{'='*60}\n")
    
    return year_stats


if __name__ == "__main__":
    # Test
    print("Testing log_manager with equity tracking...")
    
    # Test equity fetch
    equity = get_current_equity()
    print(f"Current Equity: ₹{equity:,.2f}" if equity else "Could not fetch equity")
    
    # Test current month stats
    stats = get_current_month_stats()
    print(f"Current Month Stats: R={stats[0]:.2f}, Trades={stats[1]}, WinRate={stats[2]:.1f}%")
    
    # Generate year summary
    generate_year_summary()