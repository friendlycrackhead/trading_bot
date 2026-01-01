"""
ROOT/main/main.py

Main orchestrator for VWAP Reclaim Trading Bot
Run manually at 9:00-9:15 AM - handles all hourly execution
Entry checks at XX:15 with NIFTY filter check

MONTHLY-FOCUSED: No daily archiving, monthly logs only

FIXED:
- Removed 15:29:57 entry time (too late, market closes at 15:30)
- Fixed trade counting to use open_positions.json instead of entry_signals.json
- Added proper exception handling
- Skip order_manager if no entry signals (avoid redundant calls)
"""

import sys
import time
from pathlib import Path
from datetime import datetime, time as dt_time
import subprocess
import json
import pytz

ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT))
sys.path.insert(0, str(ROOT / "main"))

# Import at top to catch errors early
from position_monitor import monitor_positions
from telegram_notifier import notify_startup, notify_market_close, notify_bot_stopped


# ============ TIMING CONFIG ============
SCANNER_TIMES = [
    dt_time(10, 16, 0),  # First valid scanner (checks 9:15-10:15 candle)
    dt_time(11, 16, 0),
    dt_time(12, 16, 0),
    dt_time(13, 16, 0),
    dt_time(14, 16, 0),
    dt_time(15, 16, 0),  # Last scanner (checks 2:15-3:15 candle)
]

ENTRY_ORDER_TIMES = [
    dt_time(11, 15, 0),  # First valid entry (uses 10:16 watchlist)
    dt_time(12, 15, 0),
    dt_time(13, 15, 0),
    dt_time(14, 15, 0),
    dt_time(15, 15, 0),  
    dt_time(15, 29, 55), # Last valid entry (uses 15:16 watchlist)
]

MARKET_CLOSE = dt_time(15, 30, 0)
POSITION_CHECK_INTERVAL = 1  # Check positions every 1 second
STATUS_UPDATE_INTERVAL = 600  # Status update every 10 minutes

# ============ MARKET HOLIDAYS ============
MARKET_HOLIDAYS_2026 = {
    "2026-01-26",  # Republic Day
    "2026-03-03",  # Holi
    "2026-03-26",  # Shri Ram Navami
    "2026-03-31",  # Shri Mahavir Jayanti
    "2026-04-03",  # Good Friday
    "2026-04-14",  # Dr. Baba Saheb Ambedkar Jayanti
    "2026-05-01",  # Maharashtra Day
    "2026-05-28",  # Bakri Id
    "2026-06-26",  # Muharram
    "2026-09-14",  # Ganesh Chaturthi
    "2026-10-02",  # Mahatma Gandhi Jayanti
    "2026-10-20",  # Dussehra
    "2026-11-09",  # Diwali-Laxmi Pujan
    "2026-11-10",  # Diwali-Balipratipada
    "2026-11-23",  # Gurunanak Jayanti
}


def run_script(script_name):
    """Execute a Python script and return success status"""
    script_path = ROOT / "main" / script_name

    print(f"\n{'='*60}")
    print(f"[RUN] {script_name} at {datetime.now().strftime('%H:%M:%S')}")
    print(f"{'='*60}\n")

    try:
        result = subprocess.run(
            [sys.executable, str(script_path)],
            cwd=ROOT,
            capture_output=False,
            check=True,
        )
        print(f"\n✅ [{script_name}] Completed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"\n❌ [{script_name}] Failed with error: {e}")
        return False


def calculate_time_remaining(target_time):
    """Calculate seconds until target time"""
    now = datetime.now().time()
    now_seconds = now.hour * 3600 + now.minute * 60 + now.second
    target_seconds = (
        target_time.hour * 3600 + target_time.minute * 60 + target_time.second
    )

    remaining = target_seconds - now_seconds
    return remaining if remaining > 0 else 0


def format_time_remaining(seconds):
    """Format seconds into readable string"""
    if seconds >= 3600:
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        return f"{hours}h {minutes}m"
    elif seconds >= 60:
        minutes = seconds // 60
        secs = seconds % 60
        return f"{minutes}m {secs}s"
    else:
        return f"{seconds}s"


def clear_entry_signals():
    """Clear entry_signals.json after processing to prevent duplicate entries"""
    signals_file = ROOT / "main" / "entry_signals.json"
    try:
        with open(signals_file, 'w') as f:
            json.dump({}, f)
    except Exception as e:
        print(f"[WARNING] Could not clear entry signals: {e}")


def get_open_positions_count():
    """Get count of currently open positions from cache"""
    positions_file = ROOT / "main" / "open_positions.json"
    try:
        if positions_file.exists():
            with open(positions_file) as f:
                positions = json.load(f)
                return len(positions)
    except Exception as e:
        print(f"[WARNING] Could not read positions: {e}")
    return 0


def main():
    """Main execution loop"""

    print(f"\n{'#'*60}")
    print(f"# VWAP RECLAIM TRADING BOT - CNC STRATEGY")
    print(f"# Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"# Strategy: VWAP Reclaim | Risk: 1% | TP: 3R | SL: Reclaim Low")
    print(f"# Filter: NIFTY Hourly SMA50 (checked at XX:15)")
    print(f"# Logs: Monthly (Year > Month structure)")
    print(f"{'#'*60}\n")

    notify_startup()

    # CHECK IF MARKET IS OPEN
    ist = pytz.timezone('Asia/Kolkata')
    now = datetime.now(ist)
    weekday = now.weekday()  # Monday=0, Sunday=6
    today_str = now.strftime('%Y-%m-%d')
    day_name = now.strftime('%A')
    
    # Check weekend
    if weekday >= 5:  # Saturday=5, Sunday=6
        print(f"\n{'!'*60}")
        print(f"[WEEKEND] Today is {day_name}")
        print(f"[WEEKEND] Market is closed - Bot will not run")
        print(f"{'!'*60}\n")
        
        notify_bot_stopped(f"Weekend ({day_name}) - Market closed")
        return
    
    # Check holiday
    if today_str in MARKET_HOLIDAYS_2026:
        holiday_names = {
            "2026-01-26": "Republic Day",
            "2026-03-03": "Holi",
            "2026-03-26": "Shri Ram Navami",
            "2026-03-31": "Shri Mahavir Jayanti",
            "2026-04-03": "Good Friday",
            "2026-04-14": "Dr. Baba Saheb Ambedkar Jayanti",
            "2026-05-01": "Maharashtra Day",
            "2026-05-28": "Bakri Id",
            "2026-06-26": "Muharram",
            "2026-09-14": "Ganesh Chaturthi",
            "2026-10-02": "Mahatma Gandhi Jayanti",
            "2026-10-20": "Dussehra",
            "2026-11-09": "Diwali-Laxmi Pujan",
            "2026-11-10": "Diwali-Balipratipada",
            "2026-11-23": "Gurunanak Jayanti",
        }
        
        holiday_name = holiday_names.get(today_str, "Market Holiday")
        
        print(f"\n{'!'*60}")
        print(f"[HOLIDAY] Today is {holiday_name}")
        print(f"[HOLIDAY] Market is closed - Bot will not run")
        print(f"{'!'*60}\n")
        
        notify_bot_stopped(f"{holiday_name} - Market closed")
        return

    # Track which times we've processed
    scanner_completed = set()
    entry_order_completed = set()
    executed_trades_count = 0
    last_position_check = time.time()
    last_status_update = time.time()
    position_check_counter = 0

    # Mark all past times as completed (skip them)
    start_time = datetime.now().time()

    scanner_skipped = 0
    for scan_time in SCANNER_TIMES:
        if start_time > scan_time:
            scanner_completed.add(scan_time)
            scanner_skipped += 1

    entry_skipped = 0
    for entry_time in ENTRY_ORDER_TIMES:
        if start_time > entry_time:
            entry_order_completed.add(entry_time)
            entry_skipped += 1

    if scanner_skipped > 0 or entry_skipped > 0:
        print(
            f"[STARTUP] Skipped {scanner_skipped} scanner times, {entry_skipped} entry times"
        )

    print(f"\n{'▓'*60}")
    print(f"▓ INITIALIZATION @ {datetime.now().strftime('%H:%M:%S')}")
    print(f"{'▓'*60}\n")
    print(f"[STARTUP] NIFTY filter will be checked by entry_checker at XX:15")

    # CRITICAL: Run scanner if started after last scanner time
    print(f"\n[STARTUP] Checking if scanner needs to run...")
    
    last_scanner_time = None
    for scan_time in SCANNER_TIMES:
        if start_time > scan_time:
            last_scanner_time = scan_time
    
    if last_scanner_time is not None:
        print(f"[STARTUP] Bot started after {last_scanner_time.strftime('%H:%M')} scanner")
        print(f"[STARTUP] Running scanner now to prepare watchlist for next entry check...")
        run_script("reclaim_scanner.py")
    else:
        print(f"[STARTUP] No scanner has run yet today - watchlist will be empty until 10:16")

    # Find next scheduled events
    next_scanner = None
    for scan_time in SCANNER_TIMES:
        if scan_time not in scanner_completed:
            next_scanner = scan_time
            break

    next_entry = None
    for entry_time in ENTRY_ORDER_TIMES:
        if entry_time not in entry_order_completed:
            next_entry = entry_time
            break

    print(f"\n{'─'*60}")
    print(f"[SCHEDULE] Upcoming Events:")
    if next_scanner:
        remaining = calculate_time_remaining(next_scanner)
        print(
            f"  • Next Scanner: {next_scanner.strftime('%H:%M:%S')} (in {format_time_remaining(remaining)})"
        )
    if next_entry:
        remaining = calculate_time_remaining(next_entry)
        print(
            f"  • Next Entry Check: {next_entry.strftime('%H:%M:%S')} (in {format_time_remaining(remaining)})"
        )
    print(f"[STATUS] Position Monitor: Active (every {POSITION_CHECK_INTERVAL}s)")
    print(f"[STATUS] NIFTY Filter: Checked at XX:15 (entry time)")
    print(f"[STATUS] Market Close: {MARKET_CLOSE.strftime('%H:%M:%S')}")
    print(f"{'─'*60}\n")

    print("Bot is now running. Press Ctrl+C to stop.\n")

    while True:
        now = datetime.now().time()
        current_time = time.time()

        # Check if market closed
        if now >= MARKET_CLOSE:
            print(f"\n{'='*60}")
            print(
                f"[MARKET CLOSE] Trading session ended at {MARKET_CLOSE.strftime('%H:%M:%S')}"
            )
            print(
                f"[SUMMARY] Scans: {len(scanner_completed)} | Entry Checks: {len(entry_order_completed)} | Trades Executed: {executed_trades_count}"
            )
            print(f"{'='*60}\n")
            
            # Monthly logs - no daily archiving needed
            print("[INFO] Trades logged to monthly file (logs/YYYY/MM_Month/trades.json)")
            
            notify_market_close(len(scanner_completed), len(entry_order_completed), executed_trades_count)
            break

        # Check for scanner execution (XX:16)
        for scan_time in SCANNER_TIMES:
            if scan_time not in scanner_completed and now >= scan_time:
                print(f"\n{'▓'*60}")
                print(f"▓ SCANNER TRIGGERED @ {datetime.now().strftime('%H:%M:%S')}")
                print(f"{'▓'*60}\n")
                
                print(f"[SCANNER] Running reclaim scanner (1 min after candle close)...")
                run_script("reclaim_scanner.py")
                scanner_completed.add(scan_time)

                # Find next scanner
                next_scanner = None
                for st in SCANNER_TIMES:
                    if st not in scanner_completed:
                        next_scanner = st
                        break

                if next_scanner:
                    remaining = calculate_time_remaining(next_scanner)
                    print(
                        f"\n[NEXT SCAN] {next_scanner.strftime('%H:%M:%S')} (in {format_time_remaining(remaining)})\n"
                    )

        # Check for entry + order execution (XX:15)
                for entry_time in ENTRY_ORDER_TIMES:
                    if entry_time not in entry_order_completed and now >= entry_time:
                        # Only trigger if within 60 seconds of scheduled time
                        now_seconds = now.hour * 3600 + now.minute * 60 + now.second
                        entry_seconds = entry_time.hour * 3600 + entry_time.minute * 60 + entry_time.second
                        seconds_past = now_seconds - entry_seconds
                        
                        if seconds_past > 60:
                            # Missed this entry time - mark as skipped
                            print(f"[SKIPPED] Entry check {entry_time.strftime('%H:%M:%S')} - started {seconds_past}s late")
                            entry_order_completed.add(entry_time)
                            continue
                        
                        print(f"\n{'▓'*60}")
                        print(
                            f"▓ ENTRY CHECK TRIGGERED @ {datetime.now().strftime('%H:%M:%S')}"
                        )
                        print(f"{'▓'*60}\n")

                        # Get positions count BEFORE running order manager
                        positions_before = get_open_positions_count()

                        print(
                            f"[ENTRY CHECK] Running entry checker (NIFTY filter + stock checks)..."
                        )
                        entry_start = time.time()
                        entry_success = run_script("entry_checker.py")
                        entry_duration = time.time() - entry_start
                        print(f"[TIMING] Entry checker completed in {entry_duration:.2f}s")

                        if entry_success:
                            # Check if there are actually signals to process
                            signals_file = ROOT / "main" / "entry_signals.json"
                            has_signals = False
                            num_signals = 0
                            try:
                                with open(signals_file) as f:
                                    signals = json.load(f)
                                    num_signals = len(signals)
                                    has_signals = num_signals > 0
                            except Exception as e:
                                print(f"[WARNING] Could not read signals file: {e}")
                            
                            if has_signals:
                                print(f"\n[ORDER MANAGER] Processing {num_signals} entry signal(s)...")
                                order_start = time.time()
                                run_script("order_manager.py")
                                order_duration = time.time() - order_start
                                print(f"[TIMING] Order manager completed in {order_duration:.2f}s")
                                print(f"[TOTAL TIMING] End-to-end: {entry_duration + order_duration:.2f}s")

                                # Count actual trades executed by comparing positions before/after
                                positions_after = get_open_positions_count()
                                new_trades = positions_after - positions_before
                                
                                if new_trades > 0:
                                    executed_trades_count += new_trades
                                    print(f"[TRADES] {new_trades} new position(s) opened this cycle")
                                else:
                                    print(f"[TRADES] No new positions opened this cycle")
                            else:
                                print(f"[ENTRY CHECK] No entry signals - skipping order manager")
                            
                            # Clear entry signals to prevent duplicates
                            clear_entry_signals()

                        entry_order_completed.add(entry_time)

                        # Find next entry
                        next_entry = None
                        for et in ENTRY_ORDER_TIMES:
                            if et not in entry_order_completed:
                                next_entry = et
                                break

                        if next_entry:
                            remaining = calculate_time_remaining(next_entry)
                            print(
                                f"\n[NEXT ENTRY] {next_entry.strftime('%H:%M:%S')} (in {format_time_remaining(remaining)})\n"
                            )
        # Monitor open positions (every 1 second)
        if current_time - last_position_check >= POSITION_CHECK_INTERVAL:
            try:
                monitor_positions()
                position_check_counter += 1
            except Exception as e:
                print(f"[ERROR] Position monitor failed: {e}")
            last_position_check = current_time

        # Status update every 10 minutes (when idle)
        if current_time - last_status_update >= STATUS_UPDATE_INTERVAL:
            next_scanner = None
            for st in SCANNER_TIMES:
                if st not in scanner_completed:
                    next_scanner = st
                    break

            next_entry = None
            for et in ENTRY_ORDER_TIMES:
                if et not in entry_order_completed:
                    next_entry = et
                    break

            status_parts = []
            if next_scanner:
                remaining = calculate_time_remaining(next_scanner)
                status_parts.append(
                    f"Next scan: {next_scanner.strftime('%H:%M:%S')} ({format_time_remaining(remaining)})"
                )
            if next_entry:
                remaining = calculate_time_remaining(next_entry)
                status_parts.append(
                    f"Next entry: {next_entry.strftime('%H:%M:%S')} ({format_time_remaining(remaining)})"
                )

            if status_parts:
                print(
                    f"[IDLE] {' | '.join(status_parts)} | Position checks: {position_check_counter}"
                )

            last_status_update = current_time

        # Sleep briefly before next check
        time.sleep(0.5)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n" + "=" * 60)
        print("[STOPPED] Bot stopped by user (Ctrl+C)")
        print("=" * 60 + "\n")
        notify_bot_stopped("User stopped (Ctrl+C)")
    except Exception as e:
        print(f"\n\n" + "=" * 60)
        print(f"[CRITICAL ERROR] {e}")
        print("=" * 60 + "\n")
        notify_bot_stopped(f"Critical error: {e}")