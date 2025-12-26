"""
ROOT/main/main.py

Main orchestrator for VWAP Reclaim Trading Bot
Run manually at 9:00-9:15 AM - handles all hourly execution
OPTIMIZED: Refreshes NIFTY filter at hourly intervals
"""

import sys
import time
from pathlib import Path
from datetime import datetime, time as dt_time
import subprocess
import json

ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT))
sys.path.insert(0, str(ROOT / "main"))

# Import at top to catch errors early
from filter import update_sma_cache
from position_monitor import monitor_positions


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
    dt_time(11, 14, 58),  # First valid entry (uses 10:16 watchlist)
    dt_time(12, 14, 58),
    dt_time(13, 14, 58),
    dt_time(14, 14, 58),
    dt_time(15, 14, 58),
    dt_time(15, 29, 58),  # Last candle (3:15-3:30)
]

MARKET_CLOSE = dt_time(15, 30, 0)
POSITION_CHECK_INTERVAL = 1  # Check positions every 1 second
STATUS_UPDATE_INTERVAL = 600  # Status update every 10 minutes


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
    except:
        pass


def main():
    """Main execution loop"""

    print(f"\n{'#'*60}")
    print(f"# VWAP RECLAIM TRADING BOT - CNC STRATEGY")
    print(f"# Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"# Strategy: VWAP Reclaim | Risk: 1% | TP: 3R | SL: Reclaim Low")
    print(f"# Filter: NIFTY Hourly SMA50")
    print(f"{'#'*60}\n")

    # Track which times we've processed
    scanner_completed = set()
    entry_order_completed = set()
    executed_trades_count = 0  # Track actual trades executed
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

    # Initialize NIFTY filter on startup
    print(f"\n{'▓'*60}")
    print(f"▓ INITIALIZATION @ {datetime.now().strftime('%H:%M:%S')}")
    print(f"{'▓'*60}\n")
    print(f"[STARTUP] Initializing NIFTY filter...")
    update_sma_cache()

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
            f"  • Next Entry/Order: {next_entry.strftime('%H:%M:%S')} (in {format_time_remaining(remaining)})"
        )
    print(f"[STATUS] Position Monitor: Active (every {POSITION_CHECK_INTERVAL}s)")
    print(f"[STATUS] NIFTY Filter: Auto-refresh (hourly with scanner)")
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
            break

        # Check for scanner execution (XX:16)
        for scan_time in SCANNER_TIMES:
            if scan_time not in scanner_completed and now >= scan_time:
                print(f"\n{'▓'*60}")
                print(f"▓ SCANNER TRIGGERED @ {datetime.now().strftime('%H:%M:%S')}")
                print(f"{'▓'*60}\n")
                
                # Update NIFTY filter before scanner (hourly update)
                print(f"[FILTER] Refreshing NIFTY filter...")
                update_sma_cache()
                print()
                
                print(f"[SCAN] Running reclaim scanner (1 min after candle close)...")
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

        # Check for entry + order execution (XX:14:58)
        for entry_time in ENTRY_ORDER_TIMES:
            if entry_time not in entry_order_completed and now >= entry_time:
                print(f"\n{'▓'*60}")
                print(
                    f"▓ ENTRY/ORDER TRIGGERED @ {datetime.now().strftime('%H:%M:%S')}"
                )
                print(f"{'▓'*60}\n")

                print(
                    f"[ENTRY CHECK] Running Entry Checker (Stock LTP vs Reclaim High)..."
                )
                entry_start = time.time()
                entry_success = run_script("entry_checker.py")
                entry_duration = time.time() - entry_start
                print(f"[TIMING] Entry checker completed in {entry_duration:.2f}s")

                if entry_success:
                    # NO SLEEP - Run order manager immediately
                    print(
                        f"\n[ORDER MANAGER] Processing Orders (Position Sizing & Execution)..."
                    )
                    order_start = time.time()
                    run_script("order_manager.py")
                    order_duration = time.time() - order_start
                    print(f"[TIMING] Order manager completed in {order_duration:.2f}s")
                    print(
                        f"[TOTAL TIMING] End-to-end: {entry_duration + order_duration:.2f}s"
                    )

                    # Count actual trades executed
                    try:
                        with open(ROOT / "main" / "entry_signals.json") as f:
                            signals = json.load(f)
                            num_signals = len(signals)
                            if num_signals > 0:
                                executed_trades_count += num_signals
                                print(
                                    f"[TRADES] {num_signals} positions opened this cycle"
                                )
                    except:
                        pass
                    
                    # Clear processed signals to prevent duplicate entries
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
    except Exception as e:
        print(f"\n\n" + "=" * 60)
        print(f"[CRITICAL ERROR] {e}")
        print("=" * 60 + "\n")