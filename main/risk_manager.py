"""
ROOT/main/risk_manager.py

Tracks monthly R and enforces -5R drawdown cap

UPDATED: Uses log_manager for statistics, keeps only DD cap enforcement
"""

import json
from pathlib import Path
from datetime import datetime
import pytz

# Import from log_manager
import sys
ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT))

from log_manager import get_current_month_stats


MONTHLY_DD_CAP = -5.0  # -5R monthly stop


def can_open_new_trades():
    """
    Check if new trades are allowed based on monthly R
    This check happens BEFORE entry, but we also check AFTER each exit
    Returns: (allowed, current_r, message)
    """
    # Use log_manager for stats
    current_r, trade_count, _, _ = get_current_month_stats()
    
    # Block if already at or below cap
    if current_r <= MONTHLY_DD_CAP:
        return False, current_r, f"Monthly DD cap hit: {current_r:.2f}R (limit: {MONTHLY_DD_CAP}R)"
    
    return True, current_r, f"Trading allowed: {current_r:.2f}R ({trade_count} trades this month)"


def check_monthly_dd_breach():
    """
    Check if monthly DD cap breached (for alerts after exit)
    Returns: (breached, current_r)
    """
    current_r, _, _, _ = get_current_month_stats()
    breached = current_r <= MONTHLY_DD_CAP
    
    return breached, current_r


# REMOVED: reset_monthly_log_if_new_month()
# No archiving needed for monthly-focused logs
# Logs stay in their monthly folders permanently


if __name__ == "__main__":
    # Test
    allowed, current_r, msg = can_open_new_trades()
    print(f"\n{msg}")
    print(f"Trading Allowed: {allowed}")