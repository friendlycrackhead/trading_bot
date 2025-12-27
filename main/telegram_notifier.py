"""
ROOT/main/telegram_notifier.py

Sends Telegram notifications for important trading events
"""
import requests
import json
from datetime import datetime
import pytz

# ============ CONFIG ============
TELEGRAM_BOT_TOKEN = "8228088339:AAE49S2enCCmjeiZBFHmhAN8LLEVjMTYAwY"
TELEGRAM_CHAT_ID = "1993755474"

# Set to False to disable notifications
NOTIFICATIONS_ENABLED = True


def send_telegram(message):
    """Send message to Telegram"""
    if not NOTIFICATIONS_ENABLED:
        return
    
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "HTML"
    }
    
    try:
        response = requests.post(url, json=payload, timeout=10)
        if response.status_code != 200:
            print(f"[TELEGRAM] Failed to send: {response.text}")
    except Exception as e:
        print(f"[TELEGRAM] Error: {e}")


def notify_startup():
    """Bot started"""
    ist = pytz.timezone('Asia/Kolkata')
    now = datetime.now(ist)
    
    message = f"""🤖 <b>VWAP BOT STARTED</b>

📅 {now.strftime('%Y-%m-%d')}
⏰ {now.strftime('%H:%M:%S')}
✅ Ready for trading"""
    
    send_telegram(message)


def notify_nifty_filter(status, close, sma50, time):
    """NIFTY filter status"""
    emoji = "🟢" if status else "🔴"
    status_text = "ON" if status else "OFF"
    
    message = f"""{emoji} <b>NIFTY FILTER: {status_text}</b>

📊 Close: ₹{close:.2f}
📈 SMA50: ₹{sma50:.2f}
⏰ {time}"""
    
    send_telegram(message)


def notify_reclaims_found(count, stocks, time):
    """Scanner found reclaims"""
    if count == 0:
        message = f"""🔍 <b>SCANNER COMPLETE</b>

❌ No reclaims found
⏰ {time}"""
    else:
        stock_list = "\n".join([f"  • {s}" for s in stocks[:5]])  # First 5
        more = f"\n  ... and {count - 5} more" if count > 5 else ""
        
        message = f"""🎯 <b>RECLAIMS FOUND: {count}</b>

{stock_list}{more}
⏰ {time}"""
    
    send_telegram(message)


def notify_entry_signals(signals):
    """Entry signals generated"""
    count = len(signals)
    
    if count == 0:
        return  # Don't notify if no signals
    
    ist = pytz.timezone('Asia/Kolkata')
    now = datetime.now(ist)
    
    signal_list = "\n".join([
        f"  • {sym}: ₹{data['entry_price']:.2f}" 
        for sym, data in list(signals.items())[:5]
    ])
    more = f"\n  ... and {count - 5} more" if count > 5 else ""
    
    message = f"""🚀 <b>ENTRY SIGNALS: {count}</b>

{signal_list}{more}
⏰ {now.strftime('%H:%M:%S')}"""
    
    send_telegram(message)


def notify_order_placed(symbol, quantity, entry, sl, tp):
    """Order executed"""
    ist = pytz.timezone('Asia/Kolkata')
    now = datetime.now(ist)
    
    risk = entry - sl
    pnl_target = tp - entry
    rupee_risk = risk * quantity
    rupee_target = pnl_target * quantity
    
    message = f"""💰 <b>ORDER EXECUTED</b>

📌 {symbol}
📊 Qty: {quantity}
💵 Entry: ₹{entry:.2f}
🛑 SL: ₹{sl:.2f} (Risk: ₹{rupee_risk:,.0f})
🎯 TP: ₹{tp:.2f} (Target: ₹{rupee_target:,.0f})
⏰ {now.strftime('%H:%M:%S')}"""
    
    send_telegram(message)


def notify_order_skipped(symbol, reason):
    """Entry skipped"""
    ist = pytz.timezone('Asia/Kolkata')
    now = datetime.now(ist)
    
    message = f"""⚠️ <b>ENTRY SKIPPED</b>

📌 {symbol}
❌ {reason}
⏰ {now.strftime('%H:%M:%S')}"""
    
    send_telegram(message)


def notify_position_exit(symbol, entry, exit_price, sl, quantity, r_value, reason):
    """Position closed"""
    ist = pytz.timezone('Asia/Kolkata')
    now = datetime.now(ist)
    
    emoji = "✅" if r_value > 0 else "❌"
    r_color = "+" if r_value > 0 else ""
    
    pnl_per_share = exit_price - entry
    total_pnl = pnl_per_share * quantity
    pnl_sign = "+" if total_pnl > 0 else ""
    
    message = f"""{emoji} <b>POSITION CLOSED</b>

📌 {symbol}
📊 {reason}
💵 Entry: ₹{entry:.2f}
💰 Exit: ₹{exit_price:.2f}
📈 P&L: {pnl_sign}₹{total_pnl:,.0f} ({r_color}{r_value:.2f}R)
⏰ {now.strftime('%H:%M:%S')}"""
    
    send_telegram(message)


def notify_monthly_dd_breach(current_r):
    """Monthly DD cap breached"""
    message = f"""⚠️ <b>MONTHLY DD CAP BREACHED</b>

📉 Current R: {current_r:.2f}R
🚫 No new trades allowed
⚠️ Existing positions still managed"""
    
    send_telegram(message)


def notify_market_close(scans, entries, trades):
    """Trading day ended"""
    ist = pytz.timezone('Asia/Kolkata')
    now = datetime.now(ist)
    
    message = f"""🏁 <b>MARKET CLOSE</b>

📅 {now.strftime('%Y-%m-%d')}
🔍 Scans: {scans}
📊 Entry Checks: {entries}
💰 Trades: {trades}
⏰ 15:30"""
    
    send_telegram(message)


def notify_bot_stopped(reason):
    """Bot stopped"""
    ist = pytz.timezone('Asia/Kolkata')
    now = datetime.now(ist)
    
    message = f"""🛑 <b>BOT STOPPED</b>

📅 {now.strftime('%Y-%m-%d')}
⏰ {now.strftime('%H:%M:%S')}
ℹ️ {reason}"""
    
    send_telegram(message)