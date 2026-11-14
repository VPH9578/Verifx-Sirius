import sys
import os
import urllib.request
import urllib.parse
from dotenv import load_dotenv

load_dotenv()

def send_telegram_alert(message):
    bot_token = os.getenv("ALERT_BOT_TOKEN")
    chat_id = os.getenv("ALERT_CHAT_ID")
    
    if not bot_token or not chat_id:
        print("[!] ALERT_BOT_TOKEN or ALERT_CHAT_ID not configured in .env; skipping alert.")
        return
    
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    data = urllib.parse.urlencode({"chat_id": chat_id, "text": message}).encode("utf-8")
    
    try:
        req = urllib.request.Request(url, data=data)
        with urllib.request.urlopen(req) as response:
            print("[+] Telegram alert sent successfully.")
    except Exception as e:
        print(f"[-] Failed to send Telegram alert: {e}")

if __name__ == "__main__":
    import os
    if os.path.exists("maintenance.flag"):
        print("[+] Maintenance flag active, suppressing alert.")
        sys.exit(0)

    # If the first argument is an app name and the second is an event type,
    # we generate the emoji string inside Python to avoid Windows CMD encoding issues.
    if len(sys.argv) >= 3:
        app_name = sys.argv[1]
        event_type = sys.argv[2]
        if event_type == "crash":
            msg = f"⚠️ The {app_name} has crashed and is restarting!"
        elif event_type == "recovery":
            msg = f"✅ The {app_name} is back up and running!"
        else:
            msg = f"ℹ️ The {app_name} sent an alert: {event_type}"
    elif len(sys.argv) == 2:
        # Fallback for old style, though emojis might be mangled
        msg = sys.argv[1]
    else:
        msg = "⚠️ Alert from application!"
        
    send_telegram_alert(msg)
