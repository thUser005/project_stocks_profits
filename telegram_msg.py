import os
import requests

BOT = os.environ["TELEGRAM_BOT_TOKEN"]
CHAT = os.environ["TELEGRAM_CHAT_ID"]

API = f"https://api.telegram.org/bot{BOT}"

def send_message(text=None, photo=None, caption=None):
    """
    Smart Telegram sender:
    - text → sendMessage
    - photo → sendPhoto
    """

    if photo:
        with open(photo, "rb") as f:
            requests.post(
                f"{API}/sendPhoto",
                data={
                    "chat_id": CHAT,
                    "caption": caption or "",
                },
                files={"photo": f},
                timeout=10,
            )
    elif text:
        requests.post(
            f"{API}/sendMessage",
            data={
                "chat_id": CHAT,
                "text": text,
            },
            timeout=5,
        )
