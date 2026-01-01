import os
import requests


def send_message(text,BOT,CHAT):
    requests.post(
        f"https://api.telegram.org/bot{BOT}/sendMessage",
        data={"chat_id": CHAT, "text": text},
        timeout=5
    )
