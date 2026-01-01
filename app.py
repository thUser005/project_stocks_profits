import os
import sys
import asyncio
import threading
import subprocess
import re
import time
import urllib.request
from flask import Flask, jsonify, request
from flask_compress import Compress

# =====================================================
# AUTO-INSTALL REQUIRED PYTHON PACKAGES
# =====================================================
def ensure_package(pkg):
    try:
        __import__(pkg.replace("-", "_"))
    except ImportError:
        print(f"📦 Installing {pkg}...")
        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", pkg],
            stdout=subprocess.DEVNULL
        )

for p in ["flask", "flask-compress", "requests", "aiohttp"]:
    ensure_package(p)

# =====================================================
# IMPORTS
# =====================================================
from test_runner import run_test_for_date
from worker import run_worker
from telegram_msg import send_message

# =====================================================
# FLASK APP
# =====================================================
app = Flask(__name__)
Compress(app)


@app.route("/")
def health():
    return jsonify({
        "status": "running",
        "worker": "enabled"
    })


@app.route("/test/candles", methods=["GET"])
def test_candles():
    """
    Example:
    /test/candles?date=2025-12-31
    """
    date = request.args.get("date")
    if not date:
        return jsonify({"error": "date=YYYY-MM-DD required"}), 400

    # isolated loop per request (SAFE)
    result = asyncio.run(run_test_for_date(date))
    return jsonify(result)


# =====================================================
# WORKER THREAD (ASYNC LOOP)
# =====================================================
def start_worker():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    send_message("🟢 Worker started successfully")

    try:
        loop.run_until_complete(run_worker())
    except Exception as e:
        send_message(f"🔴 Worker crashed:\n{e}")
        raise


# =====================================================
# CLOUDFLARED SETUP
# =====================================================
CLOUDFLARED_BIN = "./cloudflared"


def ensure_cloudflared():
    if os.path.exists(CLOUDFLARED_BIN):
        return

    print("⬇️ Downloading cloudflared...")
    url = (
        "https://github.com/cloudflare/cloudflared/releases/latest/"
        "download/cloudflared-linux-amd64"
    )
    urllib.request.urlretrieve(url, CLOUDFLARED_BIN)
    os.chmod(CLOUDFLARED_BIN, 0o755)
    print("✅ cloudflared installed")


def start_cloudflare_tunnel(port):
    ensure_cloudflared()

    print("🌐 Starting Cloudflare Tunnel...")

    process = subprocess.Popen(
        [CLOUDFLARED_BIN, "tunnel", "--url", f"http://localhost:{port}"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1
    )

    for line in iter(process.stdout.readline, ""):
        print(line.strip())

        match = re.search(r"https://[a-zA-Z0-9\-]+\.trycloudflare\.com", line)
        if match:
            public_url = match.group(0)

            test_url = f"{public_url}/test/candles?date=2025-12-31"

            print("\n✅ PUBLIC URL:")
            print(public_url)

            send_message(
                "🚀 *Dedicated Worker Server Started*\n\n"
                f"🌐 URL: {public_url}\n"
                f"❤️ Health: {public_url}/\n"
                f"🧪 Test: {test_url}\n\n"
                "✅ API + Worker + Tunnel are LIVE"
            )

            break


# =====================================================
# FLASK THREAD
# =====================================================
def start_flask():
    app.run(
        host="0.0.0.0",
        port=5000,
        debug=False,
        threaded=True,
        use_reloader=False
    )


# =====================================================
# MAIN
# =====================================================
def main():
    print("🚀 Starting Dedicated Server...")

    # -------------------------------
    # Start Flask
    # -------------------------------
    threading.Thread(
        target=start_flask,
        daemon=True
    ).start()

    time.sleep(2)

    # -------------------------------
    # Start Worker
    # -------------------------------
    threading.Thread(
        target=start_worker,
        daemon=True
    ).start()

    time.sleep(2)

    # -------------------------------
    # Start Cloudflare Tunnel
    # -------------------------------
    start_cloudflare_tunnel(5000)

    # -------------------------------
    # Keep alive
    # -------------------------------
    while True:
        time.sleep(60)


if __name__ == "__main__":
    main()
