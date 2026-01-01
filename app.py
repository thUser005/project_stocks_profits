import os
import sys
import asyncio
import threading
import subprocess
import re
import time
import urllib.request
import signal
from flask import Flask, jsonify, request
from flask_compress import Compress

# =====================================================
# CONFIG
# =====================================================
FLASK_PORT = 5000
CLOUDFLARED_BIN = "./cloudflared"

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
# 🔍 PORT INSPECTION (MANUAL)
# =====================================================
def list_ports_in_use():
    print("\n🔍 Checking ports currently in use...\n")
    try:
        output = subprocess.check_output(
            ["lsof", "-i", "-P", "-n"],
            stderr=subprocess.DEVNULL
        ).decode()
        print(output)
        return output
    except Exception as e:
        print("⚠️ Unable to list ports:", e)
        return None


def ask_and_kill_port(port):
    """
    Ask user before killing any process using the port.
    """
    try:
        result = subprocess.check_output(
            ["lsof", "-ti", f":{port}"],
            stderr=subprocess.DEVNULL
        ).decode().strip()

        if not result:
            print(f"✅ Port {port} is free")
            return True

        pids = result.split("\n")

        print(f"\n⚠️ Port {port} is currently in use by PID(s): {pids}")
        ans = input(f"❓ Do you want to STOP these process(es) on port {port}? (yes/no): ").strip().lower()

        if ans != "yes":
            print("🚫 User chose NOT to stop existing process.")
            return False

        for pid in pids:
            try:
                print(f"🔪 Killing PID {pid}")
                os.kill(int(pid), signal.SIGKILL)
            except Exception as e:
                print(f"❌ Failed to kill PID {pid}: {e}")

        time.sleep(1)
        return True

    except subprocess.CalledProcessError:
        print(f"✅ Port {port} is free")
        return True

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

    result = asyncio.run(run_test_for_date(date))
    return jsonify(result)

# =====================================================
# WORKER THREAD
# =====================================================
def start_worker():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        loop.run_until_complete(run_worker())
    except Exception as e:
        send_message(f"🔴 Worker crashed:\n{e}")
        raise

# =====================================================
# CLOUDFLARED
# =====================================================
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
    print(f"🚀 Flask starting on port {FLASK_PORT}")
    app.run(
        host="0.0.0.0",
        port=FLASK_PORT,
        debug=False,
        threaded=True,
        use_reloader=False
    )

# =====================================================
# MAIN
# =====================================================
def main():
    print("🚀 Starting Dedicated Server (Manual Port Control)...")

    # 🔍 Show all ports
    list_ports_in_use()

    # ❓ Ask before killing port
    ok = ask_and_kill_port(FLASK_PORT)
    if not ok:
        print("❌ Server start aborted by user.")
        return

    # -------------------------------
    # Start Flask
    # -------------------------------
    threading.Thread(
        target=start_flask,
        daemon=True
    ).start()

    time.sleep(3)

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
    start_cloudflare_tunnel(FLASK_PORT)

    # -------------------------------
    # Keep alive
    # -------------------------------
    while True:
        time.sleep(60)


if __name__ == "__main__":
    main()
