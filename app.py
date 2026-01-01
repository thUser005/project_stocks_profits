import os
import sys
import asyncio
import threading
import subprocess
import re
import time
import urllib.request
import socket
from flask import Flask, jsonify, request
from flask_compress import Compress

# =====================================================
# CONFIG
# =====================================================
BASE_PORT = 5000
MAX_PORT_TRIES = 20
PID_FILE = "/tmp/project_worker.pid"
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
# PORT + PROCESS HELPERS (SAFE)
# =====================================================
def is_process_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except Exception:
        return False


def is_port_free(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(("127.0.0.1", port)) != 0


def pick_free_port(base: int) -> int:
    for i in range(MAX_PORT_TRIES):
        port = base + i
        if is_port_free(port):
            return port
    raise RuntimeError("❌ No free port available")

# =====================================================
# PID LOCK (OWN PROCESS ONLY)
# =====================================================
def acquire_pid_lock():
    if os.path.exists(PID_FILE):
        with open(PID_FILE) as f:
            old_pid = int(f.read().strip())

        if is_process_alive(old_pid):
            print(f"⚠️ App already running with PID {old_pid}")
            print("➡️ Reusing existing instance. Exiting.")
            sys.exit(0)
        else:
            print("🧹 Removing stale PID file")
            os.remove(PID_FILE)

    with open(PID_FILE, "w") as f:
        f.write(str(os.getpid()))


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
def start_flask(port):
    print(f"🚀 Flask starting on port {port}")
    app.run(
        host="0.0.0.0",
        port=port,
        debug=False,
        threaded=True,
        use_reloader=False
    )

# =====================================================
# MAIN
# =====================================================
def main():
    print("🚀 Starting Dedicated Server (SAFE MODE)...")

    # 🔐 Ensure single instance of OUR app
    acquire_pid_lock()

    # 🔁 Pick free port automatically
    port = pick_free_port(BASE_PORT)
    print(f"✅ Using port {port}")

    # -------------------------------
    # Start Flask
    # -------------------------------
    threading.Thread(
        target=start_flask,
        args=(port,),
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
    start_cloudflare_tunnel(port)

    # -------------------------------
    # Keep alive
    # -------------------------------
    while True:
        time.sleep(60)


if __name__ == "__main__":
    main()
