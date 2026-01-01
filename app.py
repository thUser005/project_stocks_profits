import os
import sys
import asyncio
import threading
import subprocess
import re
import time
import urllib.request
import socket
import requests
from flask import Flask, jsonify, request
from flask_compress import Compress

# =====================================================
# CONFIG
# =====================================================
BASE_PORT = 5000
MAX_PORT_TRIES = 20
PID_FILE = "/tmp/project_worker.pid"
PORT_FILE = "/tmp/project_worker.port"
CLOUDFLARED_BIN = "./cloudflared"

# =====================================================
# AUTO-INSTALL REQUIRED PYTHON PACKAGES
# =====================================================
def ensure_package(pkg):
    try:
        __import__(pkg)
    except ImportError:
        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", pkg],
            stdout=subprocess.DEVNULL
        )

for p in ["flask", "flask_compress", "requests", "aiohttp"]:
    ensure_package(p)

# =====================================================
# IMPORTS
# =====================================================
from test_runner import run_test_for_date
from worker import run_worker
from telegram_msg import send_message

# =====================================================
# PORT HELPERS
# =====================================================
def is_port_free(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(("127.0.0.1", port)) != 0


def pick_free_port(base: int) -> int:
    for i in range(MAX_PORT_TRIES):
        p = base + i
        if is_port_free(p):
            return p
    raise RuntimeError("❌ No free port available")

# =====================================================
# PID + SOFT STOP LOGIC (🔥 NEW)
# =====================================================
def acquire_pid_lock_with_prompt():
    if not os.path.exists(PID_FILE):
        return True

    with open(PID_FILE) as f:
        old_pid = int(f.read().strip())

    if not os.path.exists(PORT_FILE):
        print("⚠️ Old app detected but port info missing.")
        return False

    with open(PORT_FILE) as f:
        old_port = int(f.read().strip())

    print(f"\n⚠️ App already running (PID {old_pid}, port {old_port})")
    ans = input("❓ Do you want to STOP old app and start new one? (yes/no): ").strip().lower()

    if ans != "yes":
        print("🚫 Keeping existing app. Exiting.")
        sys.exit(0)

    # ---- Soft stop via HTTP ----
    try:
        print("🛑 Sending soft-stop request to old app...")
        requests.post(f"http://127.0.0.1:{old_port}/admin/stop", timeout=5)
        time.sleep(2)
    except Exception as e:
        print("⚠️ Failed to contact old app:", e)

    # Cleanup
    if os.path.exists(PID_FILE):
        os.remove(PID_FILE)
    if os.path.exists(PORT_FILE):
        os.remove(PORT_FILE)

    print("✅ Old app stopped. Starting new one.")
    return True

# =====================================================
# FLASK APP
# =====================================================
app = Flask(__name__)
Compress(app)


@app.route("/")
def health():
    return jsonify({"status": "running"})


@app.route("/test/candles")
def test_candles():
    date = request.args.get("date")
    if not date:
        return jsonify({"error": "date required"}), 400
    return jsonify(asyncio.run(run_test_for_date(date)))


# 🔥 SOFT STOP ENDPOINT
@app.route("/admin/stop", methods=["POST"])
def admin_stop():
    print("🛑 Soft stop requested")
    send_message("🛑 Worker stopped via admin request")
    os._exit(0)

# =====================================================
# WORKER THREAD
# =====================================================
def start_worker():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(run_worker())

# =====================================================
# CLOUDFLARED
# =====================================================
def ensure_cloudflared():
    if os.path.exists(CLOUDFLARED_BIN):
        return
    urllib.request.urlretrieve(
        "https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64",
        CLOUDFLARED_BIN
    )
    os.chmod(CLOUDFLARED_BIN, 0o755)


def start_cloudflare_tunnel(port):
    ensure_cloudflared()
    p = subprocess.Popen(
        [CLOUDFLARED_BIN, "tunnel", "--url", f"http://localhost:{port}"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True
    )

    for line in p.stdout:
        print(line.strip())
        m = re.search(r"https://[^\s]+\.trycloudflare\.com", line)
        if m:
            url = m.group(0)
            send_message(
                f"🚀 *Server Started*\n\n🌐 {url}\n❤️ {url}/"
            )
            break

# =====================================================
# FLASK THREAD
# =====================================================
def start_flask(port):
    app.run(
        host="0.0.0.0",
        port=port,
        debug=False,
        use_reloader=False
    )

# =====================================================
# MAIN
# =====================================================
def main():
    print("🚀 Starting Dedicated Server (SMART MODE)...")

    acquire_pid_lock_with_prompt()

    port = pick_free_port(BASE_PORT)

    with open(PID_FILE, "w") as f:
        f.write(str(os.getpid()))

    with open(PORT_FILE, "w") as f:
        f.write(str(port))

    threading.Thread(target=start_flask, args=(port,), daemon=True).start()
    time.sleep(2)

    threading.Thread(target=start_worker, daemon=True).start()
    time.sleep(2)

    start_cloudflare_tunnel(port)

    while True:
        time.sleep(60)


if __name__ == "__main__":
    main()
