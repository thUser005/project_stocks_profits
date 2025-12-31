import asyncio
import os
from flask import Flask, jsonify, request
from flask_compress import Compress

from worker import run_worker
from test_runner import run_test_for_date

app = Flask(__name__)
Compress(app)   # ✅ enable gzip/br compression


@app.route("/")
def health():
    return jsonify({"status": "running"})


# 🧪 TEST ROUTE
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


# 🚀 START BACKGROUND WORKER ONLY ONCE
if os.environ.get("RUN_WORKER") == "true":
    asyncio.get_event_loop().create_task(run_worker())
