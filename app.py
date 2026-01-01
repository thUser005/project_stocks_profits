import asyncio
from flask import Flask, jsonify, request
from flask_compress import Compress

from test_runner import run_test_for_date

app = Flask(__name__)
Compress(app)   # gzip/br compression


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

    # ✅ Colab-safe async execution
    result = asyncio.run(run_test_for_date(date))
    return jsonify(result)


if __name__ == "__main__":
    # ✅ IMPORTANT: Colab requires threaded=True
    app.run(
        host="0.0.0.0",
        port=5000,
        debug=False,
        threaded=True
    )
