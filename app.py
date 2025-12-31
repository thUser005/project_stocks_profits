import asyncio
from flask import Flask, jsonify
from worker import run_worker

app = Flask(__name__)

@app.route("/")
def health():
    return jsonify({"status": "running"})


if __name__ == "__main__":
    asyncio.run(run_worker())
