import requests
from telegram_msg import send_message

GTT_API_BASE = "https://upstock-dashboard101.up.railway.app"


def trigger_gtt_trade(
    *,
    instrument,
    symbol_key,
    qty,
    transaction_type="BUY",
):
    """
    Worker responsibility:
    - Decide BUY / SELL
    - Decide SYMBOL
    - Decide QTY

    Backend responsibility:
    - Fetch OPEN / LTP
    - Calculate ENTRY / TARGET / SL
    - Enforce intraday
    """
    instrument = f"NSE_EQ|{instrument}" if "NSE" not in instrument else instrument

    payload = {
        "instrument": instrument,
        "symbol_key": symbol_key,
        "qty": qty,
        "transaction_type": transaction_type,
        "product": "I",   # 🔒 FORCE INTRADAY
    }

    try:
        r = requests.post(
            f"{GTT_API_BASE}/api/gtt/place",
            json=payload,
            timeout=10
        )
        res = r.json()

        # -------------------------------
        # SUCCESS
        # -------------------------------
        if res.get("success"):
            gtt_id = res["gtt_id"]

            print(f"[GTT] PLACED :: {symbol_key} :: {gtt_id}", flush=True)

            send_message(
                text=(
                    f"✅ *GTT ORDER PLACED*\n\n"
                    f"{symbol_key}\n"
                    f"GTT ID: {gtt_id}\n"
                    f"Qty: {qty}"
                    f"Response : {res}"
                )
            )
            return gtt_id

        # -------------------------------
        # FAILURE (API responded but failed)
        # -------------------------------
        print(f"[GTT] FAILED :: {symbol_key} :: {res}", flush=True)

        send_message(
            text=(
                f"❌ *GTT FAILED*\n\n"
                f"{symbol_key}\n"
                f"Error: {res.get('error')}"
            )
        )
        return None

    # -------------------------------
    # EXCEPTION (network / timeout)
    # -------------------------------
    except Exception as e:
        print(f"[GTT] ERROR :: {symbol_key} :: {e}", flush=True)

        send_message(
            text=(
                f"❌ *GTT API ERROR*\n\n"
                f"{symbol_key}\n"
                f"{e}"
            )
        )
        return None
