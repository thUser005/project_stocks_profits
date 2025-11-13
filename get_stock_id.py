import json
import os
from datetime import date
from nseconnect import Nse

def get_stock_data():
    file_name = 'fo_stocks.json'
    with open(file_name,'r',encoding='utf-8')as f:
        stocks_lst = json.load(f)
    return stocks_lst if stocks_lst else []

def main():
    nse = Nse()
    
    today = date.today().strftime("%Y%m%d")

    # File path
    file_path = f"BhavCopy_NSE_CM_0_0_0_{today}_F_0000.csv"

    # Download BhavCopy if not already present
    if not os.path.exists(file_path):
        print("📥 Downloading today's BhavCopy...")
        nse.equity_bhavcopy()
        print(f"✅ Bhavcopy downloaded at: {file_path}")
    else:
        print(f"✅ Bhavcopy already exists at: {file_path}")

    # Load BhavCopy data
    import pandas as pd
    df = pd.read_csv(file_path)

    # Clean column names (sometimes extra spaces exist)
    df.columns = [c.strip() for c in df.columns]

    # Stocks you want
    stocks = get_stock_data()

    results = []

    for sym in stocks:
        match = df[df["TckrSymb"] == sym]
        if not match.empty:
            isin = match.iloc[0]["ISIN"]
            results.append({
                "symbol": sym,
                "isin": isin
            })
        else:
            print(f"⚠️ Symbol not found in BhavCopy: {sym}")

    output_file = "stock_ids.json"
    with open(output_file, "w") as f:
        json.dump(results, f, indent=2)

    print(f"✅ Saved {len(results)} stock entries to {output_file}")

if __name__ == "__main__":
    main()
