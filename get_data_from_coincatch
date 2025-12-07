#
# CoinCatch Crypto Downloader

#A Python script to fetch historical OHLCV data from CoinCatch API, sort symbols by volume, and save CSV files.

## Features
#- Fetch top N cryptocurrencies
#- Download historical 15-min candles for a number of days
#- Save data as:
  #1. All mixed (sorted by time)
  #2. Grouped by symbol
  #3. Separate CSV per crypto
#- Parallel downloading with thread pool

## Requirements
#- Python 3.11+
#- Packages:
  #- requests
  #- pandas (optional if you plan to extend)

import requests
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE = "https://api.coincatch.com"

def create_session():
    session = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=0.3,
        status_forcelist=[500, 502, 503, 504]
    )
    adapter = HTTPAdapter(
        max_retries=retry,
        pool_connections=50,  # Increase connection pool
        pool_maxsize=50
    )
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

SESSION = create_session()


def get_top_symbols(limit):
    url = f"{BASE}/api/spot/v1/market/tickers"
    resp = SESSION.get(url, timeout=10)
    resp.raise_for_status()
    result = resp.json()

    if result.get("code") != "00000":
        raise RuntimeError(f"API Error: {result}")

    tickers = result["data"]

    # Correct 24h volume fields
    volume_keys = ["quoteVolume", "vol24h", "amount24h", "baseVolume"]

    def get_volume(t):
        for key in volume_keys:
            if key in t and t[key] not in [None, ""]:
                try:
                    return float(t[key])
                except:
                    continue
        return 0

    # Sort by 24h volume descending
    tickers.sort(key=lambda x: get_volume(x), reverse=True)

    # Extract symbol names
    symbols = []
    for t in tickers[:limit]:
        sym = t["symbol"]
        if sym.endswith("_SPBL"):
            sym = sym.replace("_SPBL", "")
        symbols.append(sym)

    return symbols


def fetch_ohlcv(symbol, period, end_ts_ms, limit=200):
    url = f"{BASE}/api/spot/v1/market/history-candles"
    symbol_with_suffix = f"{symbol}_SPBL" if not symbol.endswith("_SPBL") else symbol

    params = {
        "symbol": symbol_with_suffix,
        "period": period,
        "endTime": str(end_ts_ms),
        "limit": str(limit)
    }

    resp = SESSION.get(url, params=params, timeout=10)
    resp.raise_for_status()
    result = resp.json()

    if result.get("code") != "00000":
        raise RuntimeError(f"API Error: {result}")

    candles = result["data"]
    return list(reversed(candles))


def fetch_single_batch(args):
    """Helper function to fetch one batch with a short delay"""
    symbol, period, end_ts, limit = args
    try:
        time.sleep(0.05)  # Short delay to avoid rate limit
        return fetch_ohlcv(symbol, period, end_ts, limit)
    except Exception as e:
        if "429" in str(e):
            time.sleep(1)  # If rate limited, wait longer
            try:
                return fetch_ohlcv(symbol, period, end_ts, limit)
            except:
                return []
        print(f"  ⚠ Batch error at {end_ts}: {e}")
        return []


def get_history_data_parallel(symbol, days, period="15min", batch_workers=4):
    """
    Fetch multiple batches in parallel for a symbol with rate-limit control
    """
    end_ts = int(time.time() * 1000)
    start_ts = end_ts - (days * 24 * 3600 * 1000)

    # Fetch first batch to ensure API works
    try:
        first_batch = fetch_ohlcv(symbol, period, end_ts, 200)
        if not first_batch:
            print(f"  ⚠ {symbol}: No data available from API")
            return []
        time.sleep(0.1)  # Delay after first request
    except Exception as e:
        print(f"  ⚠ {symbol}: API Error - {e}")
        return []
    
    # Calculate how many batches are needed
    candles_per_day = (24 * 60) // 15  # 96 candles/day for 15min
    total_needed = days * candles_per_day
    num_batches = (total_needed // 200) + 2
    
    # Build list of timestamps for each batch
    batch_timestamps = [(symbol, period, end_ts, 200)]
    current_ts = int(first_batch[0]["ts"]) - 1  # Start from last candle
    
    for _ in range(num_batches - 1):
        if current_ts < start_ts:
            break
        batch_timestamps.append((symbol, period, current_ts, 200))
        current_ts -= (200 * 15 * 60 * 1000)
    
    # Download batches in parallel with limited workers
    all_candles = []
    seen = set()
    
    # Add first batch
    for c in first_batch:
        ts = int(c["ts"])
        if ts >= start_ts and ts not in seen:
            seen.add(ts)
            all_candles.append(c)
    
    # Download remaining batches
    if len(batch_timestamps) > 1:
        with ThreadPoolExecutor(max_workers=min(batch_workers, len(batch_timestamps))) as executor:
            futures = [executor.submit(fetch_single_batch, args) for args in batch_timestamps[1:]]
            
            for future in as_completed(futures):
                try:
                    batch_data = future.result()
                    if batch_data:
                        for c in batch_data:
                            ts = int(c["ts"])
                            if ts not in seen and ts >= start_ts:
                                seen.add(ts)
                                all_candles.append(c)
                except Exception as e:
                    continue
    
    all_candles.sort(key=lambda c: int(c["ts"]))
    return all_candles


def download_symbol_data(sym, days, batch_workers=4):
    """Download a symbol's data with parallel batches"""
    try:
        time.sleep(0.2)  # Delay between starting each symbol
        print(f"📥 Downloading {sym}...")
        candles = get_history_data_parallel(sym, days, batch_workers=batch_workers)
        
        if not candles:
            print(f"⚠️  {sym}: No data received")
            return []
        
        for c in candles:
            c["symbol"] = sym
        
        print(f"✅ {sym}: {len(candles)} candles")
        return candles
    
    except Exception as e:
        print(f"❌ {sym}: {e}")
        return []


def save_all_to_csv(rows):
    """Save all data in one CSV file"""
    filename = "crypto_all_data.csv"
    
    lines = ["time,symbol,close,open,high,low,volume\n"]
    
    for r in rows:
        ts_ms = int(r["ts"])
        dt = datetime.fromtimestamp(ts_ms / 1000)
        formatted_time = dt.strftime("%Y-%m-%d %H:%M:%S")
        symbol = r["symbol"].replace("_", "-")
        
        lines.append(
            f"{formatted_time},{symbol},{r['close']},{r['open']},{r['high']},{r['low']},{r.get('volume', 'N/A')}\n"
        )
    
    with open(filename, "w", encoding="utf-8") as f:
        f.writelines(lines)
    
    print(f"\n✅ Saved {len(rows)} rows to: {filename}")


def save_grouped_by_symbol(rows):
    """Save data grouped by symbol"""
    filename = "crypto_grouped_by_symbol.csv"
    
    # Group data by symbol
    by_symbol = {}
    for r in rows:
        sym = r["symbol"]
        if sym not in by_symbol:
            by_symbol[sym] = []
        by_symbol[sym].append(r)
    
    # Sort each group by time
    for sym in by_symbol:
        by_symbol[sym].sort(key=lambda r: int(r["ts"]))
    
    lines = ["time,symbol,close,open,high,low,volume\n"]
    
    for sym in sorted(by_symbol.keys()):
        for r in by_symbol[sym]:
            ts_ms = int(r["ts"])
            dt = datetime.fromtimestamp(ts_ms / 1000)
            formatted_time = dt.strftime("%Y-%m-%d %H:%M:%S")
            symbol = r["symbol"].replace("_", "-")
            
            lines.append(
                f"{formatted_time},{symbol},{r['close']},{r['open']},{r['high']},{r['low']},{r.get('volume', 'N/A')}\n"
            )
    
    with open(filename, "w", encoding="utf-8") as f:
        f.writelines(lines)
    
    print(f"✅ Saved grouped data: {filename} ({len(rows)} rows)")


def save_separate_files(rows):
    """Save each symbol in a separate CSV file"""
    by_symbol = {}
    for r in rows:
        sym = r["symbol"]
        if sym not in by_symbol:
            by_symbol[sym] = []
        by_symbol[sym].append(r)
    
    for sym, candles in by_symbol.items():
        candles.sort(key=lambda r: int(r["ts"]))
        
        clean_sym = sym.replace("_", "-").replace("/", "-")
        filename = f"crypto_{clean_sym}.csv"
        
        lines = ["time,close,open,high,low,volume\n"]
        
        for r in candles:
            ts_ms = int(r["ts"])
            dt = datetime.fromtimestamp(ts_ms / 1000)
            formatted_time = dt.strftime("%Y-%m-%d %H:%M:%S")
            
            lines.append(
                f"{formatted_time},{r['close']},{r['open']},{r['high']},{r['low']},{r.get('volume', 'N/A')}\n"
            )
        
        with open(filename, "w", encoding="utf-8") as f:
            f.writelines(lines)
    
    print(f"✅ Created {len(by_symbol)} separate files")


if __name__ == "__main__":
    print("=== ⚡ Optimized CoinCatch Downloader ===\n")

    top_n = int(input("Enter number of top cryptos to fetch: "))
    days = int(input("Enter how many days of data you want: "))
    
    print("\n⚙️ Performance Settings:")
    symbol_workers = int(input("Parallel symbols (3-5 recommended): ") or "4")
    batch_workers = int(input("Parallel batches per symbol (3-5 recommended): ") or "4")
    
    print("\n💾 Save Format:")
    print("1️⃣  All mixed (sorted by time)")
    print("2️⃣  Grouped by symbol (each crypto together)")
    print("3️⃣  Separate CSV per crypto")
    save_option = input("Choose (1/2/3): ").strip() or "2"

    print("\n📊 Fetching top symbols...")
    symbols = get_top_symbols(top_n)
    print(f"✅ Top {len(symbols)} symbols loaded")
    print(f"📈 Estimated total candles: {len(symbols) * days * 96}\n")

    combined = []
    start_time = time.time()

    # Download symbols in parallel
    with ThreadPoolExecutor(max_workers=symbol_workers) as executor:
        futures = {
            executor.submit(download_symbol_data, sym, days, batch_workers): sym 
            for sym in symbols
        }
        
        for future in as_completed(futures):
            candles = future.result()
            if candles:
                combined.extend(candles)

    elapsed = time.time() - start_time

    if combined:
        print(f"\n💾 Saving {len(combined)} total candles...")
        
        if save_option == "1":
            save_all_to_csv(combined)
        elif save_option == "2":
            save_grouped_by_symbol(combined)
        elif save_option == "3":
            save_separate_files(combined)
        
        print(f"\n🎉 DONE in {elapsed:.1f} seconds")
        print(f"⚡ Speed: {len(combined)/elapsed:.0f} candles/sec")
        print(f"📊 Average: {elapsed/len(symbols):.1f}s per crypto")
    else:
        print("\n❌ No data was collected.")

  
