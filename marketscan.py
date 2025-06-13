import requests
import pandas as pd
import concurrent.futures
import threading
import itertools
import time
import logging
import os
import asyncio
from telegram import Bot
import random
from tqdm import tqdm
import re
from datetime import datetime, timezone, timedelta
import json
import websocket
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed


# List of symbols to ignore in all scanning
IGNORED_SYMBOLS = {
    "USDPUSDT", "USD1USDT", "TUSDUSDT", "AEURUSDT", "USDCUSDT",
    "ZKJUSDT", "FDUSDUSDT", "XUSDUSDT", "EURUSDT", "EURIUSDT", 
    "USDYUSDT", "USDRUSDT", "ETH3LUSDT", "ETHUSDT-27MAR26", 
    "ETHUSDT-27JUN25", "ETHUSDT-26DEC25", "ETHUSDT-26SEP25"
}

# --- Level Persistence Functions ---

def save_levels_to_file(results, binance_client, bybit_client, filename="detected_levels.json"):
    """Save detected levels to JSON file in repo"""
    levels_data = {
        "last_updated": datetime.now(timezone.utc).isoformat(),
        "levels": {}
    }
    
    for exchange, symbol, market, interval, signal_type in results:
        key = f"{exchange}_{symbol}_{market}_{interval}"
        try:
            # Get the actual price level for this result
            client = binance_client if exchange == "Binance" else bybit_client
            df = client.fetch_ohlcv(symbol, interval, limit=3, market=market)
            equal_price, _ = check_equal_price_and_classify(df)
            
            if equal_price is not None:
                levels_data["levels"][key] = {
                    "exchange": exchange,
                    "symbol": symbol,
                    "market": market,
                    "interval": interval,
                    "price": float(equal_price),
                    "signal_type": signal_type,
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }
        except Exception as e:
            logging.warning(f"Failed to get price for {key}: {e}")
    
    # Save to file
    with open(filename, "w") as f:
        json.dump(levels_data, f, indent=2)
    
    logging.info(f"Saved {len(levels_data['levels'])} levels to {filename}")

def load_levels_from_file(filename="detected_levels.json"):
    """Load previously detected levels from JSON file"""
    try:
        if os.path.exists(filename):
            with open(filename, "r") as f:
                data = json.load(f)
            levels = data.get("levels", {})
            last_updated = data.get("last_updated", "Unknown")
            logging.info(f"Loaded {len(levels)} levels from {last_updated}")
            return levels
        else:
            logging.info("No previous levels file found")
            return {}
    except Exception as e:
        logging.error(f"Failed to load previous levels: {e}")
        return {}

# --- Fast Proxy Selection and Price Collection ---

def test_proxy_speed_for_exchanges(proxy, timeout=5):
    """Test proxy speed for both Binance and Bybit APIs"""
    try:
        proxies = {"http": proxy, "https": proxy}
        
        # Test Binance API speed
        start_time = time.time()
        binance_response = requests.get(
            "https://api.binance.com/api/v3/ping", 
            proxies=proxies, 
            timeout=timeout
        )
        binance_time = time.time() - start_time
        
        if binance_response.status_code != 200:
            return None
        
        # Test Bybit API speed  
        start_time = time.time()
        bybit_response = requests.get(
            "https://api.bybit.com/v5/market/time",
            proxies=proxies,
            timeout=timeout
        )
        bybit_time = time.time() - start_time
        
        if bybit_response.status_code != 200:
            return None
            
        # Return average response time
        avg_time = (binance_time + bybit_time) / 2
        return {
            "proxy": proxy,
            "avg_response_time": avg_time,
            "binance_time": binance_time,
            "bybit_time": bybit_time
        }
        
    except Exception as e:
        return None

def find_fastest_working_proxy(proxy_pool, max_test=10):
    """Find the fastest working proxy for both exchanges"""
    proxies_to_test = proxy_pool.proxies[:max_test]  # Test first 10 proxies
    
    if not proxies_to_test:
        logging.error("No proxies available to test")
        return None
    
    logging.info(f"Testing {len(proxies_to_test)} proxies for speed...")
    
    results = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_proxy = {
            executor.submit(test_proxy_speed_for_exchanges, proxy): proxy 
            for proxy in proxies_to_test
        }
        
        for future in as_completed(future_to_proxy):
            result = future.result()
            if result:
                results.append(result)
    
    if not results:
        logging.error("No working proxies found for exchanges")
        return None
    
    # Sort by average response time (fastest first)
    results.sort(key=lambda x: x['avg_response_time'])
    fastest = results[0]
    
    logging.info(f"Fastest proxy: {fastest['proxy']} (avg: {fastest['avg_response_time']:.2f}s)")
    return fastest['proxy']

def get_binance_prices_batch(symbols, proxy, batch_size=100):
    """Get Binance prices in batches using ticker/24hr endpoint"""
    prices = {}
    if not symbols:
        return prices
    
    proxies = {"http": proxy, "https": proxy}
    
    # Binance allows getting all tickers at once
    try:
        url = "https://api.binance.com/api/v3/ticker/price"
        response = requests.get(url, proxies=proxies, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            symbol_set = set(symbols)
            
            for item in data:
                symbol = item['symbol'].lower()
                if symbol in symbol_set:
                    prices[f"binance_{symbol.upper()}"] = float(item['price'])
        else:
            logging.warning(f"Binance price request failed: {response.status_code}")
            
    except Exception as e:
        logging.warning(f"Failed to get Binance prices: {e}")
    
    return prices

def get_bybit_prices_batch(symbols, proxy):
    """Get Bybit prices using tickers endpoint"""
    prices = {}
    if not symbols:
        return prices
    
    proxies = {"http": proxy, "https": proxy}
    
    try:
        # Get all linear tickers
        url = "https://api.bybit.com/v5/market/tickers"
        params = {"category": "linear"}
        response = requests.get(url, params=params, proxies=proxies, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            if "result" in data and "list" in data["result"]:
                symbol_set = set(symbols)
                
                for item in data["result"]["list"]:
                    symbol = item['symbol'].upper()
                    if symbol in symbol_set:
                        prices[f"bybit_{symbol}"] = float(item['lastPrice'])
        else:
            logging.warning(f"Bybit price request failed: {response.status_code}")
            
    except Exception as e:
        logging.warning(f"Failed to get Bybit prices: {e}")
    
    return prices

def collect_all_prices_fast(levels, proxy_pool, timeout=10):
    """Fast price collection using REST APIs with the fastest proxy"""
    # Find the fastest working proxy
    fastest_proxy = find_fastest_working_proxy(proxy_pool)
    if not fastest_proxy:
        logging.error("No working proxy found")
        return {}
    
    # Separate symbols by exchange
    binance_symbols = []
    bybit_symbols = []
    
    for level_info in levels.values():
        exchange = level_info["exchange"].lower()
        symbol = level_info["symbol"]
        
        if exchange == "binance":
            binance_symbols.append(symbol.lower())
        elif exchange == "bybit":
            bybit_symbols.append(symbol.upper())
    
    logging.info(f"Collecting prices for {len(binance_symbols)} Binance + {len(bybit_symbols)} Bybit symbols...")
    
    start_time = time.time()
    all_prices = {}
    
    # Get prices from both exchanges concurrently
    with ThreadPoolExecutor(max_workers=2) as executor:
        binance_future = executor.submit(get_binance_prices_batch, binance_symbols, fastest_proxy)
        bybit_future = executor.submit(get_bybit_prices_batch, bybit_symbols, fastest_proxy)
        
        # Collect results
        binance_prices = binance_future.result()
        bybit_prices = bybit_future.result()
        
        all_prices.update(binance_prices)
        all_prices.update(bybit_prices)
    
    elapsed_time = time.time() - start_time
    logging.info(f"Collected {len(all_prices)} prices in {elapsed_time:.1f}s using fastest proxy")
    
    return all_prices

def check_level_hits_fast(levels, proxy_pool):
    """Fast level hit checking using REST APIs"""
    current_prices = collect_all_prices_fast(levels, proxy_pool)
    hits = []
    
    for key, level_info in levels.items():
        try:
            exchange = level_info["exchange"].lower()
            symbol = level_info["symbol"]
            price_key = f"{exchange}_{symbol}"
            
            if price_key in current_prices:
                current_price = current_prices[price_key]
                level_price = level_info["price"]
                
                # Check if current price is very close to level (within 0.2% tolerance)
                tolerance = level_price * 0.002  # 0.2% tolerance
                if abs(current_price - level_price) <= tolerance:
                    hits.append({
                        "exchange": level_info["exchange"],
                        "symbol": symbol,
                        "market": level_info["market"],
                        "interval": level_info["interval"],
                        "signal_type": level_info["signal_type"],
                        "level_price": level_price,
                        "current_price": current_price,
                        "original_timestamp": level_info["timestamp"]
                    })
                    
        except Exception as e:
            logging.warning(f"Failed to check level hit for {key}: {e}")
    
    return hits

def create_alerts_telegram_report(hits):
    """Create telegram message for level hits/alerts"""
    if not hits:
        return []
    
    # Get timestamp
    utc_now = datetime.now(timezone.utc)
    utc_plus_3 = timezone(timedelta(hours=3))
    current_time = utc_now.astimezone(utc_plus_3)
    timestamp = current_time.strftime("%Y-%m-%d %H:%M:%S UTC+3")
    
    # Group by exchange and timeframe
    exchanges = {}
    for hit in hits:
        exchange = hit["exchange"]
        interval = hit["interval"]
        
        if exchange not in exchanges:
            exchanges[exchange] = {}
        if interval not in exchanges[exchange]:
            exchanges[exchange][interval] = {"bullish": [], "bearish": []}
        
        exchanges[exchange][interval][hit["signal_type"]].append(hit)
    
    messages = []
    
    # Create header message
    header = "🚨 *LEVEL ALERTS* 🚨\n\n"
    header += f"⚡ {len(hits)} levels got hit!\n\n"
    header += f"🕒 {timestamp}"
    messages.append(header)
    
    # Create detailed messages
    for exchange in ["Binance", "Bybit"]:
        if exchange not in exchanges:
            continue
            
        for interval in ["1M", "1w", "1d"]:
            if interval not in exchanges[exchange]:
                continue
                
            current_msg = f"📅 *{interval} - {exchange}*\n\n"
            
            bullish_hits = exchanges[exchange][interval]["bullish"]
            bearish_hits = exchanges[exchange][interval]["bearish"]
            
            if bullish_hits:
                current_msg += f"🍏 *Bullish Alerts ({len(bullish_hits)})*\n"
                for hit in bullish_hits:
                    price_diff = abs(hit['current_price'] - hit['level_price'])
                    price_diff_pct = (price_diff / hit['level_price']) * 100
                    current_msg += f"• {hit['symbol']} @ ${hit['level_price']:.6f} (Current: ${hit['current_price']:.6f})\n"
                current_msg += "\n"
            
            if bearish_hits:
                current_msg += f"🔻 *Bearish Alerts ({len(bearish_hits)})*\n"
                for hit in bearish_hits:
                    price_diff = abs(hit['current_price'] - hit['level_price'])
                    price_diff_pct = (price_diff / hit['level_price']) * 100
                    current_msg += f"• {hit['symbol']} @ ${hit['level_price']:.6f} (Current: ${hit['current_price']:.6f})\n"
                current_msg += "\n"
            
            if bullish_hits or bearish_hits:
                current_msg += "——————————————————\n"
                messages.append(current_msg)
    
    return messages

# --- Normalization and Priority Functions ---

def normalize_symbol(symbol: str) -> str:
    """
    Normalize symbol to CCXT style 'BASE/QUOTE' format.
    Removes suffixes like ':USDT' from futures symbols.
    """
    symbol = symbol.upper()
    if ':' in symbol:
        symbol = symbol.split(':')[0]
    if '/' in symbol:
        return symbol
    else:
        base = symbol[:-4]
        quote = symbol[-4:]
        return f"{base}/{quote}"

def apply_priority(binance_perp, binance_spot, bybit_perp, bybit_spot):
    """
    Deduplicate symbols across exchanges by priority:
    1) Binance perps
    2) Binance spots (excluding symbols already in Binance perps)
    3) Bybit perps (excluding symbols in Binance perps + spots)
    4) Bybit spots (excluding symbols in all above)
    Only symbols with USDT quote are kept.
    """

    # Normalize all symbols to 'BASE/QUOTE' and filter for USDT only
    binance_perp_norm = {normalize_symbol(s): s for s in binance_perp if s.endswith('USDT')}
    binance_spot_norm = {normalize_symbol(s): s for s in binance_spot if s.endswith('USDT')}
    bybit_perp_norm = {normalize_symbol(s): s for s in bybit_perp if s.endswith('USDT')}
    bybit_spot_norm = {normalize_symbol(s): s for s in bybit_spot if s.endswith('USDT')}

    # Step 1: Binance perps (highest priority) - keep all
    binance_perp_set = set(binance_perp_norm.keys())

    # Step 2: Binance spots - exclude symbols already in Binance perps
    binance_spot_set = set(binance_spot_norm.keys()) - binance_perp_set

    # Step 3: Bybit perps - exclude symbols in Binance perps + spots
    bybit_perp_set = set(bybit_perp_norm.keys()) - binance_perp_set - binance_spot_set

    # Step 4: Bybit spots - exclude symbols in all above three sets
    bybit_spot_set = set(bybit_spot_norm.keys()) - binance_perp_set - binance_spot_set - bybit_perp_set

    # Map back to original symbols (to preserve original naming)
    final_binance_perp = {binance_perp_norm[s] for s in binance_perp_set}
    final_binance_spot = {binance_spot_norm[s] for s in binance_spot_set}
    final_bybit_perp = {bybit_perp_norm[s] for s in bybit_perp_set}
    final_bybit_spot = {bybit_spot_norm[s] for s in bybit_spot_set}

    logging.info(f"After priority deduplication:")
    logging.info(f"  Binance Perps: {len(final_binance_perp)}")
    logging.info(f"  Binance Spots: {len(final_binance_spot)}")
    logging.info(f"  Bybit Perps: {len(final_bybit_perp)}")
    logging.info(f"  Bybit Spots: {len(final_bybit_spot)}")

    return final_binance_perp, final_binance_spot, final_bybit_perp, final_bybit_spot

# --- Proxy system ---

def fetch_proxies_from_url(url: str, default_scheme: str = "http") -> list:
    proxies = []
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        lines = response.text.strip().splitlines()
        for line in lines:
            proxy = line.strip()
            if not proxy:
                continue
            if "://" in proxy:
                proxies.append(proxy)
            else:
                proxies.append(f"{default_scheme}://{proxy}")
        logging.info("Fetched %d proxies from %s", len(proxies), url)
    except requests.exceptions.RequestException as e:
        logging.error("Error fetching proxies from URL %s: %s", url, e)
    return proxies

def test_proxy(proxy: str, timeout=5) -> bool:
    test_url = "https://api.binance.com/api/v3/time"
    try:
        response = requests.get(test_url, proxies={"http": proxy, "https": proxy}, timeout=timeout)
        return 200 <= response.status_code < 300
    except Exception:
        return False

def test_proxies_concurrently(proxies: list, max_workers: int = 50, max_working: int = 20) -> list:
    working = []
    tested = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(test_proxy, proxy): proxy for proxy in proxies}
        try:
            for future in concurrent.futures.as_completed(futures):
                tested += 1
                proxy = futures[future]
                if future.result():
                    working.append(proxy)
                    if len(working) % 5 == 0:
                        logging.info(f"Proxy check: Tested {tested} | Working: {len(working)} | Dead: {tested - len(working)}")
                    if len(working) >= max_working:
                        break
        finally:
            if len(working) >= max_working:
                for f in futures:
                    f.cancel()
    logging.info(f"Found {len(working)} working proxies (tested {tested})")
    return working[:max_working]

class ProxyPool:
    def __init__(self, max_pool_size=25, proxy_check_interval=600, max_failures=3):
        self.lock = threading.Lock()
        self.max_pool_size = max_pool_size
        self.proxy_check_interval = proxy_check_interval
        self.max_failures = max_failures

        self.proxies = []
        self.proxy_failures = {}
        self.failed_proxies = set()
        self.proxy_cycle = None
        self._stop_event = threading.Event()

    def get_next_proxy(self):
        with self.lock:
            if not self.proxies:
                logging.warning("Proxy pool empty when requesting next proxy.")
                return None
            for _ in range(len(self.proxies)):
                proxy = next(self.proxy_cycle)
                if proxy not in self.failed_proxies:
                    return proxy
            logging.warning("All proxies in pool are marked as failed.")
            return None

    def mark_proxy_failure(self, proxy):
        if proxy is None:
            return
        with self.lock:
            count = self.proxy_failures.get(proxy, 0) + 1
            self.proxy_failures[proxy] = count
            logging.warning(f"Proxy {proxy} failure count: {count}/{self.max_failures}")
            if count >= self.max_failures:
                self.failed_proxies.add(proxy)
                if proxy in self.proxies:
                    self.proxies.remove(proxy)
                    logging.warning(f"Proxy {proxy} removed from pool due to repeated failures.")
                self.proxy_cycle = itertools.cycle(self.proxies) if self.proxies else None

    def populate_from_url(self, url: str, default_scheme: str = "http"):
        new_proxies = fetch_proxies_from_url(url, default_scheme)
        working = test_proxies_concurrently(new_proxies, max_working=self.max_pool_size)
        with self.lock:
            self.proxies = working[:self.max_pool_size]
            self.proxy_failures.clear()
            self.failed_proxies.clear()
            self.proxy_cycle = itertools.cycle(self.proxies) if self.proxies else None
            logging.info(f"Proxy pool filled with {len(self.proxies)} proxies from URL.")

    def start_checker(self):
        thread = threading.Thread(target=self.proxy_checker_loop, daemon=True)
        thread.start()

    def proxy_checker_loop(self):
        while not self._stop_event.is_set():
            logging.info("Running proxy pool health check...")
            self.check_proxies()
            self._stop_event.wait(self.proxy_check_interval)

    def check_proxies(self):
        working = test_proxies_concurrently(self.proxies, max_workers=50, max_working=len(self.proxies))
        with self.lock:
            initial_count = len(self.proxies)
            self.proxies = working
            removed = initial_count - len(working)
            self.proxy_failures = {p: self.proxy_failures.get(p, 0) for p in self.proxies}
            self.failed_proxies = set()
            self.proxy_cycle = itertools.cycle(self.proxies) if self.proxies else None
        if removed > 0:
            logging.info(f"Removed {removed} dead proxies during check.")
            self.populate_to_max()

    def populate_to_max(self):
        with self.lock:
            needed = self.max_pool_size - len(self.proxies)
            if needed <= 0:
                return
        new_proxies = self.get_new_proxies(needed)
        with self.lock:
            self.proxies.extend(new_proxies)
            self.proxy_cycle = itertools.cycle(self.proxies)
            logging.info(f"Proxy pool populated to max size: {len(self.proxies)}/{self.max_pool_size}")

    def get_new_proxies(self, count: int) -> list:
        backup_url = "https://raw.githubusercontent.com/ErcinDedeoglu/proxies/main/proxies/https.txt"
        new_proxies = fetch_proxies_from_url(backup_url)
        working = test_proxies_concurrently(new_proxies, max_working=count)
        return working

    def stop(self):
        self._stop_event.set()

# --- Binance Client ---

class BinanceClient:
    def __init__(self, proxy_pool: ProxyPool, max_retries=5, retry_delay=2):
        self.proxy_pool = proxy_pool
        self.max_retries = max_retries
        self.retry_delay = retry_delay

    def _get_proxy_dict(self):
        proxy = self.proxy_pool.get_next_proxy()
        if proxy is None:
            raise RuntimeError("No working proxies available")
        return {"http": proxy, "https": proxy}

    def get_perp_symbols(self):
        url = 'https://fapi.binance.com/fapi/v1/exchangeInfo'
        for attempt in range(1, self.max_retries + 1):
            proxy = self.proxy_pool.get_next_proxy()
            if proxy is None:
                logging.error("No proxies available to fetch perp symbols")
                time.sleep(self.retry_delay)
                continue
            proxies = {"http": proxy, "https": proxy}
            try:
                resp = requests.get(url, proxies=proxies, timeout=10)
                resp.raise_for_status()
                data = resp.json()
                symbols = [
                    s['symbol'] for s in data['symbols']
                    if s.get('contractType') == 'PERPETUAL' and
                       s['status'] == 'TRADING' and
                       s.get('quoteAsset') == 'USDT'
                ]
                if symbols:
                    logging.info(f"Fetched {len(symbols)} perp USDT symbols successfully.")
                    return symbols
                else:
                    logging.warning(f"Attempt {attempt}: No perp USDT symbols returned, retrying...")
            except requests.exceptions.RequestException as e:
                logging.warning(f"Attempt {attempt} failed with proxy {proxy}: {e}")
                self.proxy_pool.mark_proxy_failure(proxy)
            time.sleep(self.retry_delay * attempt + random.uniform(0, 1))
        logging.error("All retries failed to fetch perp symbols")
        return []

    def get_spot_symbols(self):
        url = 'https://api.binance.com/api/v3/exchangeInfo'
        for attempt in range(1, self.max_retries + 1):
            try:
                proxies = self._get_proxy_dict()
                resp = requests.get(url, proxies=proxies, timeout=10)
                resp.raise_for_status()
                data = resp.json()
                spot_symbols = [
                    s['symbol']
                    for s in data['symbols']
                    if s['status'] == 'TRADING' and s.get('quoteAsset') == 'USDT'
                ]
                if spot_symbols:
                    logging.info(f"Fetched {len(spot_symbols)} spot USDT symbols successfully.")
                    return spot_symbols
                else:
                    logging.warning(f"Attempt {attempt}: No spot USDT symbols returned, retrying...")
            except Exception as e:
                logging.warning(f"Attempt {attempt} failed to fetch spot symbols: {e}")
            time.sleep(self.retry_delay * attempt + random.uniform(0, 1))
        logging.error("All retries failed to fetch spot symbols")
        return []

    def fetch_ohlcv(self, symbol, interval, limit=100, market="spot"):
        if market == "spot":
            url = 'https://api.binance.com/api/v3/klines'
        elif market == "perp":
            url = 'https://fapi.binance.com/fapi/v1/klines'
        else:
            raise ValueError(f"Unknown market type: {market}")

        params = {'symbol': symbol, 'interval': interval, 'limit': limit}
        attempt = 1
        max_proxy_refresh_attempts = 3
        proxy_refresh_attempts = 0
        proxies = None
        while attempt <= self.max_retries:
            try:
                proxies = self._get_proxy_dict()
                resp = requests.get(url, params=params, proxies=proxies, timeout=10)
                resp.raise_for_status()
                data = resp.json()
                df = pd.DataFrame(data, columns=[
                    'openTime', 'open', 'high', 'low', 'close', 'volume',
                    'closeTime', 'quoteAssetVolume', 'numberOfTrades',
                    'takerBuyBaseAssetVolume', 'takerBuyQuoteAssetVolume', 'ignore'
                ])
                df[['high', 'low', 'close', 'open', 'volume']] = df[['high', 'low', 'close', 'open', 'volume']].astype(float)
                
                # Reverse to match Bybit's newest-to-oldest order
                df = df.iloc[::-1].reset_index(drop=True)
                
                return df
            except RuntimeError as e:
                if "No working proxies available" in str(e):
                    proxy_refresh_attempts += 1
                    if proxy_refresh_attempts > max_proxy_refresh_attempts:
                        logging.error("Max proxy refresh attempts reached, aborting fetch_ohlcv")
                        raise
                    logging.warning("No working proxies available, refreshing proxy pool and retrying...")
                    proxy_url = os.getenv("PROXY_LIST_URL")
                    if proxy_url:
                        self.proxy_pool.populate_from_url(proxy_url)
                    else:
                        logging.error("PROXY_LIST_URL not set, cannot refresh proxies")
                        raise
                    time.sleep(5)
                    continue
                else:
                    raise
            except Exception as e:
                logging.warning(f"Attempt {attempt} failed fetching OHLCV for {symbol} ({market}) {interval}: {e}")
                if proxies:
                    self.proxy_pool.mark_proxy_failure(proxies.get('http'))
            time.sleep(self.retry_delay * attempt + random.uniform(0, 1))
            attempt += 1
        logging.error(f"All retries failed fetching OHLCV for {symbol} ({market}) {interval}")
        raise RuntimeError(f"Failed to fetch OHLCV for {symbol} ({market}) {interval}")

# --- Bybit Client ---

class BybitClient:
    def __init__(self, proxy_pool: ProxyPool, max_retries=5, retry_delay=2):
        self.proxy_pool = proxy_pool
        self.max_retries = max_retries
        self.retry_delay = retry_delay

    def _get_proxy_dict(self):
        proxy = self.proxy_pool.get_next_proxy()
        if proxy is None:
            raise RuntimeError("No working proxies available")
        return {"http": proxy, "https": proxy}

    def get_perp_symbols(self):
        url = 'https://api.bybit.com/v5/market/instruments-info'
        params = {'category': 'linear'}
        for attempt in range(1, self.max_retries + 1):
            try:
                proxies = self._get_proxy_dict()
                resp = requests.get(url, params=params, proxies=proxies, timeout=10)
                resp.raise_for_status()
                data = resp.json()
                symbols = [
                    s['symbol'] for s in data['result']['list']
                    if s['status'] == 'Trading' and s['quoteCoin'] == 'USDT'
                ]
                if symbols:
                    logging.info(f"Fetched {len(symbols)} Bybit perp USDT symbols successfully.")
                    return symbols
                else:
                    logging.warning(f"Attempt {attempt}: No Bybit perp USDT symbols returned, retrying...")
            except Exception as e:
                logging.warning(f"Attempt {attempt} failed to fetch Bybit perp symbols: {e}")
                self.proxy_pool.mark_proxy_failure(proxies.get('http') if 'proxies' in locals() else None)
            time.sleep(self.retry_delay * attempt + random.uniform(0, 1))
        logging.error("All retries failed to fetch Bybit perp symbols")
        return []

    def get_spot_symbols(self):
        url = 'https://api.bybit.com/v5/market/instruments-info'
        params = {'category': 'spot'}
        for attempt in range(1, self.max_retries + 1):
            try:
                proxies = self._get_proxy_dict()
                resp = requests.get(url, params=params, proxies=proxies, timeout=10)
                resp.raise_for_status()
                data = resp.json()
                symbols = [
                    s['symbol'] for s in data['result']['list']
                    if s['status'] == 'Trading' and s['quoteCoin'] == 'USDT'
                ]
                if symbols:
                    logging.info(f"Fetched {len(symbols)} Bybit spot USDT symbols successfully.")
                    return symbols
                else:
                    logging.warning(f"Attempt {attempt}: No Bybit spot USDT symbols returned, retrying...")
            except Exception as e:
                logging.warning(f"Attempt {attempt} failed to fetch Bybit spot symbols: {e}")
                self.proxy_pool.mark_proxy_failure(proxies.get('http') if 'proxies' in locals() else None)
            time.sleep(self.retry_delay * attempt + random.uniform(0, 1))
        logging.error("All retries failed to fetch Bybit spot symbols")
        return []

    def fetch_ohlcv(self, symbol, interval, limit=100, market="spot"):
        url = 'https://api.bybit.com/v5/market/kline'
        category = 'linear' if market == 'perp' else 'spot'

        interval_map = {
            "1M": "M",
            "1w": "W",
            "1d": "D"
        }
        bybit_interval = interval_map.get(interval)
        if bybit_interval is None:
            raise ValueError(f"Unsupported interval {interval} for Bybit")

        params = {
            'category': category,
            'symbol': symbol,
            'interval': bybit_interval,
            'limit': limit
        }
        attempt = 1
        max_proxy_refresh_attempts = 3
        proxy_refresh_attempts = 0
        proxies = None
        while attempt <= self.max_retries:
            try:
                proxies = self._get_proxy_dict()
                resp = requests.get(url, params=params, proxies=proxies, timeout=10)
                resp.raise_for_status()
                data = resp.json()
                klines = data['result']['list']
                if not klines:
                    raise RuntimeError(f"No kline data for {symbol} {interval} {market}")
                df = pd.DataFrame(klines, columns=['openTime', 'open', 'high', 'low', 'close', 'volume', 'turnover'])
                df[['high', 'low', 'close', 'open', 'volume']] = df[['high', 'low', 'close', 'open', 'volume']].astype(float)
                return df
            except RuntimeError as e:
                if "No working proxies available" in str(e):
                    proxy_refresh_attempts += 1
                    if proxy_refresh_attempts > max_proxy_refresh_attempts:
                        logging.error("Max proxy refresh attempts reached, aborting fetch_ohlcv")
                        raise
                    logging.warning("No working proxies available, refreshing proxy pool and retrying...")
                    proxy_url = os.getenv("PROXY_LIST_URL")
                    if proxy_url:
                        self.proxy_pool.populate_from_url(proxy_url)
                    else:
                        logging.error("PROXY_LIST_URL not set, cannot refresh proxies")
                        raise
                    time.sleep(5)
                    continue
                else:
                    raise
            except Exception as e:
                logging.warning(f"Attempt {attempt} failed fetching Bybit OHLCV for {symbol} ({market}) {interval}: {e}")
                if proxies:
                    self.proxy_pool.mark_proxy_failure(proxies.get('http'))
            time.sleep(self.retry_delay * attempt + random.uniform(0, 1))
            attempt += 1
        logging.error(f"All retries failed fetching Bybit OHLCV for {symbol} ({market}) {interval}")
        raise RuntimeError(f"Failed to fetch OHLCV for {symbol} ({market}) {interval}")

# --- Helper functions ---

def floats_are_equal(a, b, tol=1e-8):
    return abs(a - b) < tol

def check_equal_price_and_classify(df):
    """Check if last two CLOSED candles have equal close=open AND opposite colors for reversal signal"""
    if df.empty or len(df) < 3:
        return None, None
    
    # API returns newest to oldest
    # df.iloc[0] = Current candle (ignore)
    # df.iloc[1] = Last closed candle
    # df.iloc[2] = Second to last closed candle
    
    second_last_closed = df.iloc[2]  # Older candle
    last_closed = df.iloc[1]         # Newer candle
    
    # Extract values
    second_last_open = float(second_last_closed['open'])
    second_last_close = float(second_last_closed['close'])
    last_open = float(last_closed['open'])
    last_close = float(last_closed['close'])
    
    # Determine candle colors
    second_last_is_green = second_last_close > second_last_open
    second_last_is_red = second_last_close < second_last_open
    last_is_green = last_close > last_open
    last_is_red = last_close < last_open
    
    # Check if second_last_closed['close'] == last_closed['open']
    if floats_are_equal(second_last_close, last_open):
        equal_price = second_last_close
        
        # BULLISH: RED candle followed by GREEN candle (reversal pattern)
        if second_last_is_red and last_is_green:
            return equal_price, "bullish"
        
        # BEARISH: GREEN candle followed by RED candle (reversal pattern)  
        elif second_last_is_green and last_is_red:
            return equal_price, "bearish"
        
        # FILTERED OUT: Same color candles (no reversal signal)
        # This eliminates continuation patterns and keeps only reversals
    
    return None, None

def current_candle_touched_price(df, price):
    if df.empty or len(df) < 3:
        return False
    # Current candle is df.iloc[0] (newest/live)
    current_candle = df.iloc[0]
    return float(current_candle['low']) <= price <= float(current_candle['high'])

def create_beautiful_telegram_report(results):
    """Create clean telegram report with bullet points, minimal parts"""
    
    if not results:
        return ["💥 *Reversal Level Scanner*\n\n❌ No qualifying reversal patterns found at this time."]
    
    # Organize by timeframe first
    timeframes = {}
    for exchange, symbol, market, interval, signal_type in results:
        if interval not in timeframes:
            timeframes[interval] = {"Binance": {"bullish": [], "bearish": []}, "Bybit": {"bullish": [], "bearish": []}}
        timeframes[interval][exchange][signal_type].append(symbol)
    
    # Get timestamp
    from datetime import datetime, timezone, timedelta
    utc_now = datetime.now(timezone.utc)
    utc_plus_3 = timezone(timedelta(hours=3))
    current_time = utc_now.astimezone(utc_plus_3)
    timestamp = current_time.strftime("%Y-%m-%d %H:%M:%S UTC+3")
    
    # Count totals
    total_signals = len(results)
    binance_count = len([r for r in results if r[0] == "Binance"])
    bybit_count = len([r for r in results if r[0] == "Bybit"])
    
    messages = []
    
    # Summary message
    summary = "💥 *Reversal Level Scanner*\n\n"
    summary += f"✅ Total Signals: {total_signals}\n\n"
    summary += f"*Binance*: {binance_count} | *Bybit*: {bybit_count}\n\n"
    summary += f"🕒 {timestamp}"
    messages.append(summary)
    
    # Process each timeframe
    timeframe_order = ["1M", "1w", "1d"]
    for timeframe in timeframe_order:
        if timeframe not in timeframes:
            continue
            
        tf_data = timeframes[timeframe]
        tf_total = sum(len(tf_data[ex][st]) for ex in tf_data for st in tf_data[ex])
        
        if tf_total == 0:
            continue
        
        # Start building timeframe message
        current_msg = f"📅 *{timeframe} Timeframe* ({tf_total} signals)\n\n"
        
        # Process each exchange
        for exchange in ["Binance", "Bybit"]:
            exchange_bullish = sorted(tf_data[exchange]["bullish"])
            exchange_bearish = sorted(tf_data[exchange]["bearish"])
            
            if not exchange_bullish and not exchange_bearish:
                continue
                
            current_msg += f"*{exchange}*:\n"
            
            # Add bullish symbols
            if exchange_bullish:
                section_start = f"🍏 *Bullish ({len(exchange_bullish)})*\n"
                
                # Check if we need to split
                estimated_length = len(current_msg) + len(section_start) + (len(exchange_bullish) * 15)  # ~15 chars per symbol line
                
                if estimated_length > 3500:
                    # Start new message for this section
                    messages.append(current_msg.strip())
                    current_msg = f"📅 *{timeframe} - {exchange} Bullish*\n\n"
                
                current_msg += section_start
                for symbol in exchange_bullish:
                    current_msg += f"• {symbol}\n"
                current_msg += "\n"
            
            # Add bearish symbols
            if exchange_bearish:
                section_start = f"🔻 *Bearish ({len(exchange_bearish)})*\n"
                
                # Check if we need to split
                estimated_length = len(current_msg) + len(section_start) + (len(exchange_bearish) * 15)
                
                if estimated_length > 3500:
                    # Start new message for this section
                    messages.append(current_msg.strip())
                    current_msg = f"📅 *{timeframe} - {exchange} Bearish*\n\n"
                
                current_msg += section_start
                for symbol in exchange_bearish:
                    current_msg += f"• {symbol}\n"
                current_msg += "\n"
            
            current_msg += "——————————————————\n\n"
        
        # Add the completed timeframe message
        if current_msg.strip():
            messages.append(current_msg.strip())
    
    return messages

# --- Main async scanning and reporting ---

async def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    
    # Determine run mode
    run_mode = os.getenv("RUN_MODE", "full_scan")
    logging.info(f"Running in {run_mode} mode")
    
    TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
    TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
    
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        logging.error("Telegram environment variables not fully set")
        return
    
    bot = Bot(token=TELEGRAM_TOKEN)
    
    if run_mode == "price_check":
        # QUICK MODE: Only check if levels got hit using fast REST API approach
        logging.info("🔍 Quick price check mode - checking level hits via fast REST API...")
    
        levels = load_levels_from_file()
        if not levels:
            logging.info("No levels to check, skipping...")
            return
    
        # Create a small proxy pool for price checking
        proxy_url = os.getenv("PROXY_LIST_URL")
        if proxy_url:
            proxy_pool = ProxyPool(max_pool_size=5)  # Small pool for quick checks
            proxy_pool.populate_from_url(proxy_url)
        
            hits = check_level_hits_fast(levels, proxy_pool)
        
            if hits:
                logging.info(f"🚨 Found {len(hits)} level hits!")
                alert_messages = create_alerts_telegram_report(hits)
                for msg in alert_messages:
                    try:
                        await bot.send_message(chat_id=int(TELEGRAM_CHAT_ID), text=msg, parse_mode='Markdown')
                        await asyncio.sleep(0.5)  # Rate limiting
                    except Exception as e:
                        logging.error(f"Failed to send alert: {e}")
            else:
                logging.info("✅ No level hits detected")
        else:
            logging.error("PROXY_LIST_URL not set for price check mode")

    else:
        # FULL SCAN MODE: Do complete pattern detection
        logging.info("🔍 Full scan mode - performing complete pattern detection...")
        
        proxy_url = os.getenv("PROXY_LIST_URL")
        if not proxy_url:
            logging.error("PROXY_LIST_URL environment variable not set")
            return

        proxy_pool = ProxyPool(max_pool_size=25)
        proxy_pool.populate_from_url(proxy_url)
        proxy_pool.start_checker()

        binance_client = BinanceClient(proxy_pool)
        bybit_client = BybitClient(proxy_pool)

        binance_perp_raw = set(binance_client.get_perp_symbols())
        binance_spot_raw = set(binance_client.get_spot_symbols())
        bybit_perp_raw = set(bybit_client.get_perp_symbols())
        bybit_spot_raw = set(bybit_client.get_spot_symbols())

        # Apply priority deduplication with USDT filtering
        binance_perp, binance_spot, bybit_perp, bybit_spot = apply_priority(
            binance_perp_raw, binance_spot_raw, bybit_perp_raw, bybit_spot_raw
        )

        # Apply ignored symbols filter
        final_binance_perp = {s for s in binance_perp if s not in IGNORED_SYMBOLS}
        final_binance_spot = {s for s in binance_spot if s not in IGNORED_SYMBOLS}
        final_bybit_perp = {s for s in bybit_perp if s not in IGNORED_SYMBOLS}
        final_bybit_spot = {s for s in bybit_spot if s not in IGNORED_SYMBOLS}

        total_symbols = len(final_binance_perp) + len(final_binance_spot) + len(final_bybit_perp) + len(final_bybit_spot)
        logging.info(f"Total symbols to scan after priority deduplication: {total_symbols}")

        results = []
        lock = threading.Lock()

        def scan_symbol(exchange_name, client, symbol, perp_set, spot_set):
            market = "perp" if symbol in perp_set else "spot"
            for interval in ["1M", "1w", "1d"]:
                try:
                    df = client.fetch_ohlcv(symbol, interval, limit=3, market=market)
                    equal_price, signal_type = check_equal_price_and_classify(df)
                
                    if equal_price is not None and signal_type is not None:
                        if not current_candle_touched_price(df, equal_price):
                            with lock:
                                results.append((exchange_name, symbol, market, interval, signal_type))
                except Exception as e:
                    logging.warning(f"Failed {exchange_name} {symbol} {market} {interval}: {e}")

        all_symbols = []
        all_symbols.extend([("Binance", binance_client, sym, binance_perp_raw, binance_spot_raw) for sym in final_binance_perp])
        all_symbols.extend([("Binance", binance_client, sym, binance_perp_raw, binance_spot_raw) for sym in final_binance_spot])
        all_symbols.extend([("Bybit", bybit_client, sym, bybit_perp_raw, bybit_spot_raw) for sym in final_bybit_perp])
        all_symbols.extend([("Bybit", bybit_client, sym, bybit_perp_raw, bybit_spot_raw) for sym in final_bybit_spot])

        with concurrent.futures.ThreadPoolExecutor(max_workers=25) as executor:
            futures = [executor.submit(scan_symbol, *args) for args in all_symbols]
            for _ in tqdm(concurrent.futures.as_completed(futures), total=len(futures), desc="Scanning symbols"):
                pass

        # Send normal scanning results
        messages = create_beautiful_telegram_report(results)
        for msg in messages:
            try:
                await bot.send_message(chat_id=int(TELEGRAM_CHAT_ID), text=msg, parse_mode='Markdown')
                await asyncio.sleep(1)  # Rate limiting
            except Exception as e:
                logging.error(f"Failed to send Telegram message: {e}")

        # Save current results for next run
        save_levels_to_file(results, binance_client, bybit_client)
        logging.info("Saved current levels for next run")

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
