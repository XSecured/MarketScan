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


# Global debug counters
binance_scanned = 0
binance_price_matches = 0
binance_same_color_filtered = 0
binance_touched_filtered = 0
total_scans = 0
successful_scans = 0
failed_scans = 0
binance_added_signals = 0
bybit_added_signals = 0



# List of symbols to ignore in all scanning
IGNORED_SYMBOLS = {
    "USDPUSDT", "USD1USDT", "TUSDUSDT", "AEURUSDT", "USDCUSDT",
    "ZKJUSDT", "FDUSDUSDT", "XUSDUSDT", "EURUSDT", "EURIUSDT", 
    "USDYUSDT", "USDRUSDT", "ETH3LUSDT", "ETHUSDT-27MAR26", 
    "ETHUSDT-27JUN25", "ETHUSDT-26DEC25", "ETHUSDT-26SEP25"
}

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

def deduplicate_symbols(perp_list, spot_list):
    spot_only = set(spot_list) - set(perp_list)
    all_symbols = list(perp_list) + list(spot_only)
    return [s for s in all_symbols if s not in IGNORED_SYMBOLS]

def floats_are_equal(a, b, tol=1e-8):
    return abs(a - b) < tol

def check_equal_price_and_classify(df):
    """Check if last two CLOSED candles have equal close=open AND opposite colors for reversal signal"""
    if df.empty or len(df) < 3:
        return None, None
    
    # CORRECTED: API returns newest to oldest
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
    
# --- Main async scanning and reporting ---

async def main():
    # Enhanced logging configuration
    logging.basicConfig(
        level=logging.INFO, 
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('scanner_debug.log', mode='w')
        ]
    )
    
    logging.info("🚀 Starting Reversal Level Scanner")

    # Check environment variables
    proxy_url = os.getenv("PROXY_LIST_URL")
    if not proxy_url:
        logging.error("❌ PROXY_LIST_URL environment variable not set")
        return

    TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
    TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        logging.error("❌ Telegram environment variables not fully set")
        return

    logging.info("✅ Environment variables validated")

    # Initialize proxy pool
    logging.info("🔄 Initializing proxy pool...")
    proxy_pool = ProxyPool(max_pool_size=25)
    proxy_pool.populate_from_url(proxy_url)
    proxy_pool.start_checker()
    logging.info("✅ Proxy pool initialized")

    # Initialize clients
    logging.info("🔄 Initializing exchange clients...")
    binance_client = BinanceClient(proxy_pool)
    bybit_client = BybitClient(proxy_pool)
    logging.info("✅ Exchange clients initialized")

    # Fetch symbols with detailed logging
    logging.info("📊 Fetching exchange symbols...")
    
    logging.info("  🔍 Fetching Binance PERP symbols...")
    binance_perp = set(binance_client.get_perp_symbols())
    logging.info(f"  ✅ Binance PERP: {len(binance_perp)} symbols")
    
    logging.info("  🔍 Fetching Binance SPOT symbols...")
    binance_spot = set(binance_client.get_spot_symbols())
    logging.info(f"  ✅ Binance SPOT: {len(binance_spot)} symbols")
    
    logging.info("  🔍 Fetching Bybit PERP symbols...")
    bybit_perp = set(bybit_client.get_perp_symbols())
    logging.info(f"  ✅ Bybit PERP: {len(bybit_perp)} symbols")
    
    logging.info("  🔍 Fetching Bybit SPOT symbols...")
    bybit_spot = set(bybit_client.get_spot_symbols())
    logging.info(f"  ✅ Bybit SPOT: {len(bybit_spot)} symbols")

    # Deduplicate symbols with logging
    logging.info("🔄 Deduplicating symbols...")
    
    logging.info(f"  📋 Before deduplication - Binance PERP: {len(binance_perp)}, SPOT: {len(binance_spot)}")
    binance_symbols = deduplicate_symbols(binance_perp, binance_spot)
    logging.info(f"  ✅ After deduplication - Binance: {len(binance_symbols)} symbols")
    
    logging.info(f"  📋 Before deduplication - Bybit PERP: {len(bybit_perp)}, SPOT: {len(bybit_spot)}")
    bybit_symbols = deduplicate_symbols(bybit_perp, bybit_spot)
    logging.info(f"  ✅ After deduplication - Bybit: {len(bybit_symbols)} symbols")

    # Log symbol samples
    logging.info(f"🔍 Binance symbol samples: {binance_symbols[:5] if binance_symbols else 'EMPTY'}")
    logging.info(f"🔍 Bybit symbol samples: {bybit_symbols[:5] if bybit_symbols else 'EMPTY'}")
    
    # Add symbol overlap analysis
    binance_symbols_set = set(binance_symbols)
    bybit_symbols_set = set(bybit_symbols)

    overlap = binance_symbols_set.intersection(bybit_symbols_set)
    logging.info(f"📊 SYMBOL OVERLAP ANALYSIS:")
    logging.info(f"  - Binance only: {len(binance_symbols_set - bybit_symbols_set)}")
    logging.info(f"  - Bybit only: {len(bybit_symbols_set - binance_symbols_set)}")
    logging.info(f"  - Both exchanges: {len(overlap)}")
    logging.info(f"  - Common symbols sample: {list(overlap)[:10]}")
    
    # Log known problematic symbols for special debugging
    problem_symbols = ["AAVEUSDT", "PEPEUSDT", "BTCUSDT", "ETHUSDT", "SOLUSDT", "DOTUSDT"]
    logging.info(f"🔍 CHECKING PROBLEM SYMBOLS:")
    for sym in problem_symbols:
        in_binance = sym in binance_symbols_set
        in_bybit = sym in bybit_symbols_set
        logging.info(f"  - Symbol {sym}: Binance={in_binance}, Bybit={in_bybit}")
    
    # Initialize debug counters as GLOBAL
    global binance_scanned, binance_price_matches, binance_same_color_filtered, binance_touched_filtered
    global total_scans, successful_scans, failed_scans, binance_added_signals, bybit_added_signals
    
    binance_scanned = 0
    binance_price_matches = 0
    binance_same_color_filtered = 0
    binance_touched_filtered = 0
    total_scans = 0
    successful_scans = 0
    failed_scans = 0
    binance_added_signals = 0
    bybit_added_signals = 0
    
    logging.info("🔢 Debug counters initialized")

    # Initialize results storage
    results = []
    binance_results = []  # Separate storage for Binance results
    bybit_results = []    # Separate storage for Bybit results
    lock = threading.Lock()

    def scan_symbol(exchange_name, client, symbol, perp_set, spot_set):
        # Access global variables
        global total_scans, successful_scans, failed_scans, binance_added_signals, bybit_added_signals
        global binance_scanned, binance_price_matches, binance_same_color_filtered, binance_touched_filtered
        
        # Track scan start time for debugging slow scans
        scan_start = time.time()
        
        # Debug logging for specific symbols
        is_problem_symbol = symbol in problem_symbols
        if is_problem_symbol:
            logging.info(f"🔍 SCANNING {symbol} on {exchange_name}")
        
        with lock:
            total_scans += 1
            
        # Log every 100th scan
        if total_scans % 100 == 0:
            logging.info(f"📊 Scan progress: {total_scans} total, {successful_scans} successful, {failed_scans} failed")
        
        market = "perp" if symbol in perp_set else "spot"
        
        for interval in ["1M", "1w", "1d"]:
            try:
                # Increment Binance counter
                if exchange_name == "Binance":
                    with lock:
                        binance_scanned += 1
                
                # Fetch data
                df = client.fetch_ohlcv(symbol, interval, limit=3, market=market)
                
                if df.empty or len(df) < 3:
                    if is_problem_symbol:
                        logging.warning(f"⚠️ {exchange_name} {symbol} {interval}: Insufficient data ({len(df)} rows)")
                    continue
                
                # Detailed data dump for problem symbols
                if is_problem_symbol:
                    logging.info(f"📈 {exchange_name} {symbol} {interval} DATA DUMP:")
                    for i in range(min(3, len(df))):
                        logging.info(f"  Row {i}: Open={df.iloc[i]['open']:.8f}, Close={df.iloc[i]['close']:.8f}, Low={df.iloc[i]['low']:.8f}, High={df.iloc[i]['high']:.8f}")
                
                # Manual exhaustive debug check for Binance
                if exchange_name == "Binance":
                    second_last_close = float(df.iloc[2]['close'])
                    last_open = float(df.iloc[1]['open'])
                    
                    # Check if prices match
                    is_price_match = floats_are_equal(second_last_close, last_open)
                    if is_price_match:
                        with lock:
                            binance_price_matches += 1
                        
                        # Check colors
                        second_last_is_green = float(df.iloc[2]['close']) > float(df.iloc[2]['open'])
                        second_last_is_red = float(df.iloc[2]['close']) < float(df.iloc[2]['open'])
                        last_is_green = float(df.iloc[1]['close']) > float(df.iloc[1]['open'])
                        last_is_red = float(df.iloc[1]['close']) < float(df.iloc[1]['open'])
                        
                        # Check if it's a valid reversal pattern
                        is_bullish_reversal = second_last_is_red and last_is_green
                        is_bearish_reversal = second_last_is_green and last_is_red
                        is_valid_reversal = is_bullish_reversal or is_bearish_reversal
                        
                        if not is_valid_reversal:
                            with lock:
                                binance_same_color_filtered += 1
                            if is_problem_symbol:
                                logging.info(f"🚫 BINANCE FILTERED (same color): {symbol} {interval}")
                                logging.info(f"    Second last: {'GREEN' if second_last_is_green else 'RED'}, Last: {'GREEN' if last_is_green else 'RED'}")
                            continue
                        
                        # Check if current candle touched the price
                        current_candle = df.iloc[0]
                        touched = float(current_candle['low']) <= second_last_close <= float(current_candle['high'])
                        
                        if touched:
                            with lock:
                                binance_touched_filtered += 1
                            if is_problem_symbol:
                                logging.info(f"🚫 BINANCE FILTERED (touched): {symbol} {interval} - Price: {second_last_close:.8f}, Current Low: {float(current_candle['low']):.8f}, High: {float(current_candle['high']):.8f}")
                            continue
                        
                        # If we reach here, it should be a valid Binance signal
                        signal_type = "bullish" if is_bullish_reversal else "bearish"
                        logging.info(f"🎯 BINANCE SIGNAL FOUND: {symbol} {market} {interval} {signal_type} @ {second_last_close:.8f}")
                    elif is_problem_symbol:
                        diff = abs(second_last_close - last_open)
                        logging.info(f"❌ BINANCE NO MATCH: {symbol} {interval} - Second last close: {second_last_close:.8f}, Last open: {last_open:.8f}, Diff: {diff:.12f}")
                
                # Use the main function for both exchanges
                equal_price, signal_type = check_equal_price_and_classify(df)
            
                if equal_price is not None and signal_type is not None:
                    # Cross-check with current_candle_touched_price function
                    touched = current_candle_touched_price(df, equal_price)
                    if not touched:
                        with lock:
                            results.append((exchange_name, symbol, market, interval, signal_type))
                            successful_scans += 1
                            
                            # Track separately by exchange
                            if exchange_name == "Binance":
                                binance_results.append((exchange_name, symbol, market, interval, signal_type))
                                binance_added_signals += 1
                                logging.info(f"✅ BINANCE SIGNAL ADDED: {symbol} {market} {interval} {signal_type} @ {equal_price:.8f}")
                            else:
                                bybit_results.append((exchange_name, symbol, market, interval, signal_type))
                                bybit_added_signals += 1
                                if is_problem_symbol:
                                    logging.info(f"✅ BYBIT SIGNAL ADDED: {symbol} {market} {interval} {signal_type} @ {equal_price:.8f}")
                    elif is_problem_symbol:
                        logging.info(f"🚫 {exchange_name} FILTERED (touched): {symbol} {market} {interval} {signal_type} @ {equal_price:.8f}")
                elif is_problem_symbol:
                    logging.info(f"❌ {exchange_name} NO SIGNAL: {symbol} {market} {interval}")
                
            except Exception as e:
                with lock:
                    failed_scans += 1
                logging.warning(f"❌ Failed {exchange_name} {symbol} {market} {interval}: {e}")
        
        # Track scan duration for slow scans
        scan_duration = time.time() - scan_start
        if scan_duration > 5.0:  # Log slow scans over 5 seconds
            logging.warning(f"⏱️ SLOW SCAN: {exchange_name} {symbol} took {scan_duration:.2f} seconds")

    # Create task list with detailed logging
    logging.info("📋 Creating scan tasks...")
    
    # Create separate lists for Binance and Bybit tasks
    binance_tasks = [("Binance", binance_client, sym, binance_perp, binance_spot) for sym in binance_symbols]
    bybit_tasks = [("Bybit", bybit_client, sym, bybit_perp, bybit_spot) for sym in bybit_symbols]
    
    # Check if common test symbols are in both task lists
    for sym in problem_symbols:
        binance_has = any(t[2] == sym for t in binance_tasks)
        bybit_has = any(t[2] == sym for t in bybit_tasks)
        logging.info(f"  TASK LIST CHECK - {sym}: Binance={binance_has}, Bybit={bybit_has}")
    
    # Combine task lists for scanning
    all_symbols = binance_tasks + bybit_tasks
    
    # Check for duplicate tasks where the same symbol appears for both exchanges
    all_tasks_dict = {}
    for task in all_symbols:
        exchange, _, symbol, _, _ = task
        if symbol not in all_tasks_dict:
            all_tasks_dict[symbol] = []
        all_tasks_dict[symbol].append(exchange)
    
    duplicates = [sym for sym, exch_list in all_tasks_dict.items() if len(exch_list) > 1]
    logging.info(f"📊 DUPLICATE SYMBOL TASKS: {len(duplicates)} symbols in both exchanges")
    if duplicates:
        logging.info(f"  - First 10 duplicates: {duplicates[:10]}")
    
    logging.info(f"📊 Task creation summary:")
    logging.info(f"  - Binance tasks: {len(binance_tasks)}")
    logging.info(f"  - Bybit tasks: {len(bybit_tasks)}")
    logging.info(f"  - Total tasks: {len(all_symbols)}")
    
    if not all_symbols:
        logging.error("❌ No scan tasks created! Check symbol fetching.")
        return
    
    # Log first few tasks of each type
    if binance_tasks:
        logging.info(f"🔍 First 3 Binance tasks: {[(t[0], t[2]) for t in binance_tasks[:3]]}")
    if bybit_tasks:
        logging.info(f"🔍 First 3 Bybit tasks: {[(t[0], t[2]) for t in bybit_tasks[:3]]}")

    # Execute scanning with detailed monitoring
    logging.info("🚀 Starting concurrent scanning...")
    start_time = time.time()
    
    # Process Binance tasks first, then Bybit
    logging.info(f"📤 Submitting {len(binance_tasks)} Binance tasks to thread pool...")
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=25) as executor:
        binance_futures = [executor.submit(scan_symbol, *args) for args in binance_tasks]
        logging.info(f"✅ Successfully submitted {len(binance_futures)} Binance futures")
        
        completed_count = 0
        exception_count = 0
        
        for future in tqdm(concurrent.futures.as_completed(binance_futures), total=len(binance_futures), desc="Scanning Binance"):
            completed_count += 1
            
            # Check for exceptions
            try:
                future.result()
            except Exception as e:
                exception_count += 1
                logging.error(f"❌ Binance task exception #{exception_count}: {e}")
            
            # Log progress every 100 completions
            if completed_count % 100 == 0:
                logging.info(f"📊 Binance progress: {completed_count}/{len(binance_futures)} completed, {exception_count} exceptions")
    
    # Now process Bybit tasks
    logging.info(f"📤 Submitting {len(bybit_tasks)} Bybit tasks to thread pool...")
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=25) as executor:
        bybit_futures = [executor.submit(scan_symbol, *args) for args in bybit_tasks]
        logging.info(f"✅ Successfully submitted {len(bybit_futures)} Bybit futures")
        
        completed_count = 0
        exception_count = 0
        
        for future in tqdm(concurrent.futures.as_completed(bybit_futures), total=len(bybit_futures), desc="Scanning Bybit"):
            completed_count += 1
            
            # Check for exceptions
            try:
                future.result()
            except Exception as e:
                exception_count += 1
                logging.error(f"❌ Bybit task exception #{exception_count}: {e}")
            
            # Log progress every 100 completions
            if completed_count % 100 == 0:
                logging.info(f"📊 Bybit progress: {completed_count}/{len(bybit_futures)} completed, {exception_count} exceptions")

    end_time = time.time()
    scan_duration = end_time - start_time
    
    logging.info(f"✅ Scanning completed in {scan_duration:.2f} seconds")
    logging.info(f"📊 Final scan statistics:")
    logging.info(f"  - Total scans attempted: {total_scans}")
    logging.info(f"  - Successful scans: {successful_scans}")
    logging.info(f"  - Failed scans: {failed_scans}")
    
    # Detailed Binance debug summary
    logging.info(f"\n=== 🔍 BINANCE DEBUG SUMMARY ===")
    logging.info(f"Total Binance scans: {binance_scanned}")
    logging.info(f"Price matches found: {binance_price_matches}")
    logging.info(f"Filtered out (same colors): {binance_same_color_filtered}")
    logging.info(f"Filtered out (price touched): {binance_touched_filtered}")
    logging.info(f"Signals added: {binance_added_signals}")
    
    # Check for specific problems
    expected_binance_scans = len(binance_symbols) * 3  # 3 timeframes
    if binance_scanned < expected_binance_scans:
        logging.warning(f"⚠️ BINANCE SCAN SHORTFALL: Expected {expected_binance_scans}, got {binance_scanned}")
    
    if binance_price_matches == 0:
        logging.warning(f"⚠️ NO BINANCE PRICE MATCHES FOUND: This suggests data format differences")
    
    if binance_price_matches > 0 and binance_added_signals == 0:
        logging.warning(f"⚠️ BINANCE SIGNALS ALL FILTERED: {binance_same_color_filtered} same color, {binance_touched_filtered} price touched")
    
    logging.info(f"\n=== 🔍 SIGNAL COUNT SUMMARY ===")
    logging.info(f"Binance signals: {len(binance_results)}")
    logging.info(f"Bybit signals: {len(bybit_results)}")
    logging.info(f"Total signals: {len(results)}")
    
    # Verify results consistency
    if len(binance_results) + len(bybit_results) != len(results):
        logging.error(f"❌ RESULTS INCONSISTENCY: Binance({len(binance_results)}) + Bybit({len(bybit_results)}) != Total({len(results)})")
    
    # Analyze the symbols that produced signals
    binance_signal_symbols = set(r[1] for r in binance_results)
    bybit_signal_symbols = set(r[1] for r in bybit_results)
    
    common_signal_symbols = binance_signal_symbols.intersection(bybit_signal_symbols)
    logging.info(f"📊 SIGNAL SYMBOLS ANALYSIS:")
    logging.info(f"  - Binance signal symbols: {len(binance_signal_symbols)}")
    logging.info(f"  - Bybit signal symbols: {len(bybit_signal_symbols)}")
    logging.info(f"  - Common signal symbols: {len(common_signal_symbols)}")
    
    if common_signal_symbols:
        logging.info(f"  - Common symbols sample: {list(common_signal_symbols)[:10]}")

    # Create and send telegram report
    logging.info("📨 Creating Telegram report...")
    messages = create_beautiful_telegram_report(results)
    
    if not messages:
        logging.error("❌ No messages created from results")
        return

    logging.info(f"📤 Sending {len(messages)} Telegram messages...")
    bot = Bot(token=TELEGRAM_TOKEN)
    
    for i, msg in enumerate(messages):
        try:
            await bot.send_message(chat_id=int(TELEGRAM_CHAT_ID), text=msg, parse_mode='Markdown')
            logging.info(f"✅ Telegram message {i+1}/{len(messages)} sent successfully")
        except Exception as e:
            logging.error(f"❌ Failed to send Telegram message {i+1}: {e}")

    logging.info("🏁 Scanner completed successfully")


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

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
