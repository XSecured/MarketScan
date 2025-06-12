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


# Global debug counters - ADD THESE AT TOP OF FILE
binance_scanned = 0
binance_price_matches = 0
binance_same_color_filtered = 0
binance_touched_filtered = 0

# ADD THESE NEW GLOBAL VARIABLES
total_scans = 0
successful_scans = 0
failed_scans = 0


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
        format='%(asctime)s - %(levelname)s - %(message)s'
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
    
    # RESET all global counters (don't redeclare them)
    global binance_scanned, binance_price_matches, binance_same_color_filtered, binance_touched_filtered
    global total_scans, successful_scans, failed_scans
    
    binance_scanned = 0
    binance_price_matches = 0
    binance_same_color_filtered = 0
    binance_touched_filtered = 0
    total_scans = 0
    successful_scans = 0
    failed_scans = 0
    
    logging.info("🔢 Debug counters reset")

    # Initialize results
    results = []
    lock = threading.Lock()

    def scan_symbol(exchange_name, client, symbol, perp_set, spot_set):
        # Access global variables
        global total_scans, successful_scans, failed_scans
        global binance_scanned, binance_price_matches, binance_same_color_filtered, binance_touched_filtered
        
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
                    continue
                
                # Manual debug check for Binance
                if exchange_name == "Binance":
                    second_last_close = float(df.iloc[2]['close'])
                    last_open = float(df.iloc[1]['open'])
                    
                    # Check if prices match
                    if floats_are_equal(second_last_close, last_open):
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
                        
                        if not (is_bullish_reversal or is_bearish_reversal):
                            with lock:
                                binance_same_color_filtered += 1
                            continue
                        
                        # Check if current candle touched the price
                        current_candle = df.iloc[0]
                        touched = float(current_candle['low']) <= second_last_close <= float(current_candle['high'])
                        
                        if touched:
                            with lock:
                                binance_touched_filtered += 1
                            continue
                        
                        # If we reach here, it should be a valid Binance signal
                        logging.info(f"🎯 BINANCE SIGNAL FOUND: {symbol} {market} {interval} ({'bullish' if is_bullish_reversal else 'bearish'})")
                
                # Use the main function
                equal_price, signal_type = check_equal_price_and_classify(df)
            
                if equal_price is not None and signal_type is not None:
                    if not current_candle_touched_price(df, equal_price):
                        with lock:
                            results.append((exchange_name, symbol, market, interval, signal_type))
                            successful_scans += 1
                            
                        # Log Binance signals specifically
                        if exchange_name == "Binance":
                            logging.info(f"✅ BINANCE SIGNAL ADDED: {symbol} {market} {interval} {signal_type}")
                
            except Exception as e:
                with lock:
                    failed_scans += 1
                logging.warning(f"❌ Failed {exchange_name} {symbol} {market} {interval}: {e}")

    # Rest of main function stays the same...
    # Create task list with detailed logging
    logging.info("📋 Creating scan tasks...")
    
    binance_tasks = [("Binance", binance_client, sym, binance_perp, binance_spot) for sym in binance_symbols]
    bybit_tasks = [("Bybit", bybit_client, sym, bybit_perp, bybit_spot) for sym in bybit_symbols]
    all_symbols = binance_tasks + bybit_tasks
    
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
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=25) as executor:
        logging.info(f"📤 Submitting {len(all_symbols)} tasks to thread pool...")
        
        futures = [executor.submit(scan_symbol, *args) for args in all_symbols]
        logging.info(f"✅ Successfully submitted {len(futures)} futures")
        
        completed_count = 0
        exception_count = 0
        
        for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures), desc="Scanning symbols"):
            completed_count += 1
            
            # Check for exceptions
            try:
                future.result()  # This will raise any exception that occurred
            except Exception as e:
                exception_count += 1
                logging.error(f"❌ Task exception #{exception_count}: {e}")
            
            # Log progress every 200 completions
            if completed_count % 200 == 0:
                logging.info(f"📊 Progress: {completed_count}/{len(futures)} completed, {exception_count} exceptions")

    end_time = time.time()
    scan_duration = end_time - start_time
    
    logging.info(f"✅ Scanning completed in {scan_duration:.2f} seconds")
    logging.info(f"📊 Final scan statistics:")
    logging.info(f"  - Total scans attempted: {total_scans}")
    logging.info(f"  - Successful scans: {successful_scans}")
    logging.info(f"  - Failed scans: {failed_scans}")
    logging.info(f"  - Exceptions: {exception_count}")

    # Detailed Binance debug summary
    logging.info(f"\n=== 🔍 BINANCE DEBUG SUMMARY ===")
    logging.info(f"Total Binance scans: {binance_scanned}")
    logging.info(f"Price matches found: {binance_price_matches}")
    logging.info(f"Filtered out (same colors): {binance_same_color_filtered}")
    logging.info(f"Filtered out (price touched): {binance_touched_filtered}")
    
    binance_results_count = len([r for r in results if r[0] == "Binance"])
    bybit_results_count = len([r for r in results if r[0] == "Bybit"])
    
    logging.info(f"Final Binance signals: {binance_results_count}")
    logging.info(f"Final Bybit signals: {bybit_results_count}")
    logging.info(f"Total signals: {len(results)}")

    # Calculate expected Binance scans
    expected_binance_scans = len(binance_symbols) * 3  # 3 timeframes
    logging.info(f"Expected Binance scans: {expected_binance_scans}")
    
    if binance_scanned < expected_binance_scans:
        logging.warning(f"⚠️  BINANCE SCAN SHORTFALL: Expected {expected_binance_scans}, got {binance_scanned}")

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
