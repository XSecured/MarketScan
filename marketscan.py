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
import re

# List of symbols to ignore in all scanning
IGNORED_SYMBOLS = {
    "USDPUSDT", "USD1USDT", "TUSDUSDT", "AEURUSDT", "USDCUSDT",
    "ZKJUSDT", "FDUSDUSDT", "XUSDUSDT", "EURUSDT", "EURIUSDT"
}

# --- Proxy system (same as EMA bot) ---

def fetch_proxies_from_url(url: str, default_scheme: str = "http") -> list:
    """Fetches proxies from a URL, supporting both http:// and https:// schemes."""
    proxies = []
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()  # Raises HTTPError for bad responses (4xx or 5xx)
        lines = response.text.strip().splitlines()
        for line in lines:
            proxy = line.strip()
            if not proxy:
                continue  # Skip empty lines
            if "://" in proxy:
                proxies.append(proxy)  # Use proxy as is if scheme is already present
            else:
                proxies.append(f"{default_scheme}://{proxy}")  # Add default scheme if missing
        logging.info("Fetched %d proxies from %s", len(proxies), url)
    except requests.exceptions.RequestException as e:
        logging.error("Error fetching proxies from URL %s: %s", url, e)
    return proxies

def test_proxy(proxy: str, timeout=5) -> bool:
    """Tests if a proxy is working by making a request to Binance API."""
    test_url = "https://api.binance.com/api/v3/time"  # Using Binance time endpoint for testing
    try:
        response = requests.get(test_url, proxies={"http": proxy, "https": proxy}, timeout=timeout)
        return 200 <= response.status_code < 300  # Return True if status code is in 200-299 range
    except Exception:
        return False  # Return False if any exception occurred during the test

def test_proxies_concurrently(proxies: list, max_workers: int = 50, max_working: int = 20) -> list:
    """Tests proxies concurrently using a thread pool and returns a list of working proxies."""
    working = []
    tested = 0
    dead = 0
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
                else:
                    dead += 1
        finally:
            # Cancel any pending futures if enough working proxies are found
            if len(working) >= max_working:
                for f in futures:
                    f.cancel()
    logging.info(f"Found {len(working)} working proxies (tested {tested})")
    return working[:max_working]

class ProxyPool:
    """Manages a pool of proxies, handling fetching, testing, and cycling."""
    def __init__(self, max_pool_size=25, proxy_check_interval=600, max_failures=3):
        self.lock = threading.Lock()
        self.max_pool_size = max_pool_size
        self.proxy_check_interval = proxy_check_interval
        self.max_failures = max_failures

        self.proxies = []
        self.proxy_failures = {}  # proxy -> failure count
        self.failed_proxies = set()  # set of proxies that have reached max_failures
        self.proxy_cycle = None  # itertools.cycle for rotating through proxies
        self._stop_event = threading.Event()  # Event to stop background threads

    def get_next_proxy(self):
        """Returns the next working proxy from the pool."""
        with self.lock:
            if not self.proxies:
                logging.warning("Proxy pool empty when requesting next proxy.")
                return None
            for _ in range(len(self.proxies)):
                proxy = next(self.proxy_cycle)
                if proxy not in self.failed_proxies:
                    return proxy  # Return proxy if it's not marked as failed
            logging.warning("All proxies in pool are marked as failed.")
            return None

    def mark_proxy_failure(self, proxy):
        """Marks a proxy as failed, incrementing its failure count."""
        if proxy is None:
            return  # Ignore if proxy is None

        with self.lock:
            count = self.proxy_failures.get(proxy, 0) + 1
            self.proxy_failures[proxy] = count
            logging.warning(f"Proxy {proxy} failure count: {count}/{self.max_failures}")
            if count >= self.max_failures:
                self.failed_proxies.add(proxy)
                if proxy in self.proxies:
                    self.proxies.remove(proxy)
                    logging.warning(f"Proxy {proxy} removed from pool due to repeated failures.")
                self.proxy_cycle = itertools.cycle(self.proxies) if self.proxies else None  # Update cycle

    def populate_from_url(self, url: str, default_scheme: str = "http"):
        """Populates the proxy pool from a URL."""
        new_proxies = fetch_proxies_from_url(url, default_scheme)
        working = test_proxies_concurrently(new_proxies, max_working=self.max_pool_size)
        with self.lock:
            self.proxies = working[:self.max_pool_size]
            self.proxy_failures.clear()  # Reset failure counts for all proxies
            self.failed_proxies.clear()  # Clear set of failed proxies
            self.proxy_cycle = itertools.cycle(self.proxies) if self.proxies else None  # Create a new cycle
            logging.info(f"Proxy pool filled with {len(self.proxies)} proxies from URL.")

    def start_checker(self):
        """Starts a background thread to periodically check proxy health."""
        thread = threading.Thread(target=self.proxy_checker_loop, daemon=True)
        thread.start()

    def proxy_checker_loop(self):
        """Periodically checks the health of proxies in the pool."""
        while not self._stop_event.is_set():
            logging.info("Running proxy pool health check...")
            self.check_proxies()
            self._stop_event.wait(self.proxy_check_interval)

    def check_proxies(self):
        """Checks the health of proxies and updates the pool."""
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
        """Populates the proxy pool to the maximum size."""
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
        """Gets new proxies from a backup URL."""
        backup_url = "https://raw.githubusercontent.com/ErcinDedeoglu/proxies/main/proxies/https.txt"  # Using a backup proxy list URL
        new_proxies = fetch_proxies_from_url(backup_url)
        working = test_proxies_concurrently(new_proxies, max_working=count)
        return working

    def stop(self):
        """Stops the proxy checker thread."""
        self._stop_event.set()

# --- Binance Client ---

class BinanceClient:
    """Client for interacting with the Binance API, handling proxy rotation and retries."""
    def __init__(self, proxy_pool: ProxyPool, max_retries=5, retry_delay=2):
        self.proxy_pool = proxy_pool
        self.max_retries = max_retries
        self.retry_delay = retry_delay

    def _get_proxy_dict(self):
        """Gets a proxy dictionary from the proxy pool."""
        proxy = self.proxy_pool.get_next_proxy()
        if proxy is None:
            raise RuntimeError("No working proxies available")
        return {"http": proxy, "https": proxy}

    def get_perp_symbols(self):
        """Fetches perpetual futures symbols from Binance API."""
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
        """Fetches spot symbols from Binance API."""
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
        """Fetches OHLCV data from Binance API with proxy rotation and retries."""
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
        proxies = None # Initialize proxies to None
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
                    continue  # Retry without incrementing attempt
                else:
                    raise  # Re-raise other RuntimeErrors
            except Exception as e:
                logging.warning(f"Attempt {attempt} failed fetching OHLCV for {symbol} ({market}) {interval}: {e}")
                if proxies:
                    self.proxy_pool.mark_proxy_failure(proxies.get('http'))
            time.sleep(self.retry_delay * attempt + random.uniform(0, 1))
            attempt += 1
        logging.error(f"All retries failed fetching OHLCV for {symbol} ({market}) {interval}")
        raise RuntimeError(f"Failed to fetch OHLCV for {symbol} ({market}) {interval}")

# --- Bybit Client (API V5) ---

class BybitClient:
    """Client for interacting with the Bybit API, handling proxy rotation, retries, and API version compatibility."""
    def __init__(self, proxy_pool: ProxyPool, max_retries=5, retry_delay=2):
        self.proxy_pool = proxy_pool
        self.max_retries = max_retries
        self.retry_delay = retry_delay

    def _get_proxy_dict(self):
        """Gets a proxy dictionary from the proxy pool."""
        proxy = self.proxy_pool.get_next_proxy()
        if proxy is None:
            raise RuntimeError("No working proxies available")
        return {"http": proxy, "https": proxy}

    def get_perp_symbols(self):
        """Fetches perpetual futures symbols from Bybit API V5."""
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
        """Fetches spot symbols from Bybit API V5."""
        url = 'https://api.bybit.com/v5/market/instruments-info'
        params = {'category': 'spot'}
        for attempt in range(1, self.max_retries + 1):
            try:
                proxies = self._get_proxy_dict()
                resp = requests.get(url, proxies=proxies, timeout=10)
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
        """Fetches OHLCV data from Bybit API V5 with proxy rotation and retries."""
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
        proxies = None  # Initialize proxies to None
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
                df[['open', 'close', 'high', 'low', 'volume']] = df[['open', 'close', 'high', 'low', 'volume']].astype(float)
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
                    continue  # Retry without incrementing attempt
                else:
                    raise  # Re-raise other RuntimeErrors
            except Exception as e:
                logging.warning(f"Attempt {attempt} failed fetching Bybit OHLCV for {symbol} ({market}) {interval}: {e}")
                if proxies:
                    self.proxy_pool.mark_proxy_failure(proxies.get('http'))
            time.sleep(self.retry_delay * attempt + random.uniform(0, 1))
            attempt += 1
        logging.error(f"All retries failed fetching Bybit OHLCV for {symbol} ({market}) {interval}")
        raise RuntimeError(f"Failed to fetch Bybit OHLCV for {symbol} ({market}) {interval}")

# --- Helper functions ---

def deduplicate_symbols(perp_list, spot_list):
    """Deduplicates symbols, giving preference to perps."""
    spot_only = set(spot_list) - set(perp_list)
    all_symbols = list(perp_list) + list(spot_only)
    return [s for s in all_symbols if s not in IGNORED_SYMBOLS]

def is_open_close_range_untouched(df):
    """
    Checks if the current candle's open-close range is untouched by price (high or low).
    Returns True if the range is untouched, False otherwise.
    """
    if df.empty or len(df) < 1:
        return False

    current_candle = df.iloc[-1]
    open_price = current_candle['open']
    close_price = current_candle['close']
    high = current_candle['high']
    low = current_candle['low']

    lower = min(open_price, close_price)  # Lower bound of open-close range
    upper = max(open_price, close_price)  # Upper bound of open-close range

    # For bullish candle: check if low > lower bound of open-close range
    if close_price > open_price:
        return low >= upper  # untouched if low is above the high of the range

    # For bearish candle: check if high < upper bound of open-close range
    elif close_price < open_price:
        return high <= lower  # untouched if high is below the low of the range

    else:
        # Doji or equal open-close, consider touched
        return False

# --- Main async function ---

async def main():
    """Main function to fetch symbols, OHLCV data, and send Telegram reports."""
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

    proxy_url = os.getenv("PROXY_LIST_URL")
    if not proxy_url:
        logging.error("PROXY_LIST_URL environment variable not set")
        return

    proxy_pool = ProxyPool(max_pool_size=25)
    proxy_pool.populate_from_url(proxy_url)
    proxy_pool.start_checker()

    TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
    TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        logging.error("Telegram environment variables not fully set")
        return

    binance_client = BinanceClient(proxy_pool)
    bybit_client = BybitClient(proxy_pool)

    # Fetch symbols from both exchanges
    binance_perp = set(binance_client.get_perp_symbols())
    binance_spot = set(binance_client.get_spot_symbols())

    bybit_perp = set(bybit_client.get_perp_symbols())
    bybit_spot = set(bybit_client.get_spot_symbols())

    # Deduplicate symbols per exchange
    binance_symbols = deduplicate_symbols(binance_perp, binance_spot)
    bybit_symbols = deduplicate_symbols(bybit_perp, bybit_spot)

    logging.info(f"Binance symbols to scan: {len(binance_symbols)}")
    logging.info(f"Bybit symbols to scan: {len(bybit_symbols)}")

    results = []
    lock = threading.Lock()

    def scan_symbol(exchange_name, client, symbol, perp_set, spot_set):
        """Scans a single symbol for the specified candle condition."""
        market = "perp" if symbol in perp_set else "spot"
        for interval in ["1M", "1w", "1d"]:
            try:
                df = client.fetch_ohlcv(symbol, interval, limit=3, market=market)
                if is_open_close_range_untouched(df):
                    with lock:
                        results.append((exchange_name, symbol, market, interval))
            except Exception as e:
                logging.warning(f"Failed {exchange_name} {symbol} {market} {interval}: {e}")

    with concurrent.futures.ThreadPoolExecutor(max_workers=25) as executor:
        futures = {}
        for sym in binance_symbols:
            futures[executor.submit(scan_symbol, "Binance", binance_client, sym, binance_perp, binance_spot)] = sym
        for sym in bybit_symbols:
            futures[executor.submit(scan_symbol, "Bybit", bybit_client, sym, bybit_perp, bybit_spot)] = sym

        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logging.error(f"Error in scanning thread: {e}")

    # Prepare Telegram message
    if not results:
        message = "No symbols found where current candle's open-close range is untouched by price."
    else:
        message = "*Symbols with current candle open-close range untouched by price:*\n\n"
        for exch, sym, market, interval in results:
            message += f"- {exch} | {sym} | {market} | {interval}\n"

    # Send Telegram message
    bot = Bot(token=TELEGRAM_TOKEN)
    try:
        await bot.send_message(chat_id=int(TELEGRAM_CHAT_ID), text=message, parse_mode='Markdown')
        logging.info("Telegram report sent successfully.")
    except Exception as e:
        logging.error(f"Failed to send Telegram message: {e}")

# Run main
if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
