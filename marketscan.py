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
from tqdm import tqdm  # Import tqdm
import re

# List of symbols to ignore in all scanning
IGNORED_SYMBOLS = {
    "USDPUSDT", "USD1USDT", "TUSDUSDT", "AEURUSDT", "USDCUSDT",
    "ZKJUSDT", "FDUSDUSDT", "XUSDUSDT", "EURUSDT", "EURIUSDT"
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

def check_close_equals_next_open(df):
    if df.empty or len(df) < 3:
        return None
    if floats_are_equal(df.iloc[0]['close'], df.iloc[1]['open']):
        return df.iloc[0]['close']
    if floats_are_equal(df.iloc[1]['close'], df.iloc[2]['open']):
        return df.iloc[1]['close']
    return None

def current_candle_touched_price(df, price):
    if df.empty or len(df) < 1:
        return False
    current_candle = df.iloc[-1]
    return current_candle['low'] <= price <= current_candle['high']

def get_candle_bullish_bearish(df):
    if df.empty or len(df) < 1:
        return "neutral"
    current_candle = df.iloc[-1]
    if current_candle['close'] > current_candle['open']:
        return "bullish"
    elif current_candle['close'] < current_candle['open']:
        return "bearish"
    else:
        return "neutral"

# --- Main async scanning and reporting ---

async def main():
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

    binance_perp = set(binance_client.get_perp_symbols())
    binance_spot = set(binance_client.get_spot_symbols())

    bybit_perp = set(bybit_client.get_perp_symbols())
    bybit_spot = set(bybit_client.get_spot_symbols())

    binance_symbols = deduplicate_symbols(binance_perp, binance_spot)
    bybit_symbols = deduplicate_symbols(bybit_perp, bybit_spot)

    logging.info(f"Binance symbols to scan: {len(binance_symbols)}")
    logging.info(f"Bybit symbols to scan: {len(bybit_symbols)}")

    results = []
    lock = threading.Lock()

    def scan_symbol(exchange_name, client, symbol, perp_set, spot_set):
        market = "perp" if symbol in perp_set else "spot"
        for interval in ["1M", "1w", "1d"]:
            try:
                df = client.fetch_ohlcv(symbol, interval, limit=3, market=market)
                equal_price = check_close_equals_next_open(df)
                if equal_price is not None:
                    if not current_candle_touched_price(df, equal_price):
                        candle_type = get_candle_bullish_bearish(df)
                        with lock:
                            results.append((exchange_name, symbol, market, interval, candle_type))
            except Exception as e:
                logging.warning(f"Failed {exchange_name} {symbol} {market} {interval}: {e}")

    all_symbols = [("Binance", binance_client, sym, binance_perp, binance_spot) for sym in binance_symbols] + \
                  [("Bybit", bybit_client, sym, bybit_perp, bybit_spot) for sym in bybit_symbols]

    messages = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=25) as executor:
        futures = [executor.submit(scan_symbol, *args) for args in all_symbols]
        for _ in tqdm(concurrent.futures.as_completed(futures), total=len(futures), desc="Scanning symbols"):
            pass  # Just iterate and tqdm does the progress

    if not results:
        message = "No symbols found where last two closed candles have equal close==next open price and current candle has not touched that price yet."
        messages = [message]
    else:
        header = "*Symbols where last two closed candles have equal close==next open price and current candle untouched that price:*\n\n"
        lines = [
            f"- {exch} | {sym} | {market} | {interval} | {candle_type}"
            for exch, sym, market, interval, candle_type in results
        ]
        # Telegram message limit is 4096 chars
        messages = []
        chunk = header
        for line in lines:
            if len(chunk) + len(line) + 1 > 4096:
                messages.append(chunk)
                chunk = header
            chunk += line + "\n"
        if chunk.strip() != header.strip():
            messages.append(chunk)

    bot = Bot(token=TELEGRAM_TOKEN)
    for msg in messages:
        try:
            await bot.send_message(chat_id=int(TELEGRAM_CHAT_ID), text=msg, parse_mode='Markdown')
            logging.info("Telegram report sent successfully.")
        except Exception as e:
            logging.error(f"Failed to send Telegram message: {e}")

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
