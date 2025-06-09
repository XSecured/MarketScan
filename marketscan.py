import sys
import asyncio
import ccxt.async_support as ccxt
import itertools
import threading
import time
import logging
import os
from telegram import Bot

# --- Fix event loop policy on Windows ---
if sys.platform.startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# --- ProxyPool class ---

class ProxyPool:
    def __init__(self, max_pool_size: int = 25, proxy_check_interval: int = 600, max_failures: int = 3):
        self.lock = threading.Lock()
        self.max_pool_size = max_pool_size
        self.proxy_check_interval = proxy_check_interval
        self.max_failures = max_failures

        self.proxies = []
        self.proxy_failures = {}  # proxy -> failure count
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

    def reset_proxy_failures(self, proxy):
        with self.lock:
            if proxy in self.proxy_failures:
                self.proxy_failures[proxy] = 0
            if proxy in self.failed_proxies:
                self.failed_proxies.remove(proxy)
                if proxy not in self.proxies:
                    self.proxies.append(proxy)
                    self.proxy_cycle = itertools.cycle(self.proxies)

    def populate_from_list(self, proxy_list):
        with self.lock:
            self.proxies = proxy_list[:self.max_pool_size]
            self.proxy_failures.clear()
            self.failed_proxies.clear()
            self.proxy_cycle = itertools.cycle(self.proxies) if self.proxies else None
            logging.info(f"Proxy pool filled with {len(self.proxies)} proxies.")

    def stop(self):
        self._stop_event.set()

    def has_proxies(self) -> bool:
        with self.lock:
            return bool(self.proxies)

# --- Patch CCXT exchange to use rotating proxies from ProxyPool ---

def patch_exchange_with_proxy(exchange, proxy_pool):
    original_fetch_ohlcv = exchange.fetch_ohlcv

    async def fetch_ohlcv_with_proxy(symbol, timeframe='1m', since=None, limit=None, params={}):
        for attempt in range(5):
            proxy = proxy_pool.get_next_proxy()
            if proxy is None:
                raise RuntimeError("No working proxies available in pool")
            proxy_url = proxy if proxy.startswith('http') else f"http://{proxy}"
            try:
                exchange.session._default_proxy = proxy_url
                exchange.proxies = {
                    "http": proxy_url,
                    "https": proxy_url
                }
                result = await original_fetch_ohlcv(symbol, timeframe, since, limit, params)
                proxy_pool.reset_proxy_failures(proxy)
                return result
            except Exception as e:
                logging.warning(f"Proxy {proxy} failed for {symbol} {timeframe}: {e}")
                proxy_pool.mark_proxy_failure(proxy)
                await asyncio.sleep(1)
        raise RuntimeError(f"All proxies failed for fetching OHLCV {symbol} {timeframe}")

    exchange.fetch_ohlcv = fetch_ohlcv_with_proxy

# --- Telegram bot setup using python-telegram-bot v20+ async ---

TELEGRAM_BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID')

if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
    raise EnvironmentError("Please set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID environment variables")

bot = Bot(token=TELEGRAM_BOT_TOKEN)

# --- Concurrency control ---

CONCURRENT_REQUESTS = 10
semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)

# --- Filter active markets only ---

def filter_active_markets(exchange, market_type):
    active_symbols = []
    for symbol, market in exchange.markets.items():
        if market['type'] == market_type and market.get('active', False):
            active_symbols.append(symbol)
    return active_symbols

# --- Timeframes including 1M, 1w, and 1d ---

def get_timeframes():
    return ['1M', '1w', '1d']

# --- Fetch candles with concurrency control ---

async def fetch_candles(exchange, symbol, timeframe, limit=3):
    async with semaphore:
        try:
            candles = await exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
            return candles
        except Exception as e:
            logging.warning(f"[{exchange.id}] Error fetching {symbol} {timeframe}: {e}")
            return None

# --- Check candles for close==next open condition ---

async def check_candles(exchange, symbol):
    results = []
    for timeframe in get_timeframes():
        candles = await fetch_candles(exchange, symbol, timeframe, limit=3)
        if candles and len(candles) >= 3:
            close_0 = candles[0][4]
            open_1 = candles[1][1]
            matched = (close_0 == open_1)
            results.append((symbol, exchange.id, timeframe, matched))
        else:
            results.append((symbol, exchange.id, timeframe, None))
    return results

# --- Format Telegram message ---

def format_results_message(all_results):
    lines = []
    lines.append("📊 *Close == Next Open Candle Check Results* 📊\n")
    lines.append(f"{'Exchange':<8} | {'Symbol':<20} | {'TF':<3} | Result")
    lines.append("-" * 50)

    for result in all_results:
        for symbol, exch, timeframe, matched in result:
            if matched is None:
                status = "Insufficient data"
            else:
                status = "✅ MATCH" if matched else "❌ NO MATCH"
            lines.append(f"{exch:<8} | {symbol:<20} | {timeframe:<3} | {status}")

    return "\n".join(lines)

# --- Send Telegram message asynchronously ---

async def send_telegram_message(text):
    try:
        await bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=text, parse_mode='Markdown')
        logging.info("Telegram message sent successfully.")
    except Exception as e:
        logging.error(f"Failed to send Telegram message: {e}")

# --- Main async entrypoint ---

async def main():
    logging.basicConfig(level=logging.INFO)

    proxy_list_raw = os.getenv("PROXY_LIST")
    if proxy_list_raw:
        proxies = [p.strip() for p in proxy_list_raw.split(",") if p.strip()]
    else:
        proxy_file = os.getenv("PROXY_FILE", "proxies.txt")
        try:
            with open(proxy_file, "r") as f:
                proxies = [line.strip() for line in f if line.strip()]
        except Exception as e:
            logging.error(f"Failed to load proxies from file {proxy_file}: {e}")
            proxies = []

    if not proxies:
        logging.warning("No proxies loaded, proceeding without proxies (not recommended).")

    proxy_pool = ProxyPool(max_pool_size=25)
    proxy_pool.populate_from_list(proxies)

    binance = ccxt.binance({'enableRateLimit': True})
    bybit = ccxt.bybit({'enableRateLimit': True})

    patch_exchange_with_proxy(binance, proxy_pool)
    patch_exchange_with_proxy(bybit, proxy_pool)

    try:
        await binance.load_markets()
    except ccxt.ExchangeNotAvailable as e:
        logging.error(f"Binance unavailable (blocked or restricted): {e}")
        binance = None

    try:
        await bybit.load_markets()
    except ccxt.ExchangeNotAvailable as e:
        logging.error(f"Bybit unavailable (blocked or restricted): {e}")
        bybit = None

    binance_spot = filter_active_markets(binance, 'spot') if binance else []
    binance_perp = filter_active_markets(binance, 'future') if binance else []
    bybit_spot = filter_active_markets(bybit, 'spot') if bybit else []
    bybit_perp = filter_active_markets(bybit, 'future') if bybit else []

    logging.info(f"Active markets found - Binance spot: {len(binance_spot)}, Binance perp: {len(binance_perp)}, Bybit spot: {len(bybit_spot)}, Bybit perp: {len(bybit_perp)}")

    combined_symbols = binance_perp + binance_spot + bybit_perp + bybit_spot
    seen = set()
    unique_symbols = []
    for s in combined_symbols:
        if s not in seen:
            seen.add(s)
            unique_symbols.append(s)

    symbol_exchange_map = {}
    for symbol in unique_symbols:
        if binance and symbol in binance.markets:
            symbol_exchange_map[symbol] = binance
        elif bybit and symbol in bybit.markets:
            symbol_exchange_map[symbol] = bybit
        else:
            logging.warning(f"Symbol {symbol} not found on any exchange")

    logging.info(f"Total unique active symbols to scan: {len(unique_symbols)}")

    if not symbol_exchange_map:
        logging.warning("No symbols to scan, exiting.")
        return

    tasks = [check_candles(symbol_exchange_map[symbol], symbol) for symbol in unique_symbols]
    all_results = await asyncio.gather(*tasks)

    message = format_results_message(all_results)

    await send_telegram_message(message)

    if binance:
        await binance.close()
    if bybit:
        await bybit.close()

    proxy_pool.stop()

if __name__ == '__main__':
    asyncio.run(main())
