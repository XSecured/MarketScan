import asyncio
import aiohttp
import logging
import os
from telegram import Bot
import itertools
import threading

# --- ProxyPool class (your EMA bot’s) ---

class ProxyPool:
    def __init__(self, max_pool_size=25, max_failures=3):
        self.lock = threading.Lock()
        self.max_pool_size = max_pool_size
        self.max_failures = max_failures
        self.proxies = []
        self.proxy_failures = {}
        self.failed_proxies = set()
        self.proxy_cycle = None

    def populate_from_list(self, proxies):
        with self.lock:
            self.proxies = proxies[:self.max_pool_size]
            if not self.proxies:
                logging.error("No proxies provided, exiting.")
                raise RuntimeError("No proxies")
            self.proxy_failures.clear()
            self.failed_proxies.clear()
            self.proxy_cycle = itertools.cycle(self.proxies)
            logging.info(f"Proxy pool populated with {len(self.proxies)} proxies.")

    def get_next_proxy(self):
        with self.lock:
            if not self.proxies:
                logging.error("No proxies available, exiting.")
                raise RuntimeError("No proxies")
            for _ in range(len(self.proxies)):
                proxy = next(self.proxy_cycle)
                if proxy not in self.failed_proxies:
                    return proxy
            logging.error("All proxies failed, exiting.")
            raise RuntimeError("All proxies failed")

    def mark_proxy_failure(self, proxy):
        with self.lock:
            count = self.proxy_failures.get(proxy, 0) + 1
            self.proxy_failures[proxy] = count
            logging.warning(f"Proxy {proxy} failure count: {count}/{self.max_failures}")
            if count >= self.max_failures:
                self.failed_proxies.add(proxy)
                if proxy in self.proxies:
                    self.proxies.remove(proxy)
                    logging.warning(f"Proxy {proxy} removed from pool.")
                self.proxy_cycle = itertools.cycle(self.proxies) if self.proxies else None
                if not self.proxies:
                    logging.error("All proxies removed, exiting.")
                    raise RuntimeError("All proxies failed")

    def reset_proxy_failures(self, proxy):
        with self.lock:
            if proxy in self.proxy_failures:
                self.proxy_failures[proxy] = 0
            if proxy in self.failed_proxies:
                self.failed_proxies.remove(proxy)
                if proxy not in self.proxies:
                    self.proxies.append(proxy)
                    self.proxy_cycle = itertools.cycle(self.proxies)

# --- Async HTTP fetch with proxy rotation ---

async def fetch_json(session, url, proxy_pool, params=None):
    for _ in range(5):
        proxy = proxy_pool.get_next_proxy()
        proxy_url = proxy if proxy.startswith("http") else f"http://{proxy}"
        try:
            async with session.get(url, proxy=proxy_url, params=params, timeout=10) as resp:
                resp.raise_for_status()
                return await resp.json()
        except Exception as e:
            logging.warning(f"Proxy {proxy} failed for {url}: {e}")
            proxy_pool.mark_proxy_failure(proxy)
            await asyncio.sleep(1)
    raise RuntimeError(f"All proxies failed for {url}")

# --- Fetch Binance symbols ---

async def fetch_binance_symbols(session, proxy_pool):
    url = "https://api.binance.com/api/v3/exchangeInfo"
    data = await fetch_json(session, url, proxy_pool)
    symbols = [s['symbol'] for s in data['symbols'] if s['status'] == 'TRADING' and s['quoteAsset'] == 'USDT']
    return symbols

# --- Fetch Binance candles ---

async def fetch_binance_candles(session, proxy_pool, symbol, interval, limit=3):
    url = "https://api.binance.com/api/v3/klines"
    params = {'symbol': symbol, 'interval': interval, 'limit': limit}
    data = await fetch_json(session, url, proxy_pool, params)
    return data

# --- Fetch Bybit symbols ---

async def fetch_bybit_symbols(session, proxy_pool):
    url = "https://api.bybit.com/v2/public/symbols"
    data = await fetch_json(session, url, proxy_pool)
    symbols = [s['name'] for s in data.get('result', []) if s['quote_currency'] == 'USDT' and s['status'] == 'Trading']
    return symbols

# --- Fetch Bybit candles ---

async def fetch_bybit_candles(session, proxy_pool, symbol, interval, limit=3):
    url = "https://api.bybit.com/public/linear/kline"
    params = {'symbol': symbol, 'interval': interval, 'limit': limit}
    data = await fetch_json(session, url, proxy_pool, params)
    return data.get('result', [])

# --- Candle check ---

def close_equals_next_open(candles):
    try:
        if len(candles) < 2:
            return None
        close_0 = float(candles[0][4]) if isinstance(candles[0], list) else float(candles[0]['close'])
        open_1 = float(candles[1][1]) if isinstance(candles[1], list) else float(candles[1]['open'])
        return close_0 == open_1
    except Exception:
        return None

# --- Telegram message formatting ---

def format_results_message(results):
    lines = ["📊 *Close == Next Open Candle Check Results* 📊\n",
             f"{'Exchange':<8} | {'Symbol':<15} | {'TF':<3} | Result",
             "-" * 45]
    for exch, symbol, timeframe, matched in results:
        status = "Insufficient data" if matched is None else ("✅ MATCH" if matched else "❌ NO MATCH")
        lines.append(f"{exch:<8} | {symbol:<15} | {timeframe:<3} | {status}")
    return "\n".join(lines)

# --- Main async ---

async def main():
    logging.basicConfig(level=logging.INFO)

    proxy_list_raw = os.getenv("PROXY_LIST")
    if not proxy_list_raw:
        logging.error("PROXY_LIST env var not set")
        return
    proxies = [p.strip() for p in proxy_list_raw.split(",") if p.strip()]
    proxy_pool = ProxyPool()
    proxy_pool.populate_from_list(proxies)

    TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
    TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logging.error("Telegram token or chat ID not set")
        return
    bot = Bot(token=TELEGRAM_BOT_TOKEN)

    async with aiohttp.ClientSession() as session:
        # Fetch symbols
        binance_symbols = await fetch_binance_symbols(session, proxy_pool)
        bybit_symbols = await fetch_bybit_symbols(session, proxy_pool)

        logging.info(f"Binance symbols: {len(binance_symbols)}, Bybit symbols: {len(bybit_symbols)}")

        all_symbols = [('binance', sym) for sym in binance_symbols] + [('bybit', sym) for sym in bybit_symbols]

        semaphore = asyncio.Semaphore(10)

        async def check_symbol(exchange, symbol):
            async with semaphore:
                results = []
                for tf in ['1M', '1w', '1d']:
                    try:
                        if exchange == 'binance':
                            candles = await fetch_binance_candles(session, proxy_pool, symbol, tf.lower())
                        else:
                            candles = await fetch_bybit_candles(session, proxy_pool, symbol, tf.lower())
                        matched = close_equals_next_open(candles)
                        results.append((symbol, exchange, tf, matched))
                    except Exception as e:
                        logging.warning(f"Error fetching {symbol} {tf} on {exchange}: {e}")
                        results.append((symbol, exchange, tf, None))
                return results

        tasks = [check_symbol(exch, sym) for exch, sym in all_symbols]
        all_results = await asyncio.gather(*tasks)

        flat_results = [item for sublist in all_results for item in sublist]
        message = format_results_message(flat_results)

        try:
            await bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=message, parse_mode='Markdown')
            logging.info("Telegram message sent successfully.")
        except Exception as e:
            logging.error(f"Failed to send Telegram message: {e}")

if __name__ == "__main__":
    import aiohttp
    asyncio.run(main())
