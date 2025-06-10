import requests
import threading
import itertools
import time
import os
import logging
from telegram import Bot

# --- ProxyPool (EMA style) ---

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
                exit(1)
            self.proxy_failures.clear()
            self.failed_proxies.clear()
            self.proxy_cycle = itertools.cycle(self.proxies)
            logging.info(f"Proxy pool populated with {len(self.proxies)} proxies.")

    def get_next_proxy(self):
        with self.lock:
            if not self.proxies:
                logging.error("No proxies available, exiting.")
                exit(1)
            for _ in range(len(self.proxies)):
                proxy = next(self.proxy_cycle)
                if proxy not in self.failed_proxies:
                    return proxy
            logging.error("All proxies failed, exiting.")
            exit(1)

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
                    exit(1)

    def reset_proxy_failures(self, proxy):
        with self.lock:
            if proxy in self.proxy_failures:
                self.proxy_failures[proxy] = 0
            if proxy in self.failed_proxies:
                self.failed_proxies.remove(proxy)
                if proxy not in self.proxies:
                    self.proxies.append(proxy)
                    self.proxy_cycle = itertools.cycle(self.proxies)

# --- HTTP GET with proxy rotation (requests) ---

def fetch_json(url, proxy_pool, params=None):
    for _ in range(5):
        proxy = proxy_pool.get_next_proxy()
        proxy_url = proxy if proxy.startswith("http") else f"http://{proxy}"
        proxies = {"http": proxy_url, "https": proxy_url}
        try:
            resp = requests.get(url, proxies=proxies, params=params, timeout=10)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logging.warning(f"Proxy {proxy} failed for {url}: {e}")
            proxy_pool.mark_proxy_failure(proxy)
            time.sleep(1)
    raise RuntimeError(f"All proxies failed for {url}")

# --- Proxy pre-check function ---

def test_proxy(proxy):
    proxy_url = proxy if proxy.startswith("http") else f"http://{proxy}"
    proxies = {"http": proxy_url, "https": proxy_url}
    try:
        resp = requests.get("https://api.binance.com/api/v3/time", proxies=proxies, timeout=5)
        return resp.status_code == 200
    except:
        return False

def populate_and_test_proxies(proxy_list):
    valid_proxies = []
    for proxy in proxy_list:
        if test_proxy(proxy):
            valid_proxies.append(proxy)
        else:
            logging.warning(f"Proxy {proxy} failed test and will be skipped")
    return valid_proxies

# --- Fetch Binance and Bybit symbols/candles ---

def fetch_binance_symbols(proxy_pool):
    url = "https://api.binance.com/api/v3/exchangeInfo"
    data = fetch_json(url, proxy_pool)
    return [s['symbol'] for s in data['symbols'] if s['status'] == 'TRADING' and s['quoteAsset'] == 'USDT']

def fetch_binance_candles(proxy_pool, symbol, interval, limit=3):
    url = "https://api.binance.com/api/v3/klines"
    params = {'symbol': symbol, 'interval': interval, 'limit': limit}
    return fetch_json(url, proxy_pool, params)

def fetch_bybit_symbols(proxy_pool):
    url = "https://api.bybit.com/v2/public/symbols"
    data = fetch_json(url, proxy_pool)
    return [s['name'] for s in data.get('result', []) if s['quote_currency'] == 'USDT' and s['status'] == 'Trading']

def fetch_bybit_candles(proxy_pool, symbol, interval, limit=3):
    url = "https://api.bybit.com/public/linear/kline"
    params = {'symbol': symbol, 'interval': interval, 'limit': limit}
    data = fetch_json(url, proxy_pool, params)
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

# --- Main ---

def main():
    logging.basicConfig(level=logging.INFO)

    proxy_list_raw = os.getenv("PROXY_LIST")
    if not proxy_list_raw:
        logging.error("PROXY_LIST env var not set")
        exit(1)
    proxies = [p.strip() for p in proxy_list_raw.split(",") if p.strip()]

    proxy_pool = ProxyPool()

    # Pre-check proxies
    valid_proxies = populate_and_test_proxies(proxies)
    proxy_pool.populate_from_list(valid_proxies)

    # Wait for at least 20 proxies
    while len(proxy_pool.proxies) < 20:
        logging.info(f"Waiting for at least 20 valid proxies, currently {len(proxy_pool.proxies)}")
        time.sleep(10)

    TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
    TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logging.error("Telegram token or chat ID not set")
        exit(1)
    bot = Bot(token=TELEGRAM_BOT_TOKEN)

    # Fetch symbols
    binance_symbols = fetch_binance_symbols(proxy_pool)
    bybit_symbols = fetch_bybit_symbols(proxy_pool)
    logging.info(f"Binance symbols: {len(binance_symbols)}, Bybit symbols: {len(bybit_symbols)}")

    all_symbols = [('binance', sym) for sym in binance_symbols] + [('bybit', sym) for sym in bybit_symbols]

    # Threaded candle checks for speed
    results = []
    lock = threading.Lock()
    def check_symbol(exchange, symbol):
        for tf in ['1M', '1w', '1d']:
            try:
                if exchange == 'binance':
                    candles = fetch_binance_candles(proxy_pool, symbol, tf.lower())
                else:
                    candles = fetch_bybit_candles(proxy_pool, symbol, tf.lower())
                matched = close_equals_next_open(candles)
                with lock:
                    results.append((exchange, symbol, tf, matched))
            except Exception as e:
                logging.warning(f"Error fetching {symbol} {tf} on {exchange}: {e}")
                with lock:
                    results.append((exchange, symbol, tf, None))

    threads = []
    for exch, sym in all_symbols:
        t = threading.Thread(target=check_symbol, args=(exch, sym))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()

    message = format_results_message(results)
    try:
        bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=message, parse_mode='Markdown')
        logging.info("Telegram message sent successfully.")
    except Exception as e:
        logging.error(f"Failed to send Telegram message: {e}")

if __name__ == "__main__":
    main()
