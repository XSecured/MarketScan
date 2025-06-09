import asyncio
import ccxt.async_support as ccxt
import itertools
import threading
import time
import logging
import os
import pandas as pd
from telegram import Bot

# --- ProxyPool class (from your EMA distance bot) ---

class ProxyPool:
    def __init__(self, max_pool_size: int = 25, max_failures: int = 3):
        self.lock = threading.Lock()
        self.max_pool_size = max_pool_size
        self.max_failures = max_failures

        self.proxies = []
        self.proxy_failures = {}  # proxy -> failure count
        self.failed_proxies = set()
        self.proxy_cycle = None

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

# --- Helper to create CCXT exchange instance with proxy ---

def create_exchange_with_proxy(exchange_id, proxy):
    proxies = {
        'http': proxy if proxy.startswith('http') else f'http://{proxy}',
        'https': proxy if proxy.startswith('http') else f'http://{proxy}',
    }
    exchange_class = getattr(ccxt, exchange_id)
    return exchange_class({
        'enableRateLimit': True,
        'proxies': proxies,
    })

# --- EMA distance calculation function ---

def calculate_ema_distance(df):
    df = df.copy()
    df['close'] = df['close'].astype(float)
    df['EMA34'] = df['close'].ewm(span=34, adjust=False).mean()
    last = df.iloc[-1]
    distance = (last['close'] - last['EMA34']) / last['EMA34'] * 100
    return distance

# --- Telegram setup ---

TELEGRAM_BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID')

if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
    raise EnvironmentError("Please set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID environment variables")

bot = Bot(token=TELEGRAM_BOT_TOKEN)

async def send_telegram_message(text):
    try:
        await bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=text, parse_mode='Markdown')
        logging.info("Telegram message sent successfully.")
    except Exception as e:
        logging.error(f"Failed to send Telegram message: {e}")

# --- Async semaphore for concurrency control ---

CONCURRENT_REQUESTS = 10
semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)

# --- Fetch candles with concurrency control ---

async def fetch_candles(exchange, symbol, timeframe, limit=100):
    async with semaphore:
        try:
            candles = await exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
            return candles
        except Exception as e:
            logging.warning(f"[{exchange.id}] Error fetching {symbol} {timeframe}: {e}")
            return None

# --- Main scanning task per symbol per exchange instance ---

async def scan_symbol(exchange, symbol):
    # Fetch 100 candles on 1d timeframe for EMA calculation
    candles = await fetch_candles(exchange, symbol, '1d', limit=100)
    if not candles or len(candles) < 34:
        return symbol, exchange.id, None  # Not enough data

    df = pd.DataFrame(candles, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
    distance = calculate_ema_distance(df)
    return symbol, exchange.id, distance

# --- Main async function ---

async def main():
    logging.basicConfig(level=logging.INFO)

    # Load proxies from env or file
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

    # Prepare exchanges per proxy
    binance_exchanges = []
    bybit_exchanges = []

    for proxy in proxies:
        binance_exchanges.append(create_exchange_with_proxy('binance', proxy))
        bybit_exchanges.append(create_exchange_with_proxy('bybit', proxy))

    # Load markets for all exchanges
    async def load_markets(exchange):
        try:
            await exchange.load_markets()
            return True
        except Exception as e:
            logging.error(f"Failed to load markets for {exchange.id} with proxy: {e}")
            return False

    await asyncio.gather(*(load_markets(ex) for ex in binance_exchanges + bybit_exchanges))

    # Collect unique active symbols from all Binance and Bybit instances (take first successful)
    binance_symbols = set()
    bybit_symbols = set()

    for ex in binance_exchanges:
        if ex.markets:
            binance_symbols.update([s for s, m in ex.markets.items() if m['active'] and m['type'] in ['spot', 'future']])
            break

    for ex in bybit_exchanges:
        if ex.markets:
            bybit_symbols.update([s for s, m in ex.markets.items() if m['active'] and m['type'] in ['spot', 'future']])
            break

    # Deduplicate symbols
    all_symbols = list(binance_symbols.union(bybit_symbols))

    logging.info(f"Total unique active symbols to scan: {len(all_symbols)}")

    # Distribute symbols evenly among exchange instances for scanning
    tasks = []
    # Round robin assignment
    for i, symbol in enumerate(all_symbols):
        # Alternate between binance and bybit proxies for load balancing
        if i % 2 == 0 and binance_exchanges:
            ex = binance_exchanges[i % len(binance_exchanges)]
        elif bybit_exchanges:
            ex = bybit_exchanges[i % len(bybit_exchanges)]
        else:
            continue
        tasks.append(scan_symbol(ex, symbol))

    results = await asyncio.gather(*tasks)

    # Format message with EMA distance results
    lines = ["📊 *EMA Distance Scan Results* 📊\n"]
    lines.append(f"{'Exchange':<8} | {'Symbol':<15} | {'EMA Distance (%)':>15}")
    lines.append("-" * 45)
    for symbol, exch, dist in results:
        if dist is None:
            dist_str = "Insufficient data"
        else:
            dist_str = f"{dist:.2f}%"
        lines.append(f"{exch:<8} | {symbol:<15} | {dist_str:>15}")

    message = "\n".join(lines)

    await send_telegram_message(message)

    # Close all exchanges
    await asyncio.gather(*(ex.close() for ex in binance_exchanges + bybit_exchanges))

if __name__ == '__main__':
    asyncio.run(main())
