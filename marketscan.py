import asyncio
import concurrent.futures
import itertools
import logging
import os
import threading
import sys
from typing import List

import pandas as pd
import requests
from binance import AsyncClient
from pybit.unified_trading import HTTP
from telegram import Bot

# --- ProxyPool class ---

class ProxyPool:
    def __init__(self, max_pool_size: int = 25, max_failures: int = 3):
        self.lock = threading.Lock()
        self.max_pool_size = max_pool_size
        self.max_failures = max_failures

        self.proxies: List[str] = []
        self.proxy_failures = {}
        self.failed_proxies = set()
        self.proxy_cycle = None

    def get_next_proxy(self):
        with self.lock:
            if not self.proxies:
                logging.error("Proxy pool empty when requesting next proxy. Exiting!")
                sys.exit(1)
            for _ in range(len(self.proxies)):
                proxy = next(self.proxy_cycle)
                if proxy not in self.failed_proxies:
                    return proxy
            logging.error("All proxies marked as failed. Exiting!")
            sys.exit(1)

    def mark_proxy_failure(self, proxy):
        with self.lock:
            count = self.proxy_failures.get(proxy, 0) + 1
            self.proxy_failures[proxy] = count
            logging.warning(f"Proxy {proxy} failure count: {count}/{self.max_failures}")
            if count >= self.max_failures:
                self.failed_proxies.add(proxy)
                if proxy in self.proxies:
                    self.proxies.remove(proxy)
                    logging.warning(f"Proxy {proxy} removed due to repeated failures.")
                self.proxy_cycle = itertools.cycle(self.proxies) if self.proxies else None
                if not self.proxies:
                    logging.error("All proxies removed due to failures. Exiting!")
                    sys.exit(1)

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
            if not self.proxies:
                logging.error("No proxies provided to populate pool. Exiting!")
                sys.exit(1)
            self.proxy_failures.clear()
            self.failed_proxies.clear()
            self.proxy_cycle = itertools.cycle(self.proxies)
            logging.info(f"Proxy pool populated with {len(self.proxies)} proxies.")

    def has_proxies(self):
        with self.lock:
            return bool(self.proxies)

# --- Binance client wrapper with proxy support ---

class BinanceClient:
    def __init__(self, proxy_pool: ProxyPool):
        self.proxy_pool = proxy_pool
        self.clients = []
        self.lock = asyncio.Lock()

    async def initialize(self):
        for proxy in self.proxy_pool.proxies:
            proxies = {'http': proxy, 'https': proxy}
            try:
                client = await AsyncClient.create(requests_params={'proxies': proxies})
                self.clients.append((client, proxy))
                logging.info(f"Binance client created with proxy {proxy}")
            except Exception as e:
                logging.warning(f"Failed to create Binance client with proxy {proxy}: {e}")
                self.proxy_pool.mark_proxy_failure(proxy)
        if not self.clients:
            logging.error("No Binance clients could be initialized. Exiting!")
            sys.exit(1)

    async def close(self):
        for client, _ in self.clients:
            await client.close_connection()

    async def fetch_ohlcv(self, symbol, interval, limit=3):
        async with self.lock:
            for client, proxy in self.clients:
                try:
                    klines = await client.get_klines(symbol=symbol, interval=interval, limit=limit)
                    self.proxy_pool.reset_proxy_failures(proxy)
                    return klines
                except Exception as e:
                    logging.warning(f"Binance proxy {proxy} failed for {symbol} {interval}: {e}")
                    self.proxy_pool.mark_proxy_failure(proxy)
            raise RuntimeError("All Binance proxies failed")

# --- Bybit client wrapper with proxy support ---

class BybitClient:
    def __init__(self, proxy_pool: ProxyPool):
        self.proxy_pool = proxy_pool
        self.clients = []  # List of (pybit HTTP client, proxy)
        self.lock = asyncio.Lock()

    def create_client_with_proxy(self, proxy):
        session = requests.Session()
        session.proxies.update({
            'http': proxy,
            'https': proxy,
        })
        client = HTTP(testnet=False, session=session)
        return client

    async def initialize(self):
        loop = asyncio.get_event_loop()
        for proxy in self.proxy_pool.proxies:
            try:
                client = await loop.run_in_executor(None, self.create_client_with_proxy, proxy)
                self.clients.append((client, proxy))
                logging.info(f"Bybit client created with proxy {proxy}")
            except Exception as e:
                logging.warning(f"Failed to create Bybit client with proxy {proxy}: {e}")
                self.proxy_pool.mark_proxy_failure(proxy)
        if not self.clients:
            logging.error("No Bybit clients could be initialized. Exiting!")
            sys.exit(1)

    async def fetch_ohlcv(self, symbol, interval, limit=3):
        loop = asyncio.get_event_loop()
        async with self.lock:
            for client, proxy in self.clients:
                try:
                    def fetch():
                        params = {
                            "symbol": symbol,
                            "interval": interval,
                            "limit": limit
                        }
                        resp = client.session.get("https://api.bybit.com/public/linear/kline", params=params, timeout=10)
                        resp.raise_for_status()
                        data = resp.json()
                        if data.get("ret_code") == 0:
                            return data["result"]
                        else:
                            raise Exception(f"Bybit API error: {data}")
                    result = await loop.run_in_executor(None, fetch)
                    self.proxy_pool.reset_proxy_failures(proxy)
                    return result
                except Exception as e:
                    logging.warning(f"Bybit proxy {proxy} failed for {symbol} {interval}: {e}")
                    self.proxy_pool.mark_proxy_failure(proxy)
            raise RuntimeError("All Bybit proxies failed")

# --- Candle check helper ---

def close_equals_next_open(candles):
    try:
        if len(candles) < 2:
            return None
        if isinstance(candles[0], list):
            close_0 = float(candles[0][4])
            open_1 = float(candles[1][1])
        else:
            close_0 = float(candles[0]['close'])
            open_1 = float(candles[1]['open'])
        return close_0 == open_1
    except Exception:
        return None

# --- Telegram message formatting ---

def format_results_message(results):
    lines = []
    lines.append("📊 *Close == Next Open Candle Check Results* 📊\n")
    lines.append(f"{'Exchange':<8} | {'Symbol':<15} | {'TF':<3} | Result")
    lines.append("-" * 45)
    for exch, symbol, timeframe, matched in results:
        if matched is None:
            status = "Insufficient data"
        else:
            status = "✅ MATCH" if matched else "❌ NO MATCH"
        lines.append(f"{exch:<8} | {symbol:<15} | {timeframe:<3} | {status}")
    return "\n".join(lines)

# --- Main async function ---

async def main():
    logging.basicConfig(level=logging.INFO)

    proxy_list_raw = os.getenv("PROXY_LIST")
    if not proxy_list_raw:
        logging.error("PROXY_LIST environment variable not set")
        sys.exit(1)
    proxies = [p.strip() for p in proxy_list_raw.split(",") if p.strip()]

    proxy_pool = ProxyPool(max_pool_size=25)
    proxy_pool.populate_from_list(proxies)

    TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
    TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logging.error("Telegram token or chat ID not set")
        sys.exit(1)
    bot = Bot(token=TELEGRAM_BOT_TOKEN)

    binance_client = BinanceClient(proxy_pool)
    await binance_client.initialize()

    bybit_client = BybitClient(proxy_pool)
    await bybit_client.initialize()

    # Load Binance markets from first client
    binance_markets = {}
    try:
        binance_info = await binance_client.clients[0][0].get_exchange_info()
        binance_markets = {m['symbol']: m for m in binance_info['symbols']}
    except Exception as e:
        logging.error(f"Failed to load Binance markets: {e}")

    # Load Bybit markets (spot and futures)
    bybit_spot_markets = []
    bybit_futures_markets = []
    try:
        resp_spot = bybit_client.clients[0][0].session.get("https://api.bybit.com/v2/public/symbols")
        spot_data = resp_spot.json()
        if spot_data.get("ret_code") == 0:
            bybit_spot_markets = spot_data.get("result", [])

        resp_fut = bybit_client.clients[0][0].session.get("https://api.bybit.com/public/linear/symbols")
        fut_data = resp_fut.json()
        if fut_data.get("ret_code") == 0:
            bybit_futures_markets = fut_data.get("result", [])
    except Exception as e:
        logging.error(f"Failed to load Bybit markets: {e}")

    # Filter symbols
    binance_spot = [s for s, m in binance_markets.items() if m['status'] == 'TRADING' and m['quoteAsset'] == 'USDT' and m.get('contractType', '') == 'SPOT']
    binance_perp = [s for s, m in binance_markets.items() if m['status'] == 'TRADING' and m['quoteAsset'] == 'USDT' and m.get('contractType', '') == 'PERPETUAL']

    bybit_spot = [m['name'] for m in bybit_spot_markets if m['quote_currency'] == 'USDT' and m['status'] == 'Trading']
    bybit_perp = [m['name'] for m in bybit_futures_markets if m['quote_currency'] == 'USDT' and m['status'] == 'Trading']

    logging.info(f"Binance spot: {len(binance_spot)}, perp: {len(binance_perp)}")
    logging.info(f"Bybit spot: {len(bybit_spot)}, perp: {len(bybit_perp)}")

    all_symbols = []
    all_symbols.extend([(symbol, 'binance', 'spot') for symbol in binance_spot])
    all_symbols.extend([(symbol, 'binance', 'perp') for symbol in binance_perp])
    all_symbols.extend([(symbol, 'bybit', 'spot') for symbol in bybit_spot])
    all_symbols.extend([(symbol, 'bybit', 'perp') for symbol in bybit_perp])

    # Remove duplicates
    seen = set()
    unique_symbols = []
    for sym, exch, mtype in all_symbols:
        if sym not in seen:
            seen.add(sym)
            unique_symbols.append((sym, exch, mtype))

    semaphore = asyncio.Semaphore(10)

    async def check_symbol(symbol, exchange_name, market_type):
        async with semaphore:
            results = []
            for timeframe in ['1M', '1w', '1d']:
                try:
                    if exchange_name == 'binance':
                        interval = timeframe.lower()
                        klines = await binance_client.fetch_ohlcv(symbol, interval, limit=3)
                    else:
                        interval = timeframe.lower()
                        klines = await bybit_client.fetch_ohlcv(symbol, interval, limit=3)
                    matched = close_equals_next_open(klines) if klines else None
                    results.append((symbol, exchange_name, timeframe, matched))
                except Exception as e:
                    logging.warning(f"Error fetching {symbol} {timeframe} on {exchange_name}: {e}")
                    results.append((symbol, exchange_name, timeframe, None))
            return results

    tasks = [check_symbol(sym, exch, mtype) for sym, exch, mtype in unique_symbols]
    all_results = await asyncio.gather(*tasks)

    flat_results = [item for sublist in all_results for item in sublist]
    message = format_results_message(flat_results)

    try:
        await bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=message, parse_mode='Markdown')
        logging.info("Telegram message sent successfully.")
    except Exception as e:
        logging.error(f"Failed to send Telegram message: {e}")

    await binance_client.close()
    # No explicit close needed for Bybit clients

if __name__ == '__main__':
    asyncio.run(main())
