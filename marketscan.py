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
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from telegram.error import BadRequest

# Shared helpers and constants

FETCH_TIMEOUT_TOTAL = 15          # hard stop for one OHLCV fetch
REQUEST_TIMEOUT     = 5           # per-request timeout handed to requests
MAX_RETRIES         = 5           # fast-fail: only two retries
UTC_NOW_RUN         = None        # will be filled inside main()
MAX_TG_CHARS        = 4_000       # safe margin below 4 096

# List of symbols to ignore in all scanning
IGNORED_SYMBOLS = {
    "USDPUSDT", "USD1USDT", "TUSDUSDT", "AEURUSDT", "USDCUSDT",
    "ZKJUSDT", "FDUSDUSDT", "XUSDUSDT", "EURUSDT", "EURIUSDT", 
    "USDYUSDT", "USDRUSDT", "ETH3LUSDT", "ETHUSDT-27MAR26", 
    "ETHUSDT-27JUN25", "ETHUSDT-26DEC25", "ETHUSDT-26SEP25"
}

# --- Level Persistence Functions ---

def save_levels_to_file(results, filename="detected_levels.json"):
    levels_data = {"last_updated": UTC_NOW_RUN.isoformat(), "levels": {}}

    for (exchange, symbol, market, interval, signal_type, price) in results:
        levels_data["levels"][(key := f"{exchange}_{symbol}_{market}_{interval}")] = {
            "exchange": exchange, "symbol": symbol, "market": market,
            "interval": interval, "price": float(price),
            "signal_type": signal_type, "timestamp": UTC_NOW_RUN.isoformat()
        }

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

# --- Message Editing Functions ---
LEVEL_MSG_FILE = "level_alert_message.json"

def save_level_message_id(message_id: int):
    with open(LEVEL_MSG_FILE, "w") as f:
        json.dump(
            {"message_id": message_id,
             "timestamp": datetime.now(timezone.utc).isoformat()},
            f
        )

def load_level_message_id() -> int | None:
    if not os.path.exists(LEVEL_MSG_FILE):
        return None
    try:
        with open(LEVEL_MSG_FILE) as f:
            return json.load(f).get("message_id")
    except Exception:
        return None

def clear_level_message_id():
    if os.path.exists(LEVEL_MSG_FILE):
        os.remove(LEVEL_MSG_FILE)

# --- Simplified Alert System Using Existing Infrastructure ---

def check_level_hits_simple_concurrent(levels, binance_client, bybit_client):
    """Concurrent level hit checking using ThreadPoolExecutor"""
    hits = []
    
    if not levels:
        return hits
    
    logging.info(f"Checking {len(levels)} levels using concurrent infrastructure...")
    
    # Group levels by exchange, symbol, and interval for efficient checking
    symbols_to_check = {}
    for key, level_info in levels.items():
        exchange = level_info["exchange"]
        symbol = level_info["symbol"]
        market = level_info["market"]
        interval = level_info["interval"]
        
        check_key = f"{exchange}_{symbol}_{market}_{interval}"
        if check_key not in symbols_to_check:
            symbols_to_check[check_key] = []
        symbols_to_check[check_key].append(level_info)
    
    # Use ThreadPoolExecutor for concurrent checking (like full scan)
    lock = threading.Lock()
    
    def check_symbol_levels(check_key, level_list):
        """Check levels for a single symbol/timeframe combination"""
        local_hits = []
        try:
            exchange, symbol, market, interval = check_key.split("_", 3)
            client = binance_client if exchange == "Binance" else bybit_client
            
            # Use existing proven fetch_ohlcv method
            df = client.fetch_ohlcv(symbol, interval, limit=2, market=market)
            
            if df.empty or len(df) < 1:
                return
            
            # Get current price from the latest candle
            current_candle = df.iloc[0]
            current_low = float(current_candle['low'])
            current_high = float(current_candle['high'])
            current_close = float(current_candle['close'])
            
            # Check all levels for this symbol/timeframe
            for level_info in level_list:
                level_price = level_info["price"]
                
                if current_low <= level_price <= current_high:
                    local_hits.append({
                        "exchange": level_info["exchange"],
                        "symbol": symbol,
                        "market": level_info["market"],
                        "interval": level_info["interval"],
                        "signal_type": level_info["signal_type"],
                        "level_price": level_price,
                        "current_price": current_close,
                        "current_low": current_low,
                        "current_high": current_high,
                        "original_timestamp": level_info["timestamp"]
                    })
            
            # Thread-safe append
            with lock:
                hits.extend(local_hits)
                
        except Exception as e:
            logging.warning(f"Failed to check levels for {check_key}: {e}")
    
    # Use same concurrency as full scan
    with concurrent.futures.ThreadPoolExecutor(max_workers=25) as executor:
        futures = [
            executor.submit(check_symbol_levels, check_key, level_list)
            for check_key, level_list in symbols_to_check.items()
        ]
        
        # Use tqdm for progress tracking like full scan
        for _ in tqdm(concurrent.futures.as_completed(futures), 
                     total=len(futures), desc="Checking levels"):
            pass
    
    return hits

def create_alerts_telegram_report(hits):
    """Create single consolidated telegram message for level hits/alerts"""
    if not hits:
        return ["💥 *LEVEL ALERTS* 💥\n\n❌ No levels got hit at this time."]

    # timestamp (reuse global UTC_NOW_RUN)
    utc_plus_3 = timezone(timedelta(hours=3))
    timestamp = UTC_NOW_RUN.astimezone(utc_plus_3).strftime("%Y-%m-%d %H:%M:%S UTC+3")

    # Group hits by timeframe and exchange
    timeframes = {}
    for hit in hits:
        interval = hit["interval"]
        exchange = hit["exchange"]
        
        if interval not in timeframes:
            timeframes[interval] = {"Binance": {"bullish": [], "bearish": []}, "Bybit": {"bullish": [], "bearish": []}}
        
        timeframes[interval][exchange][hit["signal_type"]].append(hit)

    # Build single consolidated message
    message = f"🚨 *LEVEL ALERTS* 🚨\n\n⚡ {len(hits)} levels got hit!\n\n🕒 {timestamp}\n\n"

    # Add each timeframe section
    for interval in ["1M", "1w", "1d"]:
        if interval not in timeframes:
            continue
            
        message += f"📅 *{interval} Alerts*\n\n"
        
        for exchange in ["Binance", "Bybit"]:
            bullish_hits = timeframes[interval][exchange]["bullish"]
            bearish_hits = timeframes[interval][exchange]["bearish"]
            
            if bullish_hits or bearish_hits:
                message += f"*{exchange}*:\n"
                
                if bullish_hits:
                    message += f"🍏 *Bullish ({len(bullish_hits)})*\n"
                    for hit in bullish_hits:
                        message += f"• {hit['symbol']} @ ${hit['level_price']:.6f}\n"
                    message += "\n"
                
                if bearish_hits:
                    message += f"🔻 *Bearish ({len(bearish_hits)})*\n"
                    for hit in bearish_hits:
                        message += f"• {hit['symbol']} @ ${hit['level_price']:.6f}\n"
                    message += "\n"
        
        message += "——————————————————\n\n"

    # Return as single message in a list (to maintain compatibility with existing code)
    return [message.strip()]

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

# --------------------------------------------------
# 2.  BinanceClient  (replace the whole class body)
# --------------------------------------------------
class BinanceClient:
    """
    Thin wrapper around Binance REST endpoints that
    1) re-uses the same TCP connection via requests.Session()
    2) fails fast:  REQUEST_TIMEOUT=5 s, MAX_RETRIES=2
    3) has a global 15-second guard per OHLCV fetch
    """
    def __init__(self, proxy_pool: ProxyPool,
                 max_retries: int = MAX_RETRIES,
                 retry_delay: int = 2,
                 request_timeout: int = REQUEST_TIMEOUT,
                 global_timeout: int = FETCH_TIMEOUT_TOTAL):
        self.proxy_pool     = proxy_pool
        self.max_retries    = max_retries
        self.retry_delay    = retry_delay
        self.req_timeout    = request_timeout
        self.global_timeout = global_timeout
        self.session        = requests.Session()               # <-- keep-alive[2][23]

    # unchanged
    def _get_proxy_dict(self):
        proxy = self.proxy_pool.get_next_proxy()
        if proxy is None:
            raise RuntimeError("No working proxies available")
        return {"http": proxy, "https": proxy}

    # ↓ identical structure, only tighter retries / timeouts
    def get_perp_symbols(self):
        url = 'https://fapi.binance.com/fapi/v1/exchangeInfo'
        for attempt in range(1, self.max_retries + 1):
            proxy = self.proxy_pool.get_next_proxy()
            if proxy is None:
                logging.error("No proxies available to fetch perp symbols")
                time.sleep(self.retry_delay)
                continue
            try:
                resp = self.session.get(url, proxies={"http": proxy, "https": proxy},
                                         timeout=self.req_timeout)
                resp.raise_for_status()
                data = resp.json()
                return [s['symbol'] for s in data['symbols']
                        if s.get('contractType') == 'PERPETUAL'
                        and s['status'] == 'TRADING'
                        and s.get('quoteAsset') == 'USDT']
            except Exception as e:
                logging.warning(f"Attempt {attempt} failed with proxy {proxy}: {e}")
                self.proxy_pool.mark_proxy_failure(proxy)
                time.sleep(self.retry_delay * attempt + random.uniform(0, .5))
        return []

    def get_spot_symbols(self):
        url = 'https://api.binance.com/api/v3/exchangeInfo'
        for attempt in range(1, self.max_retries + 1):
            proxy = self.proxy_pool.get_next_proxy()
            if proxy is None:
                logging.error("No proxies available to fetch spot symbols")
                time.sleep(self.retry_delay)
                continue
            try:
                resp = self.session.get(url,
                                        proxies={"http": proxy, "https": proxy},
                                        timeout=self.req_timeout)
                resp.raise_for_status()
                data = resp.json()
                return [s['symbol'] for s in data['symbols']
                        if s['status'] == 'TRADING' and s.get('quoteAsset') == 'USDT']
            except Exception as e:
                logging.warning(f"Attempt {attempt} failed with proxy {proxy}: {e}")
                self.proxy_pool.mark_proxy_failure(proxy)
                time.sleep(self.retry_delay * attempt + random.uniform(0, .5))
        return []

    # --------------------------------------------------
    def fetch_ohlcv(self, symbol, interval, limit=100, market="spot"):
        base = 'https://api.binance.com/api/v3/klines' if market == "spot" \
               else 'https://fapi.binance.com/fapi/v1/klines'
        params = {'symbol': symbol, 'interval': interval, 'limit': limit}

        start_ts = time.perf_counter()
        attempt  = 1
        while attempt <= self.max_retries:
            if time.perf_counter() - start_ts > self.global_timeout:
                raise RuntimeError(f"Fetch OHLCV timed-out (> {self.global_timeout}s)")
            try:
                proxies = self._get_proxy_dict()
                resp = self.session.get(base, params=params,
                                         proxies=proxies, timeout=self.req_timeout)
                resp.raise_for_status()
                # dtype=float removes later astype() call[8]
                df = pd.DataFrame(resp.json(), dtype=float, columns=[
                    'openTime','open','high','low','close','volume',
                    'closeTime','quoteAssetVolume','numberOfTrades',
                    'takerBuyBase','takerBuyQuote','ignore'])
                df = df.iloc[::-1].reset_index(drop=True)
                return df
            except Exception as e:
                logging.warning(f"Attempt {attempt} failed fetch {symbol} {interval}: {e}")
                self.proxy_pool.mark_proxy_failure(proxies.get('http'))
                time.sleep(self.retry_delay * attempt + random.uniform(0, .5))
                attempt += 1
        raise RuntimeError(f"Binance OHLCV fetch failed for {symbol} {interval}")

# --------------------------------------------------
# 3.  BybitClient  (same slimming as BinanceClient)
# --------------------------------------------------
class BybitClient:
    def __init__(self, proxy_pool: ProxyPool,
                 max_retries: int = MAX_RETRIES,
                 retry_delay: int = 2,
                 request_timeout: int = REQUEST_TIMEOUT,
                 global_timeout: int = FETCH_TIMEOUT_TOTAL):
        self.proxy_pool     = proxy_pool
        self.max_retries    = max_retries
        self.retry_delay    = retry_delay
        self.req_timeout    = request_timeout
        self.global_timeout = global_timeout
        self.session        = requests.Session()               # keep-alive

    def _get_proxy_dict(self):
        proxy = self.proxy_pool.get_next_proxy()
        if proxy is None:
            raise RuntimeError("No working proxies available")
        return {"http": proxy, "https": proxy}

    # --------------------------------------------------
    # perp symbols
    # --------------------------------------------------
    def get_perp_symbols(self):
        url     = 'https://api.bybit.com/v5/market/instruments-info'
        params  = {'category': 'linear'}
        for attempt in range(1, self.max_retries + 1):
            proxy = self.proxy_pool.get_next_proxy()
            if proxy is None:
                logging.error("No proxies available to fetch Bybit perp symbols")
                time.sleep(self.retry_delay)
                continue
            try:
                resp = self.session.get(
                    url, params=params,
                    proxies={"http": proxy, "https": proxy},
                    timeout=self.req_timeout
                )
                resp.raise_for_status()
                data = resp.json()
                return [s['symbol'] for s in data['result']['list']
                        if s['status'] == 'Trading'
                        and s['quoteCoin'] == 'USDT']
            except Exception as e:
                logging.warning(f"Attempt {attempt} failed with proxy {proxy}: {e}")
                self.proxy_pool.mark_proxy_failure(proxy)
                time.sleep(self.retry_delay * attempt + random.uniform(0, .5))
        return []

    # --------------------------------------------------
    # spot symbols
    # --------------------------------------------------
    def get_spot_symbols(self):
        url     = 'https://api.bybit.com/v5/market/instruments-info'
        params  = {'category': 'spot'}
        for attempt in range(1, self.max_retries + 1):
            proxy = self.proxy_pool.get_next_proxy()
            if proxy is None:
                logging.error("No proxies available to fetch Bybit spot symbols")
                time.sleep(self.retry_delay)
                continue
            try:
                resp = self.session.get(
                    url, params=params,
                    proxies={"http": proxy, "https": proxy},
                    timeout=self.req_timeout
                )
                resp.raise_for_status()
                data = resp.json()
                return [s['symbol'] for s in data['result']['list']
                        if s['status'] == 'Trading'
                        and s['quoteCoin'] == 'USDT']
            except Exception as e:
                logging.warning(f"Attempt {attempt} failed with proxy {proxy}: {e}")
                self.proxy_pool.mark_proxy_failure(proxy)
                time.sleep(self.retry_delay * attempt + random.uniform(0, .5))
        return []

    def fetch_ohlcv(self, symbol, interval, limit=100, market="spot"):
        url       = 'https://api.bybit.com/v5/market/kline'
        category  = 'linear' if market == 'perp' else 'spot'
        bybit_int = {"1M": "M", "1w": "W", "1d": "D"}[interval]
        params    = {'category': category, 'symbol': symbol,
                     'interval': bybit_int, 'limit': limit}

        start_ts = time.perf_counter()
        attempt  = 1
        while attempt <= self.max_retries:
            if time.perf_counter() - start_ts > self.global_timeout:
                raise RuntimeError(f"Fetch OHLCV timed-out (> {self.global_timeout}s)")
            try:
                proxies = self._get_proxy_dict()
                resp = self.session.get(url, params=params,
                                         proxies=proxies, timeout=self.req_timeout)
                resp.raise_for_status()
                kl = resp.json()['result']['list']
                df = pd.DataFrame(kl, dtype=float,
                                  columns=['openTime','open','high','low',
                                           'close','volume','turnover'])
                return df
            except Exception as e:
                logging.warning(f"Attempt {attempt} failed fetch Bybit {symbol}: {e}")
                self.proxy_pool.mark_proxy_failure(proxies.get('http'))
                time.sleep(self.retry_delay * attempt + random.uniform(0, .5))
                attempt += 1
        raise RuntimeError(f"Bybit OHLCV fetch failed for {symbol} {interval}")

# --- Helper functions ---

def floats_are_equal(a: float, b: float, rel_tol: float = 0.003) -> bool:
    """
    Return True if a and b differ by less than rel_tol (0.003 = 0.30 %)
    """
    return abs(a - b) <= rel_tol * ( (a + b) / 2.0 )

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
    if not results:
        return ["💥 *Reversal Level Scanner*\n\n❌ No qualifying reversal patterns found at this time."]

    # ------------- group by TF / exchange -------------
    timeframes = {}
    for entry in results:
        # works whether the tuple is 5- or 6-element
        exchange, symbol, market, interval, signal_type = entry[:5]

        if interval not in timeframes:
            timeframes[interval] = {
                "Binance": {"bullish": [], "bearish": []},
                "Bybit":   {"bullish": [], "bearish": []}
            }
        timeframes[interval][exchange][signal_type].append(symbol)
    
    # timestamp (reuse global UTC_NOW_RUN)
    utc_plus_3 = timezone(timedelta(hours=3))
    timestamp = UTC_NOW_RUN.astimezone(utc_plus_3).strftime("%Y-%m-%d %H:%M:%S UTC+3")
    
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

async def safe_send_markdown(bot: Bot, chat_id: int, text: str):
    """
    Guarantees that no chunk exceeds Telegram’s 4096-character limit.
    Splits on newline boundaries, keeps Markdown intact.
    """
    if len(text) <= MAX_TG_CHARS:
        await bot.send_message(chat_id=chat_id, text=text, parse_mode='Markdown')
        return

    chunk = ''
    for line in text.splitlines(keepends=True):
        if len(chunk) + len(line) > MAX_TG_CHARS:
            await bot.send_message(chat_id=chat_id, text=chunk, parse_mode='Markdown')
            chunk = ''
        chunk += line
    if chunk.strip():
        await bot.send_message(chat_id=chat_id, text=chunk, parse_mode='Markdown')

# --- Main async scanning and reporting ---

async def main():
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(levelname)s %(message)s')

    # fresh timestamp for **this** run
    global UTC_NOW_RUN
    UTC_NOW_RUN = datetime.utcnow().replace(tzinfo=timezone.utc)
    
    # Determine Run Mode
    run_mode = os.getenv("RUN_MODE", "full_scan")
    logging.info(f"Running in {run_mode} mode")
    
    TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
    TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
    
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        logging.error("Telegram environment variables not fully set")
        return
    
    bot = Bot(token=TELEGRAM_TOKEN)
    
    if run_mode == "price_check":
        # CONCURRENT MODE: Check level hits using ThreadPoolExecutor like full scan
        logging.info("🔍 Concurrent price check mode - using ThreadPoolExecutor...")
    
        levels = load_levels_from_file()
        if not levels:
            logging.info("No levels to check, skipping...")
            return
    
        # Use the SAME proxy system as full scan
        proxy_url = os.getenv("PROXY_LIST_URL")
        if not proxy_url:
            logging.error("PROXY_LIST_URL environment variable not set")
            return

        # Create the SAME proxy pool and clients as full scan
        proxy_pool = ProxyPool(max_pool_size=25)
        proxy_pool.populate_from_url(proxy_url)
        proxy_pool.start_checker()

        binance_client = BinanceClient(proxy_pool)
        bybit_client = BybitClient(proxy_pool)
    
        # Use CONCURRENT level checking (Solution 1 only)
        hits = check_level_hits_simple_concurrent(levels, binance_client, bybit_client)
    
        if hits:
            logging.info(f"🚨 Found {len(hits)} level hits!")
            alert_messages = create_alerts_telegram_report(hits)
     
            # Now it's just one message instead of multiple
            msg = alert_messages[0]
            stored_id = load_level_message_id()

            try:
                if stored_id:                          # edit the existing alert
                    await bot.edit_message_text(
                        chat_id=int(TELEGRAM_CHAT_ID),
                        message_id=stored_id,
                        text=msg,
                        parse_mode='Markdown'
                    )
                    logging.info("Level alert message UPDATED")
                else:                                   # first alert of the day
                    sent = await safe_send_markdown(bot, int(TELEGRAM_CHAT_ID), msg)
                    save_level_message_id(sent.message_id)
                    logging.info("Level alert message SENT and ID stored")
                    
            except Exception as e:                     # optional fallback
                logging.warning(f"Edit failed: {e} — sending new alert")
                sent = await safe_send_markdown(bot, int(TELEGRAM_CHAT_ID), msg)
                save_level_message_id(sent.message_id)

        else:
            logging.info("✅ No level hits detected")

    else:
        # FULL SCAN MODE: Do complete pattern detection
        # new day → start a fresh LEVEL ALERT thread
        clear_level_message_id()
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
                    if equal_price and signal_type and \
                       not current_candle_touched_price(df, equal_price):
                        with lock:
                            results.append((
                                exchange_name, symbol, market,
                                interval, signal_type, equal_price   # keep tuple
                            ))
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
                await safe_send_markdown(bot, int(TELEGRAM_CHAT_ID), msg)
                await asyncio.sleep(1)  # Rate limiting
            except Exception as e:
                logging.error(f"Failed to send Telegram message: {e}")

        # Save current results for next run
        save_levels_to_file(results)
        logging.info("Saved current levels for next run")

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
