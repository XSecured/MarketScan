import asyncio
import json
import logging
import os
import random
import math
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Set, Optional, Tuple, Any
from itertools import cycle

import aiohttp
from telegram import Bot
from telegram.error import BadRequest
from tqdm.asyncio import tqdm
from redis.asyncio import Redis

# ==========================================
# CONFIGURATION
# ==========================================

@dataclass
class Config:
    FETCH_TIMEOUT_TOTAL: int = 15
    REQUEST_TIMEOUT: int = 5
    MAX_RETRIES: int = 3
    MAX_TG_CHARS: int = 4000
    MAX_CONCURRENCY: int = 50
    REDIS_URL: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    
    # Environment
    TELEGRAM_TOKEN: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
    CHAT_ID: str = os.getenv("TELEGRAM_CHAT_ID", "")
    PROXY_URL: str = os.getenv("PROXY_LIST_URL", "")
    RUN_MODE: str = os.getenv("RUN_MODE", "full_scan")

    IGNORED_SYMBOLS: Set[str] = field(default_factory=lambda: {
        "USDPUSDT", "USD1USDT", "TUSDUSDT", "AEURUSDT", "USDCUSDT",
        "ZKJUSDT", "FDUSDUSDT", "XUSDUSDT", "EURUSDT", "EURIUSDT",
        "USDYUSDT", "USDRUSDT", "ETH3LUSDT", "ETHUSDT-27MAR26",
        "ETHUSDT-27JUN25", "ETHUSDT-26DEC25", "ETHUSDT-26SEP25"
    })

CONFIG = Config()

# ==========================================
# DATA MODELS
# ==========================================

@dataclass
class Candle:
    open: float
    high: float
    low: float
    close: float

@dataclass
class LevelHit:
    exchange: str
    symbol: str
    market: str
    interval: str
    signal_type: str
    level_price: float
    current_price: float = 0.0
    timestamp: str = ""
    hit_timestamp: float = 0.0
    
    def to_dict(self):
        return {
            "exchange": self.exchange,
            "symbol": self.symbol,
            "market": self.market,
            "interval": self.interval,
            "price": self.level_price,
            "signal_type": self.signal_type,
            "timestamp": self.timestamp,
            "hit_timestamp": self.hit_timestamp
        }

@dataclass
class LowMovementHit:
    exchange: str
    symbol: str
    market: str
    interval: str
    movement_percent: float

# ==========================================
# ROBUST PROXY POOL (FIXED)
# ==========================================

class AsyncProxyPool:
    def __init__(self, max_pool_size=25):
        self.queue = asyncio.Queue()
        self.max_pool_size = max_pool_size

    async def populate(self, url: str, session: aiohttp.ClientSession):
        """Fetches AND validates proxies with high-speed early exit."""
        if not url:
            logging.warning("⚠️ No Proxy URL provided! Running without proxies.")
            return

        # 1. Fetch Proxy List
        raw_proxies = []
        try:
            logging.info(f"📥 Fetching proxies from {url}...")
            async with session.get(url, timeout=15) as resp:
                if resp.status == 200:
                    text = await resp.text()
                    for line in text.splitlines():
                        p = line.strip()
                        if p:
                            raw_proxies.append(p if "://" in p else f"http://{p}")
        except Exception as e:
            logging.error(f"❌ Failed to fetch proxy list: {e}")
            return

        logging.info(f"🔎 Validating {len(raw_proxies)} proxies (Target: {self.max_pool_size})...")
        
        # Temporary list to hold valid proxies before filling queue
        valid_proxies = []
        
        # Shuffle to avoid getting stuck in a block of bad IPs from the same subnet
        random.shuffle(raw_proxies)
        
        # 2. Create Tasks with Semaphore (Throttling)
        sem = asyncio.Semaphore(200)

        async def protected_test(p):
            async with sem:
                return await self._test_proxy(p, session)

        tasks = [asyncio.create_task(protected_test(p)) for p in raw_proxies]

        # 3. Process results AS SOON AS THEY FINISH
        for future in asyncio.as_completed(tasks):
            try:
                proxy, is_good = await future
                if is_good:
                    valid_proxies.append(proxy)
                    # EXIT IMMEDIATELY once we have enough
                    if len(valid_proxies) >= self.max_pool_size:
                        break
            except:
                pass
        
        # 4. Cleanup: Cancel remaining tasks
        cancelled_count = 0
        for t in tasks:
            if not t.done():
                t.cancel()
                cancelled_count += 1
        
        await asyncio.sleep(0.1)
        
        if valid_proxies:
            # Populate the Queue with our valid proxies
            for p in valid_proxies:
                self.queue.put_nowait(p)
                
            logging.info(f"✅ Proxy Pool Ready: {self.queue.qsize()} working proxies (Cancelled {cancelled_count} redundant checks).")
        else:
            logging.error("❌ NO WORKING PROXIES FOUND! Calls will likely fail.")

    async def _test_proxy(self, proxy: str, session: aiohttp.ClientSession) -> Tuple[str, bool]:
        try:
            # Simple connection test to Binance API
            test_url = "https://api.binance.com/api/v3/time"
            async with session.get(test_url, proxy=proxy, timeout=5) as resp:
                return proxy, resp.status == 200
        except:
            return proxy, False

    async def get_proxy(self) -> Optional[str]:
        """Returns a proxy from the queue and rotates it to the back."""
        if self.queue.empty():
            return None
        
        # Get a proxy (No waiting for lock)
        proxy = await self.queue.get()
        
        # Put it back immediately (Round Robin rotation)
        self.queue.put_nowait(proxy)
        
        return proxy
        
# ==========================================
# EXCHANGE CLIENTS
# ==========================================

class ExchangeClient:
    def __init__(self, session: aiohttp.ClientSession, proxy_pool: AsyncProxyPool):
        self.session = session
        self.proxies = proxy_pool
        # Higher concurrency only if we have proxies, otherwise limit strictly
        limit = CONFIG.MAX_CONCURRENCY if not proxy_pool.queue.empty() else 5
        self.sem = asyncio.Semaphore(limit)

    async def _request(self, url: str, params: dict = None) -> Any:
        """Robust async request with retries."""
        for attempt in range(CONFIG.MAX_RETRIES):
            proxy = await self.proxies.get_proxy()
            try:
                async with self.sem: 
                    async with self.session.get(
                        url, params=params, proxy=proxy, timeout=CONFIG.REQUEST_TIMEOUT
                    ) as resp:
                        if resp.status == 200:
                            return await resp.json()
                        elif resp.status == 429:
                            await asyncio.sleep(5) # Rate limit backoff
            except Exception:
                pass 
            
            await asyncio.sleep(0.5 * attempt)
        return None

class BinanceClient(ExchangeClient):
    async def get_perp_symbols(self) -> List[str]:
        data = await self._request('https://fapi.binance.com/fapi/v1/exchangeInfo')
        if not data: return []
        return [s['symbol'] for s in data['symbols'] 
                if s.get('contractType') == 'PERPETUAL' and s['status'] == 'TRADING' and s.get('quoteAsset') == 'USDT']

    async def get_spot_symbols(self) -> List[str]:
        data = await self._request('https://api.binance.com/api/v3/exchangeInfo')
        if not data: return []
        return [s['symbol'] for s in data['symbols'] 
                if s['status'] == 'TRADING' and s.get('quoteAsset') == 'USDT']

    async def fetch_ohlcv(self, symbol: str, interval: str, market: str, limit: int = 3) -> List[Candle]:
        base = 'https://api.binance.com/api/v3/klines' if market == "spot" else 'https://fapi.binance.com/fapi/v1/klines'
        data = await self._request(base, {'symbol': symbol, 'interval': interval, 'limit': limit})
        if not data or not isinstance(data, list): return []
        
        candles = []
        for c in data:
            try:
                candles.append(Candle(open=float(c[1]), high=float(c[2]), low=float(c[3]), close=float(c[4])))
            except: continue
        # Reverse: Binance gives Oldest->Newest. We want Newest->Oldest (Index 0 = Newest)
        return candles[::-1]

class BybitClient(ExchangeClient):
    async def get_perp_symbols(self) -> List[str]:
        data = await self._request('https://api.bybit.com/v5/market/instruments-info', {'category': 'linear'})
        if not data: return []
        return [s['symbol'] for s in data['result']['list'] if s['status'] == 'Trading' and s['quoteCoin'] == 'USDT']

    async def get_spot_symbols(self) -> List[str]:
        data = await self._request('https://api.bybit.com/v5/market/instruments-info', {'category': 'spot'})
        if not data: return []
        return [s['symbol'] for s in data['result']['list'] if s['status'] == 'Trading' and s['quoteCoin'] == 'USDT']

    async def fetch_ohlcv(self, symbol: str, interval: str, market: str, limit: int = 3) -> List[Candle]:
        url = 'https://api.bybit.com/v5/market/kline'
        cat = 'linear' if market == 'perp' else 'spot'
        bybit_int = {"1M": "M", "1w": "W", "1d": "D"}.get(interval, "D")
        
        data = await self._request(url, {'category': cat, 'symbol': symbol, 'interval': bybit_int, 'limit': limit})
        if not data: return []
        
        raw_list = data.get('result', {}).get('list', [])
        if not raw_list: return []
        
        candles = []
        for c in raw_list:
             candles.append(Candle(open=float(c[1]), high=float(c[2]), low=float(c[3]), close=float(c[4])))
        
        # Bybit V5 gives Newest->Oldest. No reverse needed. Index 0 is Newest.
        return candles 

# ==========================================
# CORE LOGIC
# ==========================================

def floats_are_equal(a: float, b: float, rel_tol: float = 0.003) -> bool:
    return abs(a - b) <= rel_tol * ((a + b) / 2.0)

def check_reversal(candles: List[Candle]) -> Tuple[Optional[float], Optional[str]]:
    """Analyzes 3 candles. Expects Index 0 = Newest."""
    if len(candles) < 3: return None, None

    # 0=Live, 1=Last Closed, 2=Prev Closed
    last_closed = candles[1]
    second_last_closed = candles[2]

    # Logic: Prev Close vs Last Open
    if floats_are_equal(second_last_closed.close, last_closed.open):
        equal_price = second_last_closed.close
        
        # Colors
        second_last_is_red = second_last_closed.close < second_last_closed.open
        second_last_is_green = second_last_closed.close > second_last_closed.open
        last_is_green = last_closed.close > last_closed.open
        last_is_red = last_closed.close < last_closed.open

        # Bullish: Red then Green
        if second_last_is_red and last_is_green:
            return equal_price, "bullish"
        
        # Bearish: Green then Red
        elif second_last_is_green and last_is_red:
            return equal_price, "bearish"

    return None, None

def current_candle_touched_price(candles: List[Candle], price: float) -> bool:
    if not candles: return False
    curr = candles[0] # Newest
    return curr.low <= price <= curr.high

def check_low_movement(candles: List[Candle], threshold_percent: float = 1.0) -> Optional[float]:
    if len(candles) < 2: return None
    last = candles[1] # Last closed
    if last.open == 0: return None
    
    move_pct = abs((last.close - last.open) / last.open) * 100
    if move_pct < threshold_percent: 
        return move_pct
    return None

# ==========================================
# STATE & UTILS
# ==========================================

import json
from redis.asyncio import Redis
from typing import List

class RedisManager:
    def __init__(self, redis_url: str, bot_prefix: str = "reversal_bot"):
        """
        Initializes Redis connection and defines namespaced keys.
        
        Args:
            redis_url: Connection string (e.g., redis://localhost:6379/0)
            bot_prefix: Namespace to separate data if running multiple bots.
        """
        self.redis = Redis.from_url(redis_url, decode_responses=True)
        self.prefix = bot_prefix
        
        # Key Patterns
        # Hits and Messages are still collections, so they use single keys
        self.KEY_HITS = f"{bot_prefix}:daily_hits"
        self.KEY_MSGS = f"{bot_prefix}:message_ids"
        
        # NOTE: Levels are stored as individual keys:
        # {prefix}:level:{exchange}:{symbol}:{interval}

    async def close(self):
        """Closes the Redis connection."""
        await self.redis.close()

    # --- 1. INDIVIDUAL LEVELS (String Keys with TTL) ---
    async def save_levels(self, results: List[LevelHit]):
        """
        Saves levels as individual keys.
        Updates TTL to 3 days for any level found in the scan.
        """
        if not results:
            return

        # Use pipeline for atomic high-speed writing
        async with self.redis.pipeline() as pipe:
            for r in results:
                # Key format enforces "One Level Per Timeframe"
                key = f"{self.prefix}:level:{r.exchange}:{r.symbol}:{r.interval}"
                
                # Save Data
                await pipe.set(key, json.dumps(r.to_dict()))
                
                # Set/Reset TTL to 3 Days (259200 seconds)
                # If a level is found again tomorrow, this resets the timer back to 3 days.
                await pipe.expire(key, 259200) 
            
            await pipe.execute()

    async def get_all_levels(self) -> List[dict]:
        """
        Efficiently retrieves all active level keys using SCAN + MGET batching.
        """
        # 1. Scan for all keys matching the bot's level pattern
        cursor = '0'
        keys = []
        pattern = f"{self.prefix}:level:*"
        
        # Loop until SCAN returns cursor 0 (iteration complete)
        while cursor != 0:
            cursor, chunk = await self.redis.scan(cursor=cursor, match=pattern, count=1000)
            keys.extend(chunk)
            
        if not keys:
            return []

        # 2. Fetch all values in batches (Network Optimization)
        values = []
        chunk_size = 500 # Fetch 500 levels per network round-trip
        
        for i in range(0, len(keys), chunk_size):
            batch_keys = keys[i : i + chunk_size]
            batch_vals = await self.redis.mget(batch_keys)
            # Filter out any None values (just in case a key expired mid-process)
            values.extend([v for v in batch_vals if v])
            
        return [json.loads(v) for v in values]

    async def remove_level(self, exchange: str, symbol: str, interval: str):
        """
        Deletes a specific level key (e.g., after it has been hit).
        """
        key = f"{self.prefix}:level:{exchange}:{symbol}:{interval}"
        await self.redis.delete(key)

    # --- 2. DAILY HITS (Sorted Set) ---
    async def clear_daily_hits(self):
        """Clears the daily hits collection (Run at start of daily scan)."""
        await self.redis.delete(self.KEY_HITS)

    async def add_hit(self, hit: LevelHit):
        """
        Adds a hit to the sorted set.
        Score = timestamp (Sorts automatically by time).
        """
        data_str = json.dumps(hit.to_dict())
        # ZADD adds to Sorted Set. duplicate data is handled automatically.
        await self.redis.zadd(self.KEY_HITS, {data_str: hit.hit_timestamp})
        # Keep the hits list alive for 25 hours (rolling window)
        await self.redis.expire(self.KEY_HITS, 90000)

    async def get_daily_hits(self) -> List[dict]:
        """
        Retrieves all hits sorted by newest first.
        """
        # ZREVRANGE key 0 -1 -> Returns all elements, High Score (Newest) to Low Score
        raw_hits = await self.redis.zrevrange(self.KEY_HITS, 0, -1)
        return [json.loads(h) for h in raw_hits]

    # --- 3. TELEGRAM MESSAGE IDs (String) ---
    async def clear_message_ids(self):
        await self.redis.delete(self.KEY_MSGS)

    async def save_message_ids(self, ids: List[int]):
        await self.redis.set(self.KEY_MSGS, json.dumps(ids), ex=90000)

    async def get_message_ids(self) -> List[int]:
        data = await self.redis.get(self.KEY_MSGS)
        return json.loads(data) if data else []
        
def normalize_symbol(symbol: str) -> str:
    s = symbol.upper().split(':')[0]
    if '/' in s: return s
    return f"{s[:-4]}/{s[-4:]}" if s.endswith("USDT") else s

# ==========================================
# MAIN BOT CLASS
# ==========================================

class MarketScanBot:
    def __init__(self):
        self.utc_now = datetime.now(timezone.utc)
        self.tg_bot = Bot(token=CONFIG.TELEGRAM_TOKEN) if CONFIG.TELEGRAM_TOKEN else None
        self.db = RedisManager(CONFIG.REDIS_URL)

    async def run(self):
        logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
        
        if not CONFIG.TELEGRAM_TOKEN or not CONFIG.CHAT_ID:
            logging.error("❌ Missing Telegram Env Vars")
            return

        # Initialize Redis Manager
        self.db = RedisManager(CONFIG.REDIS_URL)

        try:
            # Optimized Connector
            connector = aiohttp.TCPConnector(limit=100, ttl_dns_cache=300)
            async with aiohttp.ClientSession(connector=connector) as session:
                # 1. Setup Proxy
                proxies = AsyncProxyPool()
                if CONFIG.PROXY_URL:
                    await proxies.populate(CONFIG.PROXY_URL, session)
                
                # 2. Clients
                binance = BinanceClient(session, proxies)
                bybit = BybitClient(session, proxies)

                if CONFIG.RUN_MODE == "price_check":
                    await self.run_price_check(binance, bybit)
                else:
                    await self.run_full_scan(binance, bybit)
        finally:
            # Ensure Redis connection is closed cleanly
            await self.db.close()

    # ==========================================
    # PRICE CHECK MODE
    # ==========================================
    
    async def run_price_check(self, binance: BinanceClient, bybit: BybitClient):
        logging.info("🚀 Starting Price Check...")

        # 1. Get All Active Levels from Redis
        # (This performs the SCAN + MGET batching under the hood)
        levels_dicts = await self.db.get_all_levels()
        
        if not levels_dicts:
            logging.info("⚠️ No active levels found in Redis.")
            # Optional: Clear the pinned message if there's nothing to watch
            await self.send_or_update_alert_report([]) 
            return

        logging.info(f"🔍 Checking {len(levels_dicts)} active levels...")

        # 2. Create Check Tasks
        tasks = []
        for data in levels_dicts:
            client = binance if data['exchange'] == 'Binance' else bybit
            tasks.append(self.check_single_level(client, data))

        # 3. Process Results
        new_hits_count = 0
        
        # Process tasks as they complete
        for f in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Checking"):
            hit = await f
            if hit:
                # A. Add to Confirmed Hits (Redis Sorted Set)
                await self.db.add_hit(hit)
                
                # B. Remove the Level from Active Monitoring
                # Since it was hit, we delete it so it doesn't alert again.
                await self.db.remove_level(hit.exchange, hit.symbol, hit.interval)
                
                new_hits_count += 1

        if new_hits_count > 0:
            logging.info(f"⚡ Found {new_hits_count} new hits!")
        else:
            logging.info("✅ No new hits detected.")
        
        # 4. Fetch ALL Confirmed Hits for Today (Old + New)
        # Redis ZREVRANGE returns them sorted by time (Newest First)
        all_hits_dicts = await self.db.get_daily_hits()
        
        if all_hits_dicts:
            # Convert dicts back to LevelHit objects for the reporting function
            final_objects = [
                LevelHit(
                    exchange=d['exchange'],
                    symbol=d['symbol'],
                    market=d['market'],
                    interval=d['interval'],
                    signal_type=d['signal_type'],
                    level_price=d['price'],
                    timestamp=d.get('timestamp', ''),
                    hit_timestamp=d.get('hit_timestamp', 0)
                ) for d in all_hits_dicts
            ]
            
            # Update the Telegram Message
            await self.send_or_update_alert_report(final_objects)
        else:
            # If we had hits before but they expired (rare), or just empty state
            await self.send_or_update_alert_report([])

    async def check_single_level(self, client: ExchangeClient, data: dict) -> Optional[LevelHit]:
        candles = await client.fetch_ohlcv(data['symbol'], data['interval'], data['market'], limit=2)
        if not candles: return None
        
        curr = candles[0]
        target = data['price']
        
        if curr.low <= target <= curr.high:
            return LevelHit(
                exchange=data['exchange'], symbol=data['symbol'], market=data['market'],
                interval=data['interval'], signal_type=data['signal_type'],
                level_price=target, current_price=curr.close, timestamp=data['timestamp']
            )
        return None

    # ==========================================
    # FULL SCAN MODE
    # ==========================================
    async def run_full_scan(self, binance: BinanceClient, bybit: BybitClient):
        logging.info("🌍 Starting FULL Market Scan...")
        
        # 1. Reset Daily State in Redis
        await self.db.clear_message_ids()
        await self.db.clear_daily_hits()
        
        # 2. RELIABLE SYMBOL FETCHING (with Retries)
        async def fetch_symbols_with_retry(fetch_func, name, max_attempts=5):
            for attempt in range(max_attempts):
                try:
                    symbols = await fetch_func()
                    if symbols and len(symbols) > 0:
                        logging.info(f"✅ {name}: Found {len(symbols)} symbols.")
                        return symbols
                except Exception as e:
                    logging.warning(f"⚠️ {name} fetch error: {e}")
                
                wait_time = 2 * (attempt + 1)
                logging.warning(f"⚠️ {name} returned empty/failed. Retrying in {wait_time}s... (Attempt {attempt+1}/{max_attempts})")
                await asyncio.sleep(wait_time)
            
            logging.error(f"❌ {name} FAILED after {max_attempts} attempts.")
            return []
    
        tasks = [
            fetch_symbols_with_retry(binance.get_perp_symbols, "Binance Perp"),
            fetch_symbols_with_retry(binance.get_spot_symbols, "Binance Spot"),
            fetch_symbols_with_retry(bybit.get_perp_symbols, "Bybit Perp"),
            fetch_symbols_with_retry(bybit.get_spot_symbols, "Bybit Spot"),
        ]
    
        bp, bs, yp, ys = await asyncio.gather(*tasks)
    
        if not (bp or bs or yp or ys):
            logging.error("❌ CRITICAL: Failed to fetch ANY symbols. Check network/proxies.")
            return

        # 3. Deduplication & Filtering Logic
        seen_normalized = set()
        
        def filter_unique(symbols: List[str]) -> List[str]:
            unique_list = []
            for s in symbols:
                if s in CONFIG.IGNORED_SYMBOLS: continue
                if not s.endswith("USDT"): continue

                norm = normalize_symbol(s)
                if norm not in seen_normalized:
                    unique_list.append(s)
                    seen_normalized.add(norm)
            return unique_list

        final_bp = filter_unique(bp)
        final_bs = filter_unique(bs)
        final_yp = filter_unique(yp)
        final_ys = filter_unique(ys)

        logging.info(f"Scanning: B-Perp:{len(final_bp)} B-Spot:{len(final_bs)} Y-Perp:{len(final_yp)} Y-Spot:{len(final_ys)}")

        # 4. Build Scan Tasks
        scan_tasks = []
        def add_tasks(client, syms, mkt, ex_name):
            for s in syms:
                scan_tasks.append(self.scan_symbol_all_tfs(client, s, mkt, ex_name))

        add_tasks(binance, final_bp, 'perp', 'Binance')
        add_tasks(binance, final_bs, 'spot', 'Binance')
        add_tasks(bybit, final_yp, 'perp', 'Bybit')
        add_tasks(bybit, final_ys, 'spot', 'Bybit')

        # 5. Execute Scan
        results: List[LevelHit] = []
        low_movements: List[LowMovementHit] = []
        
        total_fetches_attempted = len(scan_tasks) * 3 # 3 TFs per symbol
        total_fetches_success = 0
        
        for f in tqdm(asyncio.as_completed(scan_tasks), total=len(scan_tasks), desc="Scanning"):
            revs, low, success_cnt = await f  # <--- Unpack 3 values
            
            results.extend(revs)
            if low: low_movements.append(low)
            total_fetches_success += success_cnt

        # Log the stats
        success_rate = (total_fetches_success / total_fetches_attempted) * 100 if total_fetches_attempted > 0 else 0
        logging.info(f"📊 Scan Complete. Data Fetch Success Rate: {total_fetches_success}/{total_fetches_attempted} ({success_rate:.1f}%)")

        # 6. Save & Report (Using Redis)
        await self.db.save_levels(results)  # <--- Redis Save
        
        await self.send_full_report(results, low_movements)
        await self.send_or_update_alert_report([]) # Reset the pinned alert

    async def scan_symbol_all_tfs(self, client: ExchangeClient, symbol: str, market: str, exchange: str):
        reversals = []
        low_move = None
        success_count = 0 # How many TFs loaded successfully?
        
        intervals = ["1M", "1w", "1d"]
        
        tasks = [
            client.fetch_ohlcv(symbol, interval, market, limit=3) 
            for interval in intervals
        ]
        
        results_list = await asyncio.gather(*tasks, return_exceptions=True)
        
        for i, candles in enumerate(results_list):
            interval = intervals[i]

            # Check for failure
            if isinstance(candles, Exception) or not candles:
                # Logic: If it's empty or an error, it failed.
                continue
            
            # If we are here, we have valid data!
            success_count += 1

            # Logic 1: Reversal
            price, sig = check_reversal(candles)
            if price and sig:
                if not current_candle_touched_price(candles, price):
                    reversals.append(LevelHit(exchange, symbol, market, interval, sig, price))
            
            # Logic 2: Low Movement
            if interval == "1d":
                lm = check_low_movement(candles, threshold_percent=1.0)
                if lm is not None:
                    low_move = LowMovementHit(exchange, symbol, market, interval, lm)
                    
        return reversals, low_move, success_count

    # ==========================================
    # TELEGRAM LOGIC
    # ==========================================

    async def send_chunks(self, text: str) -> List[int]:
        """Splits text and sends messages, returning list of IDs."""
        ids = []
        chunks = []
        temp_chunk = ''
        for line in text.splitlines(keepends=True):
            if len(temp_chunk) + len(line) > CONFIG.MAX_TG_CHARS:
                chunks.append(temp_chunk)
                temp_chunk = ''
            temp_chunk += line
        if temp_chunk.strip():
            chunks.append(temp_chunk)
        
        for chunk in chunks:
            try:
                m = await self.tg_bot.send_message(
                    chat_id=CONFIG.CHAT_ID,
                    text=chunk,
                    parse_mode='Markdown'
                )
                ids.append(m.message_id)
                await asyncio.sleep(0.5)
            except Exception as e:
                logging.error(f"Failed to send chunk: {e}")
        return ids

    async def send_or_update_alert_report(self, hits: List[LevelHit]):
        """Updates the pinned alert message or sends new one with New Look."""
        timestamp = self.utc_now.astimezone(timezone(timedelta(hours=3))).strftime("%d %b %H:%M UTC+3")
        
        def _clean_sym(s: str) -> str:
            return s.replace("USDT", "")
        
        def _fmt_price(p: float) -> str:
            return f"${p:g}"

        if not hits:
            text = (
                "🚨 *LEVEL ALERT SYSTEM*\n"
                "═════════════════════\n"
                "❌ No active level hits right now.\n"
                "═════════════════════\n"
                f"🕒 {timestamp}"
            )
        else:
            lines = [
                "🚨 *LEVEL ALERT SYSTEM*",
                "═════════════════════\n",
                f"⚡ *{len(hits)} LEVELS HIT!*",
                ""
            ]
            
            grouped = {}
            for h in hits:
                grouped.setdefault(h.interval, {}).setdefault(h.exchange, {}).setdefault(h.signal_type, []).append(h)
            
            tf_labels = {"1M": "MONTHLY (1M)", "1w": "WEEKLY (1w)", "1d": "DAILY (1d)"}

            for interval in ["1M", "1w", "1d"]:
                if interval not in grouped: continue
                
                lines.append(f"📅 *{tf_labels.get(interval, interval)}*\n")
                
                exchanges = [("Binance", "🟡"), ("Bybit", "⚫")]
                
                for ex_name, ex_icon in exchanges:
                    ex_data = grouped[interval].get(ex_name, {})
                    bull = sorted(ex_data.get("bullish", []), key=lambda x: x.symbol)
                    bear = sorted(ex_data.get("bearish", []), key=lambda x: x.symbol)
                    
                    if not bull and not bear:
                        continue

                    lines.append(f"┌ {ex_icon} *{ex_name.upper()}*")
                    
                    def fmt_list(items):
                        return ", ".join([f"{_clean_sym(x.symbol)} ({_fmt_price(x.level_price)})" for x in items])

                    if bull and bear:
                        lines.append(f"│ 🍏 *Bull*: {fmt_list(bull)}")
                        lines.append(f"└ 🔻 *Bear*: {fmt_list(bear)}")
                    elif bull:
                        lines.append(f"└ 🍏 *Bull*: {fmt_list(bull)}")
                    elif bear:
                        lines.append(f"└ 🔻 *Bear*: {fmt_list(bear)}")
                    
                    lines.append("") 
                
            lines.append("═════════════════════")
            lines.append(f"🕒 {timestamp}")
            text = "\n".join(lines)

        # ---------------------------------------------------------
        # Message ID Management (Using Redis)
        # ---------------------------------------------------------
        prev_ids = await self.db.get_message_ids()  # <--- Redis Load
        new_ids = []
        
        chunks = []
        temp_chunk = ''
        for line in text.splitlines(keepends=True):
            if len(temp_chunk) + len(line) > CONFIG.MAX_TG_CHARS:
                chunks.append(temp_chunk)
                temp_chunk = ''
            temp_chunk += line
        if temp_chunk.strip(): chunks.append(temp_chunk)
        
        for idx, chunk in enumerate(chunks):
            # Try to edit existing message
            if idx < len(prev_ids):
                try:
                    await self.tg_bot.edit_message_text(chat_id=CONFIG.CHAT_ID, message_id=prev_ids[idx], text=chunk, parse_mode='Markdown')
                    new_ids.append(prev_ids[idx])
                    continue
                except Exception:
                    pass 
            
            # Send new message if needed
            try:
                m = await self.tg_bot.send_message(chat_id=CONFIG.CHAT_ID, text=chunk, parse_mode='Markdown')
                new_ids.append(m.message_id)
            except Exception as e:
                logging.error(f"TG Send Error: {e}")

        await self.db.save_message_ids(new_ids) # <--- Redis Save

    async def send_full_report(self, results: List[LevelHit], low_movements: List[LowMovementHit]):
        timestamp = self.utc_now.astimezone(timezone(timedelta(hours=3))).strftime("%d %b %H:%M UTC+3")
        
        # Helper to strip USDT for clean display
        def _clean_sym(s: str) -> str:
            return s.replace("USDT", "")

        # 1. Header
        lines = [
            "🚨 *REVERSAL ALERT SYSTEM*",
            "═════════════════════\n"
        ]
        
        # 2. Reversal Sections
        grouped = {}
        for r in results:
            grouped.setdefault(r.interval, {}).setdefault(r.exchange, {}).setdefault(r.signal_type, []).append(_clean_sym(r.symbol))
            
        has_any_data = False
        for tf_label, tf_key in [("MONTHLY (1M)", "1M"), ("WEEKLY (1w)", "1w"), ("DAILY (1d)", "1d")]:
            if tf_key not in grouped: continue
            
            tf_data = grouped[tf_key]
            # Check if this timeframe actually has content to print
            if not any(tf_data.values()): continue
            
            lines.append(f"📅 *{tf_label}*\n")
            has_any_data = True

            # Define specific order and styling
            exchanges = [("Binance", "🟡"), ("Bybit", "⚫")]
            
            for ex_name, ex_icon in exchanges:
                ex_data = tf_data.get(ex_name, {})
                bull = sorted(ex_data.get("bullish", []))
                bear = sorted(ex_data.get("bearish", []))
                
                if not bull and not bear:
                    continue

                lines.append(f"┌ {ex_icon} *{ex_name.upper()}*")
                
                # Logic to handle the tree lines (│ vs └)
                # If we have both bull and bear, first one gets │, last gets └
                # If we only have one, it gets └
                
                if bull and bear:
                    lines.append(f"│ 🍏 *Bull*: {', '.join(bull)}")
                    lines.append(f"└ 🔻 *Bear*: {', '.join(bear)}")
                elif bull:
                    lines.append(f"└ 🍏 *Bull*: {', '.join(bull)}")
                elif bear:
                    lines.append(f"└ 🔻 *Bear*: {', '.join(bear)}")
                
                lines.append("") # Spacer between exchanges

            lines.append("═════════════════════\n")

        if not has_any_data:
            lines.append("❌ No Reversal Signals Detected.\n")

        # 3. Low Movement Section
        if low_movements:
            low_movements.sort(key=lambda x: x.movement_percent)
            lines.append("📉 *SQUEEZE ALERT (<1%)*")
            
            # Group by exchange
            lm_grouped = {"Binance": [], "Bybit": []}
            for x in low_movements:
                if x.exchange in lm_grouped:
                    lm_grouped[x.exchange].append(x)
            
            for ex_name, ex_icon in [("Binance", "🟡"), ("Bybit", "⚫")]:
                items = lm_grouped.get(ex_name, [])
                if not items: continue
                
                lines.append(f"┌ {ex_icon} *{ex_name.upper()}*")
                # Limit to top 10 per exchange to save space
                top_items = items[:10]
                
                for i, item in enumerate(top_items):
                    is_last = (i == len(top_items) - 1)
                    prefix = "└" if is_last else "│"
                    clean_s = _clean_sym(item.symbol)
                    lines.append(f"{prefix} {clean_s} ({item.movement_percent:.2f}%)")
                
                if len(items) > 10:
                    lines.append(f"└ ...and {len(items)-10} more")
                lines.append("")

        # 4. Footer
        lines.append(f"🕒 {timestamp}")

        # Join and Send
        full_text = "\n".join(lines)
        await self.send_chunks(full_text)

if __name__ == "__main__":
    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    bot = MarketScanBot()
    asyncio.run(bot.run())
