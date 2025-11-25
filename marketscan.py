import os
import json
import time
import logging
import asyncio
import random
import itertools
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import List, Dict, Optional, Tuple, Set, Any

# Third-party imports
import aiohttp
from telegram import Bot
from telegram.error import BadRequest
from tqdm.asyncio import tqdm

# -----------------------------------------------------------------------------
# Configuration & Constants
# -----------------------------------------------------------------------------

FETCH_TIMEOUT_TOTAL = 15  # Hard stop for one fetch operation
REQUEST_TIMEOUT = 5       # Connect/Read timeout
MAX_RETRIES = 2           # Fast-fail policy
MAX_CONCURRENCY = 100     # Max concurrent requests (adjust based on proxy count)
MAX_TG_CHARS = 4_000      # Telegram message chunk limit

# Files
LEVELS_FILE = "detected_levels.json"
LEVEL_MSG_FILE = "level_alert_message.json"

# Symbols to completely ignore
IGNORED_SYMBOLS = {
    "USDPUSDT", "USD1USDT", "TUSDUSDT", "AEURUSDT", "USDCUSDT",
    "ZKJUSDT", "FDUSDUSDT", "XUSDUSDT", "EURUSDT", "EURIUSDT",
    "USDYUSDT", "USDRUSDT", "ETH3LUSDT", "ETHUSDT-27MAR26",
    "ETHUSDT-27JUN25", "ETHUSDT-26DEC25", "ETHUSDT-26SEP25"
}

# Global runtime timestamp
UTC_NOW_RUN = None

# -----------------------------------------------------------------------------
# Data Persistence
# -----------------------------------------------------------------------------

def save_levels_to_file(results: List[Tuple], filename: str = LEVELS_FILE):
    """Persist detected levels to JSON."""
    levels_data = {
        "last_updated": UTC_NOW_RUN.isoformat(),
        "levels": {}
    }
    
    count = 0
    for (exchange, symbol, market, interval, signal_type, price) in results:
        if signal_type == "low_movement":
            continue
            
        key = f"{exchange}_{symbol}_{market}_{interval}"
        levels_data["levels"][key] = {
            "exchange": exchange,
            "symbol": symbol,
            "market": market,
            "interval": interval,
            "price": float(price),
            "signal_type": signal_type,
            "timestamp": UTC_NOW_RUN.isoformat()
        }
        count += 1
        
    try:
        with open(filename, "w") as f:
            json.dump(levels_data, f, indent=2)
        logging.info(f"Saved {count} levels to {filename}")
    except Exception as e:
        logging.error(f"Failed to save levels: {e}")

def load_levels_from_file(filename: str = LEVELS_FILE) -> Dict:
    """Load previously detected levels."""
    try:
        if os.path.exists(filename):
            with open(filename, "r") as f:
                data = json.load(f)
            levels = data.get("levels", {})
            logging.info(f"Loaded {len(levels)} levels from file.")
            return levels
    except Exception as e:
        logging.error(f"Failed to load levels: {e}")
    return {}

def save_level_message_ids(message_ids: List[int]):
    """Save Telegram message IDs for updating later."""
    try:
        with open(LEVEL_MSG_FILE, "w") as f:
            json.dump({
                "message_ids": message_ids,
                "timestamp": datetime.now(timezone.utc).isoformat()
            }, f)
    except Exception as e:
        logging.error(f"Failed to save message IDs: {e}")

def load_level_message_ids() -> List[int]:
    """Load Telegram message IDs."""
    if not os.path.exists(LEVEL_MSG_FILE):
        return []
    try:
        with open(LEVEL_MSG_FILE) as f:
            data = json.load(f)
        return data.get("message_ids", [])
    except Exception:
        return []

def clear_level_message_ids():
    """Clear stored Telegram message IDs."""
    if os.path.exists(LEVEL_MSG_FILE):
        os.remove(LEVEL_MSG_FILE)
        logging.info("Cleared stored message IDs.")

# -----------------------------------------------------------------------------
# Async Proxy System
# -----------------------------------------------------------------------------

class AsyncProxyPool:
    def __init__(self, max_pool_size=25, check_interval=600, max_failures=3):
        self.max_pool_size = max_pool_size
        self.check_interval = check_interval
        self.max_failures = max_failures
        self.proxies: List[str] = []
        self.failures: Dict[str, int] = {}
        self.failed_set: Set[str] = set()
        self.cycle = None
        self._stop_event = asyncio.Event()
        self._lock = asyncio.Lock()

    async def get_next(self) -> Optional[str]:
        async with self._lock:
            if not self.proxies:
                return None
            # Simple round-robin
            for _ in range(len(self.proxies)):
                try:
                    proxy = next(self.cycle)
                    if proxy not in self.failed_set:
                        return proxy
                except StopIteration:
                    self.cycle = itertools.cycle(self.proxies)
            return None

    async def mark_failure(self, proxy: str):
        if not proxy:
            return
        async with self._lock:
            self.failures[proxy] = self.failures.get(proxy, 0) + 1
            if self.failures[proxy] >= self.max_failures:
                self.failed_set.add(proxy)
                if proxy in self.proxies:
                    self.proxies.remove(proxy)
                    # Re-create cycle to avoid Stale references
                    self.cycle = itertools.cycle(self.proxies) if self.proxies else None
                logging.warning(f"Proxy {proxy} removed due to failures.")

    async def populate_from_url(self, url: str):
        logging.info(f"Fetching proxies from {url}...")
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(url, timeout=10) as resp:
                    resp.raise_for_status()
                    text = await resp.text()
                    raw = [line.strip() for line in text.splitlines() if line.strip()]
                    
                    # Normalize
                    candidates = []
                    for p in raw:
                        if "://" not in p:
                            candidates.append(f"http://{p}")
                        else:
                            candidates.append(p)
                    
                    # Test them
                    working = await self._test_proxies(candidates, limit=self.max_pool_size)
                    
                    async with self._lock:
                        self.proxies = working
                        self.failures.clear()
                        self.failed_set.clear()
                        self.cycle = itertools.cycle(self.proxies) if self.proxies else None
                    
                    logging.info(f"Pool populated with {len(self.proxies)} working proxies.")

            except Exception as e:
                logging.error(f"Failed to populate proxies: {e}")

    async def _test_single(self, session: aiohttp.ClientSession, proxy: str) -> bool:
        test_url = "https://api.binance.com/api/v3/time"
        try:
            async with session.get(test_url, proxy=proxy, timeout=5) as resp:
                return resp.status == 200
        except:
            return False

    async def _test_proxies(self, candidates: List[str], limit: int) -> List[str]:
        working = []
        async with aiohttp.ClientSession() as session:
            # Process in chunks to avoid opening too many local connections at once during check
            chunk_size = 50
            for i in range(0, len(candidates), chunk_size):
                if len(working) >= limit:
                    break
                chunk = candidates[i : i + chunk_size]
                tasks = [self._test_single(session, p) for p in chunk]
                results = await asyncio.gather(*tasks)
                
                for proxy, is_ok in zip(chunk, results):
                    if is_ok:
                        working.append(proxy)
                        if len(working) >= limit:
                            break
        return working

    async def start_health_check(self):
        asyncio.create_task(self._health_loop())

    async def _health_loop(self):
        while not self._stop_event.is_set():
            await asyncio.sleep(self.check_interval)
            logging.info("Running proxy health check...")
            if self.proxies:
                working = await self._test_proxies(self.proxies, len(self.proxies))
                async with self._lock:
                    self.proxies = working
                    self.cycle = itertools.cycle(self.proxies) if self.proxies else None
                
                # Replenish if needed
                if len(self.proxies) < self.max_pool_size:
                    # In a real app, you'd store the URL to re-fetch. 
                    # For now, we assume partial degradation is acceptable 
                    # or we rely on the initial big fetch.
                    pass

# -----------------------------------------------------------------------------
# Async Exchange Clients
# -----------------------------------------------------------------------------

class ExchangeClient:
    """Base class for async exchange interactions."""
    def __init__(self, proxy_pool: AsyncProxyPool):
        self.proxy_pool = proxy_pool
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=FETCH_TIMEOUT_TOTAL),
            connector=aiohttp.TCPConnector(ssl=False, limit=0) # Limit handled by Semaphore
        )

    async def close(self):
        await self.session.close()

    async def _request(self, method: str, url: str, params: dict = None) -> Any:
        for attempt in range(1, MAX_RETRIES + 1):
            proxy = await self.proxy_pool.get_next()
            if not proxy:
                # If no proxy, wait briefly and try again (or fail)
                await asyncio.sleep(1)
                continue
                
            try:
                async with self.session.request(
                    method, url, params=params, proxy=proxy, timeout=REQUEST_TIMEOUT
                ) as resp:
                    resp.raise_for_status()
                    return await resp.json()
            except Exception:
                await self.proxy_pool.mark_failure(proxy)
                # Exponential backoff with jitter
                await asyncio.sleep(0.5 * attempt + random.uniform(0, 0.2))
        
        return None

class BinanceClient(ExchangeClient):
    async def get_perp_symbols(self) -> List[str]:
        data = await self._request('GET', 'https://fapi.binance.com/fapi/v1/exchangeInfo')
        if not data: return []
        return [
            s['symbol'] for s in data['symbols']
            if s.get('contractType') == 'PERPETUAL' and s['status'] == 'TRADING' and s.get('quoteAsset') == 'USDT'
        ]

    async def get_spot_symbols(self) -> List[str]:
        data = await self._request('GET', 'https://api.binance.com/api/v3/exchangeInfo')
        if not data: return []
        return [
            s['symbol'] for s in data['symbols']
            if s['status'] == 'TRADING' and s.get('quoteAsset') == 'USDT'
        ]

    async def fetch_ohlcv(self, symbol: str, interval: str, limit: int, market: str) -> List[Dict]:
        base = 'https://api.binance.com/api/v3/klines' if market == "spot" else 'https://fapi.binance.com/fapi/v1/klines'
        params = {'symbol': symbol, 'interval': interval, 'limit': limit}
        
        data = await self._request('GET', base, params=params)
        if not data: return []
        
        # API returns [time, open, high, low, close, vol, ...]
        # We convert to list of dicts for processing, newest last in list from API?
        # Binance returns Oldest -> Newest. 
        # Logic expects: Index 0=Newest, 1=LastClosed, 2=2ndLastClosed
        # So we reverse it.
        
        results = []
        for k in reversed(data):
            results.append({
                'open': float(k[1]),
                'high': float(k[2]),
                'low': float(k[3]),
                'close': float(k[4])
            })
        return results

class BybitClient(ExchangeClient):
    async def get_perp_symbols(self) -> List[str]:
        url = 'https://api.bybit.com/v5/market/instruments-info'
        data = await self._request('GET', url, params={'category': 'linear'})
        if not data: return []
        return [
            s['symbol'] for s in data['result']['list']
            if s['status'] == 'Trading' and s['quoteCoin'] == 'USDT'
        ]

    async def get_spot_symbols(self) -> List[str]:
        url = 'https://api.bybit.com/v5/market/instruments-info'
        data = await self._request('GET', url, params={'category': 'spot'})
        if not data: return []
        return [
            s['symbol'] for s in data['result']['list']
            if s['status'] == 'Trading' and s['quoteCoin'] == 'USDT'
        ]

    async def fetch_ohlcv(self, symbol: str, interval: str, limit: int, market: str) -> List[Dict]:
        url = 'https://api.bybit.com/v5/market/kline'
        category = 'linear' if market == 'perp' else 'spot'
        bybit_int = {"1M": "M", "1w": "W", "1d": "D"}.get(interval, interval)
        params = {'category': category, 'symbol': symbol, 'interval': bybit_int, 'limit': limit}
        
        data = await self._request('GET', url, params=params)
        if not data: return []
        
        # Bybit returns: [time, open, high, low, close, vol, ...]
        # IMPORTANT: Bybit V5 returns Newest -> Oldest. 
        # So Index 0 is Newest. No need to reverse.
        
        results = []
        for k in data['result']['list']:
            results.append({
                'open': float(k[1]),
                'high': float(k[2]),
                'low': float(k[3]),
                'close': float(k[4])
            })
        return results

# -----------------------------------------------------------------------------
# Core Logic (No Pandas)
# -----------------------------------------------------------------------------

def floats_are_equal(a: float, b: float, rel_tol: float = 0.003) -> bool:
    return abs(a - b) <= rel_tol * ((a + b) / 2.0)

def analyze_candles(candles: List[Dict]) -> Tuple[Optional[float], Optional[str]]:
    """
    Analyzes candle data for reversal patterns.
    candles[0] = Live/Newest
    candles[1] = Last Closed
    candles[2] = Second Last Closed
    """
    if len(candles) < 3:
        return None, None

    second_last = candles[2]
    last = candles[1]

    sl_open, sl_close = second_last['open'], second_last['close']
    l_open, l_close = last['open'], last['close']

    # Candle Colors
    sl_is_green = sl_close > sl_open
    sl_is_red = sl_close < sl_open
    l_is_green = l_close > l_open
    l_is_red = l_close < l_open

    # Check Pattern: Close of previous == Open of current
    if floats_are_equal(sl_close, l_open):
        equal_price = sl_close
        
        # Bullish: Red -> Green
        if sl_is_red and l_is_green:
            return equal_price, "bullish"
        
        # Bearish: Green -> Red
        elif sl_is_green and l_is_red:
            return equal_price, "bearish"

    return None, None

def check_price_touch(candles: List[Dict], price: float) -> bool:
    """Check if current live candle touched the price."""
    if not candles: return False
    current = candles[0]
    return current['low'] <= price <= current['high']

def analyze_low_movement(candles: List[Dict], threshold: float = 1.0) -> Optional[float]:
    """Check if last closed daily candle had low movement."""
    if len(candles) < 2:
        return None
    
    last_closed = candles[1]
    op, cl = last_closed['open'], last_closed['close']
    
    if op == 0: return None
    
    m_pct = abs((cl - op) / op) * 100
    if m_pct < threshold:
        return m_pct
    return None

# -----------------------------------------------------------------------------
# Reporting
# -----------------------------------------------------------------------------

def create_report_sections(hits: List, low_moves: List = None) -> List[str]:
    if low_moves is None: low_moves = []
    
    # Prepare Timestamp
    utc_plus_3 = timezone(timedelta(hours=3))
    ts_str = UTC_NOW_RUN.astimezone(utc_plus_3).strftime("%Y-%m-%d %H:%M:%S UTC+3")
    
    sections = []
    
    # -- Reversal Summary --
    summary = "💥 *Reversal Level Scanner* (Async Boosted 🚀)\n\n"
    summary += f"✅ Total Signals: {len(hits)}\n"
    summary += f"🕒 {ts_str}"
    sections.append(summary)
    
    # -- Grouping --
    # Map: Interval -> Exchange -> Type -> List[Symbol]
    grouped = {} 
    
    for h in hits:
        # Unpack based on tuple size (full scan vs alert check)
        if isinstance(h, dict):
            # From alert check dict
            exc, sym, iv, st, price = h['exchange'], h['symbol'], h['interval'], h['signal_type'], h['level_price']
        else:
            # From full scan tuple
            exc, sym, _, iv, st, price = h
            
        if iv not in grouped: grouped[iv] = {}
        if exc not in grouped[iv]: grouped[iv][exc] = {"bullish": [], "bearish": []}
        grouped[iv][exc][st].append((sym, price))

    for iv in ["1M", "1w", "1d"]:
        if iv not in grouped: continue
        
        text = f"📅 *{iv} Timeframe*\n\n"
        has_data = False
        
        for exc in ["Binance", "Bybit"]:
            if exc not in grouped[iv]: continue
            bull = sorted(grouped[iv][exc]["bullish"])
            bear = sorted(grouped[iv][exc]["bearish"])
            
            if not bull and not bear: continue
            has_data = True
            
            text += f"*{exc}*:\n"
            if bull:
                text += f"🍏 *Bullish ({len(bull)})*\n" + "\n".join([f"• {s} @ {p:.4f}" for s, p in bull]) + "\n\n"
            if bear:
                text += f"🔻 *Bearish ({len(bear)})*\n" + "\n".join([f"• {s} @ {p:.4f}" for s, p in bear]) + "\n\n"
                
        if has_data:
            text += "——————————————————\n\n"
            sections.append(text.strip())

    # -- Low Movement --
    if low_moves:
        lm_text = "📉 *Low Movement Daily Candles (<1.0%)*\n\n"
        # Sort by movement
        sorted_lm = sorted(low_moves, key=lambda x: x['movement_percent'])
        
        for exc in ["Binance", "Bybit"]:
            subset = [x for x in sorted_lm if x['exchange'] == exc]
            if subset:
                lm_text += f"*{exc}*:\n"
                for item in subset:
                    lm_text += f"• {item['symbol']} ({item['movement_percent']:.2f}%)\n"
                lm_text += \n"
        
        lm_text += "——————————————————\n\n"
        sections.append(lm_text.strip())

    return sections

async def safe_send_markdown(bot: Bot, chat_id: int, text: str) -> List[int]:
    sent_ids = []
    async def _send(t):
        try:
            m = await bot.send_message(chat_id=chat_id, text=t, parse_mode='Markdown')
            sent_ids.append(m.message_id)
        except Exception as e:
            logging.error(f"TG Send Error: {e}")

    if len(text) <= MAX_TG_CHARS:
        await _send(text)
    else:
        chunk = ''
        for line in text.splitlines(keepends=True):
            if len(chunk) + len(line) > MAX_TG_CHARS:
                await _send(chunk)
                chunk = ''
            chunk += line
        if chunk.strip():
            await _send(chunk)
            
    return sent_ids

# -----------------------------------------------------------------------------
# Main Application Logic
# -----------------------------------------------------------------------------

def normalize_symbol(symbol: str) -> str:
    s = symbol.upper()
    if ':' in s: s = s.split(':')[0]
    if '/' in s: return s
    return f"{s[:-4]}/{s[-4:]}"

def apply_priority(bin_perp, bin_spot, byb_perp, byb_spot):
    """Deduplicate symbols based on priority."""
    # Helper to map normalized -> original
    def map_norm(syms): return {normalize_symbol(s): s for s in syms}
    
    bp_n = map_norm(bin_perp)
    bs_n = map_norm(bin_spot)
    byp_n = map_norm(byb_perp)
    bys_n = map_norm(byb_spot)

    # Priority Sets (Normalized keys)
    set_bp = set(bp_n.keys())
    set_bs = set(bs_n.keys()) - set_bp
    set_byp = set(byp_n.keys()) - set_bp - set_bs
    set_bys = set(bys_n.keys()) - set_bp - set_bs - set_byp

    return (
        {bp_n[k] for k in set_bp},
        {bs_n[k] for k in set_bs},
        {byp_n[k] for k in set_byp},
        {bys_n[k] for k in set_bys}
    )

async def worker_scan(sem, client, exchange, symbol, market, results, low_move_results):
    async with sem:
        for interval in ["1M", "1w", "1d"]:
            try:
                # Using our new lightweight analyzer
                candles = await client.fetch_ohlcv(symbol, interval, 3, market)
                
                # Reversal Check
                eq_price, sig_type = analyze_candles(candles)
                if eq_price and sig_type:
                    if not check_price_touch(candles, eq_price):
                        results.append((exchange, symbol, market, interval, sig_type, eq_price))

                # Daily Low Movement Check
                if interval == "1d":
                    mv = analyze_low_movement(candles, threshold=2.5)
                    if mv is not None:
                        low_move_results.append({
                            "exchange": exchange, "symbol": symbol, 
                            "market": market, "interval": interval, 
                            "movement_percent": mv
                        })

            except Exception as e:
                # logging.debug(f"Scan err {symbol}: {e}")
                pass

async def worker_check(sem, client, exchange, level_info, hits_list):
    async with sem:
        try:
            symbol = level_info["symbol"]
            interval = level_info["interval"]
            market = level_info["market"]
            target_price = level_info["price"]
            
            # Fetch only 2 candles (Live + Last Closed) is enough for touch check usually,
            # but logic requires checking live candle bounds.
            candles = await client.fetch_ohlcv(symbol, interval, 2, market)
            
            if check_price_touch(candles, target_price):
                hits_list.append({
                    "exchange": exchange,
                    "symbol": symbol,
                    "market": market,
                    "interval": interval,
                    "signal_type": level_info["signal_type"],
                    "level_price": target_price,
                    "timestamp": level_info["timestamp"]
                })
        except Exception:
            pass

async def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    
    global UTC_NOW_RUN
    UTC_NOW_RUN = datetime.now(timezone.utc)
    
    run_mode = os.getenv("RUN_MODE", "full_scan")
    tg_token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    proxy_url = os.getenv("PROXY_LIST_URL")

    if not all([tg_token, chat_id, proxy_url]):
        logging.error("Missing Env Vars")
        return

    bot = Bot(token=tg_token)
    proxy_pool = AsyncProxyPool(max_pool_size=50) # Higher pool for async
    await proxy_pool.populate_from_url(proxy_url)
    await proxy_pool.start_health_check()

    bin_client = BinanceClient(proxy_pool)
    byb_client = BybitClient(proxy_pool)

    try:
        if run_mode == "price_check":
            logging.info("🚀 Mode: Async Price Check")
            levels = load_levels_from_file()
            
            if not levels:
                report_text = "💥 *LEVEL ALERTS* 💥

❌ No levels got hit at this time."
                hits = []
            else:
                hits = []
                tasks = []
                sem = asyncio.Semaphore(MAX_CONCURRENCY)
                
                # Create tasks
                for key, info in levels.items():
                    client = bin_client if info["exchange"] == "Binance" else byb_client
                    tasks.append(worker_check(sem, client, info["exchange"], info, hits))
                
                # Run with progress bar
                for f in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Checking Levels"):
                    await f

                if hits:
                    logging.info(f"🚨 {len(hits)} Hits detected!")
                    # Re-use creation logic (hacky but works with existing format)
                    report_text = create_report_sections(hits)[0].replace("Reversal Level Scanner", "LEVEL ALERTS")
                else:
                    report_text = "💥 *LEVEL ALERTS* 💥

❌ No levels got hit at this time."

            # --- Intelligent Message Editing (Ported Logic) ---
            prev_ids = load_level_message_ids()
            new_ids = []
            
            # Split into chunks
            chunks = []
            curr_chunk = ''
            for line in report_text.splitlines(keepends=True):
                if len(curr_chunk) + len(line) > MAX_TG_CHARS:
                    chunks.append(curr_chunk); curr_chunk = ''
                curr_chunk += line
            if curr_chunk: chunks.append(curr_chunk)

            # Edit/Send
            for i, chunk in enumerate(chunks):
                if i < len(prev_ids):
                    mid = prev_ids[i]
                    try:
                        await bot.edit_message_text(chat_id=chat_id, message_id=mid, text=chunk, parse_mode='Markdown')
                        new_ids.append(mid)
                    except Exception:
                        # Fallback if edit fails
                        m = await bot.send_message(chat_id=chat_id, text=chunk, parse_mode='Markdown')
                        new_ids.append(m.message_id)
                else:
                    m = await bot.send_message(chat_id=chat_id, text=chunk, parse_mode='Markdown')
                    new_ids.append(m.message_id)
            
            save_level_message_ids(new_ids)

        else:
            logging.info("🚀 Mode: Full Async Scan")
            clear_level_message_ids()

            # 1. Fetch Symbols
            logging.info("Fetching symbols...")
            s_bp, s_bs, s_byp, s_bys = await asyncio.gather(
                bin_client.get_perp_symbols(),
                bin_client.get_spot_symbols(),
                byb_client.get_perp_symbols(),
                byb_client.get_spot_symbols()
            )

            # 2. Filter & Prioritize
            f_bp, f_bs, f_byp, f_bys = apply_priority(s_bp, s_bs, s_byp, s_bys)
            
            tasks = []
            sem = asyncio.Semaphore(MAX_CONCURRENCY)
            results = [] # Thread-safe appending is automatic in asyncio (single thread loop)
            low_moves = []

            # 3. Build Task List
            def add_tasks(syms, exc, client, mkt):
                for s in syms:
                    if s not in IGNORED_SYMBOLS:
                        tasks.append(worker_scan(sem, client, exc, s, mkt, results, low_moves))

            add_tasks(f_bp, "Binance", bin_client, "perp")
            add_tasks(f_bs, "Binance", bin_client, "spot")
            add_tasks(f_byp, "Bybit", byb_client, "perp")
            add_tasks(f_bys, "Bybit", byb_client, "spot")

            logging.info(f"Scanning {len(tasks)} symbols...")

            # 4. Execute
            for f in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Scanning"):
                await f

            # 5. Report
            sections = create_report_sections(results, low_moves)
            for section in sections:
                await safe_send_markdown(bot, chat_id, section)
                await asyncio.sleep(1)

            # 6. Initialize Alert Message
            init_msg = "💥 *LEVEL ALERTS* 💥

❌ No levels got hit at this time."
            ids = await safe_send_markdown(bot, chat_id, init_msg)
            save_level_message_ids(ids)
            
            save_levels_to_file(results)

    finally:
        await bin_client.close()
        await byb_client.close()

if __name__ == "__main__":
    try:
        # Windows selector loop policy fix
        if os.name == 'nt':
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
