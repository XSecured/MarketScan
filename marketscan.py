import asyncio
import aiohttp
import logging
import os
import json
import random
import sys
import time
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Set, Optional, Tuple, NamedTuple, Any
from dataclasses import dataclass, asdict
from pathlib import Path

# -----------------------------------------------------------------------------
# 1. Configuration & Environmental Setup
# -----------------------------------------------------------------------------

# Optimized Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(name)s | %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger("MarketScan")

# Performance Tuning (uvloop for Linux/Mac)
if sys.platform != 'win32':
    try:
        import uvloop
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        logger.info("🚀 High-Performance uvloop activated")
    except ImportError:
        pass

# Constants
class Config:
    # Infrastructure
    CONCURRENCY = 150            # Simultaneous requests
    REQ_TIMEOUT = 10             # Seconds before giving up on a proxy
    MAX_RETRIES = 3              # Retry attempts per symbol
    
    # Logic
    LOW_MOV_THRESHOLD = 1.0      # Strict 1.0%
    MAX_TG_CHARS = 4000          # Telegram limit safety buffer
    
    # Persistence (Must match user's expected file structure)
    FILE_LEVELS = Path("detected_levels.json")
    FILE_MSG_IDS = Path("level_alert_message.json")
    
    # Telegram
    TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
    CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
    PROXY_URL = os.getenv("PROXY_LIST_URL")
    RUN_MODE = os.getenv("RUN_MODE", "full_scan")

    IGNORED_SYMBOLS = {
        "USDPUSDT", "USD1USDT", "TUSDUSDT", "AEURUSDT", "USDCUSDT",
        "ZKJUSDT", "FDUSDUSDT", "XUSDUSDT", "EURUSDT", "EURIUSDT",
        "USDYUSDT", "USDRUSDT", "ETH3LUSDT", "ETHUSDT-27MAR26",
        "ETHUSDT-27JUN25", "ETHUSDT-26DEC25", "ETHUSDT-26SEP25"
    }

# -----------------------------------------------------------------------------
# 2. Data Structures (Zero-Overhead)
# -----------------------------------------------------------------------------

class Candle(NamedTuple):
    """Immutable, memory-optimized candle structure."""
    open: float
    high: float
    low: float
    close: float
    volume: float

@dataclass
class MarketLevel:
    exchange: str
    symbol: str
    market: str
    interval: str
    signal_type: str
    price: float
    timestamp: str

@dataclass
class HitResult:
    exchange: str
    symbol: str
    market: str
    interval: str
    signal_type: str
    level_price: float
    curr_price: float

@dataclass
class LowMovResult:
    exchange: str
    symbol: str
    market: str
    movement: float

# -----------------------------------------------------------------------------
# 3. High-Performance Network Layer
# -----------------------------------------------------------------------------

class ProxyManager:
    """
    Manages proxies with a fail-fast rotation strategy.
    No pre-warming (too slow). We filter as we go.
    """
    def __init__(self, url: str):
        self.url = url
        self.proxies: List[str] = []
        self.bad_proxies: Set[str] = set()
        self._lock = asyncio.Lock()

    async def fetch(self):
        """Downloads proxy list once at startup."""
        logger.info("🌐 Fetching proxy list...")
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(self.url, timeout=15) as resp:
                    if resp.status == 200:
                        text = await resp.text()
                        self.proxies = [
                            p.strip() if "://" in p.strip() else f"http://{p.strip()}"
                            for p in text.splitlines() if p.strip()
                        ]
                        logger.info(f"✅ Loaded {len(self.proxies)} proxies.")
                    else:
                        logger.error(f"Failed to fetch proxies: HTTP {resp.status}")
        except Exception as e:
            logger.error(f"Proxy fetch error: {e}")

    def get_random(self) -> Optional[str]:
        """Get a random proxy that isn't marked as bad."""
        if not self.proxies: return None
        # Try 10 times to find a good one, else return whatever
        for _ in range(10):
            p = random.choice(self.proxies)
            if p not in self.bad_proxies:
                return p
        return random.choice(self.proxies)

    def mark_bad(self, proxy: str):
        if proxy:
            self.bad_proxies.add(proxy)

class HttpClient:
    """
    Wrapper for aiohttp with automatic retries and proxy rotation.
    """
    def __init__(self, proxy_mgr: ProxyManager):
        self.pm = proxy_mgr
        self.session: Optional[aiohttp.ClientSession] = None
        # Global semaphore to prevent opening too many file descriptors
        self.sem = asyncio.Semaphore(Config.CONCURRENCY)

    async def start(self):
        # Optimized connector parameters
        conn = aiohttp.TCPConnector(
            limit=0, # Limit handled by semaphore
            ttl_dns_cache=300,
            ssl=False,
            keepalive_timeout=30
        )
        self.session = aiohttp.ClientSession(connector=conn)

    async def close(self):
        if self.session:
            await self.session.close()

    async def get(self, url: str, params: dict = None) -> Any:
        async with self.sem:
            for attempt in range(Config.MAX_RETRIES):
                proxy = self.pm.get_random()
                try:
                    async with self.session.get(
                        url, params=params, proxy=proxy, timeout=Config.REQ_TIMEOUT
                    ) as resp:
                        if resp.status == 200:
                            return await resp.json()
                        elif resp.status == 429: # Rate limit
                            await asyncio.sleep(1 + attempt)
                        elif resp.status >= 500: # Server error
                            pass # Retry
                except Exception:
                    self.pm.mark_bad(proxy) # Mark bad immediately
                
                await asyncio.sleep(0.2) # Jitter
            return None

# -----------------------------------------------------------------------------
# 4. Exchange Logic
# -----------------------------------------------------------------------------

class Binance:
    def __init__(self, http: HttpClient): self.http = http

    async def get_symbols(self) -> Tuple[Set[str], Set[str]]:
        """Returns (PerpSymbols, SpotSymbols)"""
        t1 = self.http.get('https://fapi.binance.com/fapi/v1/exchangeInfo')
        t2 = self.http.get('https://api.binance.com/api/v3/exchangeInfo')
        r1, r2 = await asyncio.gather(t1, t2)

        perps = {s['symbol'] for s in r1['symbols'] 
                 if s['contractType'] == 'PERPETUAL' and s['status'] == 'TRADING' and s['quoteAsset'] == 'USDT'} if r1 else set()
        spots = {s['symbol'] for s in r2['symbols'] 
                 if s['status'] == 'TRADING' and s['quoteAsset'] == 'USDT'} if r2 else set()
        return perps, spots

    async def get_candles(self, symbol: str, interval: str, market: str) -> List[Candle]:
        base = 'https://api.binance.com/api/v3/klines' if market == "spot" else 'https://fapi.binance.com/fapi/v1/klines'
        data = await self.http.get(base, {'symbol': symbol, 'interval': interval, 'limit': 4})
        if not data: return []
        # Binance: [Time, Open, High, Low, Close, Vol] - Oldest first
        # We reverse to get Newest first [0]
        return [Candle(float(x[1]), float(x[2]), float(x[3]), float(x[4]), float(x[5])) for x in reversed(data)]

class Bybit:
    def __init__(self, http: HttpClient): self.http = http

    async def get_symbols(self) -> Tuple[Set[str], Set[str]]:
        t1 = self.http.get('https://api.bybit.com/v5/market/instruments-info', {'category': 'linear'})
        t2 = self.http.get('https://api.bybit.com/v5/market/instruments-info', {'category': 'spot'})
        r1, r2 = await asyncio.gather(t1, t2)

        perps = {s['symbol'] for s in r1['result']['list'] if s['status'] == 'Trading' and s['quoteCoin'] == 'USDT'} if r1 else set()
        spots = {s['symbol'] for s in r2['result']['list'] if s['status'] == 'Trading' and s['quoteCoin'] == 'USDT'} if r2 else set()
        return perps, spots

    async def get_candles(self, symbol: str, interval: str, market: str) -> List[Candle]:
        cat = 'linear' if market == 'perp' else 'spot'
        # Map 1M/1w/1d to Bybit format
        b_int = {"1M": "M", "1w": "W", "1d": "D"}[interval]
        data = await self.http.get('https://api.bybit.com/v5/market/kline', 
                                   {'category': cat, 'symbol': symbol, 'interval': b_int, 'limit': 4})
        if not data or 'result' not in data: return []
        # Bybit: Newest first by default
        return [Candle(float(x[1]), float(x[2]), float(x[3]), float(x[4]), float(x[5])) for x in data['result']['list']]

# -----------------------------------------------------------------------------
# 5. Business Logic (Pure)
# -----------------------------------------------------------------------------

def analyze_reversal(candles: List[Candle]) -> Tuple[Optional[float], Optional[str]]:
    """Checks if idx 2 Close == idx 1 Open (0.3% tol). Returns (Price, Type)."""
    if len(candles) < 3: return None, None
    last, prev = candles[1], candles[2]
    
    # Equality Check
    avg = (prev.close + last.open) / 2
    if avg == 0 or abs(prev.close - last.open) > (0.003 * avg):
        return None, None
    
    price = prev.close
    
    # Pattern Logic
    # Bullish: Prev RED, Last GREEN
    if (prev.close < prev.open) and (last.close > last.open):
        return price, "bullish"
    # Bearish: Prev GREEN, Last RED
    if (prev.close > prev.open) and (last.close < last.open):
        return price, "bearish"
    return None, None

def analyze_low_movement(candles: List[Candle]) -> Optional[float]:
    """Returns movement % if < 1.0%"""
    if len(candles) < 2: return None
    c = candles[1] # Last closed
    if c.open == 0: return None
    move = abs((c.close - c.open) / c.open) * 100
    return move if move < Config.LOW_MOV_THRESHOLD else None

def is_level_hit(candles: List[Candle], level_price: float) -> bool:
    """Checks if CURRENT (Live) candle touches price"""
    if not candles: return False
    c = candles[0]
    return c.low <= level_price <= c.high

# -----------------------------------------------------------------------------
# 6. Telegram & State Manager (The Critical Fix)
# -----------------------------------------------------------------------------

class TelegramManager:
    def __init__(self):
        self.bot = None
        if Config.TOKEN and Config.CHAT_ID:
            try:
                from telegram import Bot
                self.bot = Bot(token=Config.TOKEN)
            except ImportError:
                logger.error("telegram module missing")

    async def send_smart_message(self, text_parts: List[str], load_ids_from_file: bool = False):
        """
        Handles the logic of Sending vs Editing.
        1. Consolidates text parts into chunks.
        2. If load_ids_from_file is True, it tries to EDIT existing messages.
        3. Saves the final message IDs to file.
        """
        if not self.bot: return

        # 1. Chunking
        chunks = []
        curr = ""
        for part in text_parts:
            if len(curr) + len(part) + 2 > Config.MAX_TG_CHARS:
                chunks.append(curr.strip())
                curr = ""
            curr += part + "\n\n"
        if curr.strip(): chunks.append(curr.strip())
        
        # 2. ID Resolution
        old_ids = []
        if load_ids_from_file and Config.FILE_MSG_IDS.exists():
            try:
                with open(Config.FILE_MSG_IDS, 'r') as f:
                    data = json.load(f)
                    old_ids = data.get("message_ids", [])
                    logger.info(f"📜 Loaded {len(old_ids)} existing message IDs for editing.")
            except Exception as e:
                logger.error(f"Failed to load IDs: {e}")

        # 3. Send / Edit Loop
        new_ids = []
        from telegram.error import BadRequest

        for i, chunk in enumerate(chunks):
            msg_id = None
            
            # Try Edit
            if i < len(old_ids):
                try:
                    await self.bot.edit_message_text(
                        chat_id=Config.CHAT_ID,
                        message_id=old_ids[i],
                        text=chunk,
                        parse_mode='Markdown'
                    )
                    msg_id = old_ids[i]
                except BadRequest as e:
                    if "message is not modified" in str(e):
                        # This is GOOD. It means content is same. Keep ID.
                        msg_id = old_ids[i]
                    else:
                        logger.warning(f"⚠️ Edit failed for ID {old_ids[i]}: {e}. Sending new.")
                except Exception as e:
                    logger.error(f"Edit Error: {e}")

            # Fallback to Send
            if msg_id is None:
                try:
                    m = await self.bot.send_message(
                        chat_id=Config.CHAT_ID,
                        text=chunk,
                        parse_mode='Markdown'
                    )
                    msg_id = m.message_id
                except Exception as e:
                    logger.error(f"Send Error: {e}")

            if msg_id:
                new_ids.append(msg_id)
            await asyncio.sleep(0.5)

        # 4. Save State
        try:
            with open(Config.FILE_MSG_IDS, 'w') as f:
                json.dump({"message_ids": new_ids, "updated": datetime.now().isoformat()}, f)
            logger.info(f"💾 Saved {len(new_ids)} message IDs.")
        except Exception as e:
            logger.error(f"Save IDs failed: {e}")

def get_ts_string():
    # UTC+3
    return datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=3))).strftime("%Y-%m-%d %H:%M:%S UTC+3")

# -----------------------------------------------------------------------------
# 7. Main Workflows
# -----------------------------------------------------------------------------

async def run_full_scan(clients: List[Any], tg: TelegramManager):
    logger.info("🔭 STARTING FULL SCAN")
    
    # 1. Fetch Symbols
    raw_data = {}
    for name, client in clients:
        raw_data[name] = await client.get_symbols()

    # 2. Prioritize Symbols
    # Normalization helper
    def norm(s): return s.replace(":", "").replace("/", "")
    
    tasks = [] # (Client, Symbol, Market)
    seen = set()
    
    # Priority Order
    order = [("Binance", 0, "perp"), ("Binance", 1, "spot"), ("Bybit", 0, "perp"), ("Bybit", 1, "spot")]
    client_map = {name: c for name, c in clients}

    for ex_name, set_idx, mkt in order:
        if ex_name not in raw_data: continue
        syms = raw_data[ex_name][set_idx]
        for s in syms:
            if s in Config.IGNORED_SYMBOLS: continue
            n = norm(s)
            if n not in seen:
                seen.add(n)
                tasks.append((client_map[ex_name], s, mkt))

    logger.info(f"📋 Scanning {len(tasks)} unique symbols...")

    # 3. Scan
    levels = []
    low_movs = []
    
    # Using semaphore inside HttpClient, so we can just launch tasks
    async def worker(client, s, m):
        # Check all intervals
        for interval in ["1d", "1w", "1M"]:
            try:
                candles = await client.get_candles(s, interval, m)
                
                # Reversal Logic
                p, t = analyze_reversal(candles)
                if p:
                    # Check if currently touched (Legacy logic: if touched, ignore?)
                    # Legacy: "if equal_price ... and not current_candle_touched_price"
                    if not is_level_hit(candles, p):
                        levels.append(MarketLevel(client.__class__.__name__, s, m, interval, t, p, get_ts_string()))
                
                # Low Mov Logic
                if interval == "1d":
                    lm = analyze_low_movement(candles)
                    if lm:
                        low_movs.append(LowMovResult(client.__class__.__name__, s, m, lm))
            except: pass

    # Chunking tasks for progress updates
    chunk_sz = 1000
    for i in range(0, len(tasks), chunk_sz):
        await asyncio.gather(*(worker(*t) for t in tasks[i:i+chunk_sz]))
        logger.info(f"Progress: {min(i+chunk_sz, len(tasks))}/{len(tasks)}")

    # 4. Save Levels
    logger.info(f"✅ Found {len(levels)} levels, {len(low_movs)} low movers.")
    with open(Config.FILE_LEVELS, 'w') as f:
        json.dump({
            "last_updated": get_ts_string(),
            "levels": {f"{l.exchange}_{l.symbol}_{l.interval}": asdict(l) for l in levels}
        }, f, indent=2)

    # 5. Report Full Scan
    # Build text...
    report = []
    # (Simplified text building for brevity, matches logic of previous script)
    report.append(f"💥 *Reversal Level Scanner*\n\n✅ Total: {len(levels)}\n🕒 {get_ts_string()}")
    
    # Group and format logic...
    tree = {}
    for l in levels:
        if l.interval not in tree: tree[l.interval] = {"Binance": {}, "Bybit": {}}
        d = tree[l.interval][l.exchange]
        if l.signal_type not in d: d[l.signal_type] = []
        d[l.signal_type].append(l.symbol)
        
    for iv in ["1M", "1w", "1d"]:
        if iv not in tree: continue
        sect = f"📅 *{iv} Timeframe*\n"
        has_d = False
        for ex in ["Binance", "Bybit"]:
            d = tree[iv][ex]
            if not d: continue
            bull, bear = sorted(d.get("bullish", [])), sorted(d.get("bearish", []))
            if bull or bear:
                has_d = True
                sect += f"\n*{ex}*:\n"
                if bull: sect += f"🍏 *Bullish ({len(bull)})*\n" + "\n".join([f"• {x}" for x in bull]) + "\n"
                if bear: sect += f"🔻 *Bearish ({len(bear)})*\n" + "\n".join([f"• {x}" for x in bear]) + "\n"
        if has_d: 
            sect += "——————————————————"
            report.append(sect)
            
    # Add Low Mov
    if low_movs:
        low_movs.sort(key=lambda x: x.movement)
        sect = "📉 *Low Movement Daily (<1.0%)*\n"
        for ex in ["Binance", "Bybit"]:
            lm = [x for x in low_movs if x.exchange == ex]
            if lm:
                sect += f"\n*{ex}*:\n" + "\n".join([f"• {x.symbol} ({x.movement:.2f}%)" for x in lm]) + "\n"
        sect += "——————————————————"
        report.append(sect)

    # Send Full Report (False = Send New)
    await tg.send_smart_message(report, load_ids_from_file=False)

    # 6. Init Alert Message (False = Send New, then Save IDs)
    # This prepares the file for the hourly checks
    init_alert = [f"💥 *LEVEL ALERTS* 💥\n\n❌ No levels got hit at this time.\n\n🕒 {get_ts_string()}"]
    await tg.send_smart_message(init_alert, load_ids_from_file=False)


async def run_price_check(clients: List[Any], tg: TelegramManager):
    logger.info("🔍 STARTING PRICE CHECK")
    
    if not Config.FILE_LEVELS.exists():
        logger.warning("No levels file found.")
        return

    # Load Levels
    try:
        with open(Config.FILE_LEVELS, 'r') as f:
            data = json.load(f)
            levels = [MarketLevel(**v) for v in data.get("levels", {}).values()]
    except:
        return

    # Check Levels
    hits = []
    # Group by symbol to minimize requests
    # Key: (Exchange, Symbol, Market, Interval)
    # Actually need to fetch candle per Symbol+Market+Interval
    tasks = {}
    for l in levels:
        k = (l.exchange, l.symbol, l.market, l.interval)
        if k not in tasks: tasks[k] = []
        tasks[k].append(l)

    client_map = {name: c for name, c in clients}

    async def checker(key, lvl_list):
        ex, sym, mkt, iv = key
        try:
            c_obj = client_map[ex]
            candles = await c_obj.get_candles(sym, iv, mkt)
            if not candles: return
            
            for lvl in lvl_list:
                if is_level_hit(candles, lvl.price):
                    hits.append(HitResult(ex, sym, mkt, iv, lvl.signal_type, lvl.price, candles[0].close))
        except: pass

    await asyncio.gather(*(checker(k, v) for k, v in tasks.items()))

    # Build Report
    ts = get_ts_string()
    if not hits:
        text = [f"💥 *LEVEL ALERTS* 💥\n\n❌ No levels got hit at this time.\n\n🕒 {ts}"]
    else:
        # Organize hits
        text = [f"🚨 *LEVEL ALERTS* 🚨\n\n⚡ {len(hits)} levels hit!\n🕒 {ts}\n"]
        tree = {}
        for h in hits:
            if h.interval not in tree: tree[h.interval] = {"Binance": {}, "Bybit": {}}
            d = tree[h.interval][h.exchange]
            if h.signal_type not in d: d[h.signal_type] = []
            d[h.signal_type].append(h)
            
        for iv in ["1M", "1w", "1d"]:
            if iv not in tree: continue
            sect = f"📅 *{iv} Alerts*\n"
            has_d = False
            for ex in ["Binance", "Bybit"]:
                d = tree[iv][ex]
                if not d: continue
                bull, bear = d.get("bullish", []), d.get("bearish", [])
                if bull or bear:
                    has_d = True
                    sect += f"\n*{ex}*:\n"
                    if bull: sect += f"🍏 *Bullish ({len(bull)})*\n" + "\n".join([f"• {h.symbol} @ ${h.level_price:.4f}" for h in bull]) + "\n"
                    if bear: sect += f"🔻 *Bearish ({len(bear)})*\n" + "\n".join([f"• {h.symbol} @ ${h.level_price:.4f}" for h in bear]) + "\n"
            if has_d:
                sect += "——————————————————"
                text.append(sect)

    # CRITICAL: load_ids_from_file=True
    # This ensures we EDIT the message created by full_scan
    await tg.send_smart_message(text, load_ids_from_file=True)


async def main():
    if not Config.PROXY_URL:
        logger.error("No PROXY_LIST_URL")
        return

    # Infrastructure
    pm = ProxyManager(Config.PROXY_URL)
    await pm.fetch()
    
    http = HttpClient(pm)
    await http.start()

    binance = Binance(http)
    bybit = Bybit(http)
    tg = TelegramManager()
    
    clients = [("Binance", binance), ("Bybit", bybit)]

    try:
        if Config.RUN_MODE == "price_check":
            await run_price_check(clients, tg)
        else:
            await run_full_scan(clients, tg)
    finally:
        await http.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
