import asyncio
import json
import logging
import os
import random
import math
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Set, Optional, Tuple, Any
from itertools import cycle
import aiohttp
from telegram import Bot
from tqdm.asyncio import tqdm

# ==========================================
# CONFIGURATION
# ==========================================

@dataclass
class Config:
    REQUEST_TIMEOUT: int = 5
    MAX_RETRIES: int = 3
    MAX_TG_CHARS: int = 4000
    MAX_CONCURRENCY: int = 50

    # Files
    LEVELS_FILE: str = "detected_levels.json"
    MSG_FILE: str = "level_alert_message.json"

    # Environment
    TELEGRAM_TOKEN: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
    CHAT_ID: str = os.getenv("TELEGRAM_CHAT_ID", "")
    PROXY_URL: str = os.getenv("PROXY_LIST_URL", "https://raw.githubusercontent.com/ErcinDedeoglu/proxies/main/proxies/https.txt")
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

    def to_dict(self):
        return asdict(self)

@dataclass
class LowMovementHit:
    exchange: str
    symbol: str
    market: str
    interval: str
    movement_percent: float

# ==========================================
# ROBUST PROXY POOL (HEAVY TEST)
# ==========================================

class AsyncProxyPool:
    def __init__(self, max_pool_size=20):
        self.proxies: List[str] = []
        self.max_pool_size = max_pool_size
        self.iterator = None
        self._lock = asyncio.Lock()
        self.failures: Dict[str, int] = {}

    async def populate(self, url: str, session: aiohttp.ClientSession):
        if not url: return
        
        raw = []
        try:
            logging.info(f"📥 Fetching proxies from {url}...")
            async with session.get(url, timeout=15) as resp:
                if resp.status == 200:
                    text = await resp.text()
                    for line in text.splitlines():
                        p = line.strip()
                        if p: raw.append(p if "://" in p else f"http://{p}")
        except Exception as e:
            logging.error(f"❌ Proxy fetch failed: {e}")
            return

        logging.info(f"🔎 Validating {len(raw)} proxies...")
        self.proxies = []
        random.shuffle(raw)
        
        # High concurrency for initial check to speed up startup
        sem = asyncio.Semaphore(200)
        
        async def protected_test(p):
            async with sem: return await self._test_proxy(p, session)

        tasks = [asyncio.create_task(protected_test(p)) for p in raw]
        
        for future in asyncio.as_completed(tasks):
            try:
                proxy, is_good = await future
                if is_good:
                    self.proxies.append(proxy)
                    if len(self.proxies) >= self.max_pool_size: break
            except: pass
        
        for t in tasks:
            if not t.done(): t.cancel()
        await asyncio.sleep(0.1)
        
        if self.proxies:
            self.iterator = cycle(self.proxies)
            logging.info(f"✅ Proxy Pool Ready: {len(self.proxies)} warriors.")
        else:
            logging.error("❌ NO WORKING PROXIES FOUND!")

    async def _test_proxy(self, proxy: str, session: aiohttp.ClientSession) -> Tuple[str, bool]:
        try:
            # HEAVY TEST: Actual Futures Data to filter out weak/WAF-blocked proxies
            url = "https://fapi.binance.com/fapi/v1/klines"
            params = {"symbol": "BTCUSDT", "interval": "1m", "limit": "2"}
            async with session.get(url, params=params, proxy=proxy, timeout=5) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if isinstance(data, list) and len(data) > 0:
                        return proxy, True
                return proxy, False
        except: return proxy, False

    async def get_proxy(self) -> Optional[str]:
        if not self.proxies: return None
        async with self._lock: 
            if not self.proxies: return None
            return next(self.iterator)

    async def report_failure(self, proxy: str):
        async with self._lock:
            self.failures[proxy] = self.failures.get(proxy, 0) + 1
            # 10 Strikes Rule - Aggressive removal of bad proxies
            if self.failures[proxy] >= 10:
                if proxy in self.proxies:
                    self.proxies.remove(proxy)
                    if self.proxies: self.iterator = cycle(self.proxies)
                    logging.warning(f"🚫 Banned Proxy {proxy} (10 failures)")

# ==========================================
# EXCHANGE CLIENTS
# ==========================================

class ExchangeClient:
    def __init__(self, session: aiohttp.ClientSession, proxy_pool: AsyncProxyPool):
        self.session = session
        self.proxies = proxy_pool
        limit = CONFIG.MAX_CONCURRENCY if proxy_pool.proxies else 5
        self.sem = asyncio.Semaphore(limit)

    async def _request(self, url: str, params: dict = None) -> Any:
        for attempt in range(CONFIG.MAX_RETRIES):
            proxy = await self.proxies.get_proxy()
            if not proxy: await asyncio.sleep(1); continue
            
            try:
                async with self.sem:
                    async with self.session.get(url, params=params, proxy=proxy, timeout=CONFIG.REQUEST_TIMEOUT) as resp:
                        if resp.status == 200: return await resp.json()
                        elif resp.status == 429:
                            await self.proxies.report_failure(proxy) 
                            await asyncio.sleep(2)
                        elif resp.status == 418:
                            await self.proxies.report_failure(proxy)
            except Exception as e:
                await self.proxies.report_failure(proxy)
            
            await asyncio.sleep(0.5)
        return None

class BinanceClient(ExchangeClient):
    async def get_perp_symbols(self) -> List[str]:
        data = await self._request('https://fapi.binance.com/fapi/v1/exchangeInfo')
        if not data: return []
        return [s['symbol'] for s in data['symbols'] if s.get('contractType') == 'PERPETUAL' and s['status'] == 'TRADING' and s.get('quoteAsset') == 'USDT']
    
    async def get_spot_symbols(self) -> List[str]:
        data = await self._request('https://api.binance.com/api/v3/exchangeInfo')
        if not data: return []
        return [s['symbol'] for s in data['symbols'] if s['status'] == 'TRADING' and s.get('quoteAsset') == 'USDT']

    async def fetch_ohlcv(self, symbol: str, interval: str, market: str, limit: int = 3) -> List[Candle]:
        base = 'https://api.binance.com/api/v3/klines' if market == "spot" else 'https://fapi.binance.com/fapi/v1/klines'
        data = await self._request(base, {'symbol': symbol, 'interval': interval, 'limit': limit})
        if not data or not isinstance(data, list): return []
        
        candles = []
        for c in data:
            try: candles.append(Candle(float(c[1]), float(c[2]), float(c[3]), float(c[4])))
            except: continue
        # Binance returns Oldest->Newest. Reverse to get Newest at index 0.
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
        raw = data.get('result', {}).get('list', [])
        if not raw: return []
        
        candles = []
        for c in raw:
            candles.append(Candle(float(c[1]), float(c[2]), float(c[3]), float(c[4])))
        return candles # Bybit V5 returns Newest first. No reverse needed.

# ==========================================
# CORE LOGIC
# ==========================================

def floats_are_equal(a: float, b: float, rel_tol: float = 0.003) -> bool:
    return abs(a - b) <= rel_tol * ((a + b) / 2.0)

def check_reversal(candles: List[Candle]) -> Tuple[Optional[float], Optional[str]]:
    # Expects candles[0] = Newest
    if len(candles) < 3: return None, None
    last_closed = candles[1]
    second_last_closed = candles[2]
    
    # Check if Close of candle[2] == Open of candle[1]
    if floats_are_equal(second_last_closed.close, last_closed.open):
        price = second_last_closed.close
        # Color logic
        s_red = second_last_closed.close < second_last_closed.open
        s_green = second_last_closed.close > second_last_closed.open
        l_green = last_closed.close > last_closed.open
        l_red = last_closed.close < last_closed.open
        
        if s_red and l_green: return price, "bullish"
        if s_green and l_red: return price, "bearish"
    return None, None

def current_candle_touched_price(candles: List[Candle], price: float) -> bool:
    if not candles: return False
    curr = candles[0]
    return curr.low <= price <= curr.high

def check_low_movement(candles: List[Candle], threshold: float = 1.0) -> Optional[float]:
    if len(candles) < 2: return None
    last = candles[1]
    if last.open == 0: return None
    pct = abs((last.close - last.open) / last.open) * 100
    return pct if pct < threshold else None

# ==========================================
# STATE & UTILS
# ==========================================

def load_levels() -> Dict[str, Any]:
    try:
        if os.path.exists(CONFIG.LEVELS_FILE):
            with open(CONFIG.LEVELS_FILE, "r") as f:
                return json.load(f).get("levels", {})
    except: pass
    return {}

def save_levels(results: List[LevelHit], utc_now_str: str):
    data = {"last_updated": utc_now_str, "levels": {}}
    for r in results:
        key = f"{r.exchange}_{r.symbol}_{r.market}_{r.interval}"
        data["levels"][key] = r.to_dict()
    with open(CONFIG.LEVELS_FILE, "w") as f:
        json.dump(data, f, indent=2)

def save_message_ids(ids: List[int]):
    with open(CONFIG.MSG_FILE, "w") as f:
        json.dump({"message_ids": ids, "timestamp": datetime.now(timezone.utc).isoformat()}, f)

def load_message_ids() -> List[int]:
    if not os.path.exists(CONFIG.MSG_FILE): return []
    try:
        with open(CONFIG.MSG_FILE) as f: return json.load(f).get("message_ids", [])
    except: return []

def clear_message_ids():
    if os.path.exists(CONFIG.MSG_FILE): os.remove(CONFIG.MSG_FILE)

# ==========================================
# MAIN BOT CLASS
# ==========================================

class MarketScanBot:
    def __init__(self):
        self.utc_now = datetime.now(timezone.utc)
        self.tg_bot = Bot(token=CONFIG.TELEGRAM_TOKEN) if CONFIG.TELEGRAM_TOKEN else None

    async def run(self):
        logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
        async with aiohttp.ClientSession() as session:
            # 1. Proxies
            proxies = AsyncProxyPool()
            if CONFIG.PROXY_URL: await proxies.populate(CONFIG.PROXY_URL, session)
            
            # 2. Clients
            binance = BinanceClient(session, proxies)
            bybit = BybitClient(session, proxies)
            
            if CONFIG.RUN_MODE == "price_check":
                await self.run_price_check(binance, bybit)
            else:
                await self.run_full_scan(binance, bybit)

    async def run_price_check(self, binance: BinanceClient, bybit: BybitClient):
        logging.info("🚀 Starting Price Check...")
        levels_data = load_levels()
        if not levels_data:
            logging.info("⚠️ No levels found.")
            await self.send_or_update_alert_report([])
            return

        tasks = []
        for key, data in levels_data.items():
            client = binance if data['exchange'] == 'Binance' else bybit
            tasks.append(self.check_single_level(client, data))
        
        hits = []
        for f in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Checking Levels"):
            res = await f
            if res: hits.append(res)
        
        await self.send_or_update_alert_report(hits)

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

    async def run_full_scan(self, binance, bybit):
        logging.info("🌍 Starting FULL Market Scan...")
        clear_message_ids()
        
        # 1. Fetch Symbols
        t1 = asyncio.create_task(binance.get_perp_symbols())
        t2 = asyncio.create_task(binance.get_spot_symbols())
        t3 = asyncio.create_task(bybit.get_perp_symbols())
        t4 = asyncio.create_task(bybit.get_spot_symbols())
        bp, bs, yp, ys = await asyncio.gather(t1, t2, t3, t4)
        
        counts = [len(x) for x in [bp, bs, yp, ys]]
        logging.info(f"✅ Symbols: BP:{counts[0]} | BS:{counts[1]} | YP:{counts[2]} | YS:{counts[3]}")
        
        # 2. Deduplicate
        seen = set()
        def filter_u(syms):
            res = []
            for s in syms:
                if s in CONFIG.IGNORED_SYMBOLS or not s.endswith("USDT"): continue
                norm = s.replace("USDT", "")
                if norm not in seen:
                    res.append(s)
                    seen.add(norm)
            return res
            
        final_bp, final_bs, final_yp, final_ys = filter_u(bp), filter_u(bs), filter_u(yp), filter_u(ys)
        
        # 3. Scan Setup
        async def scan_one(client, s, mkt, ex):
            revs, low_m = [], None
            for interval in ["1M", "1w", "1d"]:
                candles = await client.fetch_ohlcv(s, interval, mkt)
                if not candles: continue
                
                p, sig = check_reversal(candles)
                if p and sig and not current_candle_touched_price(candles, p):
                    revs.append(LevelHit(ex, s, mkt, interval, sig, p))
                
                if interval == "1d":
                    v = check_low_movement(candles, 1.0)
                    if v is not None:
                        low_m = LowMovementHit(ex, s, mkt, interval, v)
            return revs, low_m, True # True = Success

        # 4. Create Tasks
        tasks = []
        for client, syms, mkt, ex in [
            (binance, final_bp, 'perp', 'Binance'),
            (binance, final_bs, 'spot', 'Binance'),
            (bybit, final_yp, 'perp', 'Bybit'),
            (bybit, final_ys, 'spot', 'Bybit')
        ]:
            for s in syms:
                tasks.append(scan_one(client, s, mkt, ex))
        
        # 5. Execute
        final_results = []
        final_lows = []
        success_count = 0
        
        for f in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Scanning"):
            try:
                r, l, ok = await f
                if r: final_results.extend(r)
                if l: final_lows.append(l)
                if ok: success_count += 1
            except: pass
            
        logging.info(f"Scan Complete: Success {success_count}/{len(tasks)}")
        
        save_levels(final_results, self.utc_now.isoformat())
        await self.send_full_report(final_results, final_lows)
        await self.send_or_update_alert_report([])

    async def send_chunks(self, text: str):
        chunks = []
        curr = ""
        for line in text.splitlines(keepends=True):
            if len(curr) + len(line) > 4000:
                chunks.append(curr)
                curr = ""
            curr += line
        if curr: chunks.append(curr)
        
        for c in chunks:
            try:
                await self.tg_bot.send_message(CONFIG.CHAT_ID, text=c, parse_mode='Markdown')
                await asyncio.sleep(0.5)
            except: pass

    # --- NEW REPORTING FORMAT ---
    async def send_full_report(self, results: List[LevelHit], low_movements: List[LowMovementHit]):
        ts = self.utc_now.strftime("%d %b %H:%M UTC")
        def _clean(s): return s.replace("USDT", "")
        
        lines = ["🚨 *REVERSAL SCAN*", "═════════════════════"]
        
        # 1. Reversals
        grouped = {}
        for r in results: grouped.setdefault(r.interval, {}).setdefault(r.exchange, []).append(r)
        
        has_data = False
        for tf_name, tf_key in [("MONTHLY", "1M"), ("WEEKLY", "1w"), ("DAILY", "1d")]:
            if tf_key not in grouped: continue
            
            # Check if this TF actually has items
            if not any(grouped[tf_key].values()): continue
            
            lines.append(f"\n📅 *{tf_name}*")
            has_data = True
            
            for ex in ["Binance", "Bybit"]:
                hits = grouped[tf_key].get(ex, [])
                if not hits: continue
                
                lines.append(f"┌ 🟡 *{ex.upper()}*" if ex=="Binance" else f"┌ ⚫ *{ex.upper()}*")
                hits.sort(key=lambda x: x.symbol)
                
                for i, h in enumerate(hits):
                    pre = "└" if i == len(hits)-1 else "│"
                    icon = "🍏" if h.signal_type == "bullish" else "🔻"
                    # Format: │ 🍏 *BTC* ➜ $95000
                    lines.append(f"{pre} {icon} *{_clean(h.symbol)}* ➜ ${h.level_price:g}")
        
        if not has_data:
            lines.append("\n❌ No Reversals Found.")

        # 2. Squeeze Alerts (Top 30)
        if low_movements:
            lines.append("\n📉 *SQUEEZE ALERT (Top 30)*")
            lm_grp = {"Binance": [], "Bybit": []}
            for x in low_movements: 
                if x.exchange in lm_grp: lm_grp[x.exchange].append(x)
            
            for ex in ["Binance", "Bybit"]:
                items = lm_grp.get(ex, [])
                if not items: continue
                items.sort(key=lambda x: x.movement_percent)
                top = items[:30] # Increased to 30
                
                lines.append(f"┌ 🟡 *{ex.upper()}*" if ex=="Binance" else f"┌ ⚫ *{ex.upper()}*")
                for i, item in enumerate(top):
                    pre = "└" if i == len(top)-1 else "│"
                    lines.append(f"{pre} *{_clean(item.symbol)}* ➜ {item.movement_percent:.2f}%")
        
        lines.append("\n═════════════════════")
        lines.append(f"🕒 {ts}")
        
        await self.send_chunks("\n".join(lines))

    async def send_or_update_alert_report(self, hits: List[LevelHit]):
        # This handles the Price Check mode alerts (Simplified version for compatibility)
        # Since we usually run Full Scan, this is just a placeholder for completeness or separate mode.
        if not hits: return
        
        lines = ["🚨 *PRICE ALERT HIT*", "═════════════════════"]
        for h in hits:
            lines.append(f"🎯 *{h.symbol}* ({h.interval}) hit ${h.level_price}")
        
        await self.send_chunks("\n".join(lines))

if __name__ == "__main__":
    if os.name == 'nt': asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(MarketScanBot().run())
