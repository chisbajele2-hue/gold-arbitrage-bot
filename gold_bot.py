#!/usr/bin/env python3
"""
Gold Arbitrage Bot — Production
Real internal APIs for milli.gold, goldika.ir, wallgold.ir
Selenium fallback for talasea.ir, melligold.com
Navasan as baseline reference.
"""

import asyncio
import json
import logging
import os
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional

import aiohttp

logger = logging.getLogger("gold_bot")

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

NAVASAN_GOLD_URL = "https://raw.githubusercontent.com/HosseinOdd/Navasan-API/main/data/gold.json"

# Real internal APIs — confirmed working
REAL_APIS = {
    "milligold": {
        "url": "https://milli.gold/api/v1/public/milli-price/detail",
        "method": "GET",
        "headers": {"Referer": "https://milli.gold/", "Origin": "https://milli.gold"},
        "parser": "milligold",
        "priority": 1,
    },
    "goldika": {
        "url": "https://goldika.ir/api/public/price",
        "method": "GET",
        "headers": {},
        "parser": "goldika",
        "priority": 2,
    },
    "wallgold": {
        "url": "https://api.wallgold.ir/api/v1/markets",
        "method": "GET",
        "headers": {"Accept": "application/json"},
        "parser": "wallgold",
        "priority": 3,
    },
}

# Selenium-based scrapers for sites that block direct API access
SELENIUM_PLATFORMS = ["talasea", "melligold"]

# Platforms tracked via Navasan comparison only (no direct scraping)
NAVASAN_ONLY = ["daric", "technogold", "talapp", "digikala_gold"]

GOLD_POLL_SECONDS = 120
GOLD_SPREAD_ALERT = 1.0
GOLD_NAVASAN_ALERT = 2.0
GOLD_BUBBLE_ALERT = 2.0
GOLD_SUMMARY_INTERVAL = 3600
GOLD_CACHE_TTL = 120
GOLD_DAILY_REPORT_HOUR = 23

COIN_WEIGHTS = {
    "sekkeh": 8.133,
    "nim": 4.0665,
    "rob": 2.03325,
    "gerami": 1.0,
}

PLATFORM_NAMES_FA = {
    "milligold":    "میلی‌گلد 🥇",
    "talasea":      "طلاسی 🥈",
    "goldika":      "گلدیکا 🥉",
    "wallgold":     "وال‌گلد 🟡",
    "melligold":    "ملی‌گلد",
    "daric":        "داریک",
    "technogold":   "تکنوگلد",
    "talapp":       "طلاپ",
    "digikala_gold": "دیجی‌کالا طلا",
}

# ---------------------------------------------------------------------------
# STATE
# ---------------------------------------------------------------------------

@dataclass
class GoldState:
    running: bool = True
    last_alerts: Dict[str, float] = field(default_factory=dict)
    price_cache: Dict[str, dict] = field(default_factory=dict)
    platform_status: Dict[str, bool] = field(default_factory=dict)
    last_summary_time: float = 0.0
    last_daily_report: str = ""
    navasan_cache: Optional[dict] = None
    navasan_cache_time: float = 0.0
    selenium_available: bool = False  # set True if chromedriver works

state = GoldState()

# ---------------------------------------------------------------------------
# UTILS
# ---------------------------------------------------------------------------

def _parse_num(val) -> Optional[int]:
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return int(val)
    if isinstance(val, str):
        val = val.replace(",", "").replace("٬", "").replace(" ", "")
        try:
            return int(float(val))
        except ValueError:
            return None
    return None

# ---------------------------------------------------------------------------
# NAVASAN
# ---------------------------------------------------------------------------

async def fetch_navasan(session: aiohttp.ClientSession) -> Optional[dict]:
    now = time.time()
    if state.navasan_cache and (now - state.navasan_cache_time) < 300:
        return state.navasan_cache
    try:
        async with session.get(NAVASAN_GOLD_URL, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            if resp.status == 200:
                data = await resp.json()
                state.navasan_cache = data
                state.navasan_cache_time = now
                logger.info(f"Navasan: 18ayar={data.get('18ayar', 0):,}")
                return data
    except Exception as e:
        logger.warning(f"Navasan: {e}")
    return state.navasan_cache

# ---------------------------------------------------------------------------
# REAL API SCRAPERS
# ---------------------------------------------------------------------------

async def scrape_milligold(session: aiohttp.ClientSession) -> Optional[dict]:
    """milli.gold API — equal buy/sell, Rial per gram."""
    try:
        async with session.get(
            "https://milli.gold/api/v1/public/milli-price/detail",
            headers={"Referer": "https://milli.gold/", "Origin": "https://milli.gold",
                     "User-Agent": "Mozilla/5.0", "Accept": "application/json"},
            timeout=aiohttp.ClientTimeout(total=15)
        ) as resp:
            if resp.status != 200:
                return None
            data = await resp.json()
            price = _parse_num(data.get("data", {}).get("price18"))
            if price:
                # Convert Rial to Toman for consistency
                toman = price // 10
                return {"buy": toman, "sell": toman, "unit": "geram18", "platform": "milligold",
                        "source": "api", "ts": time.time()}
    except Exception as e:
        logger.warning(f"milligold API: {e}")
    return None

async def scrape_goldika(session: aiohttp.ClientSession) -> Optional[dict]:
    """goldika.ir API — separate buy/sell, Rial per gram."""
    try:
        async with session.get(
            "https://goldika.ir/api/public/price",
            headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"},
            timeout=aiohttp.ClientTimeout(total=15)
        ) as resp:
            if resp.status != 200:
                return None
            data = await resp.json()
            pdata = data.get("data", {}).get("price", {})
            buy = _parse_num(pdata.get("buy"))
            sell = _parse_num(pdata.get("sell"))
            if buy and sell:
                # Convert Rial to Toman
                return {"buy": buy // 10, "sell": sell // 10, "unit": "geram18", "platform": "goldika",
                        "source": "api", "ts": time.time()}
    except Exception as e:
        logger.warning(f"goldika API: {e}")
    return None

async def scrape_wallgold(session: aiohttp.ClientSession) -> Optional[dict]:
    """wallgold.ir API — prices in Toman per 0.007g unit."""
    try:
        async with session.get(
            "https://api.wallgold.ir/api/v1/markets",
            headers={"Accept": "application/json", "User-Agent": "Mozilla/5.0"},
            timeout=aiohttp.ClientTimeout(total=15)
        ) as resp:
            if resp.status != 200:
                return None
            data = await resp.json()
            if isinstance(data, list):
                for market in data:
                    if market.get("symbol") == "GLD_18C_750TMN":
                        cap = market.get("marketCap", {})
                        buy = _parse_num(cap.get("lastBuyPrice"))
                        sell = _parse_num(cap.get("lastSellPrice"))
                        if buy and sell:
                            # WallGold prices are per 0.007g, convert to per gram (Toman)
                            factor = 1.0 / 0.007  # ~142.857
                            return {
                                "buy": round(buy * factor),
                                "sell": round(sell * factor),
                                "unit": "geram18", "platform": "wallgold",
                                "source": "api", "ts": time.time()
                            }
    except Exception as e:
        logger.warning(f"wallgold API: {e}")
    return None

# ---------------------------------------------------------------------------
# SELENIUM FALLBACK
# ---------------------------------------------------------------------------

def selenium_scrape(url: str, site_name: str) -> Optional[dict]:
    """Headless Chrome scrape for talasea/melligold. Returns {buy, sell} or None."""
    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from bs4 import BeautifulSoup

        opts = Options()
        opts.add_argument("--headless=new")
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument("--disable-gpu")
        opts.add_argument("--window-size=1920,1080")
        opts.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")

        driver = webdriver.Chrome(options=opts)
        driver.set_page_load_timeout(25)
        try:
            driver.get(url)
            import time as _t; _t.sleep(8)
        except Exception:
            pass

        soup = BeautifulSoup(driver.page_source, "html.parser")
        driver.quit()

        # Find all large numbers (potential prices in Toman)
        prices = []
        for el in soup.find_all(["div", "span", "p"]):
            text = el.get_text().strip()
            matches = re.findall(r'([\d,]{5,12})', text)
            for m in matches:
                num = _parse_num(m)
                if num and 1_000_000 < num < 500_000_000:
                    prices.append(num)

        if prices:
            prices.sort()
            # Median price as midpoint
            mid = prices[len(prices)//2]
            return {"buy": mid, "sell": mid, "unit": "geram18", "platform": site_name,
                    "source": "selenium", "ts": time.time()}
    except ImportError:
        logger.warning("Selenium not installed")
        state.selenium_available = False
    except Exception as e:
        logger.error(f"Selenium {site_name}: {e}")
    return None

async def scrape_selenium_async(site_name: str, url: str) -> Optional[dict]:
    """Run selenium in thread pool so it doesn't block the event loop."""
    return await asyncio.get_event_loop().run_in_executor(None, selenium_scrape, url, site_name)

# ---------------------------------------------------------------------------
# COLLECT ALL PRICES
# ---------------------------------------------------------------------------

async def get_platform_price(session: aiohttp.ClientSession, key: str) -> Optional[dict]:
    """Dispatch to correct scraper based on platform."""
    cache_key = f"price:{key}"
    cached = state.price_cache.get(cache_key)
    if cached and (time.time() - cached.get("ts", 0)) < GOLD_CACHE_TTL:
        return cached["data"]

    result = None

    # Layer 1: Real APIs
    if key == "milligold":
        result = await scrape_milligold(session)
    elif key == "goldika":
        result = await scrape_goldika(session)
    elif key == "wallgold":
        result = await scrape_wallgold(session)
    # Layer 2: Selenium
    elif key == "talasea" and state.selenium_available:
        result = await scrape_selenium_async("talasea", "https://talasea.ir/")
    elif key == "melligold" and state.selenium_available:
        result = await scrape_selenium_async("melligold", "https://melligold.com/")

    if result:
        state.price_cache[cache_key] = {"data": result, "ts": time.time()}
        state.platform_status[key] = True
        logger.info(f"[{key}] source={result['source']} buy={result['buy']:,} sell={result['sell']:,}")
        return result

    # Layer 3: Stale cache
    if cached:
        state.platform_status[key] = True
        return cached["data"]

    state.platform_status[key] = False
    return None

async def collect_all_prices(session: aiohttp.ClientSession) -> Dict[str, dict]:
    tracked = list(REAL_APIS.keys()) + SELENIUM_PLATFORMS
    tasks = [get_platform_price(session, k) for k in tracked]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    prices = {}
    for key, result in zip(tracked, results):
        if isinstance(result, Exception):
            logger.error(f"[{key}] {result}")
            state.platform_status[key] = False
        elif result:
            prices[key] = result
    return prices

# ---------------------------------------------------------------------------
# ARBITRAGE ENGINE
# ---------------------------------------------------------------------------

@dataclass
class ArbOpp:
    buy_platform: str
    sell_platform: str
    buy_price: int
    sell_price: int
    spread_pct: float
    profit_per_gram: int

def detect_arbitrage(prices: Dict[str, dict]) -> List[ArbOpp]:
    opps = []
    items = list(prices.items())
    for i in range(len(items)):
        for j in range(len(items)):
            if i == j: continue
            ka, da = items[i]; kb, db = items[j]
            buy_from_a = da.get("sell", 0)  # they sell to us
            sell_to_b = db.get("buy", 0)    # they buy from us
            if buy_from_a <= 0 or sell_to_b <= 0 or buy_from_a >= sell_to_b:
                continue
            spread = (sell_to_b - buy_from_a) / buy_from_a * 100
            if spread >= GOLD_SPREAD_ALERT:
                opps.append(ArbOpp(ka, kb, buy_from_a, sell_to_b, round(spread, 2), sell_to_b - buy_from_a))
    opps.sort(key=lambda x: x.spread_pct, reverse=True)
    return opps

# ---------------------------------------------------------------------------
# BUBBLES
# ---------------------------------------------------------------------------

def calc_bubbles(navasan: dict) -> dict:
    g18 = navasan.get("18ayar", 0)
    if not g18: return {}
    bubbles = {}
    for ck, cw in COIN_WEIGHTS.items():
        mp = navasan.get(ck, 0)
        if not mp: continue
        iv = g18 * cw
        if iv > 0:
            pct = (mp - iv) / iv * 100
            bubbles[ck] = {"market_price": mp, "intrinsic": round(iv),
                           "bubble_pct": round(pct, 2), "bubble_toman": mp - round(iv)}
    return bubbles

# ---------------------------------------------------------------------------
# COMPARISON
# ---------------------------------------------------------------------------

def compare_navasan(prices: Dict[str, dict], navasan: dict) -> dict:
    n18 = navasan.get("18ayar", 0)
    if not n18: return {}
    comp = {}
    for k, d in prices.items():
        avg = (d.get("buy", 0) + d.get("sell", 0)) // 2
        if avg:
            diff = (avg - n18) / n18 * 100
            comp[k] = {"navasan_18": n18, "platform_avg": avg, "diff_pct": round(diff, 2), "cheaper": diff < 0}
    return comp

# ---------------------------------------------------------------------------
# ALERTS
# ---------------------------------------------------------------------------

async def alert(bot_app, chat_id, msg, key, cooldown=600):
    now = time.time()
    if now - state.last_alerts.get(key, 0) < cooldown:
        return
    state.last_alerts[key] = now
    try:
        await bot_app.send_message(chat_id=chat_id, text=msg, parse_mode="HTML")
    except Exception as e:
        logger.error(f"Alert: {e}")

# ---------------------------------------------------------------------------
# SUMMARY + DAILY REPORT
# ---------------------------------------------------------------------------

async def send_smart_summary(bot_app, chat_id, prices, navasan, bubbles):
    now = time.time()
    if now - state.last_summary_time < GOLD_SUMMARY_INTERVAL:
        return
    state.last_summary_time = now

    n18 = navasan.get("18ayar", 0)
    xau = navasan.get("usd_xau", 0)
    lines = ["🟡 <b>خلاصه بازار طلا</b>", f"📅 {datetime.now(timezone.utc).strftime('%H:%M UTC')}", ""]
    if n18: lines.append(f"💰 طلای ۱۸ (Navasan): <b>{n18:,} تومان</b>")
    if xau: lines.append(f"🌍 انس جهانی: <b>${xau:,}</b>")
    lines.extend(["", "<b>پلتفرم‌ها:</b>"])

    for key in ["milligold", "talasea", "goldika", "wallgold", "melligold"]:
        d = prices.get(key)
        if not d: continue
        name = PLATFORM_NAMES_FA.get(key, key)
        icon = "🟢" if state.platform_status.get(key, True) else "🔴"
        src = f" [{d.get('source','?')[0]}]"
        lines.append(f"{icon} {name}: خرید {d['buy']:,} | فروش {d['sell']:,}{src}")

    if bubbles:
        lines.extend(["", "<b>حباب سکه:</b>"])
        for k, b in bubbles.items():
            bn = {"sekkeh":"امامی","nim":"نیم","rob":"ربع","gerami":"گرمی"}.get(k, k)
            lines.append(f"{'🔴' if abs(b['bubble_pct'])>GOLD_BUBBLE_ALERT else '🟢'} {bn}: {b['bubble_pct']:+.1f}%")

    try: await bot_app.send_message(chat_id=chat_id, text="\n".join(lines), parse_mode="HTML")
    except: pass

async def send_daily_report(bot_app, chat_id, prices, navasan):
    now = datetime.now(timezone.utc)
    today = now.strftime("%Y-%m-%d")
    if now.hour != GOLD_DAILY_REPORT_HOUR or state.last_daily_report == today:
        return
    state.last_daily_report = today
    n18 = navasan.get("18ayar", 0)
    lines = ["🟡 <b>گزارش روزانه طلا</b>", f"📅 {today}", ""]
    for key, d in sorted(prices.items()):
        name = PLATFORM_NAMES_FA.get(key, key)
        avg = (d["buy"] + d["sell"]) // 2
        diff = ((avg - n18) / n18 * 100) if n18 else 0
        lines.append(f"• {name}: میانگین {avg:,} ({diff:+.2f}% vs Navasan)")
    try: await bot_app.send_message(chat_id=chat_id, text="\n".join(lines), parse_mode="HTML")
    except: pass

# ---------------------------------------------------------------------------
# /gold COMMAND
# ---------------------------------------------------------------------------

async def handle_gold_command(bot_app, message):
    chat_id = str(message.chat.id)
    conn = aiohttp.TCPConnector(limit=10)
    async with aiohttp.ClientSession(connector=conn) as session:
        navasan = await fetch_navasan(session)
        prices = await collect_all_prices(session)

    n18 = navasan.get("18ayar", 0) if navasan else 0
    bubbles = calc_bubbles(navasan) if navasan else {}
    comp = compare_navasan(prices, navasan) if navasan else {}
    opps = detect_arbitrage(prices)

    lines = ["🟡 <b>وضعیت کامل طلا</b>", ""]
    if navasan:
        lines.append("📊 <b>Navasan:</b>")
        lines.append(f"• طلای ۱۸: <b>{n18:,} تومان</b>")
        for k, l in [("sekkeh","سکه"),("nim","نیم"),("rob","ربع"),("gerami","گرمی")]:
            if navasan.get(k): lines.append(f"• {l}: <b>{navasan[k]:,} تومان</b>")
        if navasan.get("usd_xau"): lines.append(f"• انس: <b>${navasan['usd_xau']:,}</b>")

    lines.extend(["", "🏪 <b>پلتفرم‌ها:</b>"])
    for key in ["milligold", "talasea", "goldika", "wallgold", "melligold"]:
        d = prices.get(key)
        if not d: continue
        name = PLATFORM_NAMES_FA.get(key, key)
        icon = "🟢" if state.platform_status.get(key, True) else "🔴"
        lines.append(f"{icon} {name}: خرید {d['buy']:,} | فروش {d['sell']:,}")
        if key in comp:
            c = comp[key]; lines.append(f"   {'🔻' if c['cheaper'] else '🔺'} {c['diff_pct']:+.2f}%")

    lines.append("")
    if opps:
        lines.append("💹 <b>آربیتراژ:</b>")
        for o in opps[:5]:
            na = PLATFORM_NAMES_FA.get(o.buy_platform, o.buy_platform)
            nb = PLATFORM_NAMES_FA.get(o.sell_platform, o.sell_platform)
            lines.append(f"• {na} → {nb}: <b>+{o.spread_pct}%</b> ({o.profit_per_gram:,} تومان/گرم)")
    else:
        lines.append("💹 <b>آربیتراژ:</b> ندارد")

    if bubbles:
        lines.extend(["", "🫧 <b>حباب:</b>"])
        for k, b in bubbles.items():
            bn = {"sekkeh":"امامی","nim":"نیم","rob":"ربع","gerami":"گرمی"}.get(k, k)
            lines.append(f"{'🔴' if abs(b['bubble_pct'])>GOLD_BUBBLE_ALERT else '🟢'} {bn}: {b['bubble_pct']:+.1f}%")

    try: await bot_app.send_message(chat_id=chat_id, text="\n".join(lines), parse_mode="HTML")
    except: pass

# ---------------------------------------------------------------------------
# /gold_alert, /gold_help
# ---------------------------------------------------------------------------

async def handle_gold_alert_command(bot_app, message):
    global GOLD_SPREAD_ALERT
    chat_id = str(message.chat.id)
    parts = (message.text or "").split()
    if len(parts) >= 2:
        try:
            new = float(parts[1])
            old = GOLD_SPREAD_ALERT
            GOLD_SPREAD_ALERT = new
            await bot_app.send_message(chat_id=chat_id, text=f"✅ آستانه: {old}% → <b>{new}%</b>", parse_mode="HTML")
            return
        except ValueError: pass
    await bot_app.send_message(chat_id=chat_id, text=f"⚙️ آستانه فعلی: <b>{GOLD_SPREAD_ALERT}%</b>", parse_mode="HTML")

async def handle_gold_help_command(bot_app, message):
    await bot_app.send_message(chat_id=str(message.chat.id),
        text="🟡 <b>راهنما</b>\n\n"
             "/gold — وضعیت کامل بازار\n"
             "/gold_alert [%] — تنظیم آستانه اسپرد\n"
             "/gold_help — راهنما\n\n"
             "<b>منابع:</b>\n"
             "[a] = API مستقیم (واقعی)\n"
             "[s] = Selenium (مرورگر)\n\n"
             f"هشدار: اسپرد>{GOLD_SPREAD_ALERT}% | حباب>{GOLD_BUBBLE_ALERT}% | اختلاف>{GOLD_NAVASAN_ALERT}%",
        parse_mode="HTML")

# ---------------------------------------------------------------------------
# MONITOR LOOP
# ---------------------------------------------------------------------------

async def gold_monitor_loop(bot_app, chat_id: str):
    logger.info("Gold monitor started")
    # Test if selenium is available
    try:
        import selenium; state.selenium_available = True
        logger.info("Selenium available for talasea/melligold")
    except ImportError:
        logger.info("Selenium not available — talasea/melligold offline")

    conn = aiohttp.TCPConnector(limit=10)
    async with aiohttp.ClientSession(connector=conn) as session:
        while state.running:
            try:
                navasan = await fetch_navasan(session)
                n18 = navasan.get("18ayar", 0) if navasan else 0
                prices = await collect_all_prices(session)
                bubbles = calc_bubbles(navasan) if navasan else {}

                # Arbitrage
                for o in detect_arbitrage(prices)[:3]:
                    na = PLATFORM_NAMES_FA.get(o.buy_platform, o.buy_platform)
                    nb = PLATFORM_NAMES_FA.get(o.sell_platform, o.sell_platform)
                    await alert(bot_app, chat_id,
                        f"💹 <b>آربیتراژ!</b>\n{na} → {nb}\nاسپرد: <b>{o.spread_pct}%</b>\nسود: {o.profit_per_gram:,} تومان/گرم",
                        f"arb:{o.buy_platform}:{o.sell_platform}")

                # Navasan divergence
                for k, c in compare_navasan(prices, navasan or {}).items():
                    if abs(c["diff_pct"]) >= GOLD_NAVASAN_ALERT:
                        name = PLATFORM_NAMES_FA.get(k, k)
                        await alert(bot_app, chat_id,
                            f"⚠️ {name} {c['diff_pct']:+.2f}% {'ارزون‌تر' if c['cheaper'] else 'گرون‌تر'} از Navasan",
                            f"nav:{k}", cooldown=1800)

                # Bubbles
                for k, b in bubbles.items():
                    if abs(b["bubble_pct"]) >= GOLD_BUBBLE_ALERT:
                        bn = {"sekkeh":"سکه","nim":"نیم","rob":"ربع","gerami":"گرمی"}.get(k, k)
                        await alert(bot_app, chat_id,
                            f"🫧 <b>حباب {bn}:</b> {b['bubble_pct']:+.1f}%", f"bubble:{k}", cooldown=3600)

                # Status changes
                for k, online in list(state.platform_status.items()):
                    pk = f"st:{k}:prev"
                    prev = state.last_alerts.get(pk)
                    if prev is not None and prev != online:
                        name = PLATFORM_NAMES_FA.get(k, k)
                        if not online:
                            await alert(bot_app, chat_id, f"🔴 {name} قطع شد", f"st:{k}:off", cooldown=3600)
                        else:
                            await alert(bot_app, chat_id, f"🟢 {name} وصل شد", f"st:{k}:on", cooldown=0)
                    state.last_alerts[f"st:{k}:prev"] = online

                await send_smart_summary(bot_app, chat_id, prices, navasan or {}, bubbles)
                await send_daily_report(bot_app, chat_id, prices, navasan or {})

            except Exception as e:
                logger.error(f"Monitor: {e}", exc_info=True)
            await asyncio.sleep(GOLD_POLL_SECONDS)

# ---------------------------------------------------------------------------
# HEALTH
# ---------------------------------------------------------------------------

def create_health_app():
    from aiohttp import web
    async def health(request):
        return web.json_response({"status":"ok","service":"gold-arbitrage",
            "online":sum(1 for v in state.platform_status.values() if v),
            "total":len(state.platform_status)})
    app = web.Application()
    app.router.add_get("/", health); app.router.add_get("/health", health)
    return app

async def start_gold_service(bot_app, chat_id: str):
    asyncio.create_task(gold_monitor_loop(bot_app, chat_id))
    logger.info("Gold service started")