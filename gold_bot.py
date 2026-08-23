#!/usr/bin/env python3
"""
Gold Arbitrage Bot — Production v3
4 real internal APIs (no geo-block):
  milli.gold — /api/v1/public/milli-price/detail
  goldika.ir — /api/public/price  
  wallgold.ir — /api/v1/markets
  talasea.ir — api.talasea.ir/api/market/getGoldPrice
Navasan baseline. Selenium fallback for melligold.
"""

import asyncio, json, logging, re, time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional

import aiohttp

logger = logging.getLogger("gold_bot")

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

NAVASAN_GOLD_URL = "https://raw.githubusercontent.com/HosseinOdd/Navasan-API/main/data/gold.json"

GOLD_POLL_SECONDS = 120
GOLD_SPREAD_ALERT = 1.0
GOLD_NAVASAN_ALERT = 2.0
GOLD_BUBBLE_ALERT = 2.0
GOLD_SUMMARY_INTERVAL = 3600
GOLD_CACHE_TTL = 120
GOLD_DAILY_REPORT_HOUR = 23

COIN_WEIGHTS = {"sekkeh": 8.133, "nim": 4.0665, "rob": 2.03325, "gerami": 1.0}

PLATFORM_NAMES_FA = {
    "milligold": "میلی‌گلد 🥇", "talasea": "طلاسی 🥈", "goldika": "گلدیکا 🥉",
    "wallgold": "وال‌گلد 🟡", "melligold": "ملی‌گلد",
    "daric": "داریک", "technogold": "تکنوگلد", "talapp": "طلاپ", "digikala_gold": "دیجی‌کالا طلا",
}

TRACKED_PLATFORMS = ["milligold", "talasea", "goldika", "wallgold", "melligold"]

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
    selenium_available: bool = False

state = GoldState()

# ---------------------------------------------------------------------------
# UTILS
# ---------------------------------------------------------------------------

def _num(val) -> Optional[int]:
    if val is None: return None
    if isinstance(val, (int, float)): return int(val)
    if isinstance(val, str):
        val = val.replace(",", "").replace("٬", "").replace(" ", "")
        try: return int(float(val))
        except: return None
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
                d = await resp.json()
                state.navasan_cache = d; state.navasan_cache_time = now
                logger.info(f"Navasan: 18ayar={d.get('18ayar', 0):,}")
                return d
    except Exception as e: logger.warning(f"Navasan: {e}")
    return state.navasan_cache

# ---------------------------------------------------------------------------
# REAL API SCRAPERS
# ---------------------------------------------------------------------------

async def _api_get(session, url, headers=None, timeout=15):
    h = {"User-Agent": "Mozilla/5.0", "Accept": "application/json", **(headers or {})}
    async with session.get(url, headers=h, timeout=aiohttp.ClientTimeout(total=timeout)) as r:
        if r.status == 200: return await r.json()
        logger.warning(f"API {url[:60]}: HTTP {r.status}")
        return None

# --- milli.gold ---
async def scrape_milligold(session):
    try:
        d = await _api_get(session, "https://milli.gold/api/v1/public/milli-price/detail",
                           {"Referer": "https://milli.gold/", "Origin": "https://milli.gold"})
        p = _num(d.get("data", {}).get("price18")) if d else None
        if p:
            t = p // 10  # Rial → Toman
            return {"buy": t, "sell": t, "unit": "geram18", "platform": "milligold", "source": "api", "ts": time.time()}
    except Exception as e: logger.warning(f"milligold: {e}")
    return None

# --- goldika ---
async def scrape_goldika(session):
    try:
        d = await _api_get(session, "https://goldika.ir/api/public/price")
        pd = d.get("data", {}).get("price", {}) if d else {}
        buy, sell = _num(pd.get("buy")), _num(pd.get("sell"))
        if buy and sell:
            return {"buy": buy // 10, "sell": sell // 10, "unit": "geram18", "platform": "goldika", "source": "api", "ts": time.time()}
    except Exception as e: logger.warning(f"goldika: {e}")
    return None

# --- wallgold ---
async def scrape_wallgold(session):
    try:
        d = await _api_get(session, "https://api.wallgold.ir/api/v1/markets")
        if isinstance(d, list):
            for m in d:
                if m.get("symbol") == "GLD_18C_750TMN":
                    cap = m.get("marketCap", {})
                    buy, sell = _num(cap.get("lastBuyPrice")), _num(cap.get("lastSellPrice"))
                    if buy and sell:
                        factor = 1.0 / 0.007  # price is per 0.007g
                        return {"buy": round(buy * factor), "sell": round(sell * factor),
                                "unit": "geram18", "platform": "wallgold", "source": "api", "ts": time.time()}
    except Exception as e: logger.warning(f"wallgold: {e}")
    return None

# --- talasea (api.talasea.ir bypasses geo-block!) ---
async def scrape_talasea(session):
    try:
        d = await _api_get(session, "https://api.talasea.ir/api/market/getGoldPrice")
        fee = float(d.get("feeTable", [{}])[0].get("fee", 0.01)) if d else 0.01
        raw = _num(d.get("price")) if d else None
        if raw:
            # price is Toman per 0.1 gram with fee baked in
            # Convert to per-gram Toman: raw * 10, then remove fee
            sell_price = round(raw * 10 * (1 - fee))  # they buy from us = we sell to them
            buy_price = round(raw * 10)                 # we buy from them = they sell to us
            return {"buy": buy_price, "sell": sell_price, "unit": "geram18", "platform": "talasea",
                    "source": "api", "ts": time.time()}
    except Exception as e: logger.warning(f"talasea: {e}")
    return None

# --- Selenium fallback for melligold ---
def _selenium_scrape(url, site_name):
    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from bs4 import BeautifulSoup
        opts = Options()
        for a in ["--headless=new", "--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu", "--window-size=1280,720"]:
            opts.add_argument(a)
        opts.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
        driver = webdriver.Chrome(options=opts)
        driver.set_page_load_timeout(25)
        try: driver.get(url); import time as _t; _t.sleep(8)
        except: pass
        soup = BeautifulSoup(driver.page_source, "html.parser")
        driver.quit()
        prices = []
        for el in soup.find_all(["div", "span", "p"]):
            for m in re.findall(r'([\d,]{5,12})', el.get_text().strip()):
                n = _num(m)
                if n and 5_000_000 < n < 500_000_000: prices.append(n)
        if prices:
            prices.sort(); mid = prices[len(prices)//2]
            return {"buy": round(mid*0.99), "sell": mid, "unit": "geram18", "platform": site_name,
                    "source": "selenium", "ts": time.time()}
    except ImportError: state.selenium_available = False
    except Exception as e: logger.error(f"Selenium {site_name}: {e}")
    return None

async def scrape_selenium_async(site_name, url):
    return await asyncio.get_event_loop().run_in_executor(None, _selenium_scrape, url, site_name)

# ---------------------------------------------------------------------------
# COLLECT
# ---------------------------------------------------------------------------

SCRAPER_MAP = {
    "milligold": scrape_milligold,
    "goldika": scrape_goldika,
    "wallgold": scrape_wallgold,
    "talasea": scrape_talasea,
}

async def get_platform_price(session, key):
    ck = f"price:{key}"
    cached = state.price_cache.get(ck)
    if cached and (time.time() - cached.get("ts", 0)) < GOLD_CACHE_TTL:
        return cached["data"]

    result = None
    scraper = SCRAPER_MAP.get(key)
    if scraper:
        result = await scraper(session)
    elif key == "melligold" and state.selenium_available:
        result = await scrape_selenium_async("melligold", "https://melligold.com/")

    if result:
        state.price_cache[ck] = {"data": result, "ts": time.time()}
        state.platform_status[key] = True
        logger.info(f"[{key}] {result['source']} buy={result['buy']:,} sell={result['sell']:,}")
        return result
    if cached:
        state.platform_status[key] = True
        return cached["data"]
    state.platform_status[key] = False
    return None

async def collect_all_prices(session):
    tasks = [get_platform_price(session, k) for k in TRACKED_PLATFORMS]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    prices = {}
    for k, r in zip(TRACKED_PLATFORMS, results):
        if isinstance(r, Exception):
            state.platform_status[k] = False; logger.error(f"[{k}] {r}")
        elif r: prices[k] = r
    return prices

# ---------------------------------------------------------------------------
# ARBITRAGE
# ---------------------------------------------------------------------------

@dataclass
class Arb: buy_platform: str; sell_platform: str; buy_price: int; sell_price: int; spread_pct: float; profit_per_gram: int

def detect_arbitrage(prices):
    opps = []; items = list(prices.items())
    for i in range(len(items)):
        for j in range(len(items)):
            if i == j: continue
            ka, da = items[i]; kb, db = items[j]
            bf = da.get("sell", 0); st = db.get("buy", 0)  # buy from A, sell to B
            if bf <= 0 or st <= 0 or bf >= st: continue
            sp = (st - bf) / bf * 100
            if sp >= GOLD_SPREAD_ALERT:
                opps.append(Arb(ka, kb, bf, st, round(sp, 2), st - bf))
    opps.sort(key=lambda x: x.spread_pct, reverse=True)
    return opps

# ---------------------------------------------------------------------------
# BUBBLES + COMPARISON
# ---------------------------------------------------------------------------

def calc_bubbles(navasan):
    g18 = navasan.get("18ayar", 0)
    if not g18: return {}
    b = {}
    for ck, cw in COIN_WEIGHTS.items():
        mp = navasan.get(ck, 0)
        if not mp: continue
        iv = g18 * cw
        if iv > 0:
            pct = (mp - iv) / iv * 100
            b[ck] = {"market_price": mp, "intrinsic": round(iv), "bubble_pct": round(pct, 2), "bubble_toman": mp - round(iv)}
    return b

def compare_navasan(prices, navasan):
    n18 = navasan.get("18ayar", 0)
    if not n18: return {}
    c = {}
    for k, d in prices.items():
        avg = (d.get("buy", 0) + d.get("sell", 0)) // 2
        if avg:
            diff = (avg - n18) / n18 * 100
            c[k] = {"navasan_18": n18, "platform_avg": avg, "diff_pct": round(diff, 2), "cheaper": diff < 0}
    return c

# ---------------------------------------------------------------------------
# ALERTS
# ---------------------------------------------------------------------------

async def alert(bot_app, chat_id, msg, key, cooldown=600):
    now = time.time()
    if now - state.last_alerts.get(key, 0) < cooldown: return
    state.last_alerts[key] = now
    try: await bot_app.send_message(chat_id=chat_id, text=msg, parse_mode="HTML")
    except: pass

# ---------------------------------------------------------------------------
# SUMMARY + DAILY
# ---------------------------------------------------------------------------

async def send_summary(bot_app, chat_id, prices, navasan, bubbles):
    now = time.time()
    if now - state.last_summary_time < GOLD_SUMMARY_INTERVAL: return
    state.last_summary_time = now
    n18 = navasan.get("18ayar", 0); xau = navasan.get("usd_xau", 0)
    lines = ["🟡 <b>خلاصه بازار طلا</b>", f"📅 {datetime.now(timezone.utc).strftime('%H:%M UTC')}", ""]
    if n18: lines.append(f"💰 طلای ۱۸ (Navasan): <b>{n18:,} تومان</b>")
    if xau: lines.append(f"🌍 انس: <b>${xau:,}</b>")
    lines.extend(["", "<b>پلتفرم‌ها:</b>"])
    for key in TRACKED_PLATFORMS:
        d = prices.get(key)
        if not d: continue
        name = PLATFORM_NAMES_FA.get(key, key)
        icon = "🟢" if state.platform_status.get(key, True) else "🔴"
        lines.append(f"{icon} {name}: خرید {d['buy']:,} | فروش {d['sell']:,} [{d['source'][0]}]")
    if bubbles:
        lines.extend(["", "<b>حباب:</b>"])
        for k, b in bubbles.items():
            bn = {"sekkeh":"امامی","nim":"نیم","rob":"ربع","gerami":"گرمی"}.get(k, k)
            lines.append(f"{'🔴' if abs(b['bubble_pct'])>GOLD_BUBBLE_ALERT else '🟢'} {bn}: {b['bubble_pct']:+.1f}%")
    try: await bot_app.send_message(chat_id=chat_id, text="\n".join(lines), parse_mode="HTML")
    except: pass

async def send_daily_report(bot_app, chat_id, prices, navasan):
    now = datetime.now(timezone.utc); today = now.strftime("%Y-%m-%d")
    if now.hour != GOLD_DAILY_REPORT_HOUR or state.last_daily_report == today: return
    state.last_daily_report = today
    n18 = navasan.get("18ayar", 0)
    lines = ["🟡 <b>گزارش روزانه طلا</b>", f"📅 {today}", ""]
    for key, d in sorted(prices.items()):
        name = PLATFORM_NAMES_FA.get(key, key); avg = (d["buy"] + d["sell"]) // 2
        diff = ((avg - n18) / n18 * 100) if n18 else 0
        lines.append(f"• {name}: {avg:,} ({diff:+.2f}%)")
    try: await bot_app.send_message(chat_id=chat_id, text="\n".join(lines), parse_mode="HTML")
    except: pass

# ---------------------------------------------------------------------------
# COMMANDS
# ---------------------------------------------------------------------------

async def handle_gold_command(bot_app, message):
    chat_id = str(message.chat.id)
    conn = aiohttp.TCPConnector(limit=10)
    async with aiohttp.ClientSession(connector=conn) as session:
        navasan = await fetch_navasan(session); prices = await collect_all_prices(session)
    n18 = navasan.get("18ayar", 0) if navasan else 0
    bubbles = calc_bubbles(navasan) if navasan else {}
    comp = compare_navasan(prices, navasan) if navasan else {}
    opps = detect_arbitrage(prices)
    lines = ["🟡 <b>وضعیت کامل طلا</b>", "", "📊 <b>Navasan:</b>"]
    if navasan:
        lines.append(f"• طلای ۱۸: <b>{n18:,} تومان</b>")
        for k, l in [("sekkeh","سکه"),("nim","نیم"),("rob","ربع"),("gerami","گرمی")]:
            if navasan.get(k): lines.append(f"• {l}: <b>{navasan[k]:,} تومان</b>")
        if navasan.get("usd_xau"): lines.append(f"• انس: <b>${navasan['usd_xau']:,}</b>")
    else: lines.append("⚠️ در دسترس نیست")
    lines.extend(["", "🏪 <b>پلتفرم‌ها:</b>"])
    for key in TRACKED_PLATFORMS:
        d = prices.get(key)
        if not d: continue
        name = PLATFORM_NAMES_FA.get(key, key)
        icon = "🟢" if state.platform_status.get(key, True) else "🔴"
        lines.append(f"{icon} {name}: خرید {d['buy']:,} | فروش {d['sell']:,}")
        if key in comp:
            c = comp[key]
            lines.append(f"   {'🔻' if c['cheaper'] else '🔺'} {c['diff_pct']:+.2f}% vs Navasan")
    lines.append("")
    if opps:
        lines.append("💹 <b>آربیتراژ:</b>")
        for o in opps[:5]:
            na = PLATFORM_NAMES_FA.get(o.buy_platform, o.buy_platform)
            nb = PLATFORM_NAMES_FA.get(o.sell_platform, o.sell_platform)
            lines.append(f"• {na} → {nb}: <b>+{o.spread_pct}%</b> ({o.profit_per_gram:,} تومان/گرم)")
    else: lines.append("💹 <b>آربیتراژ:</b> ندارد")
    if bubbles:
        lines.extend(["", "🫧 <b>حباب:</b>"])
        for k, b in bubbles.items():
            bn = {"sekkeh":"امامی","nim":"نیم","rob":"ربع","gerami":"گرمی"}.get(k, k)
            lines.append(f"{'🔴' if abs(b['bubble_pct'])>GOLD_BUBBLE_ALERT else '🟢'} {bn}: {b['bubble_pct']:+.1f}%")
    try: await bot_app.send_message(chat_id=chat_id, text="\n".join(lines), parse_mode="HTML")
    except: pass

async def handle_gold_alert_command(bot_app, message):
    global GOLD_SPREAD_ALERT
    chat_id = str(message.chat.id); parts = (message.text or "").split()
    if len(parts) >= 2:
        try:
            new = float(parts[1]); old = GOLD_SPREAD_ALERT; GOLD_SPREAD_ALERT = new
            await bot_app.send_message(chat_id=chat_id, text=f"✅ آستانه: {old}% → <b>{new}%</b>", parse_mode="HTML")
            return
        except ValueError: pass
    await bot_app.send_message(chat_id=chat_id, text=f"⚙️ آستانه فعلی: <b>{GOLD_SPREAD_ALERT}%</b>", parse_mode="HTML")

async def handle_gold_help_command(bot_app, message):
    await bot_app.send_message(chat_id=str(message.chat.id),
        text="🟡 <b>راهنما</b>\n\n/gold — وضعیت کامل بازار\n/gold_alert [%] — تنظیم آستانه\n/gold_help — راهنما\n\n"
             "<b>منابع:</b> [a]=API مستقیم [s]=Selenium\n"
             f"هشدارها: اسپرد>{GOLD_SPREAD_ALERT}% | حباب>{GOLD_BUBBLE_ALERT}% | اختلاف>{GOLD_NAVASAN_ALERT}%",
        parse_mode="HTML")

# ---------------------------------------------------------------------------
# MONITOR LOOP
# ---------------------------------------------------------------------------

async def gold_monitor_loop(bot_app, chat_id: str):
    logger.info("Gold monitor started")
    try: import selenium; state.selenium_available = True; logger.info("Selenium ready")
    except ImportError: logger.info("No Selenium — melligold offline")

    conn = aiohttp.TCPConnector(limit=10)
    async with aiohttp.ClientSession(connector=conn) as session:
        while state.running:
            try:
                navasan = await fetch_navasan(session)
                n18 = navasan.get("18ayar", 0) if navasan else 0
                prices = await collect_all_prices(session)
                bubbles = calc_bubbles(navasan) if navasan else {}

                for o in detect_arbitrage(prices)[:3]:
                    na = PLATFORM_NAMES_FA.get(o.buy_platform, o.buy_platform)
                    nb = PLATFORM_NAMES_FA.get(o.sell_platform, o.sell_platform)
                    await alert(bot_app, chat_id,
                        f"💹 <b>آربیتراژ!</b>\n{na} → {nb}\nاسپرد: <b>{o.spread_pct}%</b>\nسود: {o.profit_per_gram:,} تومان/گرم",
                        f"arb:{o.buy_platform}:{o.sell_platform}")

                for k, c in compare_navasan(prices, navasan or {}).items():
                    if abs(c["diff_pct"]) >= GOLD_NAVASAN_ALERT:
                        name = PLATFORM_NAMES_FA.get(k, k)
                        await alert(bot_app, chat_id,
                            f"⚠️ {name} {c['diff_pct']:+.2f}% {'ارزون‌تر' if c['cheaper'] else 'گرون‌تر'} از Navasan",
                            f"nav:{k}", cooldown=1800)

                for k, b in bubbles.items():
                    if abs(b["bubble_pct"]) >= GOLD_BUBBLE_ALERT:
                        bn = {"sekkeh":"سکه","nim":"نیم","rob":"ربع","gerami":"گرمی"}.get(k, k)
                        await alert(bot_app, chat_id, f"🫧 <b>حباب {bn}:</b> {b['bubble_pct']:+.1f}%", f"bubble:{k}", cooldown=3600)

                for k, online in list(state.platform_status.items()):
                    pk = f"st:{k}:prev"; prev = state.last_alerts.get(pk)
                    if prev is not None and prev != online:
                        name = PLATFORM_NAMES_FA.get(k, k)
                        if not online: await alert(bot_app, chat_id, f"🔴 {name} قطع شد", f"st:{k}:off", cooldown=3600)
                        else: await alert(bot_app, chat_id, f"🟢 {name} وصل شد", f"st:{k}:on", cooldown=0)
                    state.last_alerts[f"st:{k}:prev"] = online

                await send_summary(bot_app, chat_id, prices, navasan or {}, bubbles)
                await send_daily_report(bot_app, chat_id, prices, navasan or {})
            except Exception as e: logger.error(f"Monitor: {e}", exc_info=True)
            await asyncio.sleep(GOLD_POLL_SECONDS)

# ---------------------------------------------------------------------------
# HEALTH + LAUNCH
# ---------------------------------------------------------------------------

def create_health_app():
    from aiohttp import web
    async def health(request):
        return web.json_response({"status":"ok","service":"gold-arbitrage",
            "online":sum(1 for v in state.platform_status.values() if v),
            "total":len(state.platform_status)})
    app = web.Application(); app.router.add_get("/", health); app.router.add_get("/health", health)
    return app

async def start_gold_service(bot_app, chat_id: str):
    asyncio.create_task(gold_monitor_loop(bot_app, chat_id))
    logger.info("Gold service started")