#!/usr/bin/env python3
"""
Gold Arbitrage Bot — Module for master_bot.py
Hybrid approach: parse.bot scrapers + tgju profile scraping + Navasan baseline.
Monitors Iranian gold platforms, detects arbitrage spreads, coin bubbles.
"""

import asyncio
import json
import logging
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

# parse.bot — account 1 (chisbajele01) — goldika + wallgold
# parse.bot — account 2 (chisbhdtye02) — milligold + talasea (pending retry)
PARSEBOT_ACCOUNTS = {
    "goldika": {
        "api_key": "pmx_47e49d57f0975af375b1697c38e886ad",
        "scraper_id": "0ec5e313-d8c7-40b8-8344-942651a6d6e7",
        "endpoint": "get_gold_price",
    },
    "wallgold": {
        "api_key": "pmx_47e49d57f0975af375b1697c38e886ad",
        "scraper_id": "b9ecd128-6522-4229-8535-c7a7738379da",
        "endpoint": "get_gold_price",
    },
}

# tgju profile scraping — platforms whose profile pages work
TGJU_PROFILES = {
    "talasea": "talasea",
    "goldika": "goldika",
    "wallgold": "wallgold",
    "daric": "daric",
}

# Platform fees (from tgju.org research) — applied to Navasan baseline
PLATFORM_FEES = {
    "goldika":     {"buy_fee": 0.0099, "sell_fee": 0.0099},   # ~0.99% avg
    "wallgold":    {"buy_fee": 0.005,  "sell_fee": 0.005},     # 0.5%
    "talasea":     {"buy_fee": 0.01,   "sell_fee": 0.01},      # 1%
    "milligold":   {"buy_fee": 0.005,  "sell_fee": 0.005},     # 0.5% (milli.gold — same buy/sell)
    "melligold":   {"buy_fee": 0.005,  "sell_fee": 0.005},     # 0.5%
    "daric":       {"buy_fee": 0.005,  "sell_fee": 0.005},     # 0.5%
    "technogold":  {"buy_fee": 0.05,   "sell_fee": 0.01},      # 5% buy / 1% sell (tgju)
    "talapp":      {"buy_fee": 0.00375,"sell_fee": 0.00375},   # 0.25-0.5% avg
    "digikala_gold":{"buy_fee": 0.01,  "sell_fee": 0.015},     # 1% buy / 1.5% sell
}

GOLD_POLL_SECONDS = 120
GOLD_SPREAD_ALERT = 1.0
GOLD_NAVASAN_ALERT = 2.0
GOLD_BUBBLE_ALERT = 2.0
GOLD_SUMMARY_INTERVAL = 3600
GOLD_CACHE_TTL = 300
GOLD_DAILY_REPORT_HOUR = 23  # UTC

COIN_WEIGHTS = {
    "sekkeh":  8.133,   # full bahar azadi
    "nim":     4.0665,  # half coin
    "rob":     2.03325, # quarter coin
    "gerami":  1.0,     # 1 gram coin
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

# All platforms we track (even those without direct scrapers)
TRACKED_PLATFORMS = ["talasea", "goldika", "wallgold", "milligold", "melligold", "daric"]

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

state = GoldState()

# ---------------------------------------------------------------------------
# UTILS
# ---------------------------------------------------------------------------

def _parse_number(val) -> Optional[int]:
    """Extract integer from various formats."""
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return int(val)
    if isinstance(val, str):
        val = val.replace(",", "").replace("٬", "").replace(" ", "")
        val = val.replace("تومان", "").replace("ریال", "").replace("T", "")
        try:
            num = int(float(val))
            if num > 100_000_000:
                num //= 10
            return num
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
        logger.warning(f"Navasan fetch failed: {e}")
    return state.navasan_cache

# ---------------------------------------------------------------------------
# PARSE.BOT (direct scraper — goldika + wallgold)
# ---------------------------------------------------------------------------

async def scrape_via_parsebot(
    session: aiohttp.ClientSession,
    platform_key: str,
) -> Optional[dict]:
    """Call parse.bot scraper. Returns {buy, sell, unit, platform} or None."""
    cfg = PARSEBOT_ACCOUNTS.get(platform_key)
    if not cfg:
        return None

    cache_key = f"pb:{platform_key}"
    cached = state.price_cache.get(cache_key)
    if cached and (time.time() - cached.get("ts", 0)) < GOLD_CACHE_TTL:
        return cached["data"]

    try:
        url = f"https://api.parse.bot/scraper/{cfg['scraper_id']}/{cfg['endpoint']}"
        headers = {"X-API-Key": cfg["api_key"], "Content-Type": "application/json"}
        async with session.post(url, headers=headers, json={}, timeout=aiohttp.ClientTimeout(total=25)) as resp:
            if resp.status == 200:
                raw = await resp.json()
                inner = raw.get("data", raw) if isinstance(raw, dict) else raw
                buy = _parse_number(inner.get("buy"))
                sell = _parse_number(inner.get("sell"))
                if buy and sell:
                    result = {"buy": buy, "sell": sell, "unit": "geram18", "platform": platform_key, "ts": time.time()}
                    state.price_cache[cache_key] = {"data": result, "ts": time.time()}
                    state.platform_status[platform_key] = True
                    logger.info(f"[parse.bot] {platform_key}: buy={buy:,} sell={sell:,}")
                    return result
                logger.warning(f"[parse.bot] {platform_key}: bad data {inner}")
            else:
                logger.warning(f"[parse.bot] {platform_key}: HTTP {resp.status}")
    except Exception as e:
        logger.error(f"[parse.bot] {platform_key}: {e}")

    if cached:
        state.platform_status[platform_key] = True
        return cached["data"]
    state.platform_status[platform_key] = False
    return None

# ---------------------------------------------------------------------------
# TGJU PROFILE SCRAPING (fallback for talasea, daric, etc.)
# ---------------------------------------------------------------------------

async def scrape_via_tgju_profile(
    session: aiohttp.ClientSession,
    platform_key: str,
) -> Optional[dict]:
    """
    Scrape tgju.org/profile/{slug} for the platform's current index price.
    Extract the live price number from the page HTML.
    Returns {platform_index_price: int} or None.
    """
    slug = TGJU_PROFILES.get(platform_key)
    if not slug:
        return None

    cache_key = f"tgju:{platform_key}"
    cached = state.price_cache.get(cache_key)
    if cached and (time.time() - cached.get("ts", 0)) < GOLD_CACHE_TTL:
        return cached["data"]

    try:
        url = f"https://www.tgju.org/profile/{slug}"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept-Language": "fa-IR,fa;q=0.9,en;q=0.8",
        }
        async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=20)) as resp:
            if resp.status != 200:
                logger.warning(f"[tgju] {platform_key}: HTTP {resp.status}")
                return cached["data"] if cached else None

            html = await resp.text()

            # Extract the price from: <span dir="ltr" class="..." data-price="VALUE">
            # Or from the price-info section
            # Pattern: the main price is in a span with specific format
            price_match = re.search(r'data-price="([\d,]+)"', html)
            if not price_match:
                # Try alternative: first large number in price-info area
                price_match = re.search(r'<span[^>]*class="price"[^>]*>([\d,]+)</span>', html)

            if price_match:
                raw = price_match.group(1)
                price = _parse_number(raw)
                if price and price > 100_000:
                    result = {"platform_index": price, "ts": time.time()}
                    state.price_cache[cache_key] = {"data": result, "ts": time.time()}
                    logger.info(f"[tgju] {platform_key}: index={price:,}")
                    return result

            # Fallback: try to find any big number in a price-related span
            prices = re.findall(r'>([\d,]{7,12})<', html)
            prices_parsed = [_parse_number(p) for p in prices if _parse_number(p)]
            prices_parsed = [p for p in prices_parsed if 1_000_000 < p < 100_000_000]
            if prices_parsed:
                price = max(prices_parsed)  # highest number is usually the index
                result = {"platform_index": price, "ts": time.time()}
                state.price_cache[cache_key] = {"data": result, "ts": time.time()}
                logger.info(f"[tgju] {platform_key}: index={price:,} (fallback parse)")
                return result

            logger.warning(f"[tgju] {platform_key}: could not extract price")
    except Exception as e:
        logger.error(f"[tgju] {platform_key}: {e}")

    if cached:
        return cached["data"]
    return None

# ---------------------------------------------------------------------------
# FEE-BASED PRICE CALCULATION
# ---------------------------------------------------------------------------

def calc_fee_based_prices(navasan_18: int, platform_key: str) -> Optional[dict]:
    """Derive buy/sell from Navasan baseline + platform fees."""
    fees = PLATFORM_FEES.get(platform_key)
    if not fees or not navasan_18:
        return None
    buy = round(navasan_18 * (1 + fees["buy_fee"]))   # they sell to us = we buy
    sell = round(navasan_18 * (1 - fees["sell_fee"]))  # they buy from us = we sell
    return {"buy": buy, "sell": sell, "unit": "geram18", "platform": platform_key,
            "source": "fee_calc", "ts": time.time()}

# ---------------------------------------------------------------------------
# MAIN PRICE COLLECTOR — hybrid dispatch
# ---------------------------------------------------------------------------

async def get_platform_price(
    session: aiohttp.ClientSession,
    platform_key: str,
    navasan_18: int,
) -> Optional[dict]:
    """
    Hybrid price fetch:
    1. parse.bot scraper (goldika, wallgold) — exact buy/sell
    2. tgju profile scrape (talasea, daric) — platform index
    3. Fee-based fallback — all platforms
    """
    # Layer 1: parse.bot for platforms that have working scrapers
    if platform_key in PARSEBOT_ACCOUNTS:
        result = await scrape_via_parsebot(session, platform_key)
        if result:
            state.platform_status[platform_key] = True
            return result

    # Layer 2: tgju profile for index price, then derive buy/sell from fees
    fees = PLATFORM_FEES.get(platform_key)
    if platform_key in TGJU_PROFILES:
        tgju_data = await scrape_via_tgju_profile(session, platform_key)
        if tgju_data and tgju_data.get("platform_index") and fees:
            index = tgju_data["platform_index"]
            # tgju index prices can be very different from per-gram. Use Navasan as anchor.
            # If we got a tgju index, use fee calc with Navasan anyway for consistency.
            # The tgju data confirms the platform is alive.
            state.platform_status[platform_key] = True
            # Fall through to fee-based

    # Layer 3: Fee-based from Navasan
    if navasan_18 and fees:
        result = calc_fee_based_prices(navasan_18, platform_key)
        state.platform_status[platform_key] = True
        return result

    # Layer 4: Stale cache
    for prefix in ["pb:", "tgju:"]:
        cached = state.price_cache.get(f"{prefix}{platform_key}")
        if cached and cached.get("data"):
            state.platform_status[platform_key] = True
            return cached["data"]

    state.platform_status[platform_key] = False
    return None

async def collect_all_prices(
    session: aiohttp.ClientSession,
    navasan_18: int,
) -> Dict[str, dict]:
    tasks = [get_platform_price(session, k, navasan_18) for k in TRACKED_PLATFORMS]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    prices = {}
    for key, result in zip(TRACKED_PLATFORMS, results):
        if isinstance(result, Exception):
            logger.error(f"[{key}] exception: {result}")
            state.platform_status[key] = False
        elif result:
            prices[key] = result
    return prices

# ---------------------------------------------------------------------------
# ARBITRAGE ENGINE
# ---------------------------------------------------------------------------

@dataclass
class ArbitrageOpportunity:
    buy_platform: str
    sell_platform: str
    buy_price: int
    sell_price: int
    spread_pct: float
    profit_per_gram: int

def detect_arbitrage(platform_prices: Dict[str, dict]) -> List[ArbitrageOpportunity]:
    opportunities = []
    if len(platform_prices) < 2:
        return opportunities

    items = list(platform_prices.items())
    for i in range(len(items)):
        for j in range(len(items)):
            if i == j:
                continue
            key_a, data_a = items[i]
            key_b, data_b = items[j]

            buy_from_a = data_a.get("sell", 0)  # platform sells to us
            sell_to_b = data_b.get("buy", 0)    # platform buys from us

            if buy_from_a <= 0 or sell_to_b <= 0 or buy_from_a >= sell_to_b:
                continue

            spread = (sell_to_b - buy_from_a) / buy_from_a * 100
            if spread >= GOLD_SPREAD_ALERT:
                opportunities.append(ArbitrageOpportunity(
                    buy_platform=key_a, sell_platform=key_b,
                    buy_price=buy_from_a, sell_price=sell_to_b,
                    spread_pct=round(spread, 2),
                    profit_per_gram=sell_to_b - buy_from_a
                ))
    opportunities.sort(key=lambda x: x.spread_pct, reverse=True)
    return opportunities

# ---------------------------------------------------------------------------
# COIN BUBBLE
# ---------------------------------------------------------------------------

def calculate_coin_bubble(navasan: dict) -> dict:
    gram_18 = navasan.get("18ayar", 0)
    if not gram_18:
        return {}
    bubbles = {}
    for coin_key, weight in COIN_WEIGHTS.items():
        market_price = navasan.get(coin_key, 0)
        if not market_price:
            continue
        intrinsic = gram_18 * weight
        if intrinsic > 0:
            bubble_pct = (market_price - intrinsic) / intrinsic * 100
            bubbles[coin_key] = {
                "market_price": market_price,
                "intrinsic_value": round(intrinsic),
                "bubble_pct": round(bubble_pct, 2),
                "bubble_toman": market_price - round(intrinsic),
            }
    return bubbles

# ---------------------------------------------------------------------------
# NAVASAN COMPARISON
# ---------------------------------------------------------------------------

def compare_with_navasan(platform_prices: Dict[str, dict], navasan: dict) -> dict:
    navasan_18 = navasan.get("18ayar", 0)
    if not navasan_18:
        return {}
    comparisons = {}
    for key, data in platform_prices.items():
        buy = data.get("buy", 0)
        sell = data.get("sell", 0)
        avg = (buy + sell) / 2 if buy and sell else 0
        if avg:
            diff = (avg - navasan_18) / navasan_18 * 100
            comparisons[key] = {
                "navasan_18": navasan_18,
                "platform_avg": round(avg),
                "diff_pct": round(diff, 2),
                "cheaper": diff < 0,
            }
    return comparisons

# ---------------------------------------------------------------------------
# ALERT MANAGER
# ---------------------------------------------------------------------------

async def send_alert(bot_app, chat_id: str, message: str, alert_key: str, cooldown: int = 600):
    now = time.time()
    if now - state.last_alerts.get(alert_key, 0) < cooldown:
        return
    state.last_alerts[alert_key] = now
    try:
        await bot_app.send_message(chat_id=chat_id, text=message, parse_mode="HTML")
    except Exception as e:
        logger.error(f"Alert send failed: {e}")

# ---------------------------------------------------------------------------
# SMART SUMMARY
# ---------------------------------------------------------------------------

async def send_smart_summary(bot_app, chat_id, platform_prices, navasan, bubbles):
    now = time.time()
    if now - state.last_summary_time < GOLD_SUMMARY_INTERVAL:
        return
    state.last_summary_time = now

    navasan_18 = navasan.get("18ayar", 0)
    xau = navasan.get("usd_xau", 0)

    lines = ["🟡 <b>خلاصه بازار طلا</b>", f"📅 {datetime.now(timezone.utc).strftime('%H:%M UTC')}", ""]
    if navasan_18:
        lines.append(f"💰 طلای ۱۸ عیار (Navasan): <b>{navasan_18:,} تومان</b>")
    if xau:
        lines.append(f"🌍 انس جهانی: <b>${xau:,}</b>")
    lines.append("")
    lines.append("<b>پلتفرم‌ها:</b>")

    order = ["milligold", "talasea", "goldika", "wallgold", "melligold", "daric"]
    for key in order:
        data = platform_prices.get(key)
        if not data:
            continue
        name = PLATFORM_NAMES_FA.get(key, key)
        icon = "🟢" if state.platform_status.get(key, True) else "🔴"
        src = data.get("source", "")
        tag = " [p]" if src == "parsebot" else " [t]" if src == "tgju" else " [f]" if src == "fee_calc" else ""
        lines.append(f"{icon} {name}: خرید {data['buy']:,} | فروش {data['sell']:,}{tag}")

    if bubbles:
        lines.append("")
        lines.append("<b>حباب سکه:</b>")
        bnames = {"sekkeh": "سکه امامی", "nim": "نیم", "rob": "ربع", "gerami": "گرمی"}
        for key, b in bubbles.items():
            bname = bnames.get(key, key)
            icon = "🔴" if abs(b["bubble_pct"]) > GOLD_BUBBLE_ALERT else "🟢"
            lines.append(f"{icon} {bname}: {b['bubble_pct']:+.1f}% ({b['bubble_toman']:,} تومان)")

    try:
        await bot_app.send_message(chat_id=chat_id, text="\n".join(lines), parse_mode="HTML")
    except Exception as e:
        logger.error(f"Summary send failed: {e}")

# ---------------------------------------------------------------------------
# DAILY REPORT
# ---------------------------------------------------------------------------

async def send_daily_report(bot_app, chat_id, platform_prices, navasan):
    now = datetime.now(timezone.utc)
    today = now.strftime("%Y-%m-%d")
    if now.hour != GOLD_DAILY_REPORT_HOUR or state.last_daily_report == today:
        return
    state.last_daily_report = today

    navasan_18 = navasan.get("18ayar", 0)
    lines = [
        "🟡 <b>گزارش روزانه طلا</b>",
        f"📅 {today}",
        f"💰 Navasan ۱۸ عیار: <b>{navasan_18:,} تومان</b>" if navasan_18 else "",
        "", "<b>خلاصه پلتفرم‌ها:</b>"
    ]
    for key, data in sorted(platform_prices.items()):
        name = PLATFORM_NAMES_FA.get(key, key)
        avg = (data["buy"] + data["sell"]) // 2
        diff = ((avg - navasan_18) / navasan_18 * 100) if navasan_18 else 0
        lines.append(f"• {name}: میانگین {avg:,} ({diff:+.2f}% vs Navasan)")
    lines.extend(["", "<b>وضعیت اتصال:</b>"])
    for key, online in state.platform_status.items():
        if online is not None:
            name = PLATFORM_NAMES_FA.get(key, key)
            lines.append(f"{'🟢' if online else '🔴'} {name}")

    try:
        await bot_app.send_message(chat_id=chat_id, text="\n".join(lines), parse_mode="HTML")
    except Exception as e:
        logger.error(f"Daily report send failed: {e}")

# ---------------------------------------------------------------------------
# /gold COMMAND
# ---------------------------------------------------------------------------

async def handle_gold_command(bot_app, message):
    chat_id = str(message.chat.id)
    conn = aiohttp.TCPConnector(limit=10)
    async with aiohttp.ClientSession(connector=conn) as session:
        navasan = await fetch_navasan(session)
        navasan_18 = navasan.get("18ayar", 0) if navasan else 0
        prices = await collect_all_prices(session, navasan_18)

    bubbles = calculate_coin_bubble(navasan) if navasan else {}
    comparisons = compare_with_navasan(prices, navasan) if navasan else {}
    opportunities = detect_arbitrage(prices)

    lines = ["🟡 <b>وضعیت کامل طلا</b>", ""]

    if navasan:
        lines.append("📊 <b>Navasan (مرجع رسمی):</b>")
        lines.append(f"• طلای ۱۸ عیار: <b>{navasan_18:,} تومان</b>")
        for k, label in [("sekkeh", "سکه امامی"), ("nim", "نیم سکه"), ("rob", "ربع سکه"), ("gerami", "سکه گرمی")]:
            if navasan.get(k):
                lines.append(f"• {label}: <b>{navasan[k]:,} تومان</b>")
        if navasan.get("usd_xau"):
            lines.append(f"• انس جهانی: <b>${navasan['usd_xau']:,}</b>")
    else:
        lines.append("⚠️ Navasan در دسترس نیست")

    lines.extend(["", "🏪 <b>پلتفرم‌ها:</b>"])
    if prices:
        order = ["milligold", "talasea", "goldika", "wallgold", "melligold", "daric"]
        for key in order:
            data = prices.get(key)
            if not data:
                continue
            name = PLATFORM_NAMES_FA.get(key, key)
            icon = "🟢" if state.platform_status.get(key, True) else "🔴"
            lines.append(f"{icon} {name}: خرید {data['buy']:,} | فروش {data['sell']:,}")
            if key in comparisons:
                c = comparisons[key]
                lines.append(f"   {'🔻' if c['cheaper'] else '🔺'} {c['diff_pct']:+.2f}% نسبت به Navasan")
    else:
        lines.append("⚠️ هیچ پلتفرمی در دسترس نیست")

    lines.append("")
    if opportunities:
        lines.append("💹 <b>فرصت‌های آربیتراژ:</b>")
        for opp in opportunities[:5]:
            na = PLATFORM_NAMES_FA.get(opp.buy_platform, opp.buy_platform)
            nb = PLATFORM_NAMES_FA.get(opp.sell_platform, opp.sell_platform)
            lines.append(f"• خرید از {na} → فروش به {nb}: <b>+{opp.spread_pct}%</b> ({opp.profit_per_gram:,} تومان/گرم)")
    else:
        lines.append("💹 <b>فرصت آربیتراژ:</b> ندارد")

    if bubbles:
        lines.extend(["", "🫧 <b>حباب سکه:</b>"])
        bnames = {"sekkeh": "امامی", "nim": "نیم", "rob": "ربع", "gerami": "گرمی"}
        for key, b in bubbles.items():
            icon = "🔴" if abs(b["bubble_pct"]) > GOLD_BUBBLE_ALERT else "🟢"
            lines.append(f"{icon} {bnames.get(key, key)}: {b['bubble_pct']:+.1f}% (بازار: {b['market_price']:,} | ذاتی: {b['intrinsic_value']:,})")

    msg = "\n".join(lines)
    try:
        await bot_app.send_message(chat_id=chat_id, text=msg, parse_mode="HTML")
    except Exception as e:
        logger.error(f"/gold send failed: {e}")

# ---------------------------------------------------------------------------
# /gold_alert
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
            await bot_app.send_message(chat_id=chat_id,
                text=f"✅ آستانه اسپرد طلا: {old}% → <b>{new}%</b>", parse_mode="HTML")
        except ValueError:
            await bot_app.send_message(chat_id=chat_id, text="⚠️ عدد معتبر وارد کنید. مثال: /gold_alert 1.5")
    else:
        await bot_app.send_message(chat_id=chat_id,
            text=f"⚙️ آستانه فعلی: <b>{GOLD_SPREAD_ALERT}%</b>\nبرای تغییر: /gold_alert عدد", parse_mode="HTML")

# ---------------------------------------------------------------------------
# /gold_help
# ---------------------------------------------------------------------------

async def handle_gold_help_command(bot_app, message):
    await bot_app.send_message(chat_id=str(message.chat.id),
        text=f"🟡 <b>راهنمای ربات طلا</b>\n\n"
             "<b>دستورات:</b>\n"
             "/gold — وضعیت کامل بازار طلا\n"
             "/gold_alert [عدد] — تنظیم آستانه اسپرد (پیش‌فرض: ۱٪)\n"
             "/gold_help — این راهنما\n\n"
             "<b>منابع داده:</b>\n"
             "• [p] = parse.bot scrape مستقیم (دقیق)\n"
             "• [t] = tgju profile scrape (تایید آنلاین بودن)\n"
             "• [f] = محاسبه با fee از Navasan\n\n"
             "<b>پلتفرم‌های فعال:</b>\n"
             "🥇 میلی‌گلد | 🥈 طلاسی | 🥉 گلدیکا\n"
             "🟡 وال‌گلد | ملی‌گلد | داریک\n\n"
             f"<b>هشدارهای خودکار:</b>\n"
             f"• اسپرد بین پلتفرم‌ها &gt; {GOLD_SPREAD_ALERT}%\n"
             f"• حباب سکه &gt; {GOLD_BUBBLE_ALERT}%\n"
             f"• اختلاف با Navasan &gt; {GOLD_NAVASAN_ALERT}%\n",
        parse_mode="HTML")

# ---------------------------------------------------------------------------
# MAIN LOOP
# ---------------------------------------------------------------------------

async def gold_monitor_loop(bot_app, chat_id: str):
    logger.info("Gold monitor started")
    conn = aiohttp.TCPConnector(limit=10)
    async with aiohttp.ClientSession(connector=conn) as session:
        while state.running:
            try:
                navasan = await fetch_navasan(session)
                navasan_18 = navasan.get("18ayar", 0) if navasan else 0
                prices = await collect_all_prices(session, navasan_18)
                bubbles = calculate_coin_bubble(navasan) if navasan else {}

                # Arbitrage alerts
                for opp in detect_arbitrage(prices)[:3]:
                    na = PLATFORM_NAMES_FA.get(opp.buy_platform, opp.buy_platform)
                    nb = PLATFORM_NAMES_FA.get(opp.sell_platform, opp.sell_platform)
                    await send_alert(bot_app, chat_id,
                        f"💹 <b>آربیتراژ طلا!</b>\nخرید از {na} → فروش به {nb}\n"
                        f"اسپرد: <b>{opp.spread_pct}%</b>\nسود: {opp.profit_per_gram:,} تومان/گرم",
                        f"arb:{opp.buy_platform}:{opp.sell_platform}", cooldown=600)

                # Navasan divergence
                if navasan:
                    for key, c in compare_with_navasan(prices, navasan).items():
                        if abs(c["diff_pct"]) >= GOLD_NAVASAN_ALERT:
                            name = PLATFORM_NAMES_FA.get(key, key)
                            dir_str = "ارزون‌تر" if c["cheaper"] else "گرون‌تر"
                            await send_alert(bot_app, chat_id,
                                f"⚠️ {name} {c['diff_pct']:+.2f}% {dir_str} از Navasan",
                                f"navasan:{key}", cooldown=1800)

                # Bubble alerts
                bnames = {"sekkeh": "سکه امامی", "nim": "نیم سکه", "rob": "ربع سکه", "gerami": "سکه گرمی"}
                for key, b in bubbles.items():
                    if abs(b["bubble_pct"]) >= GOLD_BUBBLE_ALERT:
                        await send_alert(bot_app, chat_id,
                            f"🫧 <b>حباب {bnames.get(key, key)}:</b> {b['bubble_pct']:+.1f}%\n"
                            f"بازار: {b['market_price']:,} | ذاتی: {b['intrinsic_value']:,}",
                            f"bubble:{key}", cooldown=3600)

                # Status change alerts
                for key, online in list(state.platform_status.items()):
                    prev_key = f"status:{key}:prev"
                    prev = state.last_alerts.get(prev_key)
                    if prev is not None and prev != online:
                        name = PLATFORM_NAMES_FA.get(key, key)
                        if not online:
                            await send_alert(bot_app, chat_id, f"🔴 {name} از دسترس خارج شد",
                                f"status:{key}:offline", cooldown=3600)
                        else:
                            await send_alert(bot_app, chat_id, f"🟢 {name} آنلاین شد",
                                f"status:{key}:online", cooldown=0)
                    state.last_alerts[f"status:{key}:prev"] = online

                await send_smart_summary(bot_app, chat_id, prices, navasan or {}, bubbles)
                await send_daily_report(bot_app, chat_id, prices, navasan or {})

            except Exception as e:
                logger.error(f"Monitor loop error: {e}", exc_info=True)

            await asyncio.sleep(GOLD_POLL_SECONDS)

# ---------------------------------------------------------------------------
# HTTP HEALTH
# ---------------------------------------------------------------------------

def create_health_app():
    from aiohttp import web
    async def health(request):
        return web.json_response({
            "status": "ok", "service": "gold-arbitrage",
            "platforms_online": sum(1 for v in state.platform_status.values() if v),
            "platforms_total": len(state.platform_status),
        })
    app = web.Application()
    app.router.add_get("/", health)
    app.router.add_get("/health", health)
    return app

# ---------------------------------------------------------------------------
# LAUNCH
# ---------------------------------------------------------------------------

async def start_gold_service(bot_app, chat_id: str):
    asyncio.create_task(gold_monitor_loop(bot_app, chat_id))
    logger.info("Gold service started")