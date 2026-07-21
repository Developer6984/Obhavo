#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
=================================================================================
 🌈 KO'P FUNKSIYALI TELEGRAM OB-HAVO BOTI — v2.0 (PREMIUM UI)
 O'zbekiston va dunyo hududlari uchun mo'ljallangan
=================================================================================

MUHIM: Bu versiya Render.com'da chiqqan quyidagi xatoliklarni TUZATISH uchun
qayta yozilgan:
    - "RuntimeError: There is no current event loop in thread 'MainThread'"
    - "RuntimeWarning: coroutine 'Updater.start_webhook' was never awaited"

Sabab: eski kod tashqi `apscheduler.AsyncIOScheduler`ni QO'LDA event loop bilan
ishga tushirayotgan edi va bu ba'zi hosting muhitlarida (Render kabi)
asyncio loop bilan to'qnashib, botni "Exited with status 1" holatiga olib
kelardi. YECHIM: bu versiyada tashqi scheduler UMUMAN ishlatilmaydi — buning
o'rniga python-telegram-bot'ning O'ZINING ichki, xavfsiz `JobQueue`
mexanizmidan foydalaniladi (application.job_queue.run_daily). Bundan tashqari,
webhook bilan bog'liq HECH QANDAY kod yo'q — faqat sof `run_polling()`.

Arxitektura (servis-klasslar):
    WeatherService, SafetyAdvisor, ClothingAdvisor, CurrencyService,
    QuoteFactService, TranslatorService, TodoManager, NewsService,
    MovieService, BookService, SportsService, PlaceFinderService,
    TravelService, TrafficService (stub)

    NAMOZ VAQTLARI integratsiyasi hali ham FAQAT KOMMENTARIYA sifatida
    saqlangan (PRAYER_TIMES_TODO bo'limiga qarang) — talabga ko'ra hozircha
    ishga tushirilmaydi.

Ishga tushirish:
    pip install -r requirements.txt
    python main.py
=================================================================================
"""

from __future__ import annotations

import os
import sys
import json
import sqlite3
import logging
import random
import asyncio
import functools
from datetime import datetime, time as dtime
from typing import Optional, List, Dict, Any, Tuple

# =================================================================================
# 0. PYTHON VERSIYASINI TEKSHIRISH
# =================================================================================
# python-telegram-bot==21.4 hozircha Python 3.13+ (jumladan 3.14) bilan TO'LIQ mos
# emas — ba'zi hosting platformalari (masalan Render) yangi chiqqan Python
# versiyasini avtomatik tanlab qo'yishi mumkin, bu esa quyidagi xatolikka olib keladi:
#   "RuntimeError: There is no current event loop in thread 'MainThread'"
# Buning oldini olish uchun repo ildizida `.python-version` va `runtime.txt`
# fayllari (python-3.12.7) qo'shilgan. Bu tekshiruv esa muammoni ILDIZIDA,
# tushunarli xabar bilan darhol ko'rsatib beradi.
if sys.version_info >= (3, 13):
    print(
        "\n"
        "❌ XATOLIK: Siz Python {}.{}.{} da ishlayapsiz.\n"
        "   python-telegram-bot==21.4 kutubxonasi Python 3.13+ bilan barqaror ishlamaydi\n"
        "   (asyncio ichki o'zgarishlari sabab 'no current event loop' xatoligi chiqadi).\n\n"
        "   YECHIM (Render.com uchun):\n"
        "   1) Repo ildizida '.python-version' fayli 3.12.7 ni ko'rsatishiga ishonch hosil qiling.\n"
        "   2) Render Dashboard -> Environment -> PYTHON_VERSION=3.12.7 qo'shing.\n"
        "   3) 'Clear build cache & deploy' tugmasini bosing.\n".format(*sys.version_info[:3])
    )
    sys.exit(1)

import requests
import pytz
from dotenv import load_dotenv

try:
    from deep_translator import GoogleTranslator
except ImportError:
    GoogleTranslator = None

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardMarkup,
)
from telegram.constants import ParseMode
from telegram.error import TelegramError
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
)

# =================================================================================
# 1. SOZLAMALAR VA MUHIT O'ZGARUVCHILARI
# =================================================================================

load_dotenv()

BOT_TOKEN: Optional[str] = os.getenv("TELEGRAM_BOT_TOKEN")
OWM_API_KEY: Optional[str] = os.getenv("OPENWEATHERMAP_API_KEY")
NEWSAPI_KEY: str = os.getenv("NEWSAPI_KEY", "")
OMDB_API_KEY: str = os.getenv("OMDB_API_KEY", "")
GOOGLE_BOOKS_API_KEY: str = os.getenv("GOOGLE_BOOKS_API_KEY", "")
THESPORTSDB_API_KEY: str = os.getenv("THESPORTSDB_API_KEY", "3")
TIMEZONE_NAME: str = os.getenv("TIMEZONE", "Asia/Tashkent")
DB_PATH: str = os.getenv("DB_PATH", "bot_database.db")
DAILY_BROADCAST_HOUR: int = int(os.getenv("DAILY_BROADCAST_HOUR", "8"))
DAILY_BROADCAST_MINUTE: int = int(os.getenv("DAILY_BROADCAST_MINUTE", "0"))

if not BOT_TOKEN:
    raise RuntimeError("❌ TELEGRAM_BOT_TOKEN topilmadi. .env faylini tekshiring.")
if not OWM_API_KEY:
    raise RuntimeError("❌ OPENWEATHERMAP_API_KEY topilmadi. .env faylini tekshiring.")

TZ = pytz.timezone(TIMEZONE_NAME)

# =================================================================================
# 2. LOGGING
# =================================================================================

logging.basicConfig(
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    level=logging.INFO,
    handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler("bot.log", encoding="utf-8")],
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger("weather_bot")

DEFAULT_TIMEOUT = 10


# =================================================================================
# 3. YORDAMCHI FUNKSIYALAR (HTTP + DB)
# =================================================================================

async def fetch_json(
    url: str,
    params: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    timeout: int = DEFAULT_TIMEOUT,
) -> Optional[Dict[str, Any]]:
    """Tashqi API'larga bloklanmaydigan (thread-executor) GET so'rov."""
    loop = asyncio.get_running_loop()
    try:
        func = functools.partial(requests.get, url, params=params, headers=headers, timeout=timeout)
        response = await loop.run_in_executor(None, func)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.Timeout:
        logger.warning("⏱ Timeout: %s", url)
    except requests.exceptions.HTTPError as e:
        logger.warning("🚫 HTTP xatolik: %s -> %s", url, e)
    except requests.exceptions.RequestException as e:
        logger.warning("🌐 Tarmoq xatoligi: %s -> %s", url, e)
    except (ValueError, json.JSONDecodeError):
        logger.warning("📄 JSON parslash xatoligi: %s", url)
    return None


def init_database() -> None:
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        cur.execute(
            """CREATE TABLE IF NOT EXISTS users (
                chat_id INTEGER PRIMARY KEY, username TEXT, city TEXT,
                lat REAL, lon REAL, lang TEXT DEFAULT 'uz',
                subscribed_daily INTEGER DEFAULT 0, created_at TEXT)"""
        )
        cur.execute(
            """CREATE TABLE IF NOT EXISTS todos (
                id INTEGER PRIMARY KEY AUTOINCREMENT, chat_id INTEGER NOT NULL,
                task TEXT NOT NULL, is_done INTEGER DEFAULT 0, created_at TEXT)"""
        )
        conn.commit()
    logger.info("✅ Ma'lumotlar bazasi tayyor: %s", DB_PATH)


def _db_execute(query: str, params: Tuple = (), fetch: bool = False) -> Any:
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute(query, params)
        if fetch:
            return [dict(r) for r in cur.fetchall()]
        conn.commit()
        return cur.lastrowid


async def db_execute(query: str, params: Tuple = (), fetch: bool = False) -> Any:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, functools.partial(_db_execute, query, params, fetch))


async def upsert_user(chat_id: int, username: Optional[str] = None) -> None:
    existing = await db_execute("SELECT chat_id FROM users WHERE chat_id = ?", (chat_id,), fetch=True)
    if not existing:
        await db_execute(
            "INSERT INTO users (chat_id, username, created_at) VALUES (?, ?, ?)",
            (chat_id, username or "", datetime.now(TZ).isoformat()),
        )


async def set_user_city(chat_id: int, city: str) -> None:
    await db_execute("UPDATE users SET city = ?, lat = NULL, lon = NULL WHERE chat_id = ?", (city, chat_id))


async def set_user_location(chat_id: int, lat: float, lon: float) -> None:
    await db_execute("UPDATE users SET lat = ?, lon = ?, city = NULL WHERE chat_id = ?", (lat, lon, chat_id))


async def get_user(chat_id: int) -> Optional[Dict[str, Any]]:
    rows = await db_execute("SELECT * FROM users WHERE chat_id = ?", (chat_id,), fetch=True)
    return rows[0] if rows else None


async def get_all_subscribed_users() -> List[Dict[str, Any]]:
    return await db_execute("SELECT * FROM users WHERE subscribed_daily = 1", fetch=True)


# =================================================================================
# 4. OB-HAVO XIZMATI (real vaqtdagi harorat, OpenWeatherMap)
# =================================================================================

class WeatherService:
    BASE_URL = "https://api.openweathermap.org/data/2.5/weather"

    def __init__(self, api_key: str):
        self.api_key = api_key

    async def get_by_city(self, city: str) -> Optional[Dict[str, Any]]:
        params = {"q": city, "appid": self.api_key, "units": "metric", "lang": "uz"}
        return self._parse(await fetch_json(self.BASE_URL, params=params))

    async def get_by_coords(self, lat: float, lon: float) -> Optional[Dict[str, Any]]:
        params = {"lat": lat, "lon": lon, "appid": self.api_key, "units": "metric", "lang": "uz"}
        return self._parse(await fetch_json(self.BASE_URL, params=params))

    @staticmethod
    def _parse(data: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        if not data or data.get("cod") not in (200, "200"):
            return None
        try:
            wb = data["weather"][0]
            return {
                "city": data.get("name", "Noma'lum"),
                "country": data.get("sys", {}).get("country", ""),
                "temp": round(data["main"]["temp"]),
                "feels_like": round(data["main"]["feels_like"]),
                "temp_min": round(data["main"]["temp_min"]),
                "temp_max": round(data["main"]["temp_max"]),
                "humidity": data["main"]["humidity"],
                "pressure": data["main"]["pressure"],
                "wind_speed": data.get("wind", {}).get("speed", 0),
                "visibility_m": data.get("visibility", 10000),
                "description": wb.get("description", "").capitalize(),
                "main_condition": wb.get("main", ""),
                "observed_at": datetime.now(TZ).strftime("%Y-%m-%d %H:%M"),
            }
        except (KeyError, IndexError, TypeError) as e:
            logger.error("Ob-havo parslash xatoligi: %s", e)
            return None

    @staticmethod
    def format_message(w: Dict[str, Any]) -> str:
        icon_map = {
            "Clear": "☀️", "Clouds": "☁️", "Rain": "🌧", "Drizzle": "🌦",
            "Thunderstorm": "⛈", "Snow": "❄️", "Mist": "🌫", "Fog": "🌫", "Haze": "🌫",
        }
        emoji = icon_map.get(w["main_condition"], "🌡")
        return (
            f"{emoji} <b>{w['city']}, {w['country']}</b>\n"
            f"🕒 <i>{w['observed_at']}</i>\n"
            f"━━━━━━━━━━━━━━━\n"
            f"🌡 Harorat: <b>{w['temp']}°C</b>  (sezilishi: {w['feels_like']}°C)\n"
            f"📉 Min/Maks: {w['temp_min']}°C / {w['temp_max']}°C\n"
            f"📝 Holat: {w['description']}\n"
            f"💧 Namlik: {w['humidity']}%\n"
            f"🧭 Bosim: {w['pressure']} hPa\n"
            f"💨 Shamol: {w['wind_speed']} m/s\n"
            f"👁 Ko'rinish: {w['visibility_m']/1000:.1f} km"
        )


weather_service = WeatherService(OWM_API_KEY)


# =================================================================================
# 5. XAVFSIZLIK OGOHLANTIRISHLARI
# =================================================================================

class SafetyAdvisor:
    EXTREME_HEAT_C = 38
    HIGH_HEAT_C = 33
    EXTREME_COLD_C = -15
    COLD_C = -5
    STRONG_WIND_MS = 14
    MODERATE_WIND_MS = 8
    LOW_VISIBILITY_M = 1000

    @classmethod
    def analyze(cls, w: Dict[str, Any]) -> List[str]:
        warnings: List[str] = []
        temp, feels = w["temp"], w["feels_like"]
        condition, wind = w["main_condition"], w["wind_speed"]
        visibility, humidity = w["visibility_m"], w["humidity"]

        if temp >= cls.EXTREME_HEAT_C or feels >= cls.EXTREME_HEAT_C:
            warnings.append(
                "🔥 <b>OGOHLANTIRISH: Haddan tashqari issiqlik!</b>\n"
                "Soyada/xonada bo'ling, kamida 2-3 litr suv iching, 12:00-17:00 orasida "
                "ochiq quyoshda yurmang, bosh kiyim taqing."
            )
        elif temp >= cls.HIGH_HEAT_C or feels >= cls.HIGH_HEAT_C:
            warnings.append("🌡 <b>Diqqat:</b> havo juda issiq — ko'proq suv iching, faollikni cheklang.")

        if temp >= cls.HIGH_HEAT_C and humidity >= 60:
            warnings.append("💦 Yuqori namlik issiqlikni kuchaytiradi — tez-tez salqin joyda dam oling.")

        if temp <= cls.EXTREME_COLD_C or feels <= cls.EXTREME_COLD_C:
            warnings.append(
                "🥶 <b>OGOHLANTIRISH: Qattiq sovuq!</b>\n"
                "Terining ochiq qolishidan saqlaning, qatlamli issiq kiyim kiying, uzoq "
                "tashqarida qolmang."
            )
        elif temp <= cls.COLD_C or feels <= cls.COLD_C:
            warnings.append("❄️ <b>Diqqat:</b> havo sovuq — issiqroq kiyining, qo'lqop/sharf taqing.")

        if condition == "Thunderstorm":
            warnings.append(
                "⛈ <b>OGOHLANTIRISH: Momaqaldiroq!</b>\nOchiq maydon, baland daraxt va suv "
                "havzalari yaqinida qolmang."
            )
        elif condition in ("Rain", "Drizzle"):
            warnings.append("🌧 Yomg'ir yog'moqda — soyabon oling, yo'llar sirpanchiq bo'lishi mumkin.")

        if condition == "Snow":
            warnings.append("🌨 Qor yog'moqda — yo'llar muzlashi mumkin, ehtiyot bo'ling.")

        if wind >= cls.STRONG_WIND_MS:
            warnings.append("💨 <b>OGOHLANTIRISH: Kuchli shamol!</b>\nBaland inshoot va daraxtlardan uzoqroq yuring.")
        elif wind >= cls.MODERATE_WIND_MS:
            warnings.append("🍃 O'rtacha shamol — ochiq soyabonlardan ehtiyot bo'ling.")

        if visibility <= cls.LOW_VISIBILITY_M:
            warnings.append("🌫 Ko'rinish past (tuman) — chiroqlarni yoqing, tezlikni kamaytiring.")

        if not warnings:
            warnings.append("✅ Hozircha maxsus xavf yo'q. Ajoyib kun tilaymiz! 🌈")

        return warnings


# =================================================================================
# 6. KIYIM TAVSIYASI
# =================================================================================

class ClothingAdvisor:
    @staticmethod
    def suggest(w: Dict[str, Any]) -> str:
        temp, condition, wind = w["feels_like"], w["main_condition"], w["wind_speed"]

        if temp >= 32:
            base = "👕 Yengil, ochiq rangli kiyim, 🧢 shlyapa, 🕶 quyoshdan ko'zoynak."
        elif temp >= 24:
            base = "👚 Yengil футболка/ko'ylak, qulay poyabzal."
        elif temp >= 16:
            base = "🧥 Yengil куртка yoki kardigan."
        elif temp >= 8:
            base = "🧥 Issiqroq куртка, sviter, yopiq poyabzal."
        elif temp >= 0:
            base = "🧣 Qishki palto, sharf, qalin sviter."
        else:
            base = "🥶 Puxovik, qalin qo'lqop, shapka, termokiyim."

        extra = []
        if condition in ("Rain", "Drizzle", "Thunderstorm"):
            extra.append("☂️ soyabon")
        if condition == "Snow":
            extra.append("👢 sirpanmaydigan qishki poyabzal")
        if wind >= 8:
            extra.append("🧢 shamolbardosh kiyim")
        if extra:
            base += "\n➕ Qo'shimcha: " + ", ".join(extra) + "."
        return base


# =================================================================================
# 7. VALYUTA KURSLARI
# =================================================================================

class CurrencyService:
    BASE_URL = "https://open.er-api.com/v6/latest/USD"
    TARGET = ["UZS", "EUR", "RUB", "GBP", "KZT"]

    async def get_rates(self) -> Optional[Dict[str, float]]:
        data = await fetch_json(self.BASE_URL)
        if not data or data.get("result") != "success":
            return None
        rates = data.get("rates", {})
        return {c: rates[c] for c in self.TARGET if c in rates}

    @staticmethod
    def format_message(rates: Dict[str, float]) -> str:
        lines = ["💱 <b>Valyuta kurslari</b> (1 USD asosida)\n━━━━━━━━━━━━━━━"]
        flags = {"UZS": "🇺🇿", "EUR": "🇪🇺", "RUB": "🇷🇺", "GBP": "🇬🇧", "KZT": "🇰🇿"}
        for cur, val in rates.items():
            if cur == "UZS":
                lines.append(f"{flags[cur]} 1 USD = <b>{val:,.0f}</b> so'm")
            else:
                lines.append(f"{flags.get(cur,'')} 1 USD = {val:.3f} {cur}")
        return "\n".join(lines)


currency_service = CurrencyService()


# =================================================================================
# 8. HIKMATLI SO'ZLAR / QIZIQARLI FAKTLAR
# =================================================================================

class QuoteFactService:
    QUOTE_API = "https://api.quotable.io/random"
    FACT_API = "https://uselessfacts.jsph.pl/api/v2/facts/random?language=en"

    FALLBACK_QUOTES = [
        "Bilim — kuchdir. 💪", "Har bir muvaffaqiyat orqasida ko'plab urinish yotadi. 🌟",
        "Bugungi mehnating ertangi natijang. 🌱", "Kichik qadamlar katta yo'lni bosib o'tadi. 👣",
        "Sabr — muvaffaqiyat kaliti. 🔑",
    ]
    FALLBACK_FACTS = [
        "Asal hech qachon buzilmaydi. 🍯", "Sakkizoyoqning uchta yuragi bor. 🐙",
        "Bir kun 23 soat 56 daqiqa davom etadi. 🌍", "Banan botanik jihatdan rezavordir. 🍌",
        "Muz suvdan yengilroq, shuning uchun suzadi. ❄️",
    ]

    async def get_quote(self) -> str:
        data = await fetch_json(self.QUOTE_API)
        if data and data.get("content"):
            return f'💬 "{data["content"]}"\n— {data.get("author","Noma\'lum")}'
        return f"💬 {random.choice(self.FALLBACK_QUOTES)}"

    async def get_fact(self) -> str:
        data = await fetch_json(self.FACT_API)
        if data and data.get("text"):
            return data["text"]
        return random.choice(self.FALLBACK_FACTS)


quote_fact_service = QuoteFactService()


# =================================================================================
# 9. TARJIMON
# =================================================================================

class TranslatorService:
    @staticmethod
    async def translate(text: str, target_lang: str) -> str:
        if GoogleTranslator is None:
            return "⚠️ Tarjimon kutubxonasi o'rnatilmagan."
        loop = asyncio.get_running_loop()
        try:
            func = functools.partial(GoogleTranslator(source="auto", target=target_lang).translate, text)
            result = await loop.run_in_executor(None, func)
            return result or "⚠️ Tarjima qilib bo'lmadi."
        except Exception as e:
            logger.warning("Tarjima xatoligi: %s", e)
            return "⚠️ Tarjima xatosi. Til kodini tekshiring (masalan: en, ru, uz)."


# =================================================================================
# 10. TODO / ESLATMALAR
# =================================================================================

class TodoManager:
    @staticmethod
    async def add_task(chat_id: int, task: str) -> int:
        return await db_execute(
            "INSERT INTO todos (chat_id, task, created_at) VALUES (?, ?, ?)",
            (chat_id, task, datetime.now(TZ).isoformat()),
        )

    @staticmethod
    async def list_tasks(chat_id: int) -> List[Dict[str, Any]]:
        return await db_execute(
            "SELECT * FROM todos WHERE chat_id = ? ORDER BY is_done ASC, id DESC", (chat_id,), fetch=True
        )

    @staticmethod
    async def mark_done(chat_id: int, task_id: int) -> None:
        await db_execute("UPDATE todos SET is_done = 1 WHERE chat_id = ? AND id = ?", (chat_id, task_id))

    @staticmethod
    async def delete_task(chat_id: int, task_id: int) -> None:
        await db_execute("DELETE FROM todos WHERE chat_id = ? AND id = ?", (chat_id, task_id))

    @staticmethod
    def format_list(tasks: List[Dict[str, Any]]) -> str:
        if not tasks:
            return "📝 Vazifalar yo'q. Qo'shish: <code>/todo_add matn</code>"
        lines = ["📝 <b>Vazifalaringiz:</b>\n━━━━━━━━━━━━━━━"]
        for t in tasks:
            mark = "✅" if t["is_done"] else "🔲"
            lines.append(f"{mark} <code>#{t['id']}</code> {t['task']}")
        lines.append("\n✔️ Bajarish: /todo_done id   🗑 O'chirish: /todo_del id")
        return "\n".join(lines)


# =================================================================================
# 11. YANGILIKLAR
# =================================================================================

class NewsService:
    BASE_URL = "https://newsapi.org/v2/everything"

    async def get_news(self, query: str = "Uzbekistan", limit: int = 5) -> Optional[List[Dict[str, str]]]:
        if not NEWSAPI_KEY:
            return None
        params = {"q": query, "language": "ru", "sortBy": "publishedAt", "pageSize": limit, "apiKey": NEWSAPI_KEY}
        data = await fetch_json(self.BASE_URL, params=params)
        if not data or data.get("status") != "ok":
            return None
        return [
            {"title": a.get("title", ""), "url": a.get("url", ""), "source": a.get("source", {}).get("name", "")}
            for a in data.get("articles", [])[:limit]
        ]

    @staticmethod
    def format_message(articles: List[Dict[str, str]]) -> str:
        if not articles:
            return "📰 Hozircha yangilik topilmadi."
        lines = ["📰 <b>So'nggi yangiliklar</b>\n━━━━━━━━━━━━━━━"]
        for i, a in enumerate(articles, 1):
            lines.append(f"{i}. <a href='{a['url']}'>{a['title']}</a>  <i>({a['source']})</i>")
        return "\n".join(lines)


news_service = NewsService()


# =================================================================================
# 12. KINO (OMDb)
# =================================================================================

class MovieService:
    BASE_URL = "https://www.omdbapi.com/"

    async def search(self, title: str) -> Optional[Dict[str, Any]]:
        if not OMDB_API_KEY:
            return None
        data = await fetch_json(self.BASE_URL, params={"t": title, "apikey": OMDB_API_KEY})
        if not data or data.get("Response") != "True":
            return None
        return data

    @staticmethod
    def format_message(m: Dict[str, Any]) -> str:
        return (
            f"🎬 <b>{m.get('Title')}</b> ({m.get('Year')})\n━━━━━━━━━━━━━━━\n"
            f"⭐ IMDB: {m.get('imdbRating','N/A')}\n🎭 Janr: {m.get('Genre','N/A')}\n"
            f"🎥 Rejissyor: {m.get('Director','N/A')}\n📝 {m.get('Plot','Mavjud emas')}"
        )


movie_service = MovieService()


# =================================================================================
# 13. KITOBLAR (Google Books)
# =================================================================================

class BookService:
    BASE_URL = "https://www.googleapis.com/books/v1/volumes"

    async def search_by_genre(self, genre: str, limit: int = 5) -> Optional[List[Dict[str, str]]]:
        params = {"q": f"subject:{genre}", "maxResults": limit}
        if GOOGLE_BOOKS_API_KEY:
            params["key"] = GOOGLE_BOOKS_API_KEY
        data = await fetch_json(self.BASE_URL, params=params)
        if not data or "items" not in data:
            return None
        out = []
        for item in data["items"][:limit]:
            info = item.get("volumeInfo", {})
            out.append(
                {
                    "title": info.get("title", "Noma'lum"),
                    "authors": ", ".join(info.get("authors", ["Noma'lum muallif"])),
                    "rating": info.get("averageRating", "—"),
                }
            )
        return out

    @staticmethod
    def format_message(books: List[Dict[str, str]], genre: str) -> str:
        if not books:
            return f"📚 '{genre}' bo'yicha kitob topilmadi."
        lines = [f"📚 <b>'{genre}' janridagi tavsiyalar</b>\n━━━━━━━━━━━━━━━"]
        for b in books:
            lines.append(f"• <b>{b['title']}</b> — {b['authors']} (⭐ {b['rating']})")
        return "\n".join(lines)


book_service = BookService()


# =================================================================================
# 14. SPORT (TheSportsDB)
# =================================================================================

class SportsService:
    LEAGUE_IDS = {"epl": "4328", "laliga": "4335", "uefa": "4480", "seriea": "4332"}

    def __init__(self, api_key: str):
        self.api_key = api_key

    async def get_last_results(self, league: str) -> Optional[List[Dict[str, str]]]:
        league_id = self.LEAGUE_IDS.get(league.lower())
        if not league_id:
            return None
        url = f"https://www.thesportsdb.com/api/v1/json/{self.api_key}/eventspastleague.php"
        data = await fetch_json(url, params={"id": league_id})
        if not data or not data.get("events"):
            return None
        return [
            {
                "home": e.get("strHomeTeam", ""), "away": e.get("strAwayTeam", ""),
                "home_score": e.get("intHomeScore") or "-", "away_score": e.get("intAwayScore") or "-",
                "date": e.get("dateEvent", ""),
            }
            for e in data["events"][:5]
        ]

    @staticmethod
    def format_message(results: List[Dict[str, str]], league: str) -> str:
        if not results:
            return f"⚽ '{league}' bo'yicha natija topilmadi."
        lines = [f"⚽ <b>So'nggi natijalar ({league.upper()})</b>\n━━━━━━━━━━━━━━━"]
        for r in results:
            lines.append(f"{r['date']}: {r['home']} <b>{r['home_score']}-{r['away_score']}</b> {r['away']}")
        return "\n".join(lines)


sports_service = SportsService(THESPORTSDB_API_KEY)


# =================================================================================
# 15. YAQIN ATROFDAGI DORIXONA / DO'KON (OpenStreetMap Overpass)
# =================================================================================

class PlaceFinderService:
    OVERPASS_URL = "https://overpass-api.de/api/interpreter"

    async def find_nearby(self, lat: float, lon: float, place_type: str = "pharmacy", radius_m: int = 1500):
        amenity_filter = (
            f'node["amenity"="pharmacy"](around:{radius_m},{lat},{lon});'
            if place_type == "pharmacy"
            else f'node["shop"="supermarket"](around:{radius_m},{lat},{lon});'
        )
        query = f"[out:json][timeout:10];({amenity_filter});out center 10;"
        loop = asyncio.get_running_loop()
        try:
            func = functools.partial(requests.get, self.OVERPASS_URL, params={"data": query}, timeout=15)
            response = await loop.run_in_executor(None, func)
            response.raise_for_status()
            data = response.json()
        except Exception as e:
            logger.warning("Overpass xatoligi: %s", e)
            return None
        elements = data.get("elements", [])[:10]
        return [
            {"name": el.get("tags", {}).get("name", "Nomsiz"), "lat": el.get("lat"), "lon": el.get("lon")}
            for el in elements
        ]

    @staticmethod
    def format_message(places, place_type: str) -> str:
        label = "dorixona" if place_type == "pharmacy" else "supermarket"
        if not places:
            return f"🔍 Yaqin atrofda {label} topilmadi."
        lines = [f"📍 <b>Yaqin atrofdagi {label}lar</b>\n━━━━━━━━━━━━━━━"]
        for p in places:
            link = f"https://www.google.com/maps?q={p['lat']},{p['lon']}"
            lines.append(f"• <a href='{link}'>{p['name']}</a>")
        return "\n".join(lines)


place_finder_service = PlaceFinderService()


# =================================================================================
# 16. SAYOHAT YO'NALISHLARI
# =================================================================================

class TravelService:
    DESTINATIONS = [
        {"name": "🇺🇿 Samarqand", "desc": "Registon maydoni va Amir Temur davri me'morchiligi."},
        {"name": "🇺🇿 Buxoro", "desc": "2000+ yillik tarix, Ark qal'asi, Poi-Kalon majmuasi."},
        {"name": "🇺🇿 Xiva", "desc": "Ichan-Qal'a — YuNESKO ro'yxatidagi ochiq osmon muzeyi."},
        {"name": "🇹🇷 Istanbul", "desc": "Ikki qit'ani bog'laydigan shahar, Ayasofya."},
        {"name": "🇦🇪 Dubay", "desc": "Zamonaviy me'morchilik, Burj Khalifa, cho'l safari."},
        {"name": "🇫🇷 Parij", "desc": "Eyfel minorasi, Luvr muzeyi, romantik muhit."},
        {"name": "🇮🇩 Bali", "desc": "Tropik plyajlar, guruch teraslari, sörf."},
        {"name": "🇮🇹 Rim", "desc": "Kolizey, Vatikan, boy antik tarix."},
    ]

    @classmethod
    def random_pick(cls, count: int = 3) -> List[Dict[str, str]]:
        return random.sample(cls.DESTINATIONS, min(count, len(cls.DESTINATIONS)))

    @staticmethod
    def format_message(destinations: List[Dict[str, str]]) -> str:
        lines = ["✈️ <b>Sayohat tavsiyalari</b>\n━━━━━━━━━━━━━━━"]
        for d in destinations:
            lines.append(f"\n📍 <b>{d['name']}</b>\n{d['desc']}")
        return "\n".join(lines)


travel_service = TravelService()


# =================================================================================
# 17. YO'L TIRBANDLIGI — STUB (kelajakda TomTom/Yandex API bilan almashtiriladi)
# =================================================================================

class TrafficService:
    async def get_traffic_info(self, city: str) -> str:
        # TODO: TomTom/Yandex/Google Traffic API integratsiyasi shu yerga qo'shiladi
        return (
            f"🚗 '{city}' uchun tirbandlik ma'lumoti hozircha mavjud emas.\n"
            "Bu funksiya tez orada qo'shiladi. 🔧"
        )


traffic_service = TrafficService()


# =================================================================================
# NAMOZ VAQTLARI — FAQAT REJA (hozircha ishga tushirilmagan) — PRAYER_TIMES_TODO
# =================================================================================
#
# class PrayerTimesService:
#     BASE_URL = "https://api.aladhan.com/v1/timingsByCity"
#
#     async def get_timings(self, city: str, country: str = "Uzbekistan", method: int = 2):
#         data = await fetch_json(self.BASE_URL, params={"city": city, "country": country, "method": method})
#         if not data or data.get("code") != 200:
#             return None
#         t = data["data"]["timings"]
#         return {
#             "🌅 Bomdod": t.get("Fajr"), "☀️ Quyosh": t.get("Sunrise"), "🕌 Peshin": t.get("Dhuhr"),
#             "🌇 Asr": t.get("Asr"), "🌆 Shom": t.get("Maghrib"), "🌙 Xufton": t.get("Isha"),
#         }
#
# Handler qo'shilganda /namoz komandasi bilan chaqiriladi. HOZIRCHA FAOLLASHTIRILMAGAN.
#
# =================================================================================


# =================================================================================
# 18. INLINE MENYU (rangli/emojili tugmalar)
# =================================================================================

def main_menu_keyboard() -> InlineKeyboardMarkup:
    buttons = [
        [InlineKeyboardButton("🌤 Ob-havo", callback_data="menu:weather"),
         InlineKeyboardButton("👕 Kiyim maslahati", callback_data="menu:clothing")],
        [InlineKeyboardButton("💱 Valyuta", callback_data="menu:currency"),
         InlineKeyboardButton("📰 Yangiliklar", callback_data="menu:news")],
        [InlineKeyboardButton("🎬 Kino", callback_data="menu:movie"),
         InlineKeyboardButton("📚 Kitoblar", callback_data="menu:book")],
        [InlineKeyboardButton("⚽️ Sport", callback_data="menu:sport"),
         InlineKeyboardButton("✈️ Sayohat", callback_data="menu:travel")],
        [InlineKeyboardButton("🏥 Dorixona", callback_data="menu:pharmacy"),
         InlineKeyboardButton("🛒 Do'kon", callback_data="menu:shop")],
        [InlineKeyboardButton("💡 Fakt/Iqtibos", callback_data="menu:quote"),
         InlineKeyboardButton("💬 Tarjimon", callback_data="menu:translate")],
        [InlineKeyboardButton("📝 Vazifalarim", callback_data="menu:todo"),
         InlineKeyboardButton("🔔 Obuna", callback_data="menu:subscribe")],
        [InlineKeyboardButton("ℹ️ Yordam", callback_data="menu:help")],
    ]
    return InlineKeyboardMarkup(buttons)


def back_button() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Orqaga", callback_data="menu:back")]])


def location_request_keyboard() -> ReplyKeyboardMarkup:
    """Faqat joylashuv so'rash uchun (Telegram inline tugma orqali lokatsiya so'ray olmaydi)."""
    return ReplyKeyboardMarkup(
        [[KeyboardButton("📍 Joylashuvni yuborish", request_location=True)]],
        resize_keyboard=True,
        one_time_keyboard=True,
    )


# =================================================================================
# 19. TELEGRAM HANDLERLAR
# =================================================================================

WELCOME_TEXT = (
    "✨ <b>Assalomu alaykum!</b> ✨\n\n"
    "🤖 Men — <b>Premium Ob-havo Bot</b> 🌈\n"
    "🌤 Real vaqtdagi ob-havo, ⚠️ xavfsizlik ogohlantirishlari, 👕 kiyim maslahati, "
    "💱 valyuta kurslari, 📰 yangiliklar, 🎬 kino, 📚 kitob tavsiyalari va yana ko'p narsa!\n\n"
    "👇 Quyidagi menyudan tanlang yoki shahar nomini yozing (masalan: <i>Toshkent</i>)."
)

HELP_TEXT = (
    "📖 <b>Barcha komandalar</b>\n━━━━━━━━━━━━━━━\n"
    "🌤 /weather &lt;shahar&gt;\n🏙 /setcity &lt;shahar&gt;\n💱 /currency\n📰 /news\n"
    "🎬 /movie &lt;nomi&gt;\n📚 /book &lt;janr&gt;\n⚽️ /sport &lt;epl|laliga|uefa|seriea&gt;\n"
    "✈️ /travel\n💡 /quote  🧠 /fact\n💬 /translate &lt;til&gt; &lt;matn&gt;\n"
    "📝 /todo_add /todo_list /todo_done /todo_del\n"
    f"🔔 /subscribe (kunlik {DAILY_BROADCAST_HOUR:02d}:{DAILY_BROADCAST_MINUTE:02d}) /unsubscribe\n"
    "📍 Joylashuv yuborsangiz — shu joy ob-havosi va yaqin dorixona/do'konlarni topaman."
)


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    username = update.effective_user.username if update.effective_user else None
    await upsert_user(chat_id, username)
    await update.message.reply_text(WELCOME_TEXT, parse_mode=ParseMode.HTML, reply_markup=main_menu_keyboard())


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(HELP_TEXT, parse_mode=ParseMode.HTML, reply_markup=back_button())


async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.error("💥 Botda kutilmagan xatolik:", exc_info=context.error)
    if isinstance(update, Update) and update.effective_message:
        try:
            await update.effective_message.reply_text("⚠️ Kechirasiz, xatolik yuz berdi. Birozdan so'ng qayta urinib ko'ring.")
        except TelegramError:
            pass


# ---- Ob-havo ----

async def _send_weather_report(chat, weather: Dict[str, Any]) -> None:
    await chat.send_message(WeatherService.format_message(weather), parse_mode=ParseMode.HTML)
    warnings = SafetyAdvisor.analyze(weather)
    await chat.send_message("\n\n".join(warnings), parse_mode=ParseMode.HTML)
    await chat.send_message(f"👕 <b>Kiyim maslahati:</b>\n{ClothingAdvisor.suggest(weather)}", parse_mode=ParseMode.HTML)


async def cmd_weather(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    if context.args:
        city = " ".join(context.args)
    else:
        user = await get_user(chat_id)
        if user and user.get("city"):
            city = user["city"]
        elif user and user.get("lat") and user.get("lon"):
            weather = await weather_service.get_by_coords(user["lat"], user["lon"])
            if weather:
                await _send_weather_report(update.effective_chat, weather)
            else:
                await update.effective_message.reply_text("⚠️ Ob-havo ma'lumotini olib bo'lmadi.")
            return
        else:
            await update.effective_message.reply_text(
                "🏙 Shahar nomini kiriting: <code>/weather Toshkent</code>\n"
                "yoki 📍 joylashuvingizni yuboring:",
                parse_mode=ParseMode.HTML,
                reply_markup=location_request_keyboard(),
            )
            return

    weather = await weather_service.get_by_city(city)
    if not weather:
        await update.effective_message.reply_text(f"⚠️ '{city}' uchun ob-havo topilmadi. Nomni tekshiring.")
        return
    await _send_weather_report(update.effective_chat, weather)


async def cmd_setcity(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    if not context.args:
        await update.message.reply_text("Foydalanish: <code>/setcity Toshkent</code>", parse_mode=ParseMode.HTML)
        return
    city = " ".join(context.args)
    weather = await weather_service.get_by_city(city)
    if not weather:
        await update.message.reply_text(f"⚠️ '{city}' shahri topilmadi.")
        return
    await set_user_city(chat_id, weather["city"])
    await update.message.reply_text(f"✅ Standart shahringiz: <b>{weather['city']}</b>", parse_mode=ParseMode.HTML)


async def on_location_received(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    loc = update.message.location
    await set_user_location(chat_id, loc.latitude, loc.longitude)
    weather = await weather_service.get_by_coords(loc.latitude, loc.longitude)
    if not weather:
        await update.message.reply_text("⚠️ Joylashuvingiz bo'yicha ob-havo topilmadi.")
        return
    await _send_weather_report(update.effective_chat, weather)
    pending = context.user_data.pop("pending_location_action", None)
    if pending == "pharmacy":
        await _handle_nearby(update, context, "pharmacy")
    elif pending == "shop":
        await _handle_nearby(update, context, "supermarket")


# ---- Valyuta / fakt / sayohat / tarjimon ----

async def cmd_currency(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    rates = await currency_service.get_rates()
    target = update.effective_message
    if not rates:
        await target.reply_text("⚠️ Valyuta kurslarini olishda xatolik.")
        return
    await target.reply_text(CurrencyService.format_message(rates), parse_mode=ParseMode.HTML)


async def cmd_quote(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    quote = await quote_fact_service.get_quote()
    fact = await quote_fact_service.get_fact()
    await update.effective_message.reply_text(
        f"{quote}\n\n🧠 <b>Qiziqarli fakt:</b>\n{fact}", parse_mode=ParseMode.HTML
    )


async def cmd_travel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    destinations = TravelService.random_pick(3)
    await update.effective_message.reply_text(TravelService.format_message(destinations), parse_mode=ParseMode.HTML)


async def cmd_translate(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if len(context.args) < 2:
        await update.message.reply_text(
            "Foydalanish: <code>/translate en Salom dunyo</code>", parse_mode=ParseMode.HTML
        )
        return
    target_lang = context.args[0]
    text = " ".join(context.args[1:])
    result = await TranslatorService.translate(text, target_lang)
    await update.message.reply_text(f"🌐 <b>Tarjima ({target_lang}):</b>\n{result}", parse_mode=ParseMode.HTML)


# ---- To-do ----

async def cmd_todo_add(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        await update.message.reply_text("Foydalanish: <code>/todo_add Kitob o'qish</code>", parse_mode=ParseMode.HTML)
        return
    task = " ".join(context.args)
    task_id = await TodoManager.add_task(update.effective_chat.id, task)
    await update.message.reply_text(f"✅ Vazifa qo'shildi (#{task_id}): {task}")


async def cmd_todo_list(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    tasks = await TodoManager.list_tasks(update.effective_chat.id)
    await update.effective_message.reply_text(TodoManager.format_list(tasks), parse_mode=ParseMode.HTML)


async def cmd_todo_done(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args or not context.args[0].isdigit():
        await update.message.reply_text("Foydalanish: <code>/todo_done id</code>", parse_mode=ParseMode.HTML)
        return
    await TodoManager.mark_done(update.effective_chat.id, int(context.args[0]))
    await update.message.reply_text("✅ Vazifa bajarilgan deb belgilandi.")


async def cmd_todo_del(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args or not context.args[0].isdigit():
        await update.message.reply_text("Foydalanish: <code>/todo_del id</code>", parse_mode=ParseMode.HTML)
        return
    await TodoManager.delete_task(update.effective_chat.id, int(context.args[0]))
    await update.message.reply_text("🗑 Vazifa o'chirildi.")


# ---- Yangiliklar / kino / kitob / sport ----

async def cmd_news(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = " ".join(context.args) if context.args else "Uzbekistan"
    target = update.effective_message
    articles = await news_service.get_news(query)
    if articles is None and not NEWSAPI_KEY:
        await target.reply_text(
            "⚠️ Yangiliklar xizmati sozlanmagan. .env faylida <code>NEWSAPI_KEY</code> ko'rsating "
            "(https://newsapi.org — bepul).",
            parse_mode=ParseMode.HTML,
        )
        return
    await target.reply_text(
        NewsService.format_message(articles or []), parse_mode=ParseMode.HTML, disable_web_page_preview=True
    )


async def cmd_movie(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        await update.message.reply_text("Foydalanish: <code>/movie Inception</code>", parse_mode=ParseMode.HTML)
        return
    if not OMDB_API_KEY:
        await update.message.reply_text(
            "⚠️ Kino xizmati sozlanmagan. .env faylida <code>OMDB_API_KEY</code> ko'rsating "
            "(https://omdbapi.com — bepul).",
            parse_mode=ParseMode.HTML,
        )
        return
    title = " ".join(context.args)
    movie = await movie_service.search(title)
    if not movie:
        await update.message.reply_text(f"⚠️ '{title}' nomli kino topilmadi.")
        return
    await update.message.reply_text(MovieService.format_message(movie), parse_mode=ParseMode.HTML)


async def cmd_book(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    genre = " ".join(context.args) if context.args else "fiction"
    target = update.effective_message
    books = await book_service.search_by_genre(genre)
    await target.reply_text(BookService.format_message(books or [], genre), parse_mode=ParseMode.HTML)


async def cmd_sport(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    league = context.args[0] if context.args else "epl"
    target = update.effective_message
    if league.lower() not in SportsService.LEAGUE_IDS:
        available = ", ".join(SportsService.LEAGUE_IDS.keys())
        await target.reply_text(f"⚠️ Noma'lum liga. Mavjud: {available}")
        return
    results = await sports_service.get_last_results(league)
    await target.reply_text(SportsService.format_message(results or [], league), parse_mode=ParseMode.HTML)


# ---- Yaqin dorixona/do'kon ----

async def cmd_nearby_pharmacy(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _handle_nearby(update, context, "pharmacy")


async def cmd_nearby_shop(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _handle_nearby(update, context, "supermarket")


async def _handle_nearby(update: Update, context: ContextTypes.DEFAULT_TYPE, place_type: str) -> None:
    chat_id = update.effective_chat.id
    user = await get_user(chat_id)
    target = update.effective_message
    if not user or not user.get("lat") or not user.get("lon"):
        context.user_data["pending_location_action"] = place_type if place_type == "pharmacy" else "shop"
        await target.reply_text(
            "📍 Iltimos, avval joylashuvingizni yuboring:", reply_markup=location_request_keyboard()
        )
        return
    places = await place_finder_service.find_nearby(user["lat"], user["lon"], place_type)
    await target.reply_text(
        PlaceFinderService.format_message(places or [], place_type),
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
    )


# ---- Yo'l tirbandligi ----

async def cmd_traffic(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    city = " ".join(context.args) if context.args else "Toshkent"
    info = await traffic_service.get_traffic_info(city)
    await update.message.reply_text(info)


# ---- Obuna ----

async def cmd_subscribe(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    await upsert_user(chat_id)
    await db_execute("UPDATE users SET subscribed_daily = 1 WHERE chat_id = ?", (chat_id,))
    await update.effective_message.reply_text(
        f"🔔 Obuna faollashtirildi! Har kuni soat "
        f"<b>{DAILY_BROADCAST_HOUR:02d}:{DAILY_BROADCAST_MINUTE:02d}</b> da ob-havo, hikmat va fakt yuboriladi.",
        parse_mode=ParseMode.HTML,
    )


async def cmd_unsubscribe(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    await db_execute("UPDATE users SET subscribed_daily = 0 WHERE chat_id = ?", (chat_id,))
    await update.effective_message.reply_text("🔕 Obuna bekor qilindi.")


# ---- Inline menyu callback router ----

async def on_callback_query(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()  # yuklanish "soat" belgisini olib tashlaydi
    data = query.data

    if data == "menu:back":
        await query.edit_message_text(WELCOME_TEXT, parse_mode=ParseMode.HTML, reply_markup=main_menu_keyboard())
        return

    if data == "menu:weather":
        await query.message.reply_text(
            "🏙 Shahar nomini yozing: <code>/weather Toshkent</code>\nyoki joylashuvingizni yuboring:",
            parse_mode=ParseMode.HTML,
            reply_markup=location_request_keyboard(),
        )
    elif data == "menu:clothing":
        await cmd_weather(update, context)  # ob-havo bilan birga kiyim maslahati ham chiqadi
    elif data == "menu:currency":
        await cmd_currency(update, context)
    elif data == "menu:news":
        await cmd_news(update, context)
    elif data == "menu:movie":
        await query.message.reply_text("🎬 Kino nomini yozing: <code>/movie Inception</code>", parse_mode=ParseMode.HTML)
    elif data == "menu:book":
        await query.message.reply_text("📚 Janrni yozing: <code>/book fantasy</code>", parse_mode=ParseMode.HTML)
    elif data == "menu:sport":
        await query.message.reply_text(
            "⚽️ Liga tanlang: <code>/sport epl</code> | laliga | uefa | seriea", parse_mode=ParseMode.HTML
        )
    elif data == "menu:travel":
        await cmd_travel(update, context)
    elif data == "menu:pharmacy":
        await cmd_nearby_pharmacy(update, context)
    elif data == "menu:shop":
        await cmd_nearby_shop(update, context)
    elif data == "menu:quote":
        await cmd_quote(update, context)
    elif data == "menu:translate":
        await query.message.reply_text(
            "💬 Foydalanish: <code>/translate en Salom, qandaysiz?</code>", parse_mode=ParseMode.HTML
        )
    elif data == "menu:todo":
        await cmd_todo_list(update, context)
    elif data == "menu:subscribe":
        await cmd_subscribe(update, context)
    elif data == "menu:help":
        await query.message.reply_text(HELP_TEXT, parse_mode=ParseMode.HTML, reply_markup=back_button())


async def on_text_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Oddiy matn (shahar nomi) yuborilganda ishlaydi — inline menyu asosiy navigatsiya."""
    text = update.message.text.strip()
    weather = await weather_service.get_by_city(text)
    if weather:
        await _send_weather_report(update.effective_chat, weather)
    else:
        await update.message.reply_text(
            "🤔 Buni tushunmadim. Shahar nomi kiriting yoki menyudan foydalaning:",
            reply_markup=main_menu_keyboard(),
        )


# =================================================================================
# 20. KUNLIK AVTOMATIK XABAR — PTB'ning O'Z ICHKI JobQueue'si orqali (XAVFSIZ)
# =================================================================================
#
# MUHIM: Bu yerda ATAYLAB tashqi `apscheduler.AsyncIOScheduler` ISHLATILMAYDI,
# chunki aynan o'sha yondashuv Render'da "no current event loop" xatoligiga
# olib kelgan edi. `application.job_queue` PTB kutubxonasining o'zi tomonidan,
# botning YAGONA asyncio event loop'i ichida ishga tushiriladi — shuning uchun
# bunday muammo umuman yuzaga kelmaydi.
#
async def daily_broadcast_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    users = await get_all_subscribed_users()
    logger.info("📤 Kunlik xabar %d foydalanuvchiga yuborilmoqda...", len(users))
    quote = await quote_fact_service.get_quote()
    fact = await quote_fact_service.get_fact()

    for user in users:
        chat_id = user["chat_id"]
        try:
            parts = [f"☀️ <b>Xayrli tong!</b>\n\n{quote}\n\n🧠 {fact}"]
            weather = None
            if user.get("city"):
                weather = await weather_service.get_by_city(user["city"])
            elif user.get("lat") and user.get("lon"):
                weather = await weather_service.get_by_coords(user["lat"], user["lon"])
            if weather:
                parts.append(WeatherService.format_message(weather))
                parts.append("\n".join(SafetyAdvisor.analyze(weather)))
                parts.append(f"👕 {ClothingAdvisor.suggest(weather)}")

            await context.bot.send_message(chat_id=chat_id, text="\n\n".join(parts), parse_mode=ParseMode.HTML)
        except TelegramError as e:
            logger.warning("Foydalanuvchi %s ga xabar yuborilmadi: %s", chat_id, e)
        await asyncio.sleep(0.05)


# =================================================================================
# 21. ASOSIY QURILISH VA ISHGA TUSHIRISH
# =================================================================================

def build_application() -> Application:
    application = ApplicationBuilder().token(BOT_TOKEN).build()

    application.add_handler(CommandHandler("start", cmd_start))
    application.add_handler(CommandHandler("help", cmd_help))

    application.add_handler(CommandHandler("weather", cmd_weather))
    application.add_handler(CommandHandler("setcity", cmd_setcity))
    application.add_handler(MessageHandler(filters.LOCATION, on_location_received))

    application.add_handler(CommandHandler("currency", cmd_currency))
    application.add_handler(CommandHandler("news", cmd_news))
    application.add_handler(CommandHandler("movie", cmd_movie))
    application.add_handler(CommandHandler("book", cmd_book))
    application.add_handler(CommandHandler("sport", cmd_sport))
    application.add_handler(CommandHandler("travel", cmd_travel))
    application.add_handler(CommandHandler("traffic", cmd_traffic))

    application.add_handler(CommandHandler("quote", cmd_quote))
    application.add_handler(CommandHandler("fact", cmd_quote))
    application.add_handler(CommandHandler("translate", cmd_translate))

    application.add_handler(CommandHandler("todo_add", cmd_todo_add))
    application.add_handler(CommandHandler("todo_list", cmd_todo_list))
    application.add_handler(CommandHandler("todo_done", cmd_todo_done))
    application.add_handler(CommandHandler("todo_del", cmd_todo_del))

    application.add_handler(CommandHandler("subscribe", cmd_subscribe))
    application.add_handler(CommandHandler("unsubscribe", cmd_unsubscribe))

    application.add_handler(CommandHandler("pharmacy", cmd_nearby_pharmacy))
    application.add_handler(CommandHandler("shop", cmd_nearby_shop))

    application.add_handler(CallbackQueryHandler(on_callback_query, pattern=r"^menu:"))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text_message))

    application.add_error_handler(on_error)
    return application


async def post_init(application: Application) -> None:
    """Bot ishga tushgach chaqiriladi: baza tayyorlanadi va kunlik job rejalashtiriladi."""
    init_database()
    # PTB'ning o'z JobQueue'si — hech qanday tashqi event-loop muammosisiz ishlaydi
    application.job_queue.run_daily(
        daily_broadcast_job,
        time=dtime(hour=DAILY_BROADCAST_HOUR, minute=DAILY_BROADCAST_MINUTE, tzinfo=TZ),
        name="daily_broadcast",
    )
    logger.info(
        "🚀 Bot muvaffaqiyatli ishga tushdi. Kunlik xabar har kuni %02d:%02d (%s) da yuboriladi.",
        DAILY_BROADCAST_HOUR, DAILY_BROADCAST_MINUTE, TIMEZONE_NAME,
    )


def main() -> None:
    application = build_application()
    application.post_init = post_init
    logger.info("⏳ Bot ishga tushmoqda...")
    # MUHIM: faqat run_polling ishlatiladi — hech qanday webhook/start_webhook chaqirilmaydi.
    # drop_pending_updates=True — qayta ishga tushganda eski/osilib qolgan yangilanishlarni tashlab yuboradi
    # (bu ham "Conflict"/eventloop bilan bog'liq g'alati holatlarning oldini oladi).
    application.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)


if __name__ == "__main__":
    main()

from __future__ import annotations

import os
import sys
import json
import sqlite3
import logging
import random
import asyncio
import functools
from datetime import datetime, time as dtime
from typing import Optional, List, Dict, Any, Tuple

import requests
import pytz
from dotenv import load_dotenv

try:
    from deep_translator import GoogleTranslator
except ImportError:
    GoogleTranslator = None

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardMarkup,
)
from telegram.constants import ParseMode
from telegram.error import TelegramError
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
)

# =================================================================================
# 1. SOZLAMALAR VA MUHIT O'ZGARUVCHILARI
# =================================================================================

load_dotenv()

BOT_TOKEN: Optional[str] = os.getenv("TELEGRAM_BOT_TOKEN")
OWM_API_KEY: Optional[str] = os.getenv("OPENWEATHERMAP_API_KEY")
NEWSAPI_KEY: str = os.getenv("NEWSAPI_KEY", "")
OMDB_API_KEY: str = os.getenv("OMDB_API_KEY", "")
GOOGLE_BOOKS_API_KEY: str = os.getenv("GOOGLE_BOOKS_API_KEY", "")
THESPORTSDB_API_KEY: str = os.getenv("THESPORTSDB_API_KEY", "3")
TIMEZONE_NAME: str = os.getenv("TIMEZONE", "Asia/Tashkent")
DB_PATH: str = os.getenv("DB_PATH", "bot_database.db")
DAILY_BROADCAST_HOUR: int = int(os.getenv("DAILY_BROADCAST_HOUR", "8"))
DAILY_BROADCAST_MINUTE: int = int(os.getenv("DAILY_BROADCAST_MINUTE", "0"))

if not BOT_TOKEN:
    raise RuntimeError("❌ TELEGRAM_BOT_TOKEN topilmadi. .env faylini tekshiring.")
if not OWM_API_KEY:
    raise RuntimeError("❌ OPENWEATHERMAP_API_KEY topilmadi. .env faylini tekshiring.")

TZ = pytz.timezone(TIMEZONE_NAME)

# =================================================================================
# 2. LOGGING
# =================================================================================

logging.basicConfig(
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    level=logging.INFO,
    handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler("bot.log", encoding="utf-8")],
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger("weather_bot")

DEFAULT_TIMEOUT = 10


# =================================================================================
# 3. YORDAMCHI FUNKSIYALAR (HTTP + DB)
# =================================================================================

async def fetch_json(
    url: str,
    params: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    timeout: int = DEFAULT_TIMEOUT,
) -> Optional[Dict[str, Any]]:
    """Tashqi API'larga bloklanmaydigan (thread-executor) GET so'rov."""
    loop = asyncio.get_running_loop()
    try:
        func = functools.partial(requests.get, url, params=params, headers=headers, timeout=timeout)
        response = await loop.run_in_executor(None, func)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.Timeout:
        logger.warning("⏱ Timeout: %s", url)
    except requests.exceptions.HTTPError as e:
        logger.warning("🚫 HTTP xatolik: %s -> %s", url, e)
    except requests.exceptions.RequestException as e:
        logger.warning("🌐 Tarmoq xatoligi: %s -> %s", url, e)
    except (ValueError, json.JSONDecodeError):
        logger.warning("📄 JSON parslash xatoligi: %s", url)
    return None


def init_database() -> None:
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        cur.execute(
            """CREATE TABLE IF NOT EXISTS users (
                chat_id INTEGER PRIMARY KEY, username TEXT, city TEXT,
                lat REAL, lon REAL, lang TEXT DEFAULT 'uz',
                subscribed_daily INTEGER DEFAULT 0, created_at TEXT)"""
        )
        cur.execute(
            """CREATE TABLE IF NOT EXISTS todos (
                id INTEGER PRIMARY KEY AUTOINCREMENT, chat_id INTEGER NOT NULL,
                task TEXT NOT NULL, is_done INTEGER DEFAULT 0, created_at TEXT)"""
        )
        conn.commit()
    logger.info("✅ Ma'lumotlar bazasi tayyor: %s", DB_PATH)


def _db_execute(query: str, params: Tuple = (), fetch: bool = False) -> Any:
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute(query, params)
        if fetch:
            return [dict(r) for r in cur.fetchall()]
        conn.commit()
        return cur.lastrowid


async def db_execute(query: str, params: Tuple = (), fetch: bool = False) -> Any:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, functools.partial(_db_execute, query, params, fetch))


async def upsert_user(chat_id: int, username: Optional[str] = None) -> None:
    existing = await db_execute("SELECT chat_id FROM users WHERE chat_id = ?", (chat_id,), fetch=True)
    if not existing:
        await db_execute(
            "INSERT INTO users (chat_id, username, created_at) VALUES (?, ?, ?)",
            (chat_id, username or "", datetime.now(TZ).isoformat()),
        )


async def set_user_city(chat_id: int, city: str) -> None:
    await db_execute("UPDATE users SET city = ?, lat = NULL, lon = NULL WHERE chat_id = ?", (city, chat_id))


async def set_user_location(chat_id: int, lat: float, lon: float) -> None:
    await db_execute("UPDATE users SET lat = ?, lon = ?, city = NULL WHERE chat_id = ?", (lat, lon, chat_id))


async def get_user(chat_id: int) -> Optional[Dict[str, Any]]:
    rows = await db_execute("SELECT * FROM users WHERE chat_id = ?", (chat_id,), fetch=True)
    return rows[0] if rows else None


async def get_all_subscribed_users() -> List[Dict[str, Any]]:
    return await db_execute("SELECT * FROM users WHERE subscribed_daily = 1", fetch=True)


# =================================================================================
# 4. OB-HAVO XIZMATI (real vaqtdagi harorat, OpenWeatherMap)
# =================================================================================

class WeatherService:
    BASE_URL = "https://api.openweathermap.org/data/2.5/weather"

    def __init__(self, api_key: str):
        self.api_key = api_key

    async def get_by_city(self, city: str) -> Optional[Dict[str, Any]]:
        params = {"q": city, "appid": self.api_key, "units": "metric", "lang": "uz"}
        return self._parse(await fetch_json(self.BASE_URL, params=params))

    async def get_by_coords(self, lat: float, lon: float) -> Optional[Dict[str, Any]]:
        params = {"lat": lat, "lon": lon, "appid": self.api_key, "units": "metric", "lang": "uz"}
        return self._parse(await fetch_json(self.BASE_URL, params=params))

    @staticmethod
    def _parse(data: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        if not data or data.get("cod") not in (200, "200"):
            return None
        try:
            wb = data["weather"][0]
            return {
                "city": data.get("name", "Noma'lum"),
                "country": data.get("sys", {}).get("country", ""),
                "temp": round(data["main"]["temp"]),
                "feels_like": round(data["main"]["feels_like"]),
                "temp_min": round(data["main"]["temp_min"]),
                "temp_max": round(data["main"]["temp_max"]),
                "humidity": data["main"]["humidity"],
                "pressure": data["main"]["pressure"],
                "wind_speed": data.get("wind", {}).get("speed", 0),
                "visibility_m": data.get("visibility", 10000),
                "description": wb.get("description", "").capitalize(),
                "main_condition": wb.get("main", ""),
                "observed_at": datetime.now(TZ).strftime("%Y-%m-%d %H:%M"),
            }
        except (KeyError, IndexError, TypeError) as e:
            logger.error("Ob-havo parslash xatoligi: %s", e)
            return None

    @staticmethod
    def format_message(w: Dict[str, Any]) -> str:
        icon_map = {
            "Clear": "☀️", "Clouds": "☁️", "Rain": "🌧", "Drizzle": "🌦",
            "Thunderstorm": "⛈", "Snow": "❄️", "Mist": "🌫", "Fog": "🌫", "Haze": "🌫",
        }
        emoji = icon_map.get(w["main_condition"], "🌡")
        return (
            f"{emoji} <b>{w['city']}, {w['country']}</b>\n"
            f"🕒 <i>{w['observed_at']}</i>\n"
            f"━━━━━━━━━━━━━━━\n"
            f"🌡 Harorat: <b>{w['temp']}°C</b>  (sezilishi: {w['feels_like']}°C)\n"
            f"📉 Min/Maks: {w['temp_min']}°C / {w['temp_max']}°C\n"
            f"📝 Holat: {w['description']}\n"
            f"💧 Namlik: {w['humidity']}%\n"
            f"🧭 Bosim: {w['pressure']} hPa\n"
            f"💨 Shamol: {w['wind_speed']} m/s\n"
            f"👁 Ko'rinish: {w['visibility_m']/1000:.1f} km"
        )


weather_service = WeatherService(OWM_API_KEY)


# =================================================================================
# 5. XAVFSIZLIK OGOHLANTIRISHLARI
# =================================================================================

class SafetyAdvisor:
    EXTREME_HEAT_C = 38
    HIGH_HEAT_C = 33
    EXTREME_COLD_C = -15
    COLD_C = -5
    STRONG_WIND_MS = 14
    MODERATE_WIND_MS = 8
    LOW_VISIBILITY_M = 1000

    @classmethod
    def analyze(cls, w: Dict[str, Any]) -> List[str]:
        warnings: List[str] = []
        temp, feels = w["temp"], w["feels_like"]
        condition, wind = w["main_condition"], w["wind_speed"]
        visibility, humidity = w["visibility_m"], w["humidity"]

        if temp >= cls.EXTREME_HEAT_C or feels >= cls.EXTREME_HEAT_C:
            warnings.append(
                "🔥 <b>OGOHLANTIRISH: Haddan tashqari issiqlik!</b>\n"
                "Soyada/xonada bo'ling, kamida 2-3 litr suv iching, 12:00-17:00 orasida "
                "ochiq quyoshda yurmang, bosh kiyim taqing."
            )
        elif temp >= cls.HIGH_HEAT_C or feels >= cls.HIGH_HEAT_C:
            warnings.append("🌡 <b>Diqqat:</b> havo juda issiq — ko'proq suv iching, faollikni cheklang.")

        if temp >= cls.HIGH_HEAT_C and humidity >= 60:
            warnings.append("💦 Yuqori namlik issiqlikni kuchaytiradi — tez-tez salqin joyda dam oling.")

        if temp <= cls.EXTREME_COLD_C or feels <= cls.EXTREME_COLD_C:
            warnings.append(
                "🥶 <b>OGOHLANTIRISH: Qattiq sovuq!</b>\n"
                "Terining ochiq qolishidan saqlaning, qatlamli issiq kiyim kiying, uzoq "
                "tashqarida qolmang."
            )
        elif temp <= cls.COLD_C or feels <= cls.COLD_C:
            warnings.append("❄️ <b>Diqqat:</b> havo sovuq — issiqroq kiyining, qo'lqop/sharf taqing.")

        if condition == "Thunderstorm":
            warnings.append(
                "⛈ <b>OGOHLANTIRISH: Momaqaldiroq!</b>\nOchiq maydon, baland daraxt va suv "
                "havzalari yaqinida qolmang."
            )
        elif condition in ("Rain", "Drizzle"):
            warnings.append("🌧 Yomg'ir yog'moqda — soyabon oling, yo'llar sirpanchiq bo'lishi mumkin.")

        if condition == "Snow":
            warnings.append("🌨 Qor yog'moqda — yo'llar muzlashi mumkin, ehtiyot bo'ling.")

        if wind >= cls.STRONG_WIND_MS:
            warnings.append("💨 <b>OGOHLANTIRISH: Kuchli shamol!</b>\nBaland inshoot va daraxtlardan uzoqroq yuring.")
        elif wind >= cls.MODERATE_WIND_MS:
            warnings.append("🍃 O'rtacha shamol — ochiq soyabonlardan ehtiyot bo'ling.")

        if visibility <= cls.LOW_VISIBILITY_M:
            warnings.append("🌫 Ko'rinish past (tuman) — chiroqlarni yoqing, tezlikni kamaytiring.")

        if not warnings:
            warnings.append("✅ Hozircha maxsus xavf yo'q. Ajoyib kun tilaymiz! 🌈")

        return warnings


# =================================================================================
# 6. KIYIM TAVSIYASI
# =================================================================================

class ClothingAdvisor:
    @staticmethod
    def suggest(w: Dict[str, Any]) -> str:
        temp, condition, wind = w["feels_like"], w["main_condition"], w["wind_speed"]

        if temp >= 32:
            base = "👕 Yengil, ochiq rangli kiyim, 🧢 shlyapa, 🕶 quyoshdan ko'zoynak."
        elif temp >= 24:
            base = "👚 Yengil футболка/ko'ylak, qulay poyabzal."
        elif temp >= 16:
            base = "🧥 Yengil куртка yoki kardigan."
        elif temp >= 8:
            base = "🧥 Issiqroq куртка, sviter, yopiq poyabzal."
        elif temp >= 0:
            base = "🧣 Qishki palto, sharf, qalin sviter."
        else:
            base = "🥶 Puxovik, qalin qo'lqop, shapka, termokiyim."

        extra = []
        if condition in ("Rain", "Drizzle", "Thunderstorm"):
            extra.append("☂️ soyabon")
        if condition == "Snow":
            extra.append("👢 sirpanmaydigan qishki poyabzal")
        if wind >= 8:
            extra.append("🧢 shamolbardosh kiyim")
        if extra:
            base += "\n➕ Qo'shimcha: " + ", ".join(extra) + "."
        return base


# =================================================================================
# 7. VALYUTA KURSLARI
# =================================================================================

class CurrencyService:
    BASE_URL = "https://open.er-api.com/v6/latest/USD"
    TARGET = ["UZS", "EUR", "RUB", "GBP", "KZT"]

    async def get_rates(self) -> Optional[Dict[str, float]]:
        data = await fetch_json(self.BASE_URL)
        if not data or data.get("result") != "success":
            return None
        rates = data.get("rates", {})
        return {c: rates[c] for c in self.TARGET if c in rates}

    @staticmethod
    def format_message(rates: Dict[str, float]) -> str:
        lines = ["💱 <b>Valyuta kurslari</b> (1 USD asosida)\n━━━━━━━━━━━━━━━"]
        flags = {"UZS": "🇺🇿", "EUR": "🇪🇺", "RUB": "🇷🇺", "GBP": "🇬🇧", "KZT": "🇰🇿"}
        for cur, val in rates.items():
            if cur == "UZS":
                lines.append(f"{flags[cur]} 1 USD = <b>{val:,.0f}</b> so'm")
            else:
                lines.append(f"{flags.get(cur,'')} 1 USD = {val:.3f} {cur}")
        return "\n".join(lines)


currency_service = CurrencyService()


# =================================================================================
# 8. HIKMATLI SO'ZLAR / QIZIQARLI FAKTLAR
# =================================================================================

class QuoteFactService:
    QUOTE_API = "https://api.quotable.io/random"
    FACT_API = "https://uselessfacts.jsph.pl/api/v2/facts/random?language=en"

    FALLBACK_QUOTES = [
        "Bilim — kuchdir. 💪", "Har bir muvaffaqiyat orqasida ko'plab urinish yotadi. 🌟",
        "Bugungi mehnating ertangi natijang. 🌱", "Kichik qadamlar katta yo'lni bosib o'tadi. 👣",
        "Sabr — muvaffaqiyat kaliti. 🔑",
    ]
    FALLBACK_FACTS = [
        "Asal hech qachon buzilmaydi. 🍯", "Sakkizoyoqning uchta yuragi bor. 🐙",
        "Bir kun 23 soat 56 daqiqa davom etadi. 🌍", "Banan botanik jihatdan rezavordir. 🍌",
        "Muz suvdan yengilroq, shuning uchun suzadi. ❄️",
    ]

    async def get_quote(self) -> str:
        data = await fetch_json(self.QUOTE_API)
        if data and data.get("content"):
            return f'💬 "{data["content"]}"\n— {data.get("author","Noma\'lum")}'
        return f"💬 {random.choice(self.FALLBACK_QUOTES)}"

    async def get_fact(self) -> str:
        data = await fetch_json(self.FACT_API)
        if data and data.get("text"):
            return data["text"]
        return random.choice(self.FALLBACK_FACTS)


quote_fact_service = QuoteFactService()


# =================================================================================
# 9. TARJIMON
# =================================================================================

class TranslatorService:
    @staticmethod
    async def translate(text: str, target_lang: str) -> str:
        if GoogleTranslator is None:
            return "⚠️ Tarjimon kutubxonasi o'rnatilmagan."
        loop = asyncio.get_running_loop()
        try:
            func = functools.partial(GoogleTranslator(source="auto", target=target_lang).translate, text)
            result = await loop.run_in_executor(None, func)
            return result or "⚠️ Tarjima qilib bo'lmadi."
        except Exception as e:
            logger.warning("Tarjima xatoligi: %s", e)
            return "⚠️ Tarjima xatosi. Til kodini tekshiring (masalan: en, ru, uz)."


# =================================================================================
# 10. TODO / ESLATMALAR
# =================================================================================

class TodoManager:
    @staticmethod
    async def add_task(chat_id: int, task: str) -> int:
        return await db_execute(
            "INSERT INTO todos (chat_id, task, created_at) VALUES (?, ?, ?)",
            (chat_id, task, datetime.now(TZ).isoformat()),
        )

    @staticmethod
    async def list_tasks(chat_id: int) -> List[Dict[str, Any]]:
        return await db_execute(
            "SELECT * FROM todos WHERE chat_id = ? ORDER BY is_done ASC, id DESC", (chat_id,), fetch=True
        )

    @staticmethod
    async def mark_done(chat_id: int, task_id: int) -> None:
        await db_execute("UPDATE todos SET is_done = 1 WHERE chat_id = ? AND id = ?", (chat_id, task_id))

    @staticmethod
    async def delete_task(chat_id: int, task_id: int) -> None:
        await db_execute("DELETE FROM todos WHERE chat_id = ? AND id = ?", (chat_id, task_id))

    @staticmethod
    def format_list(tasks: List[Dict[str, Any]]) -> str:
        if not tasks:
            return "📝 Vazifalar yo'q. Qo'shish: <code>/todo_add matn</code>"
        lines = ["📝 <b>Vazifalaringiz:</b>\n━━━━━━━━━━━━━━━"]
        for t in tasks:
            mark = "✅" if t["is_done"] else "🔲"
            lines.append(f"{mark} <code>#{t['id']}</code> {t['task']}")
        lines.append("\n✔️ Bajarish: /todo_done id   🗑 O'chirish: /todo_del id")
        return "\n".join(lines)


# =================================================================================
# 11. YANGILIKLAR
# =================================================================================

class NewsService:
    BASE_URL = "https://newsapi.org/v2/everything"

    async def get_news(self, query: str = "Uzbekistan", limit: int = 5) -> Optional[List[Dict[str, str]]]:
        if not NEWSAPI_KEY:
            return None
        params = {"q": query, "language": "ru", "sortBy": "publishedAt", "pageSize": limit, "apiKey": NEWSAPI_KEY}
        data = await fetch_json(self.BASE_URL, params=params)
        if not data or data.get("status") != "ok":
            return None
        return [
            {"title": a.get("title", ""), "url": a.get("url", ""), "source": a.get("source", {}).get("name", "")}
            for a in data.get("articles", [])[:limit]
        ]

    @staticmethod
    def format_message(articles: List[Dict[str, str]]) -> str:
        if not articles:
            return "📰 Hozircha yangilik topilmadi."
        lines = ["📰 <b>So'nggi yangiliklar</b>\n━━━━━━━━━━━━━━━"]
        for i, a in enumerate(articles, 1):
            lines.append(f"{i}. <a href='{a['url']}'>{a['title']}</a>  <i>({a['source']})</i>")
        return "\n".join(lines)


news_service = NewsService()


# =================================================================================
# 12. KINO (OMDb)
# =================================================================================

class MovieService:
    BASE_URL = "https://www.omdbapi.com/"

    async def search(self, title: str) -> Optional[Dict[str, Any]]:
        if not OMDB_API_KEY:
            return None
        data = await fetch_json(self.BASE_URL, params={"t": title, "apikey": OMDB_API_KEY})
        if not data or data.get("Response") != "True":
            return None
        return data

    @staticmethod
    def format_message(m: Dict[str, Any]) -> str:
        return (
            f"🎬 <b>{m.get('Title')}</b> ({m.get('Year')})\n━━━━━━━━━━━━━━━\n"
            f"⭐ IMDB: {m.get('imdbRating','N/A')}\n🎭 Janr: {m.get('Genre','N/A')}\n"
            f"🎥 Rejissyor: {m.get('Director','N/A')}\n📝 {m.get('Plot','Mavjud emas')}"
        )


movie_service = MovieService()


# =================================================================================
# 13. KITOBLAR (Google Books)
# =================================================================================

class BookService:
    BASE_URL = "https://www.googleapis.com/books/v1/volumes"

    async def search_by_genre(self, genre: str, limit: int = 5) -> Optional[List[Dict[str, str]]]:
        params = {"q": f"subject:{genre}", "maxResults": limit}
        if GOOGLE_BOOKS_API_KEY:
            params["key"] = GOOGLE_BOOKS_API_KEY
        data = await fetch_json(self.BASE_URL, params=params)
        if not data or "items" not in data:
            return None
        out = []
        for item in data["items"][:limit]:
            info = item.get("volumeInfo", {})
            out.append(
                {
                    "title": info.get("title", "Noma'lum"),
                    "authors": ", ".join(info.get("authors", ["Noma'lum muallif"])),
                    "rating": info.get("averageRating", "—"),
                }
            )
        return out

    @staticmethod
    def format_message(books: List[Dict[str, str]], genre: str) -> str:
        if not books:
            return f"📚 '{genre}' bo'yicha kitob topilmadi."
        lines = [f"📚 <b>'{genre}' janridagi tavsiyalar</b>\n━━━━━━━━━━━━━━━"]
        for b in books:
            lines.append(f"• <b>{b['title']}</b> — {b['authors']} (⭐ {b['rating']})")
        return "\n".join(lines)


book_service = BookService()


# =================================================================================
# 14. SPORT (TheSportsDB)
# =================================================================================

class SportsService:
    LEAGUE_IDS = {"epl": "4328", "laliga": "4335", "uefa": "4480", "seriea": "4332"}

    def __init__(self, api_key: str):
        self.api_key = api_key

    async def get_last_results(self, league: str) -> Optional[List[Dict[str, str]]]:
        league_id = self.LEAGUE_IDS.get(league.lower())
        if not league_id:
            return None
        url = f"https://www.thesportsdb.com/api/v1/json/{self.api_key}/eventspastleague.php"
        data = await fetch_json(url, params={"id": league_id})
        if not data or not data.get("events"):
            return None
        return [
            {
                "home": e.get("strHomeTeam", ""), "away": e.get("strAwayTeam", ""),
                "home_score": e.get("intHomeScore") or "-", "away_score": e.get("intAwayScore") or "-",
                "date": e.get("dateEvent", ""),
            }
            for e in data["events"][:5]
        ]

    @staticmethod
    def format_message(results: List[Dict[str, str]], league: str) -> str:
        if not results:
            return f"⚽ '{league}' bo'yicha natija topilmadi."
        lines = [f"⚽ <b>So'nggi natijalar ({league.upper()})</b>\n━━━━━━━━━━━━━━━"]
        for r in results:
            lines.append(f"{r['date']}: {r['home']} <b>{r['home_score']}-{r['away_score']}</b> {r['away']}")
        return "\n".join(lines)


sports_service = SportsService(THESPORTSDB_API_KEY)


# =================================================================================
# 15. YAQIN ATROFDAGI DORIXONA / DO'KON (OpenStreetMap Overpass)
# =================================================================================

class PlaceFinderService:
    OVERPASS_URL = "https://overpass-api.de/api/interpreter"

    async def find_nearby(self, lat: float, lon: float, place_type: str = "pharmacy", radius_m: int = 1500):
        amenity_filter = (
            f'node["amenity"="pharmacy"](around:{radius_m},{lat},{lon});'
            if place_type == "pharmacy"
            else f'node["shop"="supermarket"](around:{radius_m},{lat},{lon});'
        )
        query = f"[out:json][timeout:10];({amenity_filter});out center 10;"
        loop = asyncio.get_running_loop()
        try:
            func = functools.partial(requests.get, self.OVERPASS_URL, params={"data": query}, timeout=15)
            response = await loop.run_in_executor(None, func)
            response.raise_for_status()
            data = response.json()
        except Exception as e:
            logger.warning("Overpass xatoligi: %s", e)
            return None
        elements = data.get("elements", [])[:10]
        return [
            {"name": el.get("tags", {}).get("name", "Nomsiz"), "lat": el.get("lat"), "lon": el.get("lon")}
            for el in elements
        ]

    @staticmethod
    def format_message(places, place_type: str) -> str:
        label = "dorixona" if place_type == "pharmacy" else "supermarket"
        if not places:
            return f"🔍 Yaqin atrofda {label} topilmadi."
        lines = [f"📍 <b>Yaqin atrofdagi {label}lar</b>\n━━━━━━━━━━━━━━━"]
        for p in places:
            link = f"https://www.google.com/maps?q={p['lat']},{p['lon']}"
            lines.append(f"• <a href='{link}'>{p['name']}</a>")
        return "\n".join(lines)


place_finder_service = PlaceFinderService()


# =================================================================================
# 16. SAYOHAT YO'NALISHLARI
# =================================================================================

class TravelService:
    DESTINATIONS = [
        {"name": "🇺🇿 Samarqand", "desc": "Registon maydoni va Amir Temur davri me'morchiligi."},
        {"name": "🇺🇿 Buxoro", "desc": "2000+ yillik tarix, Ark qal'asi, Poi-Kalon majmuasi."},
        {"name": "🇺🇿 Xiva", "desc": "Ichan-Qal'a — YuNESKO ro'yxatidagi ochiq osmon muzeyi."},
        {"name": "🇹🇷 Istanbul", "desc": "Ikki qit'ani bog'laydigan shahar, Ayasofya."},
        {"name": "🇦🇪 Dubay", "desc": "Zamonaviy me'morchilik, Burj Khalifa, cho'l safari."},
        {"name": "🇫🇷 Parij", "desc": "Eyfel minorasi, Luvr muzeyi, romantik muhit."},
        {"name": "🇮🇩 Bali", "desc": "Tropik plyajlar, guruch teraslari, sörf."},
        {"name": "🇮🇹 Rim", "desc": "Kolizey, Vatikan, boy antik tarix."},
    ]

    @classmethod
    def random_pick(cls, count: int = 3) -> List[Dict[str, str]]:
        return random.sample(cls.DESTINATIONS, min(count, len(cls.DESTINATIONS)))

    @staticmethod
    def format_message(destinations: List[Dict[str, str]]) -> str:
        lines = ["✈️ <b>Sayohat tavsiyalari</b>\n━━━━━━━━━━━━━━━"]
        for d in destinations:
            lines.append(f"\n📍 <b>{d['name']}</b>\n{d['desc']}")
        return "\n".join(lines)


travel_service = TravelService()


# =================================================================================
# 17. YO'L TIRBANDLIGI — STUB (kelajakda TomTom/Yandex API bilan almashtiriladi)
# =================================================================================

class TrafficService:
    async def get_traffic_info(self, city: str) -> str:
        # TODO: TomTom/Yandex/Google Traffic API integratsiyasi shu yerga qo'shiladi
        return (
            f"🚗 '{city}' uchun tirbandlik ma'lumoti hozircha mavjud emas.\n"
            "Bu funksiya tez orada qo'shiladi. 🔧"
        )


traffic_service = TrafficService()


# =================================================================================
# NAMOZ VAQTLARI — FAQAT REJA (hozircha ishga tushirilmagan) — PRAYER_TIMES_TODO
# =================================================================================
#
# class PrayerTimesService:
#     BASE_URL = "https://api.aladhan.com/v1/timingsByCity"
#
#     async def get_timings(self, city: str, country: str = "Uzbekistan", method: int = 2):
#         data = await fetch_json(self.BASE_URL, params={"city": city, "country": country, "method": method})
#         if not data or data.get("code") != 200:
#             return None
#         t = data["data"]["timings"]
#         return {
#             "🌅 Bomdod": t.get("Fajr"), "☀️ Quyosh": t.get("Sunrise"), "🕌 Peshin": t.get("Dhuhr"),
#             "🌇 Asr": t.get("Asr"), "🌆 Shom": t.get("Maghrib"), "🌙 Xufton": t.get("Isha"),
#         }
#
# Handler qo'shilganda /namoz komandasi bilan chaqiriladi. HOZIRCHA FAOLLASHTIRILMAGAN.
#
# =================================================================================


# =================================================================================
# 18. INLINE MENYU (rangli/emojili tugmalar)
# =================================================================================

def main_menu_keyboard() -> InlineKeyboardMarkup:
    buttons = [
        [InlineKeyboardButton("🌤 Ob-havo", callback_data="menu:weather"),
         InlineKeyboardButton("👕 Kiyim maslahati", callback_data="menu:clothing")],
        [InlineKeyboardButton("💱 Valyuta", callback_data="menu:currency"),
         InlineKeyboardButton("📰 Yangiliklar", callback_data="menu:news")],
        [InlineKeyboardButton("🎬 Kino", callback_data="menu:movie"),
         InlineKeyboardButton("📚 Kitoblar", callback_data="menu:book")],
        [InlineKeyboardButton("⚽️ Sport", callback_data="menu:sport"),
         InlineKeyboardButton("✈️ Sayohat", callback_data="menu:travel")],
        [InlineKeyboardButton("🏥 Dorixona", callback_data="menu:pharmacy"),
         InlineKeyboardButton("🛒 Do'kon", callback_data="menu:shop")],
        [InlineKeyboardButton("💡 Fakt/Iqtibos", callback_data="menu:quote"),
         InlineKeyboardButton("💬 Tarjimon", callback_data="menu:translate")],
        [InlineKeyboardButton("📝 Vazifalarim", callback_data="menu:todo"),
         InlineKeyboardButton("🔔 Obuna", callback_data="menu:subscribe")],
        [InlineKeyboardButton("ℹ️ Yordam", callback_data="menu:help")],
    ]
    return InlineKeyboardMarkup(buttons)


def back_button() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Orqaga", callback_data="menu:back")]])


def location_request_keyboard() -> ReplyKeyboardMarkup:
    """Faqat joylashuv so'rash uchun (Telegram inline tugma orqali lokatsiya so'ray olmaydi)."""
    return ReplyKeyboardMarkup(
        [[KeyboardButton("📍 Joylashuvni yuborish", request_location=True)]],
        resize_keyboard=True,
        one_time_keyboard=True,
    )


# =================================================================================
# 19. TELEGRAM HANDLERLAR
# =================================================================================

WELCOME_TEXT = (
    "✨ <b>Assalomu alaykum!</b> ✨\n\n"
    "🤖 Men — <b>Premium Ob-havo Bot</b> 🌈\n"
    "🌤 Real vaqtdagi ob-havo, ⚠️ xavfsizlik ogohlantirishlari, 👕 kiyim maslahati, "
    "💱 valyuta kurslari, 📰 yangiliklar, 🎬 kino, 📚 kitob tavsiyalari va yana ko'p narsa!\n\n"
    "👇 Quyidagi menyudan tanlang yoki shahar nomini yozing (masalan: <i>Toshkent</i>)."
)

HELP_TEXT = (
    "📖 <b>Barcha komandalar</b>\n━━━━━━━━━━━━━━━\n"
    "🌤 /weather &lt;shahar&gt;\n🏙 /setcity &lt;shahar&gt;\n💱 /currency\n📰 /news\n"
    "🎬 /movie &lt;nomi&gt;\n📚 /book &lt;janr&gt;\n⚽️ /sport &lt;epl|laliga|uefa|seriea&gt;\n"
    "✈️ /travel\n💡 /quote  🧠 /fact\n💬 /translate &lt;til&gt; &lt;matn&gt;\n"
    "📝 /todo_add /todo_list /todo_done /todo_del\n"
    f"🔔 /subscribe (kunlik {DAILY_BROADCAST_HOUR:02d}:{DAILY_BROADCAST_MINUTE:02d}) /unsubscribe\n"
    "📍 Joylashuv yuborsangiz — shu joy ob-havosi va yaqin dorixona/do'konlarni topaman."
)


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    username = update.effective_user.username if update.effective_user else None
    await upsert_user(chat_id, username)
    await update.message.reply_text(WELCOME_TEXT, parse_mode=ParseMode.HTML, reply_markup=main_menu_keyboard())


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(HELP_TEXT, parse_mode=ParseMode.HTML, reply_markup=back_button())


async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.error("💥 Botda kutilmagan xatolik:", exc_info=context.error)
    if isinstance(update, Update) and update.effective_message:
        try:
            await update.effective_message.reply_text("⚠️ Kechirasiz, xatolik yuz berdi. Birozdan so'ng qayta urinib ko'ring.")
        except TelegramError:
            pass


# ---- Ob-havo ----

async def _send_weather_report(chat, weather: Dict[str, Any]) -> None:
    await chat.send_message(WeatherService.format_message(weather), parse_mode=ParseMode.HTML)
    warnings = SafetyAdvisor.analyze(weather)
    await chat.send_message("\n\n".join(warnings), parse_mode=ParseMode.HTML)
    await chat.send_message(f"👕 <b>Kiyim maslahati:</b>\n{ClothingAdvisor.suggest(weather)}", parse_mode=ParseMode.HTML)


async def cmd_weather(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    if context.args:
        city = " ".join(context.args)
    else:
        user = await get_user(chat_id)
        if user and user.get("city"):
            city = user["city"]
        elif user and user.get("lat") and user.get("lon"):
            weather = await weather_service.get_by_coords(user["lat"], user["lon"])
            if weather:
                await _send_weather_report(update.effective_chat, weather)
            else:
                await update.effective_message.reply_text("⚠️ Ob-havo ma'lumotini olib bo'lmadi.")
            return
        else:
            await update.effective_message.reply_text(
                "🏙 Shahar nomini kiriting: <code>/weather Toshkent</code>\n"
                "yoki 📍 joylashuvingizni yuboring:",
                parse_mode=ParseMode.HTML,
                reply_markup=location_request_keyboard(),
            )
            return

    weather = await weather_service.get_by_city(city)
    if not weather:
        await update.effective_message.reply_text(f"⚠️ '{city}' uchun ob-havo topilmadi. Nomni tekshiring.")
        return
    await _send_weather_report(update.effective_chat, weather)


async def cmd_setcity(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    if not context.args:
        await update.message.reply_text("Foydalanish: <code>/setcity Toshkent</code>", parse_mode=ParseMode.HTML)
        return
    city = " ".join(context.args)
    weather = await weather_service.get_by_city(city)
    if not weather:
        await update.message.reply_text(f"⚠️ '{city}' shahri topilmadi.")
        return
    await set_user_city(chat_id, weather["city"])
    await update.message.reply_text(f"✅ Standart shahringiz: <b>{weather['city']}</b>", parse_mode=ParseMode.HTML)


async def on_location_received(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    loc = update.message.location
    await set_user_location(chat_id, loc.latitude, loc.longitude)
    weather = await weather_service.get_by_coords(loc.latitude, loc.longitude)
    if not weather:
        await update.message.reply_text("⚠️ Joylashuvingiz bo'yicha ob-havo topilmadi.")
        return
    await _send_weather_report(update.effective_chat, weather)
    pending = context.user_data.pop("pending_location_action", None)
    if pending == "pharmacy":
        await _handle_nearby(update, context, "pharmacy")
    elif pending == "shop":
        await _handle_nearby(update, context, "supermarket")


# ---- Valyuta / fakt / sayohat / tarjimon ----

async def cmd_currency(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    rates = await currency_service.get_rates()
    target = update.effective_message
    if not rates:
        await target.reply_text("⚠️ Valyuta kurslarini olishda xatolik.")
        return
    await target.reply_text(CurrencyService.format_message(rates), parse_mode=ParseMode.HTML)


async def cmd_quote(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    quote = await quote_fact_service.get_quote()
    fact = await quote_fact_service.get_fact()
    await update.effective_message.reply_text(
        f"{quote}\n\n🧠 <b>Qiziqarli fakt:</b>\n{fact}", parse_mode=ParseMode.HTML
    )


async def cmd_travel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    destinations = TravelService.random_pick(3)
    await update.effective_message.reply_text(TravelService.format_message(destinations), parse_mode=ParseMode.HTML)


async def cmd_translate(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if len(context.args) < 2:
        await update.message.reply_text(
            "Foydalanish: <code>/translate en Salom dunyo</code>", parse_mode=ParseMode.HTML
        )
        return
    target_lang = context.args[0]
    text = " ".join(context.args[1:])
    result = await TranslatorService.translate(text, target_lang)
    await update.message.reply_text(f"🌐 <b>Tarjima ({target_lang}):</b>\n{result}", parse_mode=ParseMode.HTML)


# ---- To-do ----

async def cmd_todo_add(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        await update.message.reply_text("Foydalanish: <code>/todo_add Kitob o'qish</code>", parse_mode=ParseMode.HTML)
        return
    task = " ".join(context.args)
    task_id = await TodoManager.add_task(update.effective_chat.id, task)
    await update.message.reply_text(f"✅ Vazifa qo'shildi (#{task_id}): {task}")


async def cmd_todo_list(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    tasks = await TodoManager.list_tasks(update.effective_chat.id)
    await update.effective_message.reply_text(TodoManager.format_list(tasks), parse_mode=ParseMode.HTML)


async def cmd_todo_done(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args or not context.args[0].isdigit():
        await update.message.reply_text("Foydalanish: <code>/todo_done id</code>", parse_mode=ParseMode.HTML)
        return
    await TodoManager.mark_done(update.effective_chat.id, int(context.args[0]))
    await update.message.reply_text("✅ Vazifa bajarilgan deb belgilandi.")


async def cmd_todo_del(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args or not context.args[0].isdigit():
        await update.message.reply_text("Foydalanish: <code>/todo_del id</code>", parse_mode=ParseMode.HTML)
        return
    await TodoManager.delete_task(update.effective_chat.id, int(context.args[0]))
    await update.message.reply_text("🗑 Vazifa o'chirildi.")


# ---- Yangiliklar / kino / kitob / sport ----

async def cmd_news(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = " ".join(context.args) if context.args else "Uzbekistan"
    target = update.effective_message
    articles = await news_service.get_news(query)
    if articles is None and not NEWSAPI_KEY:
        await target.reply_text(
            "⚠️ Yangiliklar xizmati sozlanmagan. .env faylida <code>NEWSAPI_KEY</code> ko'rsating "
            "(https://newsapi.org — bepul).",
            parse_mode=ParseMode.HTML,
        )
        return
    await target.reply_text(
        NewsService.format_message(articles or []), parse_mode=ParseMode.HTML, disable_web_page_preview=True
    )


async def cmd_movie(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        await update.message.reply_text("Foydalanish: <code>/movie Inception</code>", parse_mode=ParseMode.HTML)
        return
    if not OMDB_API_KEY:
        await update.message.reply_text(
            "⚠️ Kino xizmati sozlanmagan. .env faylida <code>OMDB_API_KEY</code> ko'rsating "
            "(https://omdbapi.com — bepul).",
            parse_mode=ParseMode.HTML,
        )
        return
    title = " ".join(context.args)
    movie = await movie_service.search(title)
    if not movie:
        await update.message.reply_text(f"⚠️ '{title}' nomli kino topilmadi.")
        return
    await update.message.reply_text(MovieService.format_message(movie), parse_mode=ParseMode.HTML)


async def cmd_book(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    genre = " ".join(context.args) if context.args else "fiction"
    target = update.effective_message
    books = await book_service.search_by_genre(genre)
    await target.reply_text(BookService.format_message(books or [], genre), parse_mode=ParseMode.HTML)


async def cmd_sport(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    league = context.args[0] if context.args else "epl"
    target = update.effective_message
    if league.lower() not in SportsService.LEAGUE_IDS:
        available = ", ".join(SportsService.LEAGUE_IDS.keys())
        await target.reply_text(f"⚠️ Noma'lum liga. Mavjud: {available}")
        return
    results = await sports_service.get_last_results(league)
    await target.reply_text(SportsService.format_message(results or [], league), parse_mode=ParseMode.HTML)


# ---- Yaqin dorixona/do'kon ----

async def cmd_nearby_pharmacy(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _handle_nearby(update, context, "pharmacy")


async def cmd_nearby_shop(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _handle_nearby(update, context, "supermarket")


async def _handle_nearby(update: Update, context: ContextTypes.DEFAULT_TYPE, place_type: str) -> None:
    chat_id = update.effective_chat.id
    user = await get_user(chat_id)
    target = update.effective_message
    if not user or not user.get("lat") or not user.get("lon"):
        context.user_data["pending_location_action"] = place_type if place_type == "pharmacy" else "shop"
        await target.reply_text(
            "📍 Iltimos, avval joylashuvingizni yuboring:", reply_markup=location_request_keyboard()
        )
        return
    places = await place_finder_service.find_nearby(user["lat"], user["lon"], place_type)
    await target.reply_text(
        PlaceFinderService.format_message(places or [], place_type),
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
    )


# ---- Yo'l tirbandligi ----

async def cmd_traffic(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    city = " ".join(context.args) if context.args else "Toshkent"
    info = await traffic_service.get_traffic_info(city)
    await update.message.reply_text(info)


# ---- Obuna ----

async def cmd_subscribe(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    await upsert_user(chat_id)
    await db_execute("UPDATE users SET subscribed_daily = 1 WHERE chat_id = ?", (chat_id,))
    await update.effective_message.reply_text(
        f"🔔 Obuna faollashtirildi! Har kuni soat "
        f"<b>{DAILY_BROADCAST_HOUR:02d}:{DAILY_BROADCAST_MINUTE:02d}</b> da ob-havo, hikmat va fakt yuboriladi.",
        parse_mode=ParseMode.HTML,
    )


async def cmd_unsubscribe(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    await db_execute("UPDATE users SET subscribed_daily = 0 WHERE chat_id = ?", (chat_id,))
    await update.effective_message.reply_text("🔕 Obuna bekor qilindi.")


# ---- Inline menyu callback router ----

async def on_callback_query(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()  # yuklanish "soat" belgisini olib tashlaydi
    data = query.data

    if data == "menu:back":
        await query.edit_message_text(WELCOME_TEXT, parse_mode=ParseMode.HTML, reply_markup=main_menu_keyboard())
        return

    if data == "menu:weather":
        await query.message.reply_text(
            "🏙 Shahar nomini yozing: <code>/weather Toshkent</code>\nyoki joylashuvingizni yuboring:",
            parse_mode=ParseMode.HTML,
            reply_markup=location_request_keyboard(),
        )
    elif data == "menu:clothing":
        await cmd_weather(update, context)  # ob-havo bilan birga kiyim maslahati ham chiqadi
    elif data == "menu:currency":
        await cmd_currency(update, context)
    elif data == "menu:news":
        await cmd_news(update, context)
    elif data == "menu:movie":
        await query.message.reply_text("🎬 Kino nomini yozing: <code>/movie Inception</code>", parse_mode=ParseMode.HTML)
    elif data == "menu:book":
        await query.message.reply_text("📚 Janrni yozing: <code>/book fantasy</code>", parse_mode=ParseMode.HTML)
    elif data == "menu:sport":
        await query.message.reply_text(
            "⚽️ Liga tanlang: <code>/sport epl</code> | laliga | uefa | seriea", parse_mode=ParseMode.HTML
        )
    elif data == "menu:travel":
        await cmd_travel(update, context)
    elif data == "menu:pharmacy":
        await cmd_nearby_pharmacy(update, context)
    elif data == "menu:shop":
        await cmd_nearby_shop(update, context)
    elif data == "menu:quote":
        await cmd_quote(update, context)
    elif data == "menu:translate":
        await query.message.reply_text(
            "💬 Foydalanish: <code>/translate en Salom, qandaysiz?</code>", parse_mode=ParseMode.HTML
        )
    elif data == "menu:todo":
        await cmd_todo_list(update, context)
    elif data == "menu:subscribe":
        await cmd_subscribe(update, context)
    elif data == "menu:help":
        await query.message.reply_text(HELP_TEXT, parse_mode=ParseMode.HTML, reply_markup=back_button())


async def on_text_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Oddiy matn (shahar nomi) yuborilganda ishlaydi — inline menyu asosiy navigatsiya."""
    text = update.message.text.strip()
    weather = await weather_service.get_by_city(text)
    if weather:
        await _send_weather_report(update.effective_chat, weather)
    else:
        await update.message.reply_text(
            "🤔 Buni tushunmadim. Shahar nomi kiriting yoki menyudan foydalaning:",
            reply_markup=main_menu_keyboard(),
        )


# =================================================================================
# 20. KUNLIK AVTOMATIK XABAR — PTB'ning O'Z ICHKI JobQueue'si orqali (XAVFSIZ)
# =================================================================================
#
# MUHIM: Bu yerda ATAYLAB tashqi `apscheduler.AsyncIOScheduler` ISHLATILMAYDI,
# chunki aynan o'sha yondashuv Render'da "no current event loop" xatoligiga
# olib kelgan edi. `application.job_queue` PTB kutubxonasining o'zi tomonidan,
# botning YAGONA asyncio event loop'i ichida ishga tushiriladi — shuning uchun
# bunday muammo umuman yuzaga kelmaydi.
#
async def daily_broadcast_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    users = await get_all_subscribed_users()
    logger.info("📤 Kunlik xabar %d foydalanuvchiga yuborilmoqda...", len(users))
    quote = await quote_fact_service.get_quote()
    fact = await quote_fact_service.get_fact()

    for user in users:
        chat_id = user["chat_id"]
        try:
            parts = [f"☀️ <b>Xayrli tong!</b>\n\n{quote}\n\n🧠 {fact}"]
            weather = None
            if user.get("city"):
                weather = await weather_service.get_by_city(user["city"])
            elif user.get("lat") and user.get("lon"):
                weather = await weather_service.get_by_coords(user["lat"], user["lon"])
            if weather:
                parts.append(WeatherService.format_message(weather))
                parts.append("\n".join(SafetyAdvisor.analyze(weather)))
                parts.append(f"👕 {ClothingAdvisor.suggest(weather)}")

            await context.bot.send_message(chat_id=chat_id, text="\n\n".join(parts), parse_mode=ParseMode.HTML)
        except TelegramError as e:
            logger.warning("Foydalanuvchi %s ga xabar yuborilmadi: %s", chat_id, e)
        await asyncio.sleep(0.05)


# =================================================================================
# 21. ASOSIY QURILISH VA ISHGA TUSHIRISH
# =================================================================================

def build_application() -> Application:
    application = ApplicationBuilder().token(BOT_TOKEN).build()

    application.add_handler(CommandHandler("start", cmd_start))
    application.add_handler(CommandHandler("help", cmd_help))

    application.add_handler(CommandHandler("weather", cmd_weather))
    application.add_handler(CommandHandler("setcity", cmd_setcity))
    application.add_handler(MessageHandler(filters.LOCATION, on_location_received))

    application.add_handler(CommandHandler("currency", cmd_currency))
    application.add_handler(CommandHandler("news", cmd_news))
    application.add_handler(CommandHandler("movie", cmd_movie))
    application.add_handler(CommandHandler("book", cmd_book))
    application.add_handler(CommandHandler("sport", cmd_sport))
    application.add_handler(CommandHandler("travel", cmd_travel))
    application.add_handler(CommandHandler("traffic", cmd_traffic))

    application.add_handler(CommandHandler("quote", cmd_quote))
    application.add_handler(CommandHandler("fact", cmd_quote))
    application.add_handler(CommandHandler("translate", cmd_translate))

    application.add_handler(CommandHandler("todo_add", cmd_todo_add))
    application.add_handler(CommandHandler("todo_list", cmd_todo_list))
    application.add_handler(CommandHandler("todo_done", cmd_todo_done))
    application.add_handler(CommandHandler("todo_del", cmd_todo_del))

    application.add_handler(CommandHandler("subscribe", cmd_subscribe))
    application.add_handler(CommandHandler("unsubscribe", cmd_unsubscribe))

    application.add_handler(CommandHandler("pharmacy", cmd_nearby_pharmacy))
    application.add_handler(CommandHandler("shop", cmd_nearby_shop))

    application.add_handler(CallbackQueryHandler(on_callback_query, pattern=r"^menu:"))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text_message))

    application.add_error_handler(on_error)
    return application


async def post_init(application: Application) -> None:
    """Bot ishga tushgach chaqiriladi: baza tayyorlanadi va kunlik job rejalashtiriladi."""
    init_database()
    # PTB'ning o'z JobQueue'si — hech qanday tashqi event-loop muammosisiz ishlaydi
    application.job_queue.run_daily(
        daily_broadcast_job,
        time=dtime(hour=DAILY_BROADCAST_HOUR, minute=DAILY_BROADCAST_MINUTE, tzinfo=TZ),
        name="daily_broadcast",
    )
    logger.info(
        "🚀 Bot muvaffaqiyatli ishga tushdi. Kunlik xabar har kuni %02d:%02d (%s) da yuboriladi.",
        DAILY_BROADCAST_HOUR, DAILY_BROADCAST_MINUTE, TIMEZONE_NAME,
    )


def main() -> None:
    application = build_application()
    application.post_init = post_init
    logger.info("⏳ Bot ishga tushmoqda...")
    # MUHIM: faqat run_polling ishlatiladi — hech qanday webhook/start_webhook chaqirilmaydi.
    # drop_pending_updates=True — qayta ishga tushganda eski/osilib qolgan yangilanishlarni tashlab yuboradi
    # (bu ham "Conflict"/eventloop bilan bog'liq g'alati holatlarning oldini oladi).
    application.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)


if __name__ == "__main__":
    main()
