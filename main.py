#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
=================================================================================
 KO'P FUNKSIYALI TELEGRAM OB-HAVO BOTI
 O'zbekiston va dunyo hududlari uchun mo'ljallangan
=================================================================================

Muallif rejasi (arxitektura):
    - WeatherService        -> OpenWeatherMap orqali real vaqtdagi ob-havo
    - SafetyAdvisor         -> ob-havoga qarab xavfsizlik ogohlantirishlari
    - ClothingAdvisor       -> haroratga mos kiyim tavsiyalari
    - CurrencyService       -> valyuta kurslari (USD/EUR/RUB -> UZS)
    - QuoteFactService      -> kunning hikmatli so'zi / qiziqarli fakti
    - TranslatorService     -> avtomatik tarjimon
    - TodoManager           -> eslatmalar va to-do ro'yxati (SQLite)
    - NewsService           -> yangiliklar lentasi
    - MovieService          -> kino sharhlari (OMDb)
    - BookService           -> kitob/audiokitob tavsiyalari (Google Books)
    - SportsService         -> sport natijalari (TheSportsDB)
    - PlaceFinderService    -> yaqin atrofdagi dorixona/do'kon (OpenStreetMap)
    - TravelService         -> mashhur sayohat yo'nalishlari
    - TrafficService        -> yo'l tirbandligi (stub, kelajakda kengaytiriladi)

    NAMOZ VAQTLARI integratsiyasi ataylab hozircha FAQAT KOMMENTARIYA sifatida
    qoldirilgan (fayl oxiriga yaqin "PRAYER_TIMES_TODO" bo'limiga qarang).

Talab qilinadigan muhit o'zgaruvchilari (.env faylida):
    TELEGRAM_BOT_TOKEN         - Telegram bot tokeni (majburiy)
    OPENWEATHERMAP_API_KEY     - OpenWeatherMap API kaliti (majburiy)
    NEWSAPI_KEY                - NewsAPI.org kaliti (ixtiyoriy)
    OMDB_API_KEY                - OMDb API kaliti (ixtiyoriy)
    GOOGLE_BOOKS_API_KEY        - Google Books API kaliti (ixtiyoriy)
    THESPORTSDB_API_KEY        - TheSportsDB kaliti (ixtiyoriy, default "3")
    TIMEZONE                   - masalan "Asia/Tashkent"
    DB_PATH                     - SQLite fayl yo'li

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
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Tuple

import requests
import pytz
from dotenv import load_dotenv

try:
    from deep_translator import GoogleTranslator
except ImportError:  # kutubxona o'rnatilmagan bo'lsa, tarjimon funksiyasi o'chadi
    GoogleTranslator = None

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
    KeyboardButton,
)
from telegram.constants import ParseMode
from telegram.error import TelegramError
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ConversationHandler,
    ContextTypes,
    filters,
)
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger


# =================================================================================
# 1. SOZLAMALAR VA MUHIT O'ZGARUVCHILARI
# =================================================================================

load_dotenv()

BOT_TOKEN: Optional[str] = os.getenv("TELEGRAM_BOT_TOKEN")
OWM_API_KEY: Optional[str] = os.getenv("OPENWEATHERMAP_API_KEY")
NEWSAPI_KEY: str = os.getenv("NEWSAPI_KEY", "")
OMDB_API_KEY: str = os.getenv("OMDB_API_KEY", "")
GOOGLE_BOOKS_API_KEY: str = os.getenv("GOOGLE_BOOKS_API_KEY", "")
THESPORTSDB_API_KEY: str = os.getenv("THESPORTSDB_API_KEY", "3")  # "3" - ochiq test kaliti
TIMEZONE_NAME: str = os.getenv("TIMEZONE", "Asia/Tashkent")
DB_PATH: str = os.getenv("DB_PATH", "bot_database.db")
DAILY_BROADCAST_HOUR: int = int(os.getenv("DAILY_BROADCAST_HOUR", "8"))
DAILY_BROADCAST_MINUTE: int = int(os.getenv("DAILY_BROADCAST_MINUTE", "0"))

if not BOT_TOKEN:
    raise RuntimeError(
        "TELEGRAM_BOT_TOKEN topilmadi. .env faylida TELEGRAM_BOT_TOKEN='...' ko'rsating."
    )
if not OWM_API_KEY:
    raise RuntimeError(
        "OPENWEATHERMAP_API_KEY topilmadi. .env faylida OPENWEATHERMAP_API_KEY='...' ko'rsating."
    )

TZ = pytz.timezone(TIMEZONE_NAME)

# =================================================================================
# 2. LOGGING SOZLAMALARI
# =================================================================================

logging.basicConfig(
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    level=logging.INFO,
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("bot.log", encoding="utf-8"),
    ],
)
# kutubxonalarning ortiqcha "debug" xabarlarini kamaytiramiz
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("apscheduler").setLevel(logging.WARNING)
logger = logging.getLogger("weather_bot")


# =================================================================================
# 3. YORDAMCHI FUNKSIYALAR (HTTP so'rovlar, DB ishga tushirish)
# =================================================================================

DEFAULT_TIMEOUT = 10  # soniya


async def fetch_json(
    url: str,
    params: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    timeout: int = DEFAULT_TIMEOUT,
) -> Optional[Dict[str, Any]]:
    """
    Tashqi API'larga bloklanmaydigan (non-blocking) tarzda GET so'rov yuboradi.
    `requests` kutubxonasi sinxron bo'lgani uchun uni alohida thread'da ishga
    tushiramiz, shunda asyncio event loop bloklanib qolmaydi.
    """
    loop = asyncio.get_running_loop()
    try:
        func = functools.partial(
            requests.get, url, params=params, headers=headers, timeout=timeout
        )
        response = await loop.run_in_executor(None, func)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.Timeout:
        logger.warning("So'rov vaqti tugadi (timeout): %s", url)
    except requests.exceptions.HTTPError as e:
        logger.warning("HTTP xatolik: %s -> %s", url, e)
    except requests.exceptions.RequestException as e:
        logger.warning("Tarmoq xatoligi: %s -> %s", url, e)
    except (ValueError, json.JSONDecodeError):
        logger.warning("JSON parslashda xatolik: %s", url)
    return None


def init_database() -> None:
    """SQLite bazasini va kerakli jadvallarni yaratadi (birinchi ishga tushganda)."""
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                chat_id INTEGER PRIMARY KEY,
                username TEXT,
                city TEXT,
                lat REAL,
                lon REAL,
                lang TEXT DEFAULT 'uz',
                subscribed_daily INTEGER DEFAULT 0,
                created_at TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS todos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                task TEXT NOT NULL,
                is_done INTEGER DEFAULT 0,
                created_at TEXT
            )
            """
        )
        conn.commit()
    logger.info("Ma'lumotlar bazasi tayyor: %s", DB_PATH)


def _db_execute(query: str, params: Tuple = (), fetch: bool = False) -> Any:
    """Sinxron SQLite so'rovini bajaruvchi ichki funksiya (thread ichida chaqiriladi)."""
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute(query, params)
        if fetch:
            rows = cur.fetchall()
            return [dict(r) for r in rows]
        conn.commit()
        return cur.lastrowid


async def db_execute(query: str, params: Tuple = (), fetch: bool = False) -> Any:
    """`_db_execute` ni asosiy event loop'ni bloklamasdan chaqiradi."""
    loop = asyncio.get_running_loop()
    func = functools.partial(_db_execute, query, params, fetch)
    return await loop.run_in_executor(None, func)


async def upsert_user(chat_id: int, username: Optional[str] = None) -> None:
    """Foydalanuvchini bazaga qo'shadi (mavjud bo'lsa, o'zgartirmaydi)."""
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
# 4. OB-HAVO XIZMATI (OpenWeatherMap) — HOZIRGI VAQTDAGI HARORAT
# =================================================================================

class WeatherService:
    """
    OpenWeatherMap "Current Weather Data" (data/2.5/weather) endpoint'i orqali
    FAQAT hozirgi (real-time) ob-havo ma'lumotini oladi. Prognoz (forecast) ishlatilmaydi,
    chunki talabga ko'ra faqat aniq hozirgi holat kerak.
    """

    BASE_URL = "https://api.openweathermap.org/data/2.5/weather"

    def __init__(self, api_key: str):
        self.api_key = api_key

    async def get_by_city(self, city: str) -> Optional[Dict[str, Any]]:
        params = {
            "q": city,
            "appid": self.api_key,
            "units": "metric",
            "lang": "uz",
        }
        data = await fetch_json(self.BASE_URL, params=params)
        return self._parse(data)

    async def get_by_coords(self, lat: float, lon: float) -> Optional[Dict[str, Any]]:
        params = {
            "lat": lat,
            "lon": lon,
            "appid": self.api_key,
            "units": "metric",
            "lang": "uz",
        }
        data = await fetch_json(self.BASE_URL, params=params)
        return self._parse(data)

    @staticmethod
    def _parse(data: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """API javobini botga qulay formatga o'tkazadi."""
        if not data or data.get("cod") not in (200, "200"):
            return None
        try:
            weather_block = data["weather"][0]
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
                "description": weather_block.get("description", "").capitalize(),
                "main_condition": weather_block.get("main", ""),  # Rain, Snow, Clear, ...
                "icon": weather_block.get("icon", ""),
                "sunrise": data["sys"].get("sunrise"),
                "sunset": data["sys"].get("sunset"),
                "timezone_offset": data.get("timezone", 0),
                "observed_at": datetime.now(TZ).strftime("%Y-%m-%d %H:%M"),
            }
        except (KeyError, IndexError, TypeError) as e:
            logger.error("Ob-havo javobini parslashda xatolik: %s", e)
            return None

    @staticmethod
    def format_message(w: Dict[str, Any]) -> str:
        icon_map = {
            "Clear": "☀️", "Clouds": "☁️", "Rain": "🌧",
            "Drizzle": "🌦", "Thunderstorm": "⛈", "Snow": "❄️",
            "Mist": "🌫", "Fog": "🌫", "Haze": "🌫",
        }
        emoji = icon_map.get(w["main_condition"], "🌡")
        return (
            f"{emoji} <b>{w['city']}, {w['country']}</b>\n"
            f"🕒 Hozirgi holat ({w['observed_at']})\n\n"
            f"🌡 Harorat: <b>{w['temp']}°C</b> (sezilishi: {w['feels_like']}°C)\n"
            f"📉 Min/Maks: {w['temp_min']}°C / {w['temp_max']}°C\n"
            f"📝 Holat: {w['description']}\n"
            f"💧 Namlik: {w['humidity']}%\n"
            f"🧭 Bosim: {w['pressure']} hPa\n"
            f"💨 Shamol: {w['wind_speed']} m/s\n"
            f"👁 Ko'rinish: {w['visibility_m']/1000:.1f} km"
        )


weather_service = WeatherService(OWM_API_KEY)


# =================================================================================
# 5. XAVFSIZLIK OGOHLANTIRISH TIZIMI (SafetyAdvisor)
# =================================================================================

class SafetyAdvisor:
    """
    Ob-havo ko'rsatkichlariga asoslanib, foydalanuvchiga tegishli xavfsizlik
    ogohlantirishlarini shakllantiradi. Har bir chegara qiymat (threshold)
    O'zbekiston iqlim sharoitlarini hisobga olgan holda tanlangan.
    """

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
        temp = w["temp"]
        feels = w["feels_like"]
        condition = w["main_condition"]
        wind = w["wind_speed"]
        visibility = w["visibility_m"]
        humidity = w["humidity"]

        # --- Haddan tashqari issiqlik ---
        if temp >= cls.EXTREME_HEAT_C or feels >= cls.EXTREME_HEAT_C:
            warnings.append(
                "🔥 <b>OGOHLANTIRISH: Haddan tashqari issiqlik!</b>\n"
                "Iloji boricha soyada yoki xonada bo'ling, kuniga kamida 2-3 litr suv iching, "
                "kunning eng issiq vaqtida (12:00-17:00) ochiq quyoshda yurmang, "
                "bosh kiyim va yengil kiyim taqing."
            )
        elif temp >= cls.HIGH_HEAT_C or feels >= cls.HIGH_HEAT_C:
            warnings.append(
                "🌡 <b>Diqqat: Havo juda issiq.</b>\n"
                "Ko'proq suv iching, quyoshdan himoyalaning, jismoniy faollikni cheklang."
            )

        # --- Yuqori namlik + issiqlik (issiqlik indeksi xavfi) ---
        if temp >= cls.HIGH_HEAT_C and humidity >= 60:
            warnings.append(
                "💦 Yuqori namlik tufayli issiqlikni his qilish kuchayadi — "
                "salqin joyda tez-tez dam oling."
            )

        # --- Haddan tashqari sovuq ---
        if temp <= cls.EXTREME_COLD_C or feels <= cls.EXTREME_COLD_C:
            warnings.append(
                "🥶 <b>OGOHLANTIRISH: Qattiq sovuq!</b>\n"
                "Terining ochiq qolishidan saqlaning (sovuq urishi xavfi bor), "
                "issiq va bir necha qatlamli kiyim kiying, uzoq vaqt tashqarida qolmang, "
                "muzlagan yo'llarda ehtiyot bo'ling."
            )
        elif temp <= cls.COLD_C or feels <= cls.COLD_C:
            warnings.append(
                "❄️ <b>Diqqat: Havo sovuq.</b>\n"
                "Issiqroq kiyinib chiqing, qo'lqop va sharf taqishni unutmang."
            )

        # --- Yomg'ir / momaqaldiroq ---
        if condition == "Thunderstorm":
            warnings.append(
                "⛈ <b>OGOHLANTIRISH: Momaqaldiroq!</b>\n"
                "Ochiq maydonlarda, baland daraxtlar tagida va suv havzalari yaqinida "
                "qolmang, elektr asboblardan ehtiyot bo'ling."
            )
        elif condition in ("Rain", "Drizzle"):
            warnings.append(
                "🌧 Yomg'ir yog'moqda — soyabon oling, yo'llar sirpanchiq bo'lishi mumkin, "
                "haydash paytida ehtiyot bo'ling."
            )

        # --- Qor ---
        if condition == "Snow":
            warnings.append(
                "🌨 Qor yog'moqda — yo'llar muzlashi mumkin, mashinada zanjir/qishki shina "
                "borligiga ishonch hosil qiling, piyoda yurganda ehtiyot bo'ling."
            )

        # --- Kuchli shamol ---
        if wind >= cls.STRONG_WIND_MS:
            warnings.append(
                "💨 <b>OGOHLANTIRISH: Kuchli shamol!</b>\n"
                "Baland inshootlar, reklama taxtalari va daraxtlar yaqinida ehtiyot bo'ling, "
                "imkon qadar uyda qoling."
            )
        elif wind >= cls.MODERATE_WIND_MS:
            warnings.append("🍃 O'rtacha shamol kutilmoqda — ochiq soyabonlardan ehtiyot bo'ling.")

        # --- Past ko'rinish (tuman) ---
        if visibility <= cls.LOW_VISIBILITY_M:
            warnings.append(
                "🌫 Ko'rinish darajasi past (tuman) — avtomobil chiroqlarini yoqing va "
                "tezlikni kamaytiring."
            )

        if not warnings:
            warnings.append("✅ Hozircha maxsus xavfsizlik xavfi yo'q. Yaxshi kun tilaymiz!")

        return warnings


# =================================================================================
# 6. KIYIM TAVSIYASI (ClothingAdvisor)
# =================================================================================

class ClothingAdvisor:
    """Ob-havo ko'rsatkichlariga mos kiyim tanlash bo'yicha tavsiyalar beradi."""

    @staticmethod
    def suggest(w: Dict[str, Any]) -> str:
        temp = w["feels_like"]
        condition = w["main_condition"]
        wind = w["wind_speed"]

        if temp >= 32:
            base = "👕 Yengil, ochiq rangli va keng kiyimlar, shlyapa, quyoshdan ko'zoynak."
        elif temp >= 24:
            base = "👚 Yengil futbolka/ko'ylak, shim yoki yubka, qulay poyabzal."
        elif temp >= 16:
            base = "🧥 Yengil куртка yoki kardigan, uzun yeng."
        elif temp >= 8:
            base = "🧥 Issiqroq куртка, sviter, yopiq poyabzal."
        elif temp >= 0:
            base = "🧣 Qishki palto, sharf, qalin sviter, issiq poyabzal."
        else:
            base = "🥶 Puxovik, qalin qo'lqop, shapka, termokiyim va issiq etik."

        extra = []
        if condition in ("Rain", "Drizzle", "Thunderstorm"):
            extra.append("☂️ soyabon yoki yomg'irpana")
        if condition == "Snow":
            extra.append("👢 sirpanmaydigan qishki poyabzal")
        if wind >= 8:
            extra.append("🧢 shamolga chidamli tashqi kiyim")

        if extra:
            base += "\nQo'shimcha: " + ", ".join(extra) + "."
        return base


# =================================================================================
# 7. VALYUTA KURSLARI (CurrencyService)
# =================================================================================

class CurrencyService:
    """
    Bepul, kalitsiz ochiq API (open.er-api.com) orqali valyuta kurslarini oladi.
    Baza sifatida USD ishlatiladi, so'ngra kerakli valyutalarga hisoblanadi.
    """

    BASE_URL = "https://open.er-api.com/v6/latest/USD"
    TARGET_CURRENCIES = ["UZS", "EUR", "RUB", "GBP", "KZT"]

    async def get_rates(self) -> Optional[Dict[str, float]]:
        data = await fetch_json(self.BASE_URL)
        if not data or data.get("result") != "success":
            return None
        rates = data.get("rates", {})
        return {cur: rates[cur] for cur in self.TARGET_CURRENCIES if cur in rates}

    @staticmethod
    def format_message(rates: Dict[str, float]) -> str:
        usd_to_uzs = rates.get("UZS")
        lines = ["💱 <b>Valyuta kurslari (1 USD asosida)</b>\n"]
        for cur, val in rates.items():
            if cur == "UZS":
                lines.append(f"🇺🇸 1 USD = {val:,.0f} so'm")
            else:
                lines.append(f"1 USD = {val:.3f} {cur}")
        if usd_to_uzs:
            eur_to_uzs = usd_to_uzs / rates["EUR"] if "EUR" in rates else None
            rub_to_uzs = usd_to_uzs / rates["RUB"] if "RUB" in rates else None
            lines.append("")
            if eur_to_uzs:
                lines.append(f"🇪🇺 1 EUR ≈ {eur_to_uzs:,.0f} so'm")
            if rub_to_uzs:
                lines.append(f"🇷🇺 1 RUB ≈ {rub_to_uzs:,.0f} so'm")
        return "\n".join(lines)


currency_service = CurrencyService()


# =================================================================================
# 8. HIKMATLI SO'ZLAR VA QIZIQARLI FAKTLAR (QuoteFactService)
# =================================================================================

class QuoteFactService:
    """
    Tashqi API ishlamay qolgan taqdirda ham bot to'xtab qolmasligi uchun
    lokal (zaxira) ro'yxatlar bilan ta'minlangan.
    """

    QUOTE_API = "https://api.quotable.io/random"
    FACT_API = "https://uselessfacts.jsph.pl/api/v2/facts/random?language=en"

    FALLBACK_QUOTES = [
        "Bilim — kuchdir.",
        "Har bir muvaffaqiyat orqasida ko'plab urinishlar yotadi.",
        "Bugun qilgan mehnating ertangi natijang.",
        "Kichik qadamlar katta yo'lni bosib o'tadi.",
        "Sabr — muvaffaqiyatning kalitidir.",
    ]

    FALLBACK_FACTS = [
        "Asal hech qachon buzilmaydi — arxeologlar minglab yillik asalni yeb ko'rishgan.",
        "Sakkizoyoqning uchta yuragi bor.",
        "Bir kun Yerda 24 soatdan biroz kamroq (23 soat 56 daqiqa) davom etadi.",
        "Bananlar botanik jihatdan rezavorlar hisoblanadi.",
        "Muz suvdan yengilroq, shuning uchun u suzadi.",
    ]

    async def get_quote(self) -> str:
        data = await fetch_json(self.QUOTE_API)
        if data and data.get("content"):
            author = data.get("author", "Noma'lum")
            return f'"{data["content"]}"\n— {author}'
        return random.choice(self.FALLBACK_QUOTES)

    async def get_fact(self) -> str:
        data = await fetch_json(self.FACT_API)
        if data and data.get("text"):
            return data["text"]
        return random.choice(self.FALLBACK_FACTS)


quote_fact_service = QuoteFactService()


# =================================================================================
# 9. TARJIMON (TranslatorService)
# =================================================================================

class TranslatorService:
    """deep-translator kutubxonasi orqali (Google Translate backend) matn tarjima qiladi."""

    @staticmethod
    async def translate(text: str, target_lang: str) -> str:
        if GoogleTranslator is None:
            return "⚠️ Tarjimon kutubxonasi o'rnatilmagan (deep-translator)."
        loop = asyncio.get_running_loop()
        try:
            func = functools.partial(
                GoogleTranslator(source="auto", target=target_lang).translate, text
            )
            result = await loop.run_in_executor(None, func)
            return result or "⚠️ Tarjima qilib bo'lmadi."
        except Exception as e:  # tarjimon kutubxonasi turli xil xatolik chiqarishi mumkin
            logger.warning("Tarjima xatoligi: %s", e)
            return f"⚠️ Tarjima qilishda xatolik yuz berdi. Til kodini tekshiring (masalan: en, ru, uz)."


# =================================================================================
# 10. ESLATMALAR VA TO-DO RO'YXATI (TodoManager)
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
            "SELECT * FROM todos WHERE chat_id = ? ORDER BY is_done ASC, id DESC",
            (chat_id,),
            fetch=True,
        )

    @staticmethod
    async def mark_done(chat_id: int, task_id: int) -> None:
        await db_execute(
            "UPDATE todos SET is_done = 1 WHERE chat_id = ? AND id = ?", (chat_id, task_id)
        )

    @staticmethod
    async def delete_task(chat_id: int, task_id: int) -> None:
        await db_execute("DELETE FROM todos WHERE chat_id = ? AND id = ?", (chat_id, task_id))

    @staticmethod
    def format_list(tasks: List[Dict[str, Any]]) -> str:
        if not tasks:
            return "📝 Sizda hozircha vazifalar yo'q. Qo'shish uchun: /todo_add <matn>"
        lines = ["📝 <b>Sizning vazifalaringiz:</b>\n"]
        for t in tasks:
            mark = "✅" if t["is_done"] else "🔲"
            lines.append(f"{mark} <code>#{t['id']}</code> {t['task']}")
        lines.append("\nBajarish: /todo_done <id>  |  O'chirish: /todo_del <id>")
        return "\n".join(lines)


# =================================================================================
# 11. YANGILIKLAR LENTASI (NewsService)
# =================================================================================

class NewsService:
    """
    NewsAPI.org orqali ishlaydi. Bepul API kalit talab qiladi (.env -> NEWSAPI_KEY).
    Kalit bo'lmasa, foydalanuvchiga aniq xabar ko'rsatiladi (xatolik emas).
    """

    BASE_URL = "https://newsapi.org/v2/everything"

    async def get_news(self, query: str = "Uzbekistan", limit: int = 5) -> Optional[List[Dict[str, str]]]:
        if not NEWSAPI_KEY:
            return None
        params = {
            "q": query,
            "language": "ru",  # O'zbekiston bo'yicha ko'proq rus tilidagi manbalar mavjud
            "sortBy": "publishedAt",
            "pageSize": limit,
            "apiKey": NEWSAPI_KEY,
        }
        data = await fetch_json(self.BASE_URL, params=params)
        if not data or data.get("status") != "ok":
            return None
        articles = data.get("articles", [])[:limit]
        return [
            {"title": a.get("title", ""), "url": a.get("url", ""), "source": a.get("source", {}).get("name", "")}
            for a in articles
        ]

    @staticmethod
    def format_message(articles: List[Dict[str, str]]) -> str:
        if not articles:
            return "📰 Hozircha yangiliklar topilmadi."
        lines = ["📰 <b>So'nggi yangiliklar:</b>\n"]
        for i, a in enumerate(articles, start=1):
            lines.append(f"{i}. <a href='{a['url']}'>{a['title']}</a> ({a['source']})")
        return "\n".join(lines)


news_service = NewsService()


# =================================================================================
# 12. KINO SHARHLARI (MovieService — OMDb API)
# =================================================================================

class MovieService:
    BASE_URL = "https://www.omdbapi.com/"

    async def search(self, title: str) -> Optional[Dict[str, Any]]:
        if not OMDB_API_KEY:
            return None
        params = {"t": title, "apikey": OMDB_API_KEY}
        data = await fetch_json(self.BASE_URL, params=params)
        if not data or data.get("Response") != "True":
            return None
        return data

    @staticmethod
    def format_message(m: Dict[str, Any]) -> str:
        return (
            f"🎬 <b>{m.get('Title')}</b> ({m.get('Year')})\n"
            f"⭐ IMDB reytingi: {m.get('imdbRating', 'N/A')}\n"
            f"🎭 Janr: {m.get('Genre', 'N/A')}\n"
            f"🎥 Rejissyor: {m.get('Director', 'N/A')}\n"
            f"📝 Syujet: {m.get('Plot', 'Mavjud emas')}"
        )


movie_service = MovieService()


# =================================================================================
# 13. KITOB VA AUDIOKITOB TAVSIYALARI (BookService — Google Books API)
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
        results = []
        for item in data["items"][:limit]:
            info = item.get("volumeInfo", {})
            results.append(
                {
                    "title": info.get("title", "Noma'lum"),
                    "authors": ", ".join(info.get("authors", ["Noma'lum muallif"])),
                    "rating": info.get("averageRating", "—"),
                }
            )
        return results

    @staticmethod
    def format_message(books: List[Dict[str, str]], genre: str) -> str:
        if not books:
            return f"📚 '{genre}' janri bo'yicha kitoblar topilmadi."
        lines = [f"📚 <b>'{genre}' janridagi tavsiyalar:</b>\n"]
        for b in books:
            lines.append(f"• <b>{b['title']}</b> — {b['authors']} (⭐ {b['rating']})")
        return "\n".join(lines)


book_service = BookService()


# =================================================================================
# 14. SPORT NATIJALARI (SportsService — TheSportsDB)
# =================================================================================

class SportsService:
    """
    TheSportsDB bepul (test) API kaliti bilan ishlaydi. Muayyan liganing so'nggi
    o'tkazilgan o'yinlari natijalarini qaytaradi.
    """

    LEAGUE_IDS = {
        "epl": "4328",       # Angliya Premer-ligasi
        "laliga": "4335",    # Ispaniya La Liga
        "uefa": "4480",      # UEFA Champions League
        "seriea": "4332",    # Italiya Serie A
    }

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
        events = data["events"][:5]
        return [
            {
                "home": e.get("strHomeTeam", ""),
                "away": e.get("strAwayTeam", ""),
                "home_score": e.get("intHomeScore") or "-",
                "away_score": e.get("intAwayScore") or "-",
                "date": e.get("dateEvent", ""),
            }
            for e in events
        ]

    @staticmethod
    def format_message(results: List[Dict[str, str]], league: str) -> str:
        if not results:
            return f"⚽ '{league}' bo'yicha natijalar topilmadi."
        lines = [f"⚽ <b>So'nggi natijalar ({league.upper()}):</b>\n"]
        for r in results:
            lines.append(f"{r['date']}: {r['home']} {r['home_score']} - {r['away_score']} {r['away']}")
        return "\n".join(lines)


sports_service = SportsService(THESPORTSDB_API_KEY)


# =================================================================================
# 15. YAQIN ATROFDAGI DORIXONA / DO'KON (PlaceFinderService — OpenStreetMap Overpass)
# =================================================================================

class PlaceFinderService:
    """
    OpenStreetMap Overpass API orqali ishlaydi — kalit talab qilinmaydi.
    Foydalanuvchi geolokatsiyasi asosida yaqin atrofdagi dorixona/do'konlarni topadi.
    """

    OVERPASS_URL = "https://overpass-api.de/api/interpreter"

    async def find_nearby(
        self, lat: float, lon: float, place_type: str = "pharmacy", radius_m: int = 1500
    ) -> Optional[List[Dict[str, Any]]]:
        # place_type: "pharmacy" (dorixona) yoki "supermarket" (do'kon)
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
            logger.warning("Overpass API xatoligi: %s", e)
            return None

        elements = data.get("elements", [])[:10]
        results = []
        for el in elements:
            tags = el.get("tags", {})
            results.append(
                {
                    "name": tags.get("name", "Nomsiz"),
                    "lat": el.get("lat"),
                    "lon": el.get("lon"),
                }
            )
        return results

    @staticmethod
    def format_message(places: List[Dict[str, Any]], place_type: str) -> str:
        label = "dorixona" if place_type == "pharmacy" else "supermarket"
        if not places:
            return f"🔍 Yaqin atrofda {label} topilmadi."
        lines = [f"📍 <b>Yaqin atrofdagi {label}lar:</b>\n"]
        for p in places:
            maps_link = f"https://www.google.com/maps?q={p['lat']},{p['lon']}"
            lines.append(f"• <a href='{maps_link}'>{p['name']}</a>")
        return "\n".join(lines)


place_finder_service = PlaceFinderService()


# =================================================================================
# 16. SAYOHAT YO'NALISHLARI (TravelService — statik/kuratsiya qilingan ma'lumot)
# =================================================================================

class TravelService:
    """
    Tashqi to'lov talab qiluvchi sayohat API'lariga bog'lanmaslik uchun,
    mashhur yo'nalishlar haqida qisqa, kuratsiya qilingan ma'lumotlar bazasi.
    Kelajakda istalgan Travel API (masalan, Amadeus) bilan almashtirish mumkin.
    """

    DESTINATIONS = [
        {"name": "Samarqand, O'zbekiston", "desc": "Registon maydoni va Amir Temur davri me'morchiligi bilan mashhur."},
        {"name": "Buxoro, O'zbekiston", "desc": "2000 yildan ortiq tarixga ega, Ark qal'asi va Poi-Kalon majmuasi."},
        {"name": "Xiva, O'zbekiston", "desc": "Ichan-Qal'a — YuNESKO ro'yxatidagi ochiq osmon muzeyi."},
        {"name": "Istanbul, Turkiya", "desc": "Ikki qit'ani bog'laydigan shahar, Ayasofya va Ko'k masjid."},
        {"name": "Dubay, BAA", "desc": "Zamonaviy me'morchilik, Burj Khalifa va cho'l safarlari."},
        {"name": "Parij, Fransiya", "desc": "Eyfel minorasi, Luvr muzeyi va romantik atmosfera."},
        {"name": "Bali, Indoneziya", "desc": "Tropik plyajlar, guruch teraslari va sörf sporti."},
        {"name": "Rim, Italiya", "desc": "Kolizey, Vatikan va boy antik tarix."},
    ]

    @classmethod
    def random_pick(cls, count: int = 3) -> List[Dict[str, str]]:
        return random.sample(cls.DESTINATIONS, min(count, len(cls.DESTINATIONS)))

    @staticmethod
    def format_message(destinations: List[Dict[str, str]]) -> str:
        lines = ["✈️ <b>Sayohat uchun tavsiyalar:</b>\n"]
        for d in destinations:
            lines.append(f"📍 <b>{d['name']}</b>\n{d['desc']}\n")
        return "\n".join(lines)


travel_service = TravelService()


# =================================================================================
# 17. YO'L TIRBANDLIGI (TrafficService) — HOZIRCHA "STUB" (bo'sh joy) IMPLEMENTATSIYA
# =================================================================================
#
# Yo'l tirbandligi ma'lumotlari uchun odatda pullik API'lar kerak bo'ladi
# (Google Maps Roads API, Yandex Maps API yoki TomTom Traffic API).
# Hozircha bu funksiya foydalanuvchiga tushunarli xabar qaytaradi va
# kelajakda haqiqiy API bilan osongina almashtirilishi mumkin bo'lgan
# aniq interfeys (interface) sifatida qoldirilgan.
#
class TrafficService:
    async def get_traffic_info(self, city: str) -> str:
        # TODO: Bu yerga TomTom / Yandex / Google Maps Traffic API integratsiyasini qo'shish kerak.
        # Masalan: https://api.tomtom.com/traffic/services/4/flowSegmentData/...
        return (
            f"🚗 '{city}' shahri uchun tirbandlik ma'lumoti hozircha mavjud emas.\n"
            "Bu funksiya tez orada (TomTom/Yandex Maps API integratsiyasi orqali) qo'shiladi."
        )


traffic_service = TrafficService()


# =================================================================================
# =================================================================================
# NAMOZ VAQTLARI INTEGRATSIYASI — HOZIRCHA FAQAT REJA/KOMMENTARIYA SIFATIDA
# =================================================================================
# =================================================================================
#
# PRAYER_TIMES_TODO:
# Kelajakda Aladhan API (https://aladhan.com/prayer-times-api) orqali quyidagicha
# integratsiya qilinishi rejalashtirilgan:
#
# class PrayerTimesService:
#     BASE_URL = "https://api.aladhan.com/v1/timingsByCity"
#
#     async def get_timings(self, city: str, country: str = "Uzbekistan", method: int = 2):
#         params = {"city": city, "country": country, "method": method}
#         data = await fetch_json(self.BASE_URL, params=params)
#         if not data or data.get("code") != 200:
#             return None
#         timings = data["data"]["timings"]
#         return {
#             "Bomdod": timings.get("Fajr"),
#             "Quyosh": timings.get("Sunrise"),
#             "Peshin": timings.get("Dhuhr"),
#             "Asr": timings.get("Asr"),
#             "Shom": timings.get("Maghrib"),
#             "Xufton": timings.get("Isha"),
#         }
#
# Handler qo'shilganda /namoz yoki /prayertimes komandasi orqali chaqiriladi va
# foydalanuvchining saqlangan shahri asosida vaqtlar ko'rsatiladi.
# HOZIRCHA BU FUNKSIYA ISHGA TUSHIRILMAGAN — faqat reja sifatida saqlanmoqda.
#
# =================================================================================


# =================================================================================
# 18. TELEGRAM UI — TUGMALAR (KEYBOARDS)
# =================================================================================

MAIN_MENU_BUTTONS = [
    [KeyboardButton("🌤 Ob-havo"), KeyboardButton("📍 Joylashuvni yuborish", request_location=True)],
    [KeyboardButton("👕 Kiyim maslahati"), KeyboardButton("💱 Valyuta")],
    [KeyboardButton("📰 Yangiliklar"), KeyboardButton("🎬 Kino")],
    [KeyboardButton("📚 Kitoblar"), KeyboardButton("⚽ Sport")],
    [KeyboardButton("🏥 Yaqin dorixona"), KeyboardButton("🛒 Yaqin do'kon")],
    [KeyboardButton("✈️ Sayohat"), KeyboardButton("💡 Fakt/Iqtibos")],
    [KeyboardButton("📝 Vazifalarim"), KeyboardButton("💬 Tarjimon")],
    [KeyboardButton("⚙️ Sozlamalar")],
]


def main_menu_markup() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(MAIN_MENU_BUTTONS, resize_keyboard=True)


# =================================================================================
# 19. TELEGRAM HANDLERLAR
# =================================================================================

# ---- /start va asosiy menyu ----

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    username = update.effective_user.username if update.effective_user else None
    await upsert_user(chat_id, username)
    text = (
        "👋 Assalomu alaykum! Men <b>Ko'p funksiyali Ob-havo Boti</b>man.\n\n"
        "🌤 Hozirgi ob-havo, xavfsizlik ogohlantirishlari, kiyim maslahatlari, "
        "valyuta kurslari, yangiliklar, kino/kitob tavsiyalari va boshqa ko'plab "
        "funksiyalarni taqdim etaman.\n\n"
        "Boshlash uchun quyidagi menyudan foydalaning yoki shahringiz nomini yozing "
        "(masalan: <i>Toshkent</i>) yoki 📍 joylashuvingizni yuboring.\n\n"
        "Barcha komandalar uchun: /help"
    )
    await update.message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=main_menu_markup())


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    text = (
        "📖 <b>Mavjud komandalar:</b>\n\n"
        "/weather &lt;shahar&gt; — hozirgi ob-havo\n"
        "/setcity &lt;shahar&gt; — standart shaharni saqlash\n"
        "/currency — valyuta kurslari\n"
        "/news — so'nggi yangiliklar\n"
        "/movie &lt;nomi&gt; — kino haqida ma'lumot\n"
        "/book &lt;janr&gt; — kitob tavsiyalari\n"
        "/sport &lt;epl|laliga|uefa|seriea&gt; — sport natijalari\n"
        "/travel — sayohat yo'nalishlari\n"
        "/quote — hikmatli so'z\n"
        "/fact — qiziqarli fakt\n"
        "/translate &lt;til&gt; &lt;matn&gt; — tarjima (masalan: /translate en Salom)\n"
        "/todo_add &lt;matn&gt; — vazifa qo'shish\n"
        "/todo_list — vazifalar ro'yxati\n"
        "/todo_done &lt;id&gt; — vazifani bajarilgan deb belgilash\n"
        "/todo_del &lt;id&gt; — vazifani o'chirish\n"
        "/subscribe — kunlik xabarlarga obuna bo'lish (ertalab soat "
        f"{DAILY_BROADCAST_HOUR:02d}:{DAILY_BROADCAST_MINUTE:02d})\n"
        "/unsubscribe — obunani bekor qilish\n"
        "📍 Joylashuv yuborsangiz — shu joy bo'yicha ob-havo va yaqin atrofdagi "
        "dorixona/do'konlarni topaman."
    )
    await update.message.reply_text(text, parse_mode=ParseMode.HTML)


async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.error("Botda kutilmagan xatolik yuz berdi", exc_info=context.error)
    if isinstance(update, Update) and update.effective_message:
        try:
            await update.effective_message.reply_text(
                "⚠️ Kechirasiz, xatolik yuz berdi. Birozdan so'ng qayta urinib ko'ring."
            )
        except TelegramError:
            pass


# ---- Ob-havo ----

async def _send_weather_report(update: Update, weather: Dict[str, Any]) -> None:
    await update.message.reply_text(
        WeatherService.format_message(weather), parse_mode=ParseMode.HTML
    )
    warnings = SafetyAdvisor.analyze(weather)
    await update.message.reply_text(
        "\n\n".join(warnings), parse_mode=ParseMode.HTML
    )
    clothing = ClothingAdvisor.suggest(weather)
    await update.message.reply_text(f"👕 <b>Kiyim maslahati:</b>\n{clothing}", parse_mode=ParseMode.HTML)


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
                await _send_weather_report(update, weather)
            else:
                await update.message.reply_text("⚠️ Ob-havo ma'lumotini olishda xatolik yuz berdi.")
            return
        else:
            await update.message.reply_text(
                "Iltimos, shahar nomini kiriting: /weather Toshkent\n"
                "yoki 📍 joylashuvingizni yuboring."
            )
            return

    weather = await weather_service.get_by_city(city)
    if not weather:
        await update.message.reply_text(
            f"⚠️ '{city}' uchun ob-havo ma'lumoti topilmadi. Shahar nomini tekshirib qayta urinib ko'ring."
        )
        return
    await _send_weather_report(update, weather)


async def cmd_setcity(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    if not context.args:
        await update.message.reply_text("Foydalanish: /setcity Toshkent")
        return
    city = " ".join(context.args)
    # avval haqiqatan ham mavjudligini tekshiramiz
    weather = await weather_service.get_by_city(city)
    if not weather:
        await update.message.reply_text(f"⚠️ '{city}' shahri topilmadi. Nomni tekshiring.")
        return
    await set_user_city(chat_id, weather["city"])
    await update.message.reply_text(f"✅ Standart shahringiz '{weather['city']}' etib saqlandi.")


async def on_location_received(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    loc = update.message.location
    await set_user_location(chat_id, loc.latitude, loc.longitude)
    weather = await weather_service.get_by_coords(loc.latitude, loc.longitude)
    if not weather:
        await update.message.reply_text("⚠️ Joylashuvingiz bo'yicha ob-havo topilmadi.")
        return
    await _send_weather_report(update, weather)
    context.user_data["last_location"] = (loc.latitude, loc.longitude)


async def on_text_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Asosiy menyudagi tugmalar bosilganda yoki oddiy matn (shahar nomi) yuborilganda ishlaydi."""
    text = update.message.text.strip()
    chat_id = update.effective_chat.id

    menu_actions = {
        "🌤 Ob-havo": cmd_weather,
        "👕 Kiyim maslahati": cmd_weather,  # shahar bo'lsa avtomatik kiyim maslahati ham chiqadi
        "💱 Valyuta": cmd_currency,
        "📰 Yangiliklar": cmd_news,
        "🎬 Kino": None,
        "📚 Kitoblar": None,
        "⚽ Sport": None,
        "🏥 Yaqin dorixona": None,
        "🛒 Yaqin do'kon": None,
        "✈️ Sayohat": cmd_travel,
        "💡 Fakt/Iqtibos": cmd_quote,
        "📝 Vazifalarim": cmd_todo_list,
        "💬 Tarjimon": None,
        "⚙️ Sozlamalar": cmd_help,
    }

    if text in menu_actions and menu_actions[text] is not None:
        await menu_actions[text](update, context)
        return

    if text == "🎬 Kino":
        await update.message.reply_text("Kino nomini shunday yuboring: /movie Inception")
        return
    if text == "📚 Kitoblar":
        await update.message.reply_text("Janrni shunday yuboring: /book fantasy")
        return
    if text == "⚽ Sport":
        await update.message.reply_text("Liga nomini tanlang: /sport epl | laliga | uefa | seriea")
        return
    if text == "🏥 Yaqin dorixona":
        await cmd_nearby_pharmacy(update, context)
        return
    if text == "🛒 Yaqin do'kon":
        await cmd_nearby_shop(update, context)
        return
    if text == "💬 Tarjimon":
        await update.message.reply_text("Foydalanish: /translate en Salom, qandaysiz?")
        return

    # Agar boshqa hech narsaga mos kelmasa — shahar nomi deb hisoblab, ob-havo qaytaramiz
    weather = await weather_service.get_by_city(text)
    if weather:
        await _send_weather_report(update, weather)
    else:
        await update.message.reply_text(
            "🤔 Buni tushunmadim. Shahar nomi kiriting yoki /help orqali komandalar bilan tanishing."
        )


# ---- Valyuta ----

async def cmd_currency(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    rates = await currency_service.get_rates()
    if not rates:
        await update.message.reply_text("⚠️ Valyuta kurslarini olishda xatolik yuz berdi.")
        return
    await update.message.reply_text(CurrencyService.format_message(rates), parse_mode=ParseMode.HTML)


# ---- Iqtibos / fakt ----

async def cmd_quote(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    quote = await quote_fact_service.get_quote()
    fact = await quote_fact_service.get_fact()
    await update.message.reply_text(f"💡 <b>Kunning hikmati:</b>\n{quote}\n\n🧠 <b>Qiziqarli fakt:</b>\n{fact}", parse_mode=ParseMode.HTML)


# ---- Tarjimon ----

async def cmd_translate(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if len(context.args) < 2:
        await update.message.reply_text("Foydalanish: /translate <til_kodi> <matn>\nMasalan: /translate en Salom dunyo")
        return
    target_lang = context.args[0]
    text = " ".join(context.args[1:])
    result = await TranslatorService.translate(text, target_lang)
    await update.message.reply_text(f"🌐 <b>Tarjima ({target_lang}):</b>\n{result}", parse_mode=ParseMode.HTML)


# ---- To-do / vazifalar ----

async def cmd_todo_add(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        await update.message.reply_text("Foydalanish: /todo_add Kitob o'qish")
        return
    task = " ".join(context.args)
    task_id = await TodoManager.add_task(update.effective_chat.id, task)
    await update.message.reply_text(f"✅ Vazifa qo'shildi (#{task_id}): {task}")


async def cmd_todo_list(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    tasks = await TodoManager.list_tasks(update.effective_chat.id)
    await update.message.reply_text(TodoManager.format_list(tasks), parse_mode=ParseMode.HTML)


async def cmd_todo_done(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args or not context.args[0].isdigit():
        await update.message.reply_text("Foydalanish: /todo_done <id>")
        return
    await TodoManager.mark_done(update.effective_chat.id, int(context.args[0]))
    await update.message.reply_text("✅ Vazifa bajarilgan deb belgilandi.")


async def cmd_todo_del(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args or not context.args[0].isdigit():
        await update.message.reply_text("Foydalanish: /todo_del <id>")
        return
    await TodoManager.delete_task(update.effective_chat.id, int(context.args[0]))
    await update.message.reply_text("🗑 Vazifa o'chirildi.")


# ---- Yangiliklar ----

async def cmd_news(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = " ".join(context.args) if context.args else "Uzbekistan"
    articles = await news_service.get_news(query)
    if articles is None and not NEWSAPI_KEY:
        await update.message.reply_text(
            "⚠️ Yangiliklar xizmati sozlanmagan. .env faylida NEWSAPI_KEY ko'rsating "
            "(https://newsapi.org saytidan bepul olish mumkin)."
        )
        return
    await update.message.reply_text(
        NewsService.format_message(articles or []), parse_mode=ParseMode.HTML, disable_web_page_preview=True
    )


# ---- Kino ----

async def cmd_movie(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        await update.message.reply_text("Foydalanish: /movie Inception")
        return
    if not OMDB_API_KEY:
        await update.message.reply_text(
            "⚠️ Kino xizmati sozlanmagan. .env faylida OMDB_API_KEY ko'rsating "
            "(https://omdbapi.com saytidan bepul olish mumkin)."
        )
        return
    title = " ".join(context.args)
    movie = await movie_service.search(title)
    if not movie:
        await update.message.reply_text(f"⚠️ '{title}' nomli kino topilmadi.")
        return
    await update.message.reply_text(MovieService.format_message(movie), parse_mode=ParseMode.HTML)


# ---- Kitoblar ----

async def cmd_book(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    genre = " ".join(context.args) if context.args else "fiction"
    books = await book_service.search_by_genre(genre)
    await update.message.reply_text(BookService.format_message(books or [], genre), parse_mode=ParseMode.HTML)


# ---- Sport ----

async def cmd_sport(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    league = context.args[0] if context.args else "epl"
    if league.lower() not in SportsService.LEAGUE_IDS:
        available = ", ".join(SportsService.LEAGUE_IDS.keys())
        await update.message.reply_text(f"⚠️ Noma'lum liga. Mavjud variantlar: {available}")
        return
    results = await sports_service.get_last_results(league)
    await update.message.reply_text(
        SportsService.format_message(results or [], league), parse_mode=ParseMode.HTML
    )


# ---- Yaqin atrofdagi dorixona / do'kon ----

async def cmd_nearby_pharmacy(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _handle_nearby(update, context, "pharmacy")


async def cmd_nearby_shop(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _handle_nearby(update, context, "supermarket")


async def _handle_nearby(update: Update, context: ContextTypes.DEFAULT_TYPE, place_type: str) -> None:
    chat_id = update.effective_chat.id
    user = await get_user(chat_id)
    if not user or not user.get("lat") or not user.get("lon"):
        await update.message.reply_text(
            "📍 Iltimos, avval joylashuvingizni yuboring (asosiy menyudagi tugma orqali)."
        )
        return
    places = await place_finder_service.find_nearby(user["lat"], user["lon"], place_type)
    await update.message.reply_text(
        PlaceFinderService.format_message(places or [], place_type),
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
    )


# ---- Sayohat ----

async def cmd_travel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    destinations = TravelService.random_pick(3)
    await update.message.reply_text(TravelService.format_message(destinations), parse_mode=ParseMode.HTML)


# ---- Yo'l tirbandligi ----

async def cmd_traffic(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    city = " ".join(context.args) if context.args else "Toshkent"
    info = await traffic_service.get_traffic_info(city)
    await update.message.reply_text(info)


# ---- Kunlik obuna ----

async def cmd_subscribe(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    await upsert_user(chat_id)
    await db_execute("UPDATE users SET subscribed_daily = 1 WHERE chat_id = ?", (chat_id,))
    await update.message.reply_text(
        f"✅ Kunlik xabarlarga obuna bo'ldingiz. Har kuni soat "
        f"{DAILY_BROADCAST_HOUR:02d}:{DAILY_BROADCAST_MINUTE:02d} da ob-havo, "
        "hikmatli so'z va fakt yuboriladi."
    )


async def cmd_unsubscribe(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    await db_execute("UPDATE users SET subscribed_daily = 0 WHERE chat_id = ?", (chat_id,))
    await update.message.reply_text("❌ Kunlik obuna bekor qilindi.")


# =================================================================================
# 20. KUNLIK AVTOMATIK XABAR (APScheduler)
# =================================================================================

async def send_daily_broadcast(application: Application) -> None:
    """Barcha obuna bo'lgan foydalanuvchilarga har kuni ertalab xabar yuboradi."""
    users = await get_all_subscribed_users()
    logger.info("Kunlik xabar %d foydalanuvchiga yuborilmoqda...", len(users))
    quote = await quote_fact_service.get_quote()
    fact = await quote_fact_service.get_fact()

    for user in users:
        chat_id = user["chat_id"]
        try:
            parts = [f"☀️ <b>Xayrli tong!</b>\n\n💡 {quote}\n\n🧠 {fact}"]

            weather = None
            if user.get("city"):
                weather = await weather_service.get_by_city(user["city"])
            elif user.get("lat") and user.get("lon"):
                weather = await weather_service.get_by_coords(user["lat"], user["lon"])

            if weather:
                parts.append(WeatherService.format_message(weather))
                warnings = SafetyAdvisor.analyze(weather)
                parts.append("\n".join(warnings))
                parts.append(f"👕 {ClothingAdvisor.suggest(weather)}")

            await application.bot.send_message(
                chat_id=chat_id, text="\n\n".join(parts), parse_mode=ParseMode.HTML
            )
        except TelegramError as e:
            logger.warning("Foydalanuvchi %s ga xabar yuborib bo'lmadi: %s", chat_id, e)
        await asyncio.sleep(0.05)  # Telegram rate-limit'iga hurmat


def setup_scheduler(application: Application) -> AsyncIOScheduler:
    scheduler = AsyncIOScheduler(timezone=TZ)
    scheduler.add_job(
        send_daily_broadcast,
        trigger=CronTrigger(hour=DAILY_BROADCAST_HOUR, minute=DAILY_BROADCAST_MINUTE, timezone=TZ),
        args=[application],
        id="daily_broadcast",
        replace_existing=True,
    )
    scheduler.start()
    logger.info(
        "Rejalashtiruvchi ishga tushdi. Kunlik xabar har kuni %02d:%02d (%s) da yuboriladi.",
        DAILY_BROADCAST_HOUR, DAILY_BROADCAST_MINUTE, TIMEZONE_NAME,
    )
    return scheduler


# =================================================================================
# 21. ASOSIY FUNKSIYA (main)
# =================================================================================

def build_application() -> Application:
    application = ApplicationBuilder().token(BOT_TOKEN).build()

    # Asosiy komandalar
    application.add_handler(CommandHandler("start", cmd_start))
    application.add_handler(CommandHandler("help", cmd_help))

    # Ob-havo
    application.add_handler(CommandHandler("weather", cmd_weather))
    application.add_handler(CommandHandler("setcity", cmd_setcity))
    application.add_handler(MessageHandler(filters.LOCATION, on_location_received))

    # Valyuta / yangiliklar / kino / kitob / sport / sayohat / tirbandlik
    application.add_handler(CommandHandler("currency", cmd_currency))
    application.add_handler(CommandHandler("news", cmd_news))
    application.add_handler(CommandHandler("movie", cmd_movie))
    application.add_handler(CommandHandler("book", cmd_book))
    application.add_handler(CommandHandler("sport", cmd_sport))
    application.add_handler(CommandHandler("travel", cmd_travel))
    application.add_handler(CommandHandler("traffic", cmd_traffic))

    # Fakt / iqtibos / tarjimon
    application.add_handler(CommandHandler("quote", cmd_quote))
    application.add_handler(CommandHandler("fact", cmd_quote))
    application.add_handler(CommandHandler("translate", cmd_translate))

    # To-do
    application.add_handler(CommandHandler("todo_add", cmd_todo_add))
    application.add_handler(CommandHandler("todo_list", cmd_todo_list))
    application.add_handler(CommandHandler("todo_done", cmd_todo_done))
    application.add_handler(CommandHandler("todo_del", cmd_todo_del))

    # Obuna
    application.add_handler(CommandHandler("subscribe", cmd_subscribe))
    application.add_handler(CommandHandler("unsubscribe", cmd_unsubscribe))

    # Nearby (dorixona/do'kon) — to'g'ridan-to'g'ri komanda sifatida ham
    application.add_handler(CommandHandler("pharmacy", cmd_nearby_pharmacy))
    application.add_handler(CommandHandler("shop", cmd_nearby_shop))

    # Oddiy matn xabarlari (menyu tugmalari + shahar nomlari)
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text_message))

    # Xatoliklarni ushlash
    application.add_error_handler(on_error)

    return application


async def post_init(application: Application) -> None:
    """Bot ishga tushgandan so'ng chaqiriladigan tayyorgarlik funksiyasi."""
    init_database()
    setup_scheduler(application)
    logger.info("Bot muvaffaqiyatli ishga tushdi.")


def main() -> None:
    application = build_application()
    application.post_init = post_init
    logger.info("Bot ishga tushmoqda...")
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
