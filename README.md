# Ko'p funksiyali Telegram Ob-havo Boti

## O'rnatish

```bash
python3 -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env
```

`.env` faylini ochib, kamida quyidagi ikkitasini to'ldiring:

- `TELEGRAM_BOT_TOKEN` — @BotFather orqali olinadi
- `OPENWEATHERMAP_API_KEY` — https://openweathermap.org/api saytidan bepul ro'yxatdan o'tib olinadi

Qolgan kalitlar (`NEWSAPI_KEY`, `OMDB_API_KEY`, `GOOGLE_BOOKS_API_KEY`) ixtiyoriy —
ular bo'lmasa, mos funksiyalar ishlaydi, lekin foydalanuvchiga tushunarli
"xizmat sozlanmagan" xabarini beradi (bot yiqilib qolmaydi).

## Ishga tushirish

```bash
python main.py
```

## Arxitektura

Barcha xizmatlar mustaqil klasslar sifatida ajratilgan (`WeatherService`,
`SafetyAdvisor`, `ClothingAdvisor`, `CurrencyService`, ...), shuning uchun
har birini alohida test qilish yoki kelajakda boshqa API bilan almashtirish oson.

`main.py` ichidagi **PRAYER_TIMES_TODO** bo'limida namoz vaqtlari
integratsiyasi uchun tayyor reja (kommentariya ko'rinishida) mavjud —
buni keyinroq faollashtirish uchun shunchaki klassni kommentariyadan
chiqarib, `build_application()` ichiga tegishli handler qo'shish kifoya.

## Ma'lumotlar bazasi

SQLite (`bot_database.db`) ishlatiladi, jadvallar avtomatik yaratiladi:
- `users` — foydalanuvchi shahri/joylashuvi va obuna holati
- `todos` — har bir foydalanuvchining shaxsiy vazifalar ro'yxati

## Muhim eslatma

Kod hozircha bepul/ochiq API'larga (OpenWeatherMap, open.er-api.com,
OpenStreetMap Overpass, TheSportsDB test kaliti, quotable.io) tayanadi.
Yuqori yuklama (production) uchun tegishli API'larning pullik/rate-limit
siyosatini alohida ko'rib chiqish tavsiya etiladi.
