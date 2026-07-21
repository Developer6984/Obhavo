# Ko'p funksiyali Telegram Ob-havo Boti (v2.0 — Premium UI)

## ⚠️ Render'da "Exited with status 1" xatoligi haqida

Agar avval quyidagi xatolikni ko'rgan bo'lsangiz:

```
RuntimeError: There is no current event loop in thread 'MainThread'.
RuntimeWarning: coroutine 'Updater.start_webhook' was never awaited
```

Bu — kodda tashqi `apscheduler.AsyncIOScheduler`ni qo'lda ishga tushirish
oqibatida yuzaga kelgan edi. **v2.0 versiyada bu butunlay bartaraf etilgan**:
endi kunlik xabarlar uchun `python-telegram-bot`ning o'zining ichki
`job_queue` mexanizmi ishlatiladi, hech qanday tashqi scheduler yoki
webhook kodi yo'q — faqat toza `run_polling()`.

Deploy qilishdan oldin **Render'da "Clear build cache & deploy"** qiling,
shunda eski (keshlangan) kutubxona versiyalari o'rniga yangi
`requirements.txt`dagi pinned versiyalar o'rnatiladi.

## ⚠️⚠️ IKKINCHI (asosiy) xatolik: Python 3.14 muvofiqsizligi

Agar hamon xuddi shu `RuntimeError: no current event loop` xatoligini
ko'rsangiz-u, lekin log'da `.../Python-3.14.3/...` degan yo'l ko'rinsa —
muammo kodda emas, balki **Render avtomatik ravishda Python 3.14'ni
tanlab qo'yganida**. `python-telegram-bot==21.4` hali Python 3.13+ bilan
to'liq mos emas.

**Yechim (Render.com'da, MAJBURIY):**

1. Ushbu loyihada `.python-version` va `runtime.txt` fayllari qo'shilgan
   (ikkalasi ham `3.12.7`ni ko'rsatadi) — ularni repo ILDIZIGA
   (`main.py` bilan bir qatorda) joylashtiring.
2. Qo'shimcha ishonch uchun: Render Dashboard → sizning xizmatingiz →
   **Environment** → "Add Environment Variable" → 
   `PYTHON_VERSION` = `3.12.7`
3. Render Dashboard → **Manual Deploy** → **"Clear build cache & deploy"**
   tugmasini bosing (oddiy "Deploy" emas — kesh tozalanishi shart).
4. Build loglarida yuqorida `Python 3.12.7` ishlatilayotganini tasdiqlang.

Agar shunga qaramay muammo davom etsa, kodning o'zi ham endi buni ushlab,
tushunarli xabar bilan to'xtaydi (o'zingiz osongina aniqlaysiz).


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
