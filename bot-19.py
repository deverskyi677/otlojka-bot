"""
Игровой Telegram-бот — единый файл (все модули склеены для удобства деплоя).
Игры: Число, Мафия, Рулетка, Кости, КНБ, Виселица, Быки и коровы,
Дуэль на реакции, Крестики-нолики, Колесо фортуны.
Плюс: секретная /admin панель, напоминания /remind, обратная связь.
"""

import os
import re
import time
import random
import asyncio
import logging

from dotenv import load_dotenv

import aiosqlite
import json

from aiogram import Bot, Dispatcher, Router, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    Message,
    CallbackQuery,
    ChatMemberUpdated,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)
from apscheduler.schedulers.asyncio import AsyncIOScheduler


# ==============================================================================
# КОНФИГУРАЦИЯ (переменные окружения из .env или Railway Variables)
# ==============================================================================
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "")

# ID владельца бота — только он видит /admin. Узнать свой ID: @userinfobot
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))

DB_PATH = os.getenv("DB_PATH", "bot.db")

FEEDBACK_USERNAME = os.getenv("FEEDBACK_USERNAME", "deverskyi")


# ===============================================================================
# БАЗА ДАННЫХ
# ===============================================================================

# ---------------------------------------------------------------------------
# Тексты и кнопки по умолчанию — всё это редактируется потом через /admin
# ---------------------------------------------------------------------------
DEFAULT_TEXTS = {
    "welcome": (
        "👋 <b>Привет! Я — игровой бот</b>\n\n"
        "Добавь меня в любой чат (с правами админа), и твои друзья "
        "смогут играть прямо там: Мафия, Число, Рулетка и ещё 7 игр.\n\n"
        "Выбирай игру снизу 👇"
    ),
    "farewell": "👋 До встречи! Возвращайся, когда захочется поиграть.",
    "help": (
        "ℹ️ <b>Как играть</b>\n\n"
        "1. Добавь бота в чат и выдай права администратора\n"
        "2. Напиши /start в чате\n"
        "3. Выбери игру и следуй инструкциям на кнопках\n\n"
        "<b>Команды бота:</b>\n"
        "/start — открыть главное меню\n"
        "/help — показать эту справку\n"
        "/remind [время] [текст] — поставить напоминание\n"
        "   пример: <code>/remind 10m Проверить пиццу</code>\n\n"
        "<b>Доступные игры:</b>\n"
        "🔢 Число · 🕵️ Мафия · 🎡 Рулетка · 🎲 Кости\n"
        "✂️ КНБ · 📝 Виселица · 🐮 Быки и коровы\n"
        "⚡ Дуэль на реакции · ❌⭕ Крестики-нолики · 🎁 Колесо фортуны\n\n"
        "Есть идея новой игры или вопрос? Жми «Обратная связь» в меню."
    ),
    "btn_games": "🎮 Игры",
    "btn_help": "ℹ️ Помощь",
    "btn_feedback": "💬 Обратная связь",
    "btn_back": "⬅️ Назад",
    "btn_reminders": "⏰ Напоминания",
}

DEFAULT_EMOJI = {
    "number": "🔢",
    "mafia": "🕵️",
    "roulette": "🎡",
    "dice": "🎲",
    "rps": "✂️",
    "hangman": "📝",
    "bulls_cows": "🐮",
    "reaction": "⚡",
    "tictactoe": "❌",
    "wheel": "🎁",
    "back": "⬅️",
    "vs_bot": "🤖",
    "vs_player": "👤",
}

SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER PRIMARY KEY,
    username TEXT,
    first_name TEXT,
    is_premium INTEGER DEFAULT 0,
    joined_at INTEGER,
    games_played INTEGER DEFAULT 0,
    wins INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS chats (
    chat_id INTEGER PRIMARY KEY,
    title TEXT,
    added_at INTEGER
);

CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT
);

CREATE TABLE IF NOT EXISTS reminders (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    chat_id INTEGER,
    text TEXT,
    remind_at INTEGER,
    is_done INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS game_stats (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    game_name TEXT,
    chat_id INTEGER,
    winner_id INTEGER,
    played_at INTEGER
);
"""


from contextlib import asynccontextmanager


@asynccontextmanager
async def db_connect():
    """
    Общая точка открытия соединения с БД.
    WAL позволяет читать и писать одновременно из разных соединений
    (иначе scheduler и обработчики команд периодически ловят
    "database is locked", и бот в этот момент не отвечает).
    busy_timeout — если блокировка всё же случилась, соединение ждёт
    до 5 секунд вместо мгновенного падения с ошибкой.
    """
    async with aiosqlite.connect(DB_PATH, timeout=5) as db:
        await db.execute("PRAGMA journal_mode=WAL")
        await db.execute("PRAGMA busy_timeout=5000")
        yield db


async def init_db():
    async with db_connect() as db:
        await db.executescript(SCHEMA)
        await db.commit()

        # Инициализация текстов/кнопок/эмодзи, если их ещё нет
        cur = await db.execute("SELECT value FROM settings WHERE key = 'texts'")
        row = await cur.fetchone()
        if not row:
            await db.execute(
                "INSERT INTO settings (key, value) VALUES ('texts', ?)",
                (json.dumps(DEFAULT_TEXTS, ensure_ascii=False),),
            )
        cur = await db.execute("SELECT value FROM settings WHERE key = 'emoji'")
        row = await cur.fetchone()
        if not row:
            await db.execute(
                "INSERT INTO settings (key, value) VALUES ('emoji', ?)",
                (json.dumps(DEFAULT_EMOJI, ensure_ascii=False),),
            )
        await db.commit()


# ---------------------------------------------------------------------------
# Пользователи / чаты
# ---------------------------------------------------------------------------
async def upsert_user(user_id: int, username: str, first_name: str, is_premium: bool = False):
    async with db_connect() as db:
        await db.execute(
            """INSERT INTO users (user_id, username, first_name, is_premium, joined_at)
               VALUES (?, ?, ?, ?, ?)
               ON CONFLICT(user_id) DO UPDATE SET
                   username=excluded.username,
                   first_name=excluded.first_name,
                   is_premium=excluded.is_premium""",
            (user_id, username, first_name, int(is_premium), int(time.time())),
        )
        await db.commit()


async def upsert_chat(chat_id: int, title: str):
    async with db_connect() as db:
        await db.execute(
            """INSERT INTO chats (chat_id, title, added_at) VALUES (?, ?, ?)
               ON CONFLICT(chat_id) DO UPDATE SET title=excluded.title""",
            (chat_id, title, int(time.time())),
        )
        await db.commit()


async def get_stats_summary() -> dict:
    async with db_connect() as db:
        cur = await db.execute("SELECT COUNT(*) FROM users")
        users_count = (await cur.fetchone())[0]
        cur = await db.execute("SELECT COUNT(*) FROM chats")
        chats_count = (await cur.fetchone())[0]
        cur = await db.execute("SELECT COUNT(*) FROM game_stats")
        games_count = (await cur.fetchone())[0]
        return {"users": users_count, "chats": chats_count, "games": games_count}


async def log_game_result(game_name: str, chat_id: int, winner_id: int | None):
    async with db_connect() as db:
        await db.execute(
            "INSERT INTO game_stats (game_name, chat_id, winner_id, played_at) VALUES (?, ?, ?, ?)",
            (game_name, chat_id, winner_id, int(time.time())),
        )
        if winner_id:
            await db.execute(
                "UPDATE users SET wins = wins + 1, games_played = games_played + 1 WHERE user_id = ?",
                (winner_id,),
            )
        await db.commit()


# ---------------------------------------------------------------------------
# Настройки (тексты / кнопки / эмодзи) — редактируются из /admin
# ---------------------------------------------------------------------------
async def get_setting(key: str) -> dict:
    async with db_connect() as db:
        cur = await db.execute("SELECT value FROM settings WHERE key = ?", (key,))
        row = await cur.fetchone()
        return json.loads(row[0]) if row else {}


async def set_setting_value(key: str, field: str, value: str):
    data = await get_setting(key)
    data[field] = value
    async with db_connect() as db:
        await db.execute(
            """INSERT INTO settings (key, value) VALUES (?, ?)
               ON CONFLICT(key) DO UPDATE SET value=excluded.value""",
            (key, json.dumps(data, ensure_ascii=False)),
        )
        await db.commit()


async def get_text(field: str) -> str:
    texts = await get_setting("texts")
    return texts.get(field, DEFAULT_TEXTS.get(field, ""))


async def get_emoji(field: str) -> str:
    emoji = await get_setting("emoji")
    return emoji.get(field, DEFAULT_EMOJI.get(field, ""))


# ---------------------------------------------------------------------------
# Напоминания
# ---------------------------------------------------------------------------
async def add_reminder(user_id: int, chat_id: int, text: str, remind_at: int) -> int:
    async with db_connect() as db:
        cur = await db.execute(
            "INSERT INTO reminders (user_id, chat_id, text, remind_at) VALUES (?, ?, ?, ?)",
            (user_id, chat_id, text, remind_at),
        )
        await db.commit()
        return cur.lastrowid


async def get_due_reminders(now_ts: int) -> list:
    async with db_connect() as db:
        cur = await db.execute(
            "SELECT id, user_id, chat_id, text FROM reminders WHERE is_done = 0 AND remind_at <= ?",
            (now_ts,),
        )
        return await cur.fetchall()


async def mark_reminder_done(reminder_id: int):
    async with db_connect() as db:
        await db.execute("UPDATE reminders SET is_done = 1 WHERE id = ?", (reminder_id,))
        await db.commit()

# ===============================================================================
# КЛАВИАТУРЫ ГЛАВНОГО МЕНЮ
# ===============================================================================

# ===============================================================================
# ОБЩИЕ ХЕНДЛЕРЫ: /start, меню, помощь, обратная связь
# ===============================================================================


common_router = Router()


@common_router.message(CommandStart())
async def cmd_start(message: Message):
    logging.info("Получена команда /start от user_id=%s chat_id=%s", message.from_user.id, message.chat.id)
    user = message.from_user
    await upsert_user(user.id, user.username or "", user.first_name or "", user.is_premium or False)
    if message.chat.type in ("group", "supergroup"):
        await upsert_chat(message.chat.id, message.chat.title or "")

    text = await get_text("welcome")
    await message.answer(text, reply_markup=await main_menu_kb())
    logging.info("Ответ на /start отправлен")


@common_router.callback_query(F.data == "menu:main")
async def cb_menu_main(callback: CallbackQuery):
    text = await get_text("welcome")
    await callback.message.edit_text(text, reply_markup=await main_menu_kb())
    await callback.answer()


@common_router.callback_query(F.data == "menu:games")
async def cb_menu_games(callback: CallbackQuery):
    await callback.message.edit_text(
        "🎮 <b>Выбери игру</b>\n\nНажми на кнопку, чтобы запустить игру в этом чате.",
        reply_markup=await games_menu_kb(),
    )
    await callback.answer()


@common_router.callback_query(F.data == "menu:help")
async def cb_menu_help(callback: CallbackQuery):
    text = await get_text("help")
    await callback.message.edit_text(text, reply_markup=await back_to_main_kb())
    await callback.answer()


@common_router.message(Command("help"))
async def cmd_help(message: Message):
    text = await get_text("help")
    await message.answer(text, reply_markup=await back_to_main_kb())


@common_router.callback_query(F.data == "menu:feedback")
async def cb_menu_feedback(callback: CallbackQuery):
    from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✉️ Написать", url=f"https://t.me/{FEEDBACK_USERNAME}")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="menu:main")],
    ])
    await callback.message.edit_text(
        "💬 <b>Обратная связь</b>\n\n"
        "Нашёл баг, хочешь предложить новую игру или есть вопрос?\n"
        "Пиши напрямую — обязательно отвечу.",
        reply_markup=kb,
    )
    await callback.answer()

# ===============================================================================
# СЕКРЕТНАЯ АДМИН-ПАНЕЛЬ /admin
# ===============================================================================


admin_router = Router()


class AdminEdit(StatesGroup):
    waiting_text_value = State()
    waiting_emoji_value = State()


def admin_only(func):
    async def wrapper(event, *args, **kwargs):
        user_id = event.from_user.id
        if user_id != ADMIN_ID:
            return
        return await func(event, *args, **kwargs)
    return wrapper


def admin_main_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Статистика", callback_data="adm:stats")],
        [InlineKeyboardButton(text="✏️ Тексты и кнопки", callback_data="adm:texts")],
        [InlineKeyboardButton(text="🎨 Эмодзи", callback_data="adm:emoji")],
        [InlineKeyboardButton(text="❌ Закрыть", callback_data="adm:close")],
    ])


@admin_router.message(Command("admin"))
async def cmd_admin(message: Message):
    if message.from_user.id != ADMIN_ID:
        return  # молчим — панель секретная, никто не должен знать о её существовании
    await message.answer(
        "🔐 <b>Админ-панель</b>\n\nВыбери раздел:",
        reply_markup=admin_main_kb(),
    )


@admin_router.callback_query(F.data == "adm:close")
async def adm_close(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer()
        return
    await callback.message.delete()
    await callback.answer()


@admin_router.callback_query(F.data == "adm:stats")
async def adm_stats(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer()
        return
    stats = await get_stats_summary()
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="adm:back")],
    ])
    await callback.message.edit_text(
        "📊 <b>Статистика</b>\n\n"
        f"👤 Пользователей: {stats['users']}\n"
        f"💬 Чатов: {stats['chats']}\n"
        f"🎮 Сыграно игр: {stats['games']}",
        reply_markup=kb,
    )
    await callback.answer()


@admin_router.callback_query(F.data == "adm:back")
async def adm_back(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer()
        return
    await callback.message.edit_text(
        "🔐 <b>Админ-панель</b>\n\nВыбери раздел:",
        reply_markup=admin_main_kb(),
    )
    await callback.answer()


# ---------------------------------------------------------------------------
# Редактирование текстов и кнопок
# ---------------------------------------------------------------------------
FIELD_LABELS = {
    "welcome": "Текст приветствия (/start)",
    "farewell": "Текст прощания",
    "help": "Текст помощи",
    "btn_games": "Кнопка «Игры»",
    "btn_help": "Кнопка «Помощь»",
    "btn_feedback": "Кнопка «Обратная связь»",
    "btn_back": "Кнопка «Назад»",
    "btn_reminders": "Кнопка «Напоминания»",
}


def texts_list_kb() -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text=label, callback_data=f"adm:text:{key}")]
        for key, label in FIELD_LABELS.items()
    ]
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="adm:back")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


@admin_router.callback_query(F.data == "adm:texts")
async def adm_texts(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer()
        return
    await callback.message.edit_text(
        "✏️ <b>Тексты и кнопки</b>\n\nВыбери, что изменить:",
        reply_markup=texts_list_kb(),
    )
    await callback.answer()


@admin_router.callback_query(F.data.startswith("adm:text:"))
async def adm_text_field(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer()
        return
    field = callback.data.split(":")[2]
    texts = await get_setting("texts")
    current = texts.get(field, DEFAULT_TEXTS.get(field, ""))

    await state.update_data(field=field)
    await state.set_state(AdminEdit.waiting_text_value)

    await callback.message.edit_text(
        f"✏️ <b>{FIELD_LABELS.get(field, field)}</b>\n\n"
        f"Текущее значение:\n<code>{current}</code>\n\n"
        "Пришли новое значение сообщением. HTML-теги (например &lt;b&gt;) поддерживаются.\n"
        "Команда /cancel — отмена."
    )
    await callback.answer()


@admin_router.message(AdminEdit.waiting_text_value, F.text == "/cancel")
async def adm_text_cancel(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    await state.clear()
    await message.answer("Отменено.", reply_markup=admin_main_kb())


@admin_router.message(AdminEdit.waiting_text_value, F.text, ~F.text.startswith("/"))
async def adm_text_save(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return

    data = await state.get_data()
    field = data["field"]
    await set_setting_value("texts", field, message.text)
    await state.clear()
    await message.answer(
        f"✅ Значение «{FIELD_LABELS.get(field, field)}» обновлено.",
        reply_markup=admin_main_kb(),
    )


# ---------------------------------------------------------------------------
# Редактирование эмодзи (обычных и премиум)
# ---------------------------------------------------------------------------
EMOJI_LABELS = {
    "number": "Число",
    "mafia": "Мафия",
    "roulette": "Рулетка",
    "dice": "Кости",
    "rps": "КНБ",
    "hangman": "Виселица",
    "bulls_cows": "Быки и коровы",
    "reaction": "На реакцию",
    "tictactoe": "Крестики-нолики",
    "wheel": "Колесо фортуны",
    "back": "Кнопка Назад",
}


def emoji_list_kb() -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text=label, callback_data=f"adm:emo:{key}")]
        for key, label in EMOJI_LABELS.items()
    ]
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="adm:back")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


@admin_router.callback_query(F.data == "adm:emoji")
async def adm_emoji(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer()
        return
    await callback.message.edit_text(
        "🎨 <b>Эмодзи кнопок</b>\n\n"
        "Выбери, для какой игры сменить эмодзи.\n\n"
        "⚠️ Важно: Telegram технически не позволяет ставить премиум-эмодзи "
        "(анимированные, из наборов) на inline-кнопки — это ограничение API, "
        "не бота. На кнопках можно использовать только обычные юникод-эмодзи. "
        "Премиум-эмодзи можно вставлять в тексты сообщений (приветствие, помощь) — "
        "там они отображаются корректно.",
        reply_markup=emoji_list_kb(),
    )
    await callback.answer()


@admin_router.callback_query(F.data.startswith("adm:emo:"))
async def adm_emoji_field(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer()
        return
    field = callback.data.split(":")[2]
    emoji = await get_setting("emoji")
    current = emoji.get(field, DEFAULT_EMOJI.get(field, ""))

    await state.update_data(field=field)
    await state.set_state(AdminEdit.waiting_emoji_value)

    await callback.message.edit_text(
        f"🎨 <b>{EMOJI_LABELS.get(field, field)}</b>\n\n"
        f"Текущий эмодзи: {current}\n\n"
        "Пришли новый эмодзи сообщением (обычный или премиум).\n"
        "Команда /cancel — отмена."
    )
    await callback.answer()


@admin_router.message(AdminEdit.waiting_emoji_value, F.text == "/cancel")
async def adm_emoji_cancel(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    await state.clear()
    await message.answer("Отменено.", reply_markup=admin_main_kb())


@admin_router.message(AdminEdit.waiting_emoji_value, F.text, ~F.text.startswith("/"))
async def adm_emoji_save(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return

    data = await state.get_data()
    field = data["field"]

    # Если это премиум-эмодзи, в message.entities будет entity типа custom_emoji —
    # aiogram сохранит его как обычный текст-плейсхолдер, этого достаточно для
    # отображения в кнопках (Bot API рендерит custom emoji по entity в тексте
    # сообщений, но inline-кнопки поддерживают только текст/юникод-эмодзи).
    await set_setting_value("emoji", field, message.text)
    await state.clear()
    await message.answer(
        f"✅ Эмодзи «{EMOJI_LABELS.get(field, field)}» обновлён.",
        reply_markup=admin_main_kb(),
    )

# ===============================================================================
# НАПОМИНАНИЯ /remind
# ===============================================================================


reminders_router = Router()

# /remind 10m Не забыть проверить пиццу
# /remind 2h Позвонить другу
# /remind 1d30m Купить корм коту
UNIT_SECONDS = {"d": 86400, "h": 3600, "m": 60, "s": 1}
DURATION_RE = re.compile(r"(\d+)([dhms])")


def parse_duration(raw: str) -> int | None:
    matches = DURATION_RE.findall(raw)
    if not matches:
        return None
    total = 0
    for value, unit in matches:
        total += int(value) * UNIT_SECONDS[unit]
    return total if total > 0 else None


@reminders_router.callback_query(F.data == "menu:reminders")
async def cb_menu_reminders(callback: CallbackQuery):
    await callback.message.edit_text(
        "⏰ <b>Напоминания</b>\n\n"
        "Команда:\n"
        "<code>/remind [время] [текст]</code>\n\n"
        "Примеры:\n"
        "<code>/remind 10m Проверить пиццу</code>\n"
        "<code>/remind 2h Позвонить другу</code>\n"
        "<code>/remind 1d30m Купить корм коту</code>\n\n"
        "d — дни, h — часы, m — минуты, s — секунды",
        reply_markup=await back_to_main_kb(),
    )
    await callback.answer()


@reminders_router.message(Command("remind"))
async def cmd_remind(message: Message):
    args = message.text.split(maxsplit=2)
    if len(args) < 3:
        await message.reply(
            "⚠️ Формат: <code>/remind 10m текст напоминания</code>"
        )
        return

    duration_raw, text = args[1], args[2]
    seconds = parse_duration(duration_raw)
    if seconds is None:
        await message.reply(
            "⚠️ Не понял время. Пример: <code>10m</code>, <code>2h</code>, <code>1d30m</code>"
        )
        return

    remind_at = int(time.time()) + seconds
    await add_reminder(message.from_user.id, message.chat.id, text, remind_at)

    await message.reply(
        f"✅ Напомню через {duration_raw}: <b>{text}</b>"
    )

# ===============================================================================
# ОБРАБОТКА ДОБАВЛЕНИЯ БОТА В ЧАТ
# ===============================================================================


mychatmember_router = Router()


@mychatmember_router.my_chat_member()
async def on_bot_added(event: ChatMemberUpdated, bot: Bot):
    new_status = event.new_chat_member.status
    if new_status not in ("member", "administrator"):
        return

    chat = event.chat
    await upsert_chat(chat.id, chat.title or "")

    if new_status == "member":
        # Бота добавили, но не сделали админом — многие игры (особенно Мафия
        # с рассылкой в ЛС и модерацией) работают лучше с правами админа.
        try:
            await bot.send_message(
                chat.id,
                "👋 Спасибо, что добавили меня!\n\n"
                "Чтобы все игры работали без ограничений (закрепление сообщений, "
                "модерация), выдайте мне права администратора.\n\n"
                "Затем напишите /start, чтобы начать.",
            )
        except Exception:
            pass

# ===============================================================================
# ИГРА: ЧИСЛО
# ===============================================================================


number_router = Router()

# Состояние игр в памяти: ключ — chat_id, значение — данные игры
# Для простого продакшена SQLite-персистентность не нужна — игры короткие,
# но если хочешь переживать рестарт бота, можно перенести в БД.
active_games: dict[int, dict] = {}


def number_start_kb(mode: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🤖 Против бота", callback_data=f"num:new:bot")],
        [InlineKeyboardButton(text="👥 Против игрока", callback_data=f"num:new:player")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="menu:games")],
    ])


def number_play_kb(chat_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="⬇️ Меньше", callback_data="num:guess:low"),
            InlineKeyboardButton(text="⬆️ Больше", callback_data="num:guess:high"),
        ],
        [InlineKeyboardButton(text="🎯 Угадать точно", callback_data="num:guess:exact")],
    ])


@number_router.callback_query(F.data == "game:number")
async def number_menu(callback: CallbackQuery):
    await callback.message.edit_text(
        "🔢 <b>Игра «Число»</b>\n\n"
        "Один игрок загадывает число от 1 до 100, остальные пытаются угадать "
        "с помощью подсказок «больше» / «меньше».\n\n"
        "С кем играем?",
        reply_markup=number_start_kb("choose"),
    )
    await callback.answer()


@number_router.callback_query(F.data == "num:new:bot")
async def number_vs_bot(callback: CallbackQuery):
    chat_id = callback.message.chat.id
    secret = random.randint(1, 100)
    active_games[chat_id] = {
        "mode": "bot",
        "secret": secret,
        "guesser": callback.from_user.id,
        "low": 1,
        "high": 100,
        "attempts": 0,
    }
    await callback.message.edit_text(
        "🔢 <b>Число загадано ботом (1–100)</b>\n\n"
        f"{callback.from_user.first_name}, попробуй угадать!\n"
        "Напиши число сообщением в чат.",
    )
    await callback.answer()


@number_router.callback_query(F.data == "num:new:player")
async def number_vs_player(callback: CallbackQuery):
    chat_id = callback.message.chat.id
    active_games[chat_id] = {
        "mode": "player",
        "secret": None,
        "setter": callback.from_user.id,
        "attempts": 0,
    }
    await callback.message.edit_text(
        f"🔢 <b>{callback.from_user.first_name} загадывает число от 1 до 100</b>\n\n"
        "Отправь его мне в личные сообщения боту — остальные не увидят.\n\n"
        "⚠️ Внимание: чтобы число осталось в секрете, загадай его в ЛС боту "
        "командой <code>/setnumber 42</code>, затем возвращайся в чат — "
        "игроки будут угадывать здесь.",
    )
    await callback.answer()


@number_router.message(F.text.regexp(r"^/setnumber\s+\d+$"))
async def set_number_private(message):
    if message.chat.type != "private":
        return
    value = int(message.text.split()[1])
    if not (1 <= value <= 100):
        await message.reply("⚠️ Число должно быть от 1 до 100.")
        return
    # В реальном проекте здесь нужно сопоставление user -> group chat_id,
    # которое запрашивается на предыдущем шаге. Для MVP сохраняем per-user.
    active_games.setdefault("_pending_secrets", {})
    active_games["_pending_secrets"][message.from_user.id] = value
    await message.reply(f"✅ Число {value} сохранено в секрете. Возвращайся в чат!")


@number_router.message(F.text.regexp(r"^\d+$"))
async def number_guess_message(message):
    chat_id = message.chat.id
    game = active_games.get(chat_id)
    if not game or game.get("mode") != "bot":
        return

    guess = int(message.text)
    game["attempts"] += 1
    secret = game["secret"]

    if guess == secret:
        winner = message.from_user
        await message.reply(
            f"🎉 <b>{winner.first_name} угадал(а) число {secret}!</b>\n"
            f"Попыток: {game['attempts']}"
        )
        await log_game_result("number", chat_id, winner.id)
        del active_games[chat_id]
    elif guess < secret:
        await message.reply("⬆️ Больше!")
    else:
        await message.reply("⬇️ Меньше!")

# ===============================================================================
# ИГРА: МАФИЯ
# ===============================================================================


mafia_router = Router()

MIN_PLAYERS = 4
NIGHT_PHASE = "night"
DAY_PHASE = "day"

lobbies: dict[int, dict] = {}
# chat_id -> {
#   "players": {user_id: name},
#   "roles": {user_id: "mafia"/"citizen"/"doctor"/"detective"},
#   "alive": set(user_id),
#   "phase": "night"/"day",
#   "night_kill": user_id or None,
#   "doctor_save": user_id or None,
#   "votes": {voter_id: target_id},
#   "started": bool,
# }


def lobby_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🙋 Присоединиться", callback_data="mafia:join")],
        [InlineKeyboardButton(text="▶️ Начать игру", callback_data="mafia:launch")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="menu:games")],
    ])


def assign_roles(player_ids: list[int]) -> dict:
    n = len(player_ids)
    mafia_count = max(1, n // 4)
    shuffled = player_ids[:]
    random.shuffle(shuffled)

    roles = {}
    for uid in shuffled[:mafia_count]:
        roles[uid] = "mafia"

    rest = shuffled[mafia_count:]
    if rest:
        roles[rest[0]] = "doctor"
    if len(rest) > 1:
        roles[rest[1]] = "detective"
    for uid in rest[2:]:
        roles[uid] = "citizen"

    return roles


ROLE_NAMES = {
    "mafia": "🕵️ Мафия",
    "citizen": "👤 Мирный житель",
    "doctor": "💉 Доктор",
    "detective": "🔍 Детектив",
}

ROLE_DESCRIPTIONS = {
    "mafia": "Ночью выбирай, кого устранить. Твоя цель — уравнять число мафии и мирных.",
    "citizen": "Днём вычисляй мафию и голосуй за казнь. Ночью просто спи.",
    "doctor": "Ночью выбирай, кого спасти от мафии.",
    "detective": "Ночью можешь проверить одного игрока — мафия он или нет.",
}


@mafia_router.callback_query(F.data == "game:mafia")
async def mafia_menu(callback: CallbackQuery):
    chat_id = callback.message.chat.id
    if callback.message.chat.type == "private":
        await callback.answer("🕵️ Мафия играется только в групповых чатах!", show_alert=True)
        return

    lobbies[chat_id] = {
        "players": {callback.from_user.id: callback.from_user.first_name},
        "started": False,
    }
    await callback.message.edit_text(
        "🕵️ <b>Мафия</b>\n\n"
        f"Лобби создано! Минимум игроков: {MIN_PLAYERS}\n\n"
        f"Участники (1):\n• {callback.from_user.first_name}",
        reply_markup=lobby_kb(),
    )
    await callback.answer()


@mafia_router.callback_query(F.data == "mafia:join")
async def mafia_join(callback: CallbackQuery):
    chat_id = callback.message.chat.id
    lobby = lobbies.get(chat_id)
    if not lobby or lobby["started"]:
        await callback.answer("Лобби не найдено или игра уже началась", show_alert=True)
        return

    lobby["players"][callback.from_user.id] = callback.from_user.first_name
    names = "\n".join(f"• {n}" for n in lobby["players"].values())
    await callback.message.edit_text(
        "🕵️ <b>Мафия</b>\n\n"
        f"Лобби создано! Минимум игроков: {MIN_PLAYERS}\n\n"
        f"Участники ({len(lobby['players'])}):\n{names}",
        reply_markup=lobby_kb(),
    )
    await callback.answer("Ты в игре!")


@mafia_router.callback_query(F.data == "mafia:launch")
async def mafia_launch(callback: CallbackQuery, bot: Bot):
    chat_id = callback.message.chat.id
    lobby = lobbies.get(chat_id)
    if not lobby or lobby["started"]:
        await callback.answer("Лобби не найдено", show_alert=True)
        return
    if len(lobby["players"]) < MIN_PLAYERS:
        await callback.answer(f"Нужно минимум {MIN_PLAYERS} игрока", show_alert=True)
        return

    player_ids = list(lobby["players"].keys())
    roles = assign_roles(player_ids)

    lobby.update({
        "roles": roles,
        "alive": set(player_ids),
        "phase": NIGHT_PHASE,
        "night_kill": None,
        "doctor_save": None,
        "detective_check": None,
        "votes": {},
        "started": True,
    })

    # Раздаём роли в личку — если ЛС недоступны, предупреждаем в чате
    failed = []
    for uid, role in roles.items():
        try:
            await bot.send_message(
                uid,
                f"🎭 <b>Твоя роль: {ROLE_NAMES[role]}</b>\n\n{ROLE_DESCRIPTIONS[role]}\n\n"
                "Игра началась в чате — следи за сообщениями там.",
            )
        except Exception:
            failed.append(lobby["players"][uid])

    text = (
        "🌙 <b>Игра началась! Наступает ночь.</b>\n\n"
        "Роли разосланы в личные сообщения.\n"
        f"Мафия ({sum(1 for r in roles.values() if r == 'mafia')}) выбирает жертву.\n\n"
        "Ждите утра — результаты ночи объявит бот."
    )
    if failed:
        text += (
            "\n\n⚠️ Не удалось написать в ЛС: " + ", ".join(failed) +
            "\nПопросите их сначала написать боту /start в личке."
        )

    await callback.message.edit_text(text)
    await callback.answer()

    # Упрощение: ночная фаза — авто-переход через голосование в общем чате днём.
    # Полноценная ночная логика (выбор жертвы мафией в ЛС) требует отдельного
    # маршрута callback'ов с проверкой роли — заложено ниже через day_vote.
    await start_day_vote(callback, chat_id, bot)


async def start_day_vote(callback: CallbackQuery, chat_id: int, bot: Bot):
    lobby = lobbies[chat_id]
    lobby["phase"] = DAY_PHASE
    lobby["votes"] = {}

    alive_ids = list(lobby["alive"])
    kb_rows = []
    for uid in alive_ids:
        name = lobby["players"][uid]
        kb_rows.append([InlineKeyboardButton(text=f"🗳 {name}", callback_data=f"mafia:vote:{uid}")])
    kb_rows.append([InlineKeyboardButton(text="📊 Итоги голосования", callback_data="mafia:tally")])
    kb = InlineKeyboardMarkup(inline_keyboard=kb_rows)

    await bot.send_message(
        chat_id,
        "☀️ <b>Наступил день!</b>\n\n"
        "Обсудите, кто похож на мафию, и проголосуйте за казнь.",
        reply_markup=kb,
    )


@mafia_router.callback_query(F.data.startswith("mafia:vote:"))
async def mafia_vote(callback: CallbackQuery):
    chat_id = callback.message.chat.id
    lobby = lobbies.get(chat_id)
    if not lobby or not lobby.get("started"):
        await callback.answer("Игра не найдена", show_alert=True)
        return
    if callback.from_user.id not in lobby["alive"]:
        await callback.answer("Ты выбыл из игры и не можешь голосовать", show_alert=True)
        return

    target_id = int(callback.data.split(":")[2])
    lobby["votes"][callback.from_user.id] = target_id
    await callback.answer(f"Голос учтён: {lobby['players'][target_id]}")


@mafia_router.callback_query(F.data == "mafia:tally")
async def mafia_tally(callback: CallbackQuery, bot: Bot):
    chat_id = callback.message.chat.id
    lobby = lobbies.get(chat_id)
    if not lobby or not lobby.get("started"):
        await callback.answer("Игра не найдена", show_alert=True)
        return

    votes = lobby["votes"]
    if not votes:
        await callback.answer("Ещё никто не проголосовал", show_alert=True)
        return

    tally: dict[int, int] = {}
    for target in votes.values():
        tally[target] = tally.get(target, 0) + 1

    executed_id = max(tally, key=tally.get)
    executed_name = lobby["players"][executed_id]
    executed_role = lobby["roles"][executed_id]
    lobby["alive"].discard(executed_id)

    result_text = (
        f"⚖️ <b>Казнён: {executed_name}</b>\n"
        f"Роль: {ROLE_NAMES[executed_role]}\n\n"
    )

    mafia_alive = [uid for uid in lobby["alive"] if lobby["roles"][uid] == "mafia"]
    citizens_alive = [uid for uid in lobby["alive"] if lobby["roles"][uid] != "mafia"]

    if not mafia_alive:
        result_text += "🎉 <b>Мирные жители победили!</b>"
        await callback.message.edit_text(result_text)
        for uid in citizens_alive:
            await log_game_result("mafia", chat_id, uid)
        del lobbies[chat_id]
    elif len(mafia_alive) >= len(citizens_alive):
        result_text += "🕵️ <b>Мафия победила!</b>"
        await callback.message.edit_text(result_text)
        for uid in mafia_alive:
            await log_game_result("mafia", chat_id, uid)
        del lobbies[chat_id]
    else:
        result_text += "Игра продолжается — новое голосование:"
        await callback.message.edit_text(result_text)
        await start_day_vote(callback, chat_id, bot)

    await callback.answer()

# ===============================================================================
# ИГРА: РУЛЕТКА
# ===============================================================================


roulette_router = Router()

roulette_pending: dict[int, dict] = {}  # chat_id -> {"bets": {user_id: (name, color)}}

COLORS = {"red": "🔴 Красное", "black": "⚫ Чёрное", "green": "🟢 Зеро"}
WEIGHTS = {"red": 45, "black": 45, "green": 10}  # шанс выпадения


def roulette_bet_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🔴 Красное", callback_data="roul:bet:red"),
            InlineKeyboardButton(text="⚫ Чёрное", callback_data="roul:bet:black"),
            InlineKeyboardButton(text="🟢 Зеро", callback_data="roul:bet:green"),
        ],
        [InlineKeyboardButton(text="🎡 Крутить!", callback_data="roul:spin")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="menu:games")],
    ])


@roulette_router.callback_query(F.data == "game:roulette")
async def roulette_menu(callback: CallbackQuery):
    chat_id = callback.message.chat.id
    roulette_pending[chat_id] = {"bets": {}}
    await callback.message.edit_text(
        "🎡 <b>Рулетка</b>\n\n"
        "Все желающие делают ставку на цвет, потом кто-то крутит колесо.\n"
        "Совпал цвет — победа!\n\n"
        "Ставок сделано: 0",
        reply_markup=roulette_bet_kb(),
    )
    await callback.answer()


@roulette_router.callback_query(F.data.startswith("roul:bet:"))
async def roulette_bet(callback: CallbackQuery):
    chat_id = callback.message.chat.id
    game = roulette_pending.get(chat_id)
    if not game:
        await callback.answer("Игра не найдена, начните заново", show_alert=True)
        return

    color = callback.data.split(":")[2]
    game["bets"][callback.from_user.id] = (callback.from_user.first_name, color)

    await callback.message.edit_text(
        "🎡 <b>Рулетка</b>\n\n"
        "Все желающие делают ставку на цвет, потом кто-то крутит колесо.\n"
        "Совпал цвет — победа!\n\n"
        f"Ставок сделано: {len(game['bets'])}",
        reply_markup=roulette_bet_kb(),
    )
    await callback.answer(f"Ставка принята: {COLORS[color]}")


@roulette_router.callback_query(F.data == "roul:spin")
async def roulette_spin(callback: CallbackQuery):
    chat_id = callback.message.chat.id
    game = roulette_pending.get(chat_id)
    if not game or not game["bets"]:
        await callback.answer("Сначала кто-то должен сделать ставку", show_alert=True)
        return

    result_color = random.choices(list(WEIGHTS.keys()), weights=list(WEIGHTS.values()))[0]
    winners = [name for name, color in game["bets"].values() if color == result_color]

    text = f"🎡 <b>Выпало: {COLORS[result_color]}</b>\n\n"
    if winners:
        text += "🏆 Победители:\n" + "\n".join(f"• {w}" for w in winners)
    else:
        text += "😔 В этот раз никто не угадал."

    await callback.message.edit_text(text)

    winner_id = None
    for uid, (name, color) in game["bets"].items():
        if color == result_color:
            winner_id = uid
            break
    await log_game_result("roulette", chat_id, winner_id)
    del roulette_pending[chat_id]
    await callback.answer()

# ===============================================================================
# ИГРА: КОСТИ
# ===============================================================================


dice_router = Router()

dice_pending: dict[int, dict] = {}  # chat_id -> {"p1": id, "p1_name": str}


def dice_join_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎲 Присоединиться", callback_data="dice:join")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="menu:games")],
    ])


@dice_router.callback_query(F.data == "game:dice")
async def dice_menu(callback: CallbackQuery):
    chat_id = callback.message.chat.id
    dice_pending[chat_id] = {"p1": callback.from_user.id, "p1_name": callback.from_user.first_name}
    await callback.message.edit_text(
        "🎲 <b>Кости</b>\n\n"
        f"{callback.from_user.first_name} бросает вызов!\n"
        "Кто присоединится — у кого сумма больше, тот побеждает.",
        reply_markup=dice_join_kb(),
    )
    await callback.answer()


@dice_router.callback_query(F.data == "dice:join")
async def dice_join(callback: CallbackQuery):
    chat_id = callback.message.chat.id
    game = dice_pending.get(chat_id)
    if not game:
        await callback.answer("Игра уже неактуальна, начните заново", show_alert=True)
        return
    if callback.from_user.id == game["p1"]:
        await callback.answer("Нельзя играть самому с собой 😄", show_alert=True)
        return

    p1_roll = random.randint(1, 6) + random.randint(1, 6)
    p2_roll = random.randint(1, 6) + random.randint(1, 6)

    if p1_roll > p2_roll:
        result = f"🏆 Победитель: <b>{game['p1_name']}</b>"
        winner_id = game["p1"]
    elif p2_roll > p1_roll:
        result = f"🏆 Победитель: <b>{callback.from_user.first_name}</b>"
        winner_id = callback.from_user.id
    else:
        result = "🤝 Ничья!"
        winner_id = None

    await callback.message.edit_text(
        "🎲 <b>Результаты броска</b>\n\n"
        f"{game['p1_name']}: {p1_roll}\n"
        f"{callback.from_user.first_name}: {p2_roll}\n\n"
        f"{result}",
    )
    await log_game_result("dice", chat_id, winner_id)
    del dice_pending[chat_id]
    await callback.answer()

# ===============================================================================
# ИГРА: КАМЕНЬ-НОЖНИЦЫ-БУМАГА
# ===============================================================================


rps_router = Router()

CHOICES = {"rock": "🪨 Камень", "scissors": "✂️ Ножницы", "paper": "📄 Бумага"}
BEATS = {"rock": "scissors", "scissors": "paper", "paper": "rock"}

rps_pending: dict[int, dict] = {}  # chat_id -> {p1_id, p1_name, p1_choice, p2_id, p2_name, p2_choice}


def rps_join_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✊ Присоединиться", callback_data="rps:join")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="menu:games")],
    ])


def rps_choice_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🪨", callback_data="rps:pick:rock"),
            InlineKeyboardButton(text="✂️", callback_data="rps:pick:scissors"),
            InlineKeyboardButton(text="📄", callback_data="rps:pick:paper"),
        ]
    ])


@rps_router.callback_query(F.data == "game:rps")
async def rps_menu(callback: CallbackQuery):
    chat_id = callback.message.chat.id
    rps_pending[chat_id] = {"p1_id": callback.from_user.id, "p1_name": callback.from_user.first_name}
    await callback.message.edit_text(
        "✂️ <b>Камень-Ножницы-Бумага</b>\n\n"
        f"{callback.from_user.first_name} вызывает соперника!",
        reply_markup=rps_join_kb(),
    )
    await callback.answer()


@rps_router.callback_query(F.data == "rps:join")
async def rps_join(callback: CallbackQuery):
    chat_id = callback.message.chat.id
    game = rps_pending.get(chat_id)
    if not game or "p2_id" in game:
        await callback.answer("Игра уже неактуальна", show_alert=True)
        return
    if callback.from_user.id == game["p1_id"]:
        await callback.answer("Нужен второй игрок 😄", show_alert=True)
        return

    game["p2_id"] = callback.from_user.id
    game["p2_name"] = callback.from_user.first_name
    await callback.message.edit_text(
        "✂️ <b>Камень-Ножницы-Бумага</b>\n\n"
        f"{game['p1_name']} 🆚 {game['p2_name']}\n\n"
        "Оба игрока — выбирайте втайне, кнопки одинаковые для обоих.",
        reply_markup=rps_choice_kb(),
    )
    await callback.answer()


@rps_router.callback_query(F.data.startswith("rps:pick:"))
async def rps_pick(callback: CallbackQuery):
    chat_id = callback.message.chat.id
    game = rps_pending.get(chat_id)
    if not game or "p2_id" not in game:
        await callback.answer("Игра ещё не готова", show_alert=True)
        return

    choice = callback.data.split(":")[2]
    uid = callback.from_user.id

    if uid == game["p1_id"]:
        game["p1_choice"] = choice
        await callback.answer(f"Ты выбрал {CHOICES[choice]}", show_alert=True)
    elif uid == game["p2_id"]:
        game["p2_choice"] = choice
        await callback.answer(f"Ты выбрал {CHOICES[choice]}", show_alert=True)
    else:
        await callback.answer("Ты не участвуешь в этой игре", show_alert=True)
        return

    if "p1_choice" in game and "p2_choice" in game:
        c1, c2 = game["p1_choice"], game["p2_choice"]
        if c1 == c2:
            result_text = "🤝 Ничья!"
            winner_id = None
        elif BEATS[c1] == c2:
            result_text = f"🏆 Победитель: <b>{game['p1_name']}</b>"
            winner_id = game["p1_id"]
        else:
            result_text = f"🏆 Победитель: <b>{game['p2_name']}</b>"
            winner_id = game["p2_id"]

        await callback.message.edit_text(
            "✂️ <b>Результат</b>\n\n"
            f"{game['p1_name']}: {CHOICES[c1]}\n"
            f"{game['p2_name']}: {CHOICES[c2]}\n\n"
            f"{result_text}",
        )
        await log_game_result("rps", chat_id, winner_id)
        del rps_pending[chat_id]

# ===============================================================================
# ИГРА: ВИСЕЛИЦА
# ===============================================================================


hangman_router = Router()

WORDS = [
    "телеграм", "питон", "ракета", "клавиатура", "гитара",
    "вертолет", "мороженое", "шоколад", "велосипед", "фонарик",
    "апельсин", "дракон", "космос", "пиратство", "холодильник",
]

hangman_active: dict[int, dict] = {}  # chat_id -> {"word", "guessed": set(), "wrong": int, "max_wrong": int}

MAX_WRONG = 6
STAGES = ["🙂", "😐", "😟", "😨", "😰", "😵", "💀"]


def render_word(word: str, guessed: set) -> str:
    return " ".join(c if c in guessed else "▁" for c in word)


@hangman_router.callback_query(F.data == "game:hangman")
async def hangman_menu(callback: CallbackQuery):
    chat_id = callback.message.chat.id
    word = random.choice(WORDS)
    hangman_active[chat_id] = {"word": word, "guessed": set(), "wrong": 0}

    await callback.message.edit_text(
        "📝 <b>Виселица</b>\n\n"
        f"Слово: {render_word(word, set())}\n"
        f"Состояние: {STAGES[0]}\n"
        f"Ошибок: 0/{MAX_WRONG}\n\n"
        "Пишите буквы в чат по одной!",
    )
    await callback.answer()


@hangman_router.message(F.text.regexp(r"^[а-яёa-z]$"))
async def hangman_letter(message: Message):
    chat_id = message.chat.id
    game = hangman_active.get(chat_id)
    if not game:
        return

    letter = message.text.lower()
    word = game["word"]

    if letter in game["guessed"]:
        await message.reply("Эта буква уже была 🙂")
        return

    game["guessed"].add(letter)

    if letter not in word:
        game["wrong"] += 1

    stage_idx = min(game["wrong"], MAX_WRONG)
    display = render_word(word, game["guessed"])

    if all(c in game["guessed"] for c in word):
        await message.reply(
            f"🎉 <b>Слово отгадано: {word}!</b>\n"
            f"Последнюю букву назвал(а): {message.from_user.first_name}"
        )
        await log_game_result("hangman", chat_id, message.from_user.id)
        del hangman_active[chat_id]
    elif game["wrong"] >= MAX_WRONG:
        await message.reply(
            f"💀 <b>Поражение! Слово было: {word}</b>"
        )
        await log_game_result("hangman", chat_id, None)
        del hangman_active[chat_id]
    else:
        await message.reply(
            f"Слово: {display}\n"
            f"Состояние: {STAGES[stage_idx]}\n"
            f"Ошибок: {game['wrong']}/{MAX_WRONG}"
        )

# ===============================================================================
# ИГРА: БЫКИ И КОРОВЫ
# ===============================================================================


bulls_cows_router = Router()

bc_active: dict[int, dict] = {}  # chat_id -> {"secret": "1234", "attempts": 0}


def generate_secret(length: int = 4) -> str:
    digits = list("0123456789")
    random.shuffle(digits)
    return "".join(digits[:length])


def evaluate(secret: str, guess: str) -> tuple[int, int]:
    bulls = sum(1 for s, g in zip(secret, guess) if s == g)
    cows = sum(1 for g in guess if g in secret) - bulls
    return bulls, cows


@bulls_cows_router.callback_query(F.data == "game:bulls_cows")
async def bulls_cows_menu(callback: CallbackQuery):
    chat_id = callback.message.chat.id
    bc_active[chat_id] = {"secret": generate_secret(), "attempts": 0}
    await callback.message.edit_text(
        "🐮 <b>Быки и коровы</b>\n\n"
        "Я загадал число из 4 разных цифр (0-9).\n"
        "Пиши свой вариант в чат, например: <code>1234</code>\n\n"
        "🎯 Бык — цифра на своём месте\n"
        "🔄 Корова — цифра есть, но не на своём месте",
    )
    await callback.answer()


@bulls_cows_router.message(F.text.regexp(r"^\d{4}$"))
async def bulls_cows_guess(message: Message):
    chat_id = message.chat.id
    game = bc_active.get(chat_id)
    if not game:
        return

    guess = message.text
    if len(set(guess)) != 4:
        await message.reply("⚠️ Все 4 цифры должны быть разными.")
        return

    game["attempts"] += 1
    bulls, cows = evaluate(game["secret"], guess)

    if bulls == 4:
        await message.reply(
            f"🎉 <b>{message.from_user.first_name} угадал(а)!</b>\n"
            f"Число: {game['secret']}, попыток: {game['attempts']}"
        )
        await log_game_result("bulls_cows", chat_id, message.from_user.id)
        del bc_active[chat_id]
    else:
        await message.reply(f"🎯 Быков: {bulls} | 🔄 Коров: {cows}")

# ===============================================================================
# ИГРА: ДУЭЛЬ НА РЕАКЦИИ
# ===============================================================================


reaction_router = Router()

reaction_active: dict[int, dict] = {}  # chat_id -> {"ready": bool, "winner": None}


def waiting_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⏳ Жди сигнала...", callback_data="reaction:early")],
    ])


def ready_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⚡ ЖМИ!", callback_data="reaction:press")],
    ])


@reaction_router.callback_query(F.data == "game:reaction")
async def reaction_menu(callback: CallbackQuery):
    chat_id = callback.message.chat.id
    reaction_active[chat_id] = {"ready": False, "winner": None}

    await callback.message.edit_text(
        "⚡ <b>Дуэль на реакции</b>\n\n"
        "Кнопка появится в случайный момент — жми первым!\n"
        "Жать раньше времени нельзя 😉",
        reply_markup=waiting_kb(),
    )
    await callback.answer()

    delay = random.uniform(2, 6)
    await asyncio.sleep(delay)

    game = reaction_active.get(chat_id)
    if game is None:
        return
    game["ready"] = True
    await callback.message.edit_text(
        "⚡ <b>ЖМИ СЕЙЧАС!</b>",
        reply_markup=ready_kb(),
    )


@reaction_router.callback_query(F.data == "reaction:early")
async def reaction_early(callback: CallbackQuery):
    chat_id = callback.message.chat.id
    game = reaction_active.get(chat_id)
    if game and not game["ready"]:
        await callback.answer("🚫 Рано! Дисквалификация за фальстарт", show_alert=True)
        del reaction_active[chat_id]
        await callback.message.edit_text("🚫 <b>Фальстарт!</b> Игра прервана.")
    else:
        await callback.answer()


@reaction_router.callback_query(F.data == "reaction:press")
async def reaction_press(callback: CallbackQuery):
    chat_id = callback.message.chat.id
    game = reaction_active.get(chat_id)
    if not game or not game["ready"] or game["winner"]:
        await callback.answer()
        return

    game["winner"] = callback.from_user.id
    await callback.message.edit_text(
        f"🏆 <b>{callback.from_user.first_name} победил(а) в реакции!</b>"
    )
    await log_game_result("reaction", chat_id, callback.from_user.id)
    del reaction_active[chat_id]
    await callback.answer("Ты первый!")

# ===============================================================================
# ИГРА: КРЕСТИКИ-НОЛИКИ
# ===============================================================================


tictactoe_router = Router()

ttt_pending: dict[int, dict] = {}

WIN_LINES = [
    (0, 1, 2), (3, 4, 5), (6, 7, 8),
    (0, 3, 6), (1, 4, 7), (2, 5, 8),
    (0, 4, 8), (2, 4, 6),
]


def render_board(board: list, chat_id: int) -> InlineKeyboardMarkup:
    rows = []
    for r in range(3):
        row = []
        for c in range(3):
            i = r * 3 + c
            symbol = board[i] if board[i] else " "
            row.append(InlineKeyboardButton(text=symbol, callback_data=f"ttt:move:{i}"))
        rows.append(row)
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="menu:games")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def check_winner(board: list) -> str | None:
    for a, b, c in WIN_LINES:
        if board[a] and board[a] == board[b] == board[c]:
            return board[a]
    return None


@tictactoe_router.callback_query(F.data == "game:tictactoe")
async def ttt_menu(callback: CallbackQuery):
    chat_id = callback.message.chat.id
    ttt_pending[chat_id] = {"p1_id": callback.from_user.id, "p1_name": callback.from_user.first_name}
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="❌ Присоединиться", callback_data="ttt:join")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="menu:games")],
    ])
    await callback.message.edit_text(
        "❌⭕ <b>Крестики-нолики</b>\n\n"
        f"{callback.from_user.first_name} ждёт соперника!",
        reply_markup=kb,
    )
    await callback.answer()


@tictactoe_router.callback_query(F.data == "ttt:join")
async def ttt_join(callback: CallbackQuery):
    chat_id = callback.message.chat.id
    game = ttt_pending.get(chat_id)
    if not game or "p2_id" in game:
        await callback.answer("Игра неактуальна", show_alert=True)
        return
    if callback.from_user.id == game["p1_id"]:
        await callback.answer("Нужен второй игрок 😄", show_alert=True)
        return

    game.update({
        "p2_id": callback.from_user.id,
        "p2_name": callback.from_user.first_name,
        "board": [""] * 9,
        "turn": game["p1_id"],  # p1 ходит крестиком
    })

    await callback.message.edit_text(
        f"❌ {game['p1_name']} 🆚 ⭕ {game['p2_name']}\n\n"
        f"Ходит: <b>{game['p1_name']}</b> (❌)",
        reply_markup=render_board(game["board"], chat_id),
    )
    await callback.answer()


@tictactoe_router.callback_query(F.data.startswith("ttt:move:"))
async def ttt_move(callback: CallbackQuery):
    chat_id = callback.message.chat.id
    game = ttt_pending.get(chat_id)
    if not game or "board" not in game:
        await callback.answer("Игра не найдена", show_alert=True)
        return

    uid = callback.from_user.id
    if uid not in (game["p1_id"], game["p2_id"]):
        await callback.answer("Ты не участвуешь в этой игре", show_alert=True)
        return
    if uid != game["turn"]:
        await callback.answer("Сейчас не твой ход", show_alert=True)
        return

    idx = int(callback.data.split(":")[2])
    if game["board"][idx]:
        await callback.answer("Клетка занята", show_alert=True)
        return

    symbol = "❌" if uid == game["p1_id"] else "⭕"
    game["board"][idx] = symbol

    winner_symbol = check_winner(game["board"])
    is_draw = all(game["board"]) and not winner_symbol

    if winner_symbol:
        winner_name = game["p1_name"] if winner_symbol == "❌" else game["p2_name"]
        winner_id = game["p1_id"] if winner_symbol == "❌" else game["p2_id"]
        await callback.message.edit_text(
            f"❌⭕ <b>Игра окончена!</b>\n\n🏆 Победитель: <b>{winner_name}</b>",
            reply_markup=render_board(game["board"], chat_id),
        )
        await log_game_result("tictactoe", chat_id, winner_id)
        del ttt_pending[chat_id]
    elif is_draw:
        await callback.message.edit_text(
            "❌⭕ <b>Ничья!</b>",
            reply_markup=render_board(game["board"], chat_id),
        )
        await log_game_result("tictactoe", chat_id, None)
        del ttt_pending[chat_id]
    else:
        game["turn"] = game["p2_id"] if uid == game["p1_id"] else game["p1_id"]
        next_name = game["p1_name"] if game["turn"] == game["p1_id"] else game["p2_name"]
        next_symbol = "❌" if game["turn"] == game["p1_id"] else "⭕"
        await callback.message.edit_text(
            f"❌ {game['p1_name']} 🆚 ⭕ {game['p2_name']}\n\n"
            f"Ходит: <b>{next_name}</b> ({next_symbol})",
            reply_markup=render_board(game["board"], chat_id),
        )

    await callback.answer()

# ===============================================================================
# ИГРА: КОЛЕСО ФОРТУНЫ
# ===============================================================================

wheel_router = Router()

PRIZES = [
    "🎉 Ничего не выпало, повезёт завтра!",
    "⭐ +10 очков удачи",
    "🍀 Двойная удача завтра",
    "🎁 Секретный приз",
    "💎 Джекпот! Ты счастливчик дня",
    "😅 Пусто",
    "🔥 Огненная серия началась",
]

last_spin: dict[int, float] = {}  # user_id -> timestamp
COOLDOWN = 24 * 3600


def wheel_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎁 Крутить колесо", callback_data="wheel:spin")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="menu:games")],
    ])


@wheel_router.callback_query(F.data == "game:wheel")
async def wheel_menu(callback: CallbackQuery):
    await callback.message.edit_text(
        "🎁 <b>Колесо фортуны</b>\n\n"
        "Раз в 24 часа можно крутить колесо и получить случайный приз!",
        reply_markup=wheel_kb(),
    )
    await callback.answer()


@wheel_router.callback_query(F.data == "wheel:spin")
async def wheel_spin(callback: CallbackQuery):
    uid = callback.from_user.id
    now = time.time()
    last = last_spin.get(uid, 0)

    if now - last < COOLDOWN:
        remaining = int(COOLDOWN - (now - last))
        hours, minutes = remaining // 3600, (remaining % 3600) // 60
        await callback.answer(
            f"⏳ Уже крутил(а) сегодня! Повтори через {hours}ч {minutes}м",
            show_alert=True,
        )
        return

    last_spin[uid] = now
    prize = random.choice(PRIZES)

    await callback.message.edit_text(
        f"🎁 <b>Колесо фортуны</b>\n\n"
        f"{callback.from_user.first_name}, твой приз:\n\n"
        f"<b>{prize}</b>\n\n"
        "Возвращайся через 24 часа за новым призом!",
        reply_markup=wheel_kb(),
    )
    await callback.answer()

# ===============================================================================
# ПЛАНИРОВЩИК НАПОМИНАНИЙ
# ===============================================================================



async def check_reminders(bot: Bot):
    now_ts = int(time.time())
    try:
        due = await get_due_reminders(now_ts)
    except Exception:
        logging.exception("Ошибка при получении напоминаний из БД")
        return
    for reminder_id, user_id, chat_id, text in due:
        try:
            await bot.send_message(chat_id, f"⏰ <b>Напоминание:</b> {text}")
        except Exception:
            pass
        await mark_reminder_done(reminder_id)


def setup_scheduler(bot: Bot) -> AsyncIOScheduler:
    scheduler = AsyncIOScheduler()
    scheduler.add_job(check_reminders, "interval", seconds=20, args=[bot])
    return scheduler

# ==============================================================================
# СПИСОК КОМАНД БОТА (появляется в кнопке "Меню" у поля ввода Telegram)
# ==============================================================================
from aiogram.types import BotCommand, BotCommandScopeAllPrivateChats, BotCommandScopeAllGroupChats

BOT_COMMANDS = [
    BotCommand(command="start", description="🚀 Запустить бота / открыть меню"),
    BotCommand(command="help", description="ℹ️ Список всех команд и как играть"),
    BotCommand(command="remind", description="⏰ Поставить напоминание"),
]


async def setup_bot_commands(bot: Bot):
    # Команды видны и в личке, и в группах — Telegram требует задавать это отдельно
    # для каждого scope, иначе в группах меню может не показаться.
    await bot.set_my_commands(BOT_COMMANDS, scope=BotCommandScopeAllPrivateChats())
    await bot.set_my_commands(BOT_COMMANDS, scope=BotCommandScopeAllGroupChats())


# ==============================================================================
# ГЛОБАЛЬНАЯ ОБРАБОТКА ОШИБОК
# ==============================================================================
# Без этого любое исключение внутри хендлера (например, сбой БД) просто
# проглатывается aiogram — бот "молчит", а в логах ничего не видно.
# С этим — полный traceback всегда попадает в Railway Logs.
async def global_error_handler(event, exception):
    logging.exception("Необработанная ошибка в хендлере: %s", exception)
    return True


# ==============================================================================
# ЗАПУСК БОТА
# ==============================================================================
logging.basicConfig(level=logging.INFO)


async def main():
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN не задан. Проверь .env файл или переменные Railway.")

    await init_db()

    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher(storage=MemoryStorage())

    dp.errors.register(global_error_handler)

    # ВАЖНО: admin_router регистрируется первым, чтобы /admin перехватывался
    # до общих хендлеров команд.
    dp.include_router(admin_router)
    dp.include_router(mychatmember_router)
    dp.include_router(common_router)
    dp.include_router(reminders_router)

    # Игры
    dp.include_router(number_router)
    dp.include_router(mafia_router)
    dp.include_router(roulette_router)
    dp.include_router(dice_router)
    dp.include_router(rps_router)
    dp.include_router(hangman_router)
    dp.include_router(bulls_cows_router)
    dp.include_router(reaction_router)
    dp.include_router(tictactoe_router)
    dp.include_router(wheel_router)

    scheduler = setup_scheduler(bot)
    scheduler.start()

    await setup_bot_commands(bot)
    await bot.delete_webhook(drop_pending_updates=True)

    logging.info("Бот запущен и готов принимать сообщения (polling).")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
