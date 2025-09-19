import os
import json
import re
from datetime import datetime
from typing import Dict, Any

from telegram import Update, ChatMember
from telegram.constants import ChatType
from telegram.ext import (
    Application, ApplicationBuilder, CommandHandler, MessageHandler,
    ContextTypes, filters, ChatMemberHandler
)

# ---------- Defaults ----------
DEFAULT_FIELD = "Горизонт-арена"
DEFAULT_TIME = "20:00-22:00"
STATE_FILE = "state.json"

WEEKDAY_RU = ["Понедельник", "Вторник", "Среда", "Четверг",
              "Пятница", "Суббота", "Воскресенье"]

PLUS_PATTERN = re.compile(r"^\s*(\+|➕)\s*$")
MINUS_PATTERN = re.compile(r"^\s*(-|—|–|➖)\s*$")
PLUS_ONE_PATTERN = re.compile(r"^\s*(\+|➕)\s*1\s*$")
MINUS_ONE_PATTERN = re.compile(r"^\s*(-|—|–|➖)\s*1\s*$")

# Невидимый маркер для записи "гость без хозяина"
GUEST_ONLY_SUFFIX = " +1\u200b"   # \u200b = zero-width space (не виден в Telegram)

# ---------- Simple storage ----------
# state per chat_id:
# {
#   "open": bool,
#   "date": "21/09/25",
#   "time": "20:00-22:00",
#   "field": "Горизонт-арена",
#   "limit": 0,  # 0 = unlimited
#   "users": ["Имя (@username)", ...]
# }
state: Dict[str, Dict[str, Any]] = {}

def load_state():
    global state
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            state = json.load(f)
    else:
        state = {}

def save_state():
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)

def ensure_chat(chat_id: int):
    cid = str(chat_id)
    if cid not in state:
        state[cid] = {
            "open": False,
            "date": datetime.now().strftime("%d/%m/%y"),
            "time": DEFAULT_TIME,
            "field": DEFAULT_FIELD,
            "limit": 0,
            "users": []
        }

def is_admin_member(member: ChatMember) -> bool:
    return member.status in ("administrator", "creator", "owner")

async def is_admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    if update.effective_chat is None or update.effective_user is None:
        return False
    chat = update.effective_chat
    user_id = update.effective_user.id
    try:
        member = await context.bot.get_chat_member(chat.id, user_id)
        return is_admin_member(member)
    except Exception:
        return False

def format_header(chat_state: Dict[str, Any]) -> str:
    # weekday from date
    try:
        d = datetime.strptime(chat_state["date"], "%d/%m/%y")
        weekday = WEEKDAY_RU[d.weekday()]
    except Exception:
        weekday = "?"

    date_str = chat_state["date"]
    field = chat_state.get("field", DEFAULT_FIELD)
    time = chat_state.get("time", DEFAULT_TIME)
    return f"📅 {date_str} ({weekday})\n🏟️ Поле: {field}\n⏰ Время: {time}"

def format_list(chat_state: Dict[str, Any]) -> str:
    header = format_header(chat_state)
    users = chat_state.get("users", [])
    limit = chat_state.get("limit", 0)

    # ↓ добавлено
    expanded = _expanded_users(users)
    count = len(expanded)

    if expanded:
        body = "\n".join(f"{i+1}. {u}" for i, u in enumerate(expanded))
    else:
        body = "Пока пусто. Пиши '+' чтобы записаться."

    cap = f"\n\n⚠️ Достигнут лимит ({limit})." if limit and count >= limit else ""
    status = "Открыто ✅" if chat_state.get("open") else "Закрыто ⛔️"
    return f"{header}\n\nСтатус: {status}\nУчастников: {count}{cap}\n\n{body}"


def display_name_from_update(update: Update) -> str:
    u = update.effective_user
    if not u:
        return "Без имени"
    name_parts = []
    if u.first_name:
        name_parts.append(u.first_name)
    if u.last_name:
        name_parts.append(u.last_name)
    name = " ".join(name_parts) if name_parts else (u.username or str(u.id))
    if u.username:
        return f"{name} (@{u.username})"
    return name

def parse_date(s: str) -> str:
    datetime.strptime(s, "%d/%m/%y")
    return s

def parse_time(s: str) -> str:
    if re.match(r"^\\d{2}:\\d{2}-\\d{2}:\\d{2}$", s):
        return s
    raise ValueError("Неверный формат времени. Пример: 20:00-22:00")

def _normalize_entry(entry: str) -> str:
    # "Имя (@user) +1" -> "Имя (@user)"
    return entry[:-3].strip() if entry.endswith(" +1") else entry.strip()

def _find_user_index(users, display_name: str, username: str | None):
    dn = display_name.lower().strip()
    un = ("@" + username.lower()) if username else None
    for i, e in enumerate(users):
        el = e.lower()
        if un and un in el:
            return i
        if _normalize_entry(el) == dn:
            return i
    return -1

def _total_count(users) -> int:
    # Считает людей с учётом +1
    c = 0
    for e in users:
        c += 1
        if e.endswith(" +1"):
            c += 1
    return c
    
def _expanded_users(users):
    """Возвращает список строк для отображения: если у записи есть +1,
    выводим две строки — базовую и ту же с суффиксом ' +1'."""
    out = []
    for e in users:
        if e.endswith(" +1"):
            base = e[:-3].strip()
            out.append(base)  # основная строка
            out.append(e)     # гость как отдельная строка
        else:
            out.append(e)
    return out

def _strip_guest_suffix(entry: str) -> str:
    if entry.endswith(" +1"):
        return entry[:-3].strip()
    if entry.endswith(GUEST_ONLY_SUFFIX):
        return entry[:-(len(GUEST_ONLY_SUFFIX))].strip()
    return entry.strip()

def _has_guest(entry: str) -> bool:
    return entry.endswith(" +1") or entry.endswith(GUEST_ONLY_SUFFIX)

def _is_guest_only(entry: str) -> bool:
    return entry.endswith(GUEST_ONLY_SUFFIX)

def _find_user_index(users, display_name: str, username: str | None) -> int:
    dn = display_name.lower().strip()
    un = ("@" + username.lower()) if username else None
    for i, e in enumerate(users):
        el = e.lower()
        if un and un in el:
            return i
        if _strip_guest_suffix(el) == dn:
            return i
    return -1

def _total_count(users) -> int:
    # 2 человека для " +1", 1 человек для " +1\u200b" (гость без хозяина) и для обычной записи
    c = 0
    for e in users:
        if e.endswith(" +1"):
            c += 2
        else:
            c += 1
    return c

def _expanded_users(users):
    """Для отображения: разворачиваем пары в две строки,
    а 'гость без хозяина' показываем одной строкой."""
    out = []
    for e in users:
        if e.endswith(" +1"):
            base = e[:-3].strip()
            out.append(base)
            out.append(base + " +1")
        elif e.endswith(GUEST_ONLY_SUFFIX):
            base = e[:-(len(GUEST_ONLY_SUFFIX))].strip()
            out.append(base + " +1")   # только гость
        else:
            out.append(e)
    return out

# ---------- Commands ----------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat is None:
        return
    ensure_chat(update.effective_chat.id)
    await update.message.reply_text(
        "Привет! Я веду список на футбол.\n"
        "Участникам: просто напишите '+'\n\n"
        "Админам:\n"
        "/open [ДД/ММ/ГГ] [ЧЧ:ММ-ЧЧ:ММ]\n"
        "/setdate ДД/ММ/ГГ\n"
        "/settime ЧЧ:ММ-ЧЧ:ММ\n"
        "/setfield ТЕКСТ\n"
        "/setlimit N\n"
        "/remove @username|Имя\n"
        "/list\n"
        "/reset\n"
        "/close\n"
        "/help"
    )

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await start(update, context)

async def open_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat is None:
        return
    if not await is_admin(update, context):
        return
    ensure_chat(update.effective_chat.id)
    chat_state = state[str(update.effective_chat.id)]

    args = context.args
    try:
        if len(args) >= 1:
            chat_state["date"] = parse_date(args[0])
        if len(args) >= 2:
            chat_state["time"] = parse_time(args[1])
        chat_state["open"] = True
        save_state()
        await update.message.reply_text("Запись открыта ✅\n\n" + format_list(chat_state))
    except ValueError as e:
        await update.message.reply_text(f"Ошибка: {e}")

async def setdate_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat is None:
        return
    if not await is_admin(update, context):
        return
    ensure_chat(update.effective_chat.id)
    chat_state = state[str(update.effective_chat.id)]
    if not context.args:
        await update.message.reply_text("Укажите дату: /setdate ДД/ММ/ГГ")
        return
    try:
        chat_state["date"] = parse_date(context.args[0])
        save_state()
        await update.message.reply_text("Дата обновлена ✅\n\n" + format_list(chat_state))
    except ValueError as e:
        await update.message.reply_text(f"Ошибка: {e}")

async def settime_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat is None:
        return
    if not await is_admin(update, context):
        return
    ensure_chat(update.effective_chat.id)
    chat_state = state[str(update.effective_chat.id)]
    if not context.args:
        await update.message.reply_text("Укажите время: /settime ЧЧ:ММ-ЧЧ:ММ")
        return
    try:
        chat_state["time"] = parse_time(context.args[0])
        save_state()
        await update.message.reply_text("Время обновлено ✅\n\n" + format_list(chat_state))
    except ValueError as e:
        await update.message.reply_text(f"Ошибка: {e}")

async def setfield_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat is None:
        return
    if not await is_admin(update, context):
        return
    ensure_chat(update.effective_chat.id)
    chat_state = state[str(update.effective_chat.id)]
    text = " ".join(context.args).strip()
    if not text:
        await update.message.reply_text("Укажите название поля: /setfield Горизонт-арена")
        return
    chat_state["field"] = text
    save_state()
    await update.message.reply_text("Поле обновлено ✅\n\n" + format_list(chat_state))

async def setlimit_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat is None:
        return
    if not await is_admin(update, context):
        return
    ensure_chat(update.effective_chat.id)
    chat_state = state[str(update.effective_chat.id)]
    if not context.args:
        await update.message.reply_text("Укажите лимит: /setlimit 28 (0 = без лимита)")
        return
    try:
        lim = int(context.args[0])
        chat_state["limit"] = max(0, lim)
        save_state()
        await update.message.reply_text("Лимит обновлён ✅\n\n" + format_list(chat_state))
    except Exception:
        await update.message.reply_text("Неверное значение. Пример: /setlimit 28")

async def remove_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat is None:
        return
    if not await is_admin(update, context):
        return
    ensure_chat(update.effective_chat.id)
    chat_state = state[str(update.effective_chat.id)]
    key = " ".join(context.args).strip()
    if not key:
        await update.message.reply_text("Кого убрать? /remove @username или /remove Имя")
        return
    users = chat_state["users"]
    before = len(users)
    users = [u for u in users if key.lower() not in u.lower()]
    chat_state["users"] = users
    save_state()
    removed = before - len(users)
    await update.message.reply_text(f"Убрано: {removed}\n\n" + format_list(chat_state))

async def list_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat is None:
        return
    ensure_chat(update.effective_chat.id)
    chat_state = state[str(update.effective_chat.id)]
    await update.message.reply_text(format_list(chat_state))

async def reset_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat is None:
        return
    if not await is_admin(update, context):
        return
    ensure_chat(update.effective_chat.id)
    chat_state = state[str(update.effective_chat.id)]
    chat_state["users"] = []
    save_state()
    await update.message.reply_text("Список очищен 🧹\n\n" + format_list(chat_state))

async def close_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat is None:
        return
    if not await is_admin(update, context):
        return
    ensure_chat(update.effective_chat.id)
    chat_state = state[str(update.effective_chat.id)]
    chat_state["open"] = False
    save_state()
    await update.message.reply_text("Запись закрыта ⛔️\n\n" + format_list(chat_state))

# '+' handler
async def plus_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat is None or update.message is None:
        return
    if update.effective_chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        return

    ensure_chat(update.effective_chat.id)
    chat_state = state[str(update.effective_chat.id)]
    if not chat_state.get("open", False):
        await update.message.reply_text("Запись закрыта ⛔️. Админам: /open")
        return

    users = chat_state["users"]
    limit = chat_state.get("limit", 0)
    cur = _total_count(users)

    disp = display_name_from_update(update)
    uname = update.effective_user.username if update.effective_user else None

    # уже в списке?
    idx = _find_user_index(users, disp, uname)
    if idx != -1:
        await update.message.reply_text("Ты уже в списке ✅")
        return

    # проверка лимита (+1 человек)
    if limit and cur + 1 > limit:
        await update.message.reply_text(f"⚠️ Нет свободных мест (лимит {limit}).")
        return

    users.append(disp)
    save_state()
    await update.message.reply_text("Записал! ✅\n\n" + format_list(chat_state))
    
async def plus_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat is None or update.message is None:
        return
    if update.effective_chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        return

    ensure_chat(update.effective_chat.id)
    chat_state = state[str(update.effective_chat.id)]
    if not chat_state.get("open", False):
        await update.message.reply_text("Запись закрыта ⛔️. Админам: /open")
        return

    users = chat_state["users"]
    limit = chat_state.get("limit", 0)
    cur = _total_count(users)

    disp = display_name_from_update(update)
    uname = update.effective_user.username if update.effective_user else None
    idx = _find_user_index(users, disp, uname)

    if idx != -1:
        entry = users[idx]
        if _is_guest_only(entry):
            # вернуть хозяина (станет двое)
            if limit and cur + 1 > limit:
                await update.message.reply_text(f"⚠️ Нет свободных мест (лимит {limit}).")
                return
            users[idx] = _strip_guest_suffix(entry) + " +1"
            save_state()
            await update.message.reply_text("Вернул тебя, гость остаётся 👥✅\n\n" + format_list(chat_state))
            return
        else:
            await update.message.reply_text("Ты уже в списке ✅")
            return

    # не было в списке — добавляем одного
    if limit and cur + 1 > limit:
        await update.message.reply_text(f"⚠️ Нет свободных мест (лимит {limit}).")
        return
    users.append(disp)
    save_state()
    await update.message.reply_text("Записал! ✅\n\n" + format_list(chat_state))
        
async def minus_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat is None or update.message is None:
        return
    if update.effective_chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        return

    ensure_chat(update.effective_chat.id)
    chat_state = state[str(update.effective_chat.id)]
    users = chat_state["users"]

    disp = display_name_from_update(update)
    uname = update.effective_user.username if update.effective_user else None
    idx = _find_user_index(users, disp, uname)

    if idx == -1:
        await update.message.reply_text("Тебя нет в списке — ничего не удалял.")
        return

    entry = users[idx]
    if entry.endswith(" +1"):
        # хозяин уходит, гость остаётся
        users[idx] = _strip_guest_suffix(entry) + GUEST_ONLY_SUFFIX
        save_state()
        await update.message.reply_text("Убрал тебя, гость остаётся 👤➡️👥\n\n" + format_list(chat_state))
        return

    if _is_guest_only(entry):
        # уже остался только гость — хозяина и так нет
        await update.message.reply_text("У тебя уже остался только +1. Чтобы убрать гостя — отправь -1.")
        return

    # обычная запись без гостя — удаляем полностью
    del users[idx]
    save_state()
    await update.message.reply_text("Убрал тебя из списка 👌\n\n" + format_list(chat_state))

async def minus_one_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat is None or update.message is None:
        return
    if update.effective_chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        return

    ensure_chat(update.effective_chat.id)
    chat_state = state[str(update.effective_chat.id)]
    users = chat_state["users"]

    disp = display_name_from_update(update)
    uname = update.effective_user.username if update.effective_user else None
    idx = _find_user_index(users, disp, uname)

    if idx == -1:
        await update.message.reply_text("Тебя нет в списке — нечего менять.")
        return

    entry = users[idx]
    if entry.endswith(" +1"):
        users[idx] = _strip_guest_suffix(entry)  # остаётся только хозяин
        save_state()
        await update.message.reply_text("Убрал твоего +1 👌\n\n" + format_list(chat_state))
    elif _is_guest_only(entry):
        del users[idx]  # остался один гость — теперь никто
        save_state()
        await update.message.reply_text("Убрал твоего +1 👌\n\n" + format_list(chat_state))
    else:
        await update.message.reply_text("У тебя не было +1.")

def main():
    load_state()
    token = os.getenv("BOT_TOKEN")
    if not token:
        raise RuntimeError("Установите переменную окружения BOT_TOKEN")

    app = ApplicationBuilder().token(token).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("open", open_cmd))
    app.add_handler(CommandHandler("setdate", setdate_cmd))
    app.add_handler(CommandHandler("settime", settime_cmd))
    app.add_handler(CommandHandler("setfield", setfield_cmd))
    app.add_handler(CommandHandler("setlimit", setlimit_cmd))
    app.add_handler(CommandHandler("remove", remove_cmd))
    app.add_handler(CommandHandler("list", list_cmd))
    app.add_handler(CommandHandler("reset", reset_cmd))
    app.add_handler(CommandHandler("close", close_cmd))
    
    app.add_handler(MessageHandler(filters.TEXT & filters.Regex(PLUS_ONE_PATTERN), plus_one_message))
    app.add_handler(MessageHandler(filters.TEXT & filters.Regex(MINUS_ONE_PATTERN), minus_one_message))
    app.add_handler(MessageHandler(filters.TEXT & filters.Regex(PLUS_PATTERN), plus_message))
    app.add_handler(MessageHandler(filters.TEXT & filters.Regex(MINUS_PATTERN), minus_message))
    app.add_handler(ChatMemberHandler(handle_member_update, ChatMemberHandler.CHAT_MEMBER))

    print("Bot is running. Press Ctrl+C to stop.")
    # ВАЖНО: без await/asyncio
    app.run_polling()

if __name__ == "__main__":
    try:
        import dotenv
        dotenv.load_dotenv()
    except Exception:
        pass
    main()
