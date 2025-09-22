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
STATE_FILE = "state.json"  # при желании: os.getenv("STATE_FILE", "/data/state.json")

WEEKDAY_RU = ["Понедельник", "Вторник", "Среда", "Четверг",
              "Пятница", "Суббота", "Воскресенье"]

# Ограничение на гостей от одного человека
MAX_GUESTS_PER_HOST = 5
ZWS = "\u200b"  # zero-width space

# ---------- Patterns ----------
PLUS_PATTERN = re.compile(r"^\s*(\+|➕)\s*$")
MINUS_PATTERN = re.compile(r"^\s*(-|—|–|➖)\s*$")
PLUS_ONE_PATTERN = re.compile(r"^\s*(\+|➕)\s*1\s*$")
MINUS_ONE_PATTERN = re.compile(r"^\s*(-|—|–|➖)\s*1\s*$")
# +N / -N (N = 2..5)
PLUS_N_PATTERN = re.compile(r"^\s*(\+|➕)\s*([2-5])\s*$")
MINUS_N_PATTERN = re.compile(r"^\s*(-|—|–|➖)\s*([2-5])\s*$")

# ---------- Simple storage ----------
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
    try:
        member = await context.bot.get_chat_member(update.effective_chat.id, update.effective_user.id)
        return is_admin_member(member)
    except Exception:
        return False

# ---------- Helpers: guests parsing ----------
# запись хранится как:
# "Имя"                  -> хозяин без гостей
# "Имя +N"               -> хозяин с N гостями
# "Имя +N{ZWS}"          -> только гости (хозяина нет), N гостей

_guest_re = re.compile(r"\s\+(\d+)(\u200b)?$")

def _guest_count(entry: str) -> int:
    m = _guest_re.search(entry)
    return int(m.group(1)) if m else 0

def _is_guest_only(entry: str) -> bool:
    m = _guest_re.search(entry)
    return bool(m and m.group(2))

def _strip_guest(entry: str) -> str:
    return _guest_re.sub("", entry).strip()

def _make_entry(display: str, n: int, guest_only: bool) -> str:
    display = display.strip()
    if n <= 0:
        return display
    return f"{display} +{n}{ZWS if guest_only else ''}"

def _find_user_index(users, display_name: str, username: str | None) -> int:
    dn = display_name.lower().strip()
    un = ("@" + username.lower()) if username else None
    for i, e in enumerate(users):
        el = e.lower()
        if un and un in el:
            return i
        if _strip_guest(el) == dn:
            return i
    return -1

def _total_count(users) -> int:
    # owner+guests -> 1+N; guest-only -> N; solo -> 1
    c = 0
    for e in users:
        n = _guest_count(e)
        if n:
            c += n if _is_guest_only(e) else (1 + n)
        else:
            c += 1
    return c

def _expanded_users(users):
    """Для отображения: хозяина и каждого гостя выводим отдельной строкой."""
    out = []
    for e in users:
        n = _guest_count(e)
        base = _strip_guest(e)
        if n:
            if not _is_guest_only(e):
                out.append(base)  # хозяин
            out.extend([f"{base} +1"] * n)  # n строк гостей
        else:
            out.append(base)
    return out

def format_header(chat_state: Dict[str, Any]) -> str:
    try:
        d = datetime.strptime(chat_state["date"], "%d/%m/%y")
        weekday = WEEKDAY_RU[d.weekday()]
    except Exception:
        weekday = "?"
    return (
        f"📅 {chat_state['date']} ({weekday})\n"
        f"🏟️ Поле: {chat_state.get('field', DEFAULT_FIELD)}\n"
        f"⏰ Время: {chat_state.get('time', DEFAULT_TIME)}"
    )

def format_list(chat_state: Dict[str, Any]) -> str:
    header = format_header(chat_state)
    users = chat_state.get("users", [])
    limit = chat_state.get("limit", 0)

    expanded = _expanded_users(users)
    count = len(expanded)

    body = "\n".join(f"{i+1}. {u}" for i, u in enumerate(expanded)) if expanded \
        else "Пока пусто. Пиши '+' чтобы записаться."

    cap = f"\n\n⚠️ Достигнут лимит ({limit})." if limit and count >= limit else ""
    status = "Открыто ✅" if chat_state.get("open") else "Закрыто ⛔️"
    return f"{header}\n\nСтатус: {status}\nУчастников: {count}{cap}\n\n{body}"

def display_name_from_update(update: Update) -> str:
    u = update.effective_user
    if not u:
        return "Без имени"
    parts = []
    if u.first_name: parts.append(u.first_name)
    if u.last_name: parts.append(u.last_name)
    name = " ".join(parts) if parts else (u.username or str(u.id))
    return f"{name} (@{u.username})" if u.username else name

def parse_date(s: str) -> str:
    datetime.strptime(s, "%d/%m/%y")
    return s

def parse_time(s: str) -> str:
    if re.match(r"^\d{2}:\d{2}-\d{2}:\d{2}$", s):
        return s
    raise ValueError("Неверный формат времени. Пример: 20:00-22:00")

# ---------- Commands ----------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat is None:
        return
    ensure_chat(update.effective_chat.id)
    await update.message.reply_text(
        "Привет! Я веду список на футбол.\n"
        "Участникам: '+', '-'; '+1'/'-1'; '+N'/'-N' (N=2..5).\n\n"
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
    if update.effective_chat is None or not await is_admin(update, context):
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
    if update.effective_chat is None or not await is_admin(update, context):
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
    if update.effective_chat is None or not await is_admin(update, context):
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
    if update.effective_chat is None or not await is_admin(update, context):
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
    if update.effective_chat is None or not await is_admin(update, context):
        return
    ensure_chat(update.effective_chat.id)
    chat_state = state[str(update.effective_chat.id)]
    if not context.args:
        await update.message.reply_text("Укажите лимит: /setlimit 28 (0 = без лимита)")
        return
    try:
        chat_state["limit"] = max(0, int(context.args[0]))
        save_state()
        await update.message.reply_text("Лимит обновлён ✅\n\n" + format_list(chat_state))
    except Exception:
        await update.message.reply_text("Неверное значение. Пример: /setlimit 28")

async def remove_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat is None or not await is_admin(update, context):
        return
    ensure_chat(update.effective_chat.id)
    chat_state = state[str(update.effective_chat.id)]
    key = " ".join(context.args).strip()
    if not key:
        await update.message.reply_text("Кого убрать? /remove @username или /remove Имя")
        return
    before = len(chat_state["users"])
    chat_state["users"] = [u for u in chat_state["users"] if key.lower() not in u.lower()]
    removed = before - len(chat_state["users"])
    save_state()
    await update.message.reply_text(f"Убрано: {removed}\n\n" + format_list(chat_state))

async def list_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat is None:
        return
    ensure_chat(update.effective_chat.id)
    await update.message.reply_text(format_list(state[str(update.effective_chat.id)]))

async def reset_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat is None or not await is_admin(update, context):
        return
    ensure_chat(update.effective_chat.id)
    state[str(update.effective_chat.id)]["users"] = []
    save_state()
    await update.message.reply_text("Список очищен 🧹\n\n" + format_list(state[str(update.effective_chat.id)]))

async def close_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat is None or not await is_admin(update, context):
        return
    ensure_chat(update.effective_chat.id)
    state[str(update.effective_chat.id)]["open"] = False
    save_state()
    await update.message.reply_text("Запись закрыта ⛔️\n\n" + format_list(state[str(update.effective_chat.id)]))

# ---------- Message Handlers ----------
async def plus_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """'+': добавить хозяина (если был guest-only, вернуть хозяина)."""
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
            # вернуть хозяина, гостей не меняем
            if limit and cur + 1 > limit:
                await update.message.reply_text(f"⚠️ Нет свободных мест (лимит {limit}).")
                return
            n = _guest_count(entry)
            users[idx] = _make_entry(_strip_guest(entry), n, guest_only=False)
            save_state()
            await update.message.reply_text("Вернул тебя, гость(и) остаются 👥✅\n\n" + format_list(chat_state))
            return
        else:
            await update.message.reply_text("Ты уже в списке ✅")
            return

    if limit and cur + 1 > limit:
        await update.message.reply_text(f"⚠️ Нет свободных мест (лимит {limit}).")
        return
    users.append(_make_entry(disp, 0, False))
    save_state()
    await update.message.reply_text("Записал! ✅\n\n" + format_list(chat_state))

async def _plus_n_common(update: Update, n: int):
    """Общая логика для '+N' (включая '+1')."""
    chat = update.effective_chat
    if chat is None or update.message is None:
        return
    ensure_chat(chat.id)
    chat_state = state[str(chat.id)]
    if not chat_state.get("open", False):
        await update.message.reply_text("Запись закрыта ⛔️. Админам: /open")
        return

    users = chat_state["users"]
    limit = chat_state.get("limit", 0)
    cur = _total_count(users)
    disp = display_name_from_update(update)
    uname = update.effective_user.username if update.effective_user else None
    idx = _find_user_index(users, disp, uname)

    if idx == -1:
        # нет в списке → хозяин + N гостей
        if n > MAX_GUESTS_PER_HOST:
            await update.message.reply_text(f"Максимум +{MAX_GUESTS_PER_HOST}.")
            return
        if limit and cur + (1 + n) > limit:
            await update.message.reply_text(f"⚠️ Не хватает мест для +{n} (лимит {limit}).")
            return
        users.append(_make_entry(disp, n, False))
        save_state()
        await update.message.reply_text(f"Записал тебя с +{n} 👥✅\n\n" + format_list(chat_state))
        return

    entry = users[idx]
    base = _strip_guest(entry)
    cur_n = _guest_count(entry)
    is_guest_only = _is_guest_only(entry)

    # сколько доп. мест потребуется (для проверки лимита)
    extra_slots = n + (1 if is_guest_only else 0)

    if cur_n + n > MAX_GUESTS_PER_HOST:
        await update.message.reply_text(f"Слишком много гостей. У тебя уже +{cur_n}. Максимум +{MAX_GUESTS_PER_HOST}.")
        return

    if limit and cur + extra_slots > limit:
        await update.message.reply_text(f"⚠️ Не хватает мест для +{n} (лимит {limit}).")
        return

    new_n = cur_n + n
    users[idx] = _make_entry(base, new_n, False)  # хозяин точно будет после '+N'
    save_state()
    await update.message.reply_text(f"Добавил +{n} 👥✅\n\n" + format_list(chat_state))

async def plus_one_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _plus_n_common(update, 1)

async def plus_n_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # вытащим N из текста
    m = PLUS_N_PATTERN.match(update.message.text or "")
    n = int(m.group(2)) if m else 0
    if n:
        await _plus_n_common(update, n)

async def minus_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """'-': убрать хозяина. Если были гости — останутся как guest-only."""
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
    n = _guest_count(entry)
    if n > 0 and not _is_guest_only(entry):
        users[idx] = _make_entry(_strip_guest(entry), n, True)  # остаются только гости
        save_state()
        await update.message.reply_text("Убрал тебя, гости остаются 👤➡️👥\n\n" + format_list(chat_state))
        return

    if _is_guest_only(entry):
        await update.message.reply_text("У тебя уже остались только гости. Чтобы убрать их — используй -1 или -N.")
        return

    # обычная запись без гостей — удаляем
    del users[idx]
    save_state()
    await update.message.reply_text("Убрал тебя из списка 👌\n\n" + format_list(chat_state))

async def _minus_n_common(update: Update, n: int):
    """Общая логика для '-N' (включая '-1'): убавить гостей."""
    chat = update.effective_chat
    if chat is None or update.message is None:
        return
    ensure_chat(chat.id)
    chat_state = state[str(chat.id)]
    users = chat_state["users"]

    disp = display_name_from_update(update)
    uname = update.effective_user.username if update.effective_user else None
    idx = _find_user_index(users, disp, uname)
    if idx == -1:
        await update.message.reply_text("Тебя нет в списке — нечего менять.")
        return

    entry = users[idx]
    base = _strip_guest(entry)
    cur_n = _guest_count(entry)
    is_guest_only = _is_guest_only(entry)

    if cur_n == 0:
        await update.message.reply_text("У тебя нет гостей.")
        return

    if n >= cur_n:
        # снимаем всех гостей
        if is_guest_only:
            del users[idx]  # был только гость -> никого не осталось
        else:
            users[idx] = base  # остаётся только хозяин
        save_state()
        await update.message.reply_text("Убрал твоих гостей 👌\n\n" + format_list(chat_state))
        return

    # иначе уменьшаем счётчик
    new_n = cur_n - n
    users[idx] = _make_entry(base, new_n, is_guest_only)
    save_state()
    await update.message.reply_text(f"Убрал {n} гостя(ей) 👌\n\n" + format_list(chat_state))

async def minus_one_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _minus_n_common(update, 1)

async def minus_n_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    m = MINUS_N_PATTERN.match(update.message.text or "")
    n = int(m.group(2)) if m else 0
    if n:
        await _minus_n_common(update, n)

async def handle_member_update(update: Update, context: ContextTypes.DEFAULT_TYPE):
    return

# ---------- main ----------
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

    # Порядок важен: сначала числовые, потом единичные, потом просто +/-
    app.add_handler(MessageHandler(filters.TEXT & filters.Regex(PLUS_N_PATTERN), plus_n_message))
    app.add_handler(MessageHandler(filters.TEXT & filters.Regex(MINUS_N_PATTERN), minus_n_message))
    app.add_handler(MessageHandler(filters.TEXT & filters.Regex(PLUS_ONE_PATTERN), plus_one_message))
    app.add_handler(MessageHandler(filters.TEXT & filters.Regex(MINUS_ONE_PATTERN), minus_one_message))
    app.add_handler(MessageHandler(filters.TEXT & filters.Regex(PLUS_PATTERN), plus_message))
    app.add_handler(MessageHandler(filters.TEXT & filters.Regex(MINUS_PATTERN), minus_message))

    app.add_handler(ChatMemberHandler(handle_member_update, ChatMemberHandler.CHAT_MEMBER))

    print("Bot is running. Press Ctrl+C to stop.")
    app.run_polling()

if __name__ == "__main__":
    try:
        import dotenv
        dotenv.load_dotenv()
    except Exception:
        pass
    main()
