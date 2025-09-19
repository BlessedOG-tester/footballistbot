import os
import json
import re
import asyncio
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

PLUS_PATTERN = re.compile(r"^\\s*\\+\\s*$")   # strict '+' message

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
    count = len(users)

    if users:
        body = "\\n".join([f"{i+1}. {u}" for i, u in enumerate(users)])
    else:
        body = "Пока пусто. Пиши '+' чтобы записаться."

    cap = f"\\n\\n⚠️ Достигнут лимит ({limit})." if limit and count >= limit else ""
    status = "Открыто ✅" if chat_state.get("open") else "Закрыто ⛔️"
    return f"{header}\\n\\nСтатус: {status}\\nУчастников: {count}" + cap + f"\\n\\n{body}"

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

# ---------- Commands ----------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat is None:
        return
    ensure_chat(update.effective_chat.id)
    await update.message.reply_text(
        "Привет! Я веду список на футбол.\\n"
        "Участникам: просто напишите '+'\\n\\n"
        "Админам:\\n"
        "/open [ДД/ММ/ГГ] [ЧЧ:ММ-ЧЧ:ММ]\\n"
        "/setdate ДД/ММ/ГГ\\n"
        "/settime ЧЧ:ММ-ЧЧ:ММ\\n"
        "/setfield ТЕКСТ\\n"
        "/setlimit N\\n"
        "/remove @username|Имя\\n"
        "/list\\n"
        "/reset\\n"
        "/close\\n"
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
        await update.message.reply_text("Запись открыта ✅\\n\\n" + format_list(chat_state))
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
        await update.message.reply_text("Дата обновлена ✅\\n\\n" + format_list(chat_state))
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
        await update.message.reply_text("Время обновлено ✅\\n\\n" + format_list(chat_state))
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
    await update.message.reply_text("Поле обновлено ✅\\n\\n" + format_list(chat_state))

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
        await update.message.reply_text("Лимит обновлён ✅\\n\\n" + format_list(chat_state))
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
    await update.message.reply_text(f"Убрано: {removed}\\n\\n" + format_list(chat_state))

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
    await update.message.reply_text("Список очищен 🧹\\n\\n" + format_list(chat_state))

async def close_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat is None:
        return
    if not await is_admin(update, context):
        return
    ensure_chat(update.effective_chat.id)
    chat_state = state[str(update.effective_chat.id)]
    chat_state["open"] = False
    save_state()
    await update.message.reply_text("Запись закрыта ⛔️\\n\\n" + format_list(chat_state))

# '+' handler
async def plus_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat is None or update.message is None:
        return
    if update.effective_chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        return
    ensure_chat(update.effective_chat.id)
    chat_state = state[str(update.effective_chat.id)]
    if not chat_state.get("open", False):
        return

    limit = chat_state.get("limit", 0)
    if limit and len(chat_state["users"]) >= limit:
        await update.message.reply_text("⚠️ Лимит достигнут. Свободных мест нет.")
        return

    name = display_name_from_update(update)
    if any(name.lower() == u.lower() for u in chat_state["users"]):
        await update.message.reply_text("Ты уже в списке ✅")
        return

    chat_state["users"].append(name)
    save_state()
    await update.message.reply_text("Записал! ✅\\n\\n" + format_list(chat_state))

async def handle_member_update(update: Update, context: ContextTypes.DEFAULT_TYPE):
    return

async def main():
    load_state()
    token = os.getenv("BOT_TOKEN")
    if not token:
        raise RuntimeError("Установите переменную окружения BOT_TOKEN")

    app: Application = ApplicationBuilder().token(token).build()

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

    app.add_handler(MessageHandler(filters.TEXT & filters.Regex(PLUS_PATTERN), plus_message))
    app.add_handler(ChatMemberHandler(handle_member_update, ChatMemberHandler.CHAT_MEMBER))

    print("Bot is running. Press Ctrl+C to stop.")
    await app.run_polling(close_loop=False)

if __name__ == "__main__":
    try:
        import dotenv
        dotenv.load_dotenv()
    except Exception:
        pass
    asyncio.run(main())
