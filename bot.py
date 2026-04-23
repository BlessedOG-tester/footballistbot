import asyncio
import json
import logging
import os
import re
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from telegram import (
    ChatMember,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ReplyKeyboardRemove,
    Update,
)
from telegram.constants import ChatType
from telegram.error import BadRequest, Forbidden, TelegramError
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CallbackQueryHandler,
    ChatMemberHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)


STATE_FILE = "state.json"
DEFAULT_FIELD = "Горизонт-арена"
DEFAULT_START = "20:30"
DEFAULT_END = "22:30"
DEFAULT_LIMIT = 18
TZ = timezone(timedelta(hours=5))
GOOGLE_SHEETS_WEBHOOK_URL_ENV = "GOOGLE_SHEETS_WEBHOOK_URL"

PLUS_RE = re.compile(r"^\s*\+(?P<count>[1-5])?\s*$")
MINUS_RE = re.compile(r"^\s*-(?P<count>[1-5])?\s*$|^\s*минус\s*$", re.IGNORECASE)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
state: Dict[str, Dict[str, Any]] = {}


def today_str() -> str:
    return datetime.now(TZ).strftime("%d/%m/%y")


def now_iso() -> str:
    return datetime.now(TZ).isoformat(timespec="seconds")


def default_game() -> Dict[str, Any]:
    return {
        "date": today_str(),
        "field": DEFAULT_FIELD,
        "start": DEFAULT_START,
        "end": DEFAULT_END,
        "limit": DEFAULT_LIMIT,
        "open": False,
        "reminder_sent": False,
        "winners_prompt_message_id": None,
    }


def default_chat_state(chat_title: str = "", chat_type: str = "") -> Dict[str, Any]:
    return {
        "chat_title": chat_title,
        "chat_type": chat_type,
        "active": True,
        "game": default_game(),
        "players": [],
        "history": [],
    }


def normalize_chat(chat_state: Dict[str, Any]) -> Dict[str, Any]:
    normalized = default_chat_state()
    if isinstance(chat_state, dict):
        normalized.update(chat_state)

    game = default_game()
    if isinstance(normalized.get("game"), dict):
        game.update(normalized["game"])
    if isinstance(chat_state, dict):
        if chat_state.get("date"):
            game["date"] = chat_state["date"]
        if chat_state.get("field"):
            game["field"] = chat_state["field"]
        if chat_state.get("limit") is not None:
            try:
                game["limit"] = int(chat_state["limit"])
            except (TypeError, ValueError):
                pass
        if chat_state.get("open") is not None:
            game["open"] = bool(chat_state["open"])
        if isinstance(chat_state.get("time"), str) and "-" in chat_state["time"]:
            try:
                start, end = parse_time_range(chat_state["time"])
                game["start"] = start
                game["end"] = end
            except ValueError:
                pass
    normalized["game"] = game

    normalized["players"] = normalize_players(normalized.get("players", []))
    if not isinstance(normalized.get("history"), list):
        normalized["history"] = []
    return normalized


def normalize_players(raw_players: Any) -> List[Dict[str, Any]]:
    if not isinstance(raw_players, list):
        return []

    players: List[Dict[str, Any]] = []
    for item in raw_players:
        if isinstance(item, str):
            players.append(
                {
                    "kind": "player",
                    "user_id": None,
                    "username": "",
                    "name": item,
                    "owner_id": None,
                    "owner_name": item,
                    "joined_at": "",
                }
            )
            continue

        if not isinstance(item, dict):
            continue

        kind = "guest" if item.get("kind") == "guest" else "player"
        players.append(
            {
                "kind": kind,
                "user_id": item.get("user_id") if kind == "player" else None,
                "username": item.get("username", "") if kind == "player" else "",
                "name": item.get("name") or item.get("display_name") or "",
                "owner_id": item.get("owner_id") or item.get("owner_user_id"),
                "owner_name": item.get("owner_name") or item.get("owner_display_name") or "",
                "owner_username": item.get("owner_username", ""),
                "joined_at": item.get("joined_at", ""),
            }
        )
    return players


def load_state():
    global state
    if not os.path.exists(STATE_FILE):
        state = {}
        return
    with open(STATE_FILE, "r", encoding="utf-8") as file:
        raw = json.load(file)
    state = {str(chat_id): normalize_chat(chat_state) for chat_id, chat_state in raw.items()}


def save_state():
    with open(STATE_FILE, "w", encoding="utf-8") as file:
        json.dump(state, file, ensure_ascii=False, indent=2)


def ensure_chat(chat_id: int, chat_title: str = "", chat_type: str = "") -> Dict[str, Any]:
    cid = str(chat_id)
    if cid not in state:
        state[cid] = default_chat_state(chat_title, chat_type)
    state[cid] = normalize_chat(state[cid])
    if chat_title:
        state[cid]["chat_title"] = chat_title
    if chat_type:
        state[cid]["chat_type"] = chat_type
    return state[cid]


def full_name(user) -> str:
    parts = [part for part in [user.first_name, user.last_name] if part]
    return " ".join(parts) if parts else (user.username or str(user.id))


def player_label(player: Dict[str, Any]) -> str:
    if player.get("kind") == "guest":
        owner = player.get("owner_name", "игрока")
        username = player.get("owner_username", "")
        suffix = f" (@{username})" if username and f"@{username}" not in owner else ""
        return f"+1 от {owner}{suffix}"
    username = player.get("username", "")
    suffix = f" (@{username})" if username and f"@{username}" not in player.get("name", "") else ""
    return f"{player.get('name') or 'Игрок'}{suffix}"


def user_player_index(chat_state: Dict[str, Any], user_id: int) -> Optional[int]:
    for index, player in enumerate(chat_state["players"]):
        if player.get("kind") == "player" and player.get("user_id") == user_id:
            return index
    return None


def guest_indexes(chat_state: Dict[str, Any], user_id: int) -> List[int]:
    return [
        index
        for index, player in enumerate(chat_state["players"])
        if player.get("kind") == "guest" and player.get("owner_id") == user_id
    ]


def main_players(chat_state: Dict[str, Any]) -> List[Dict[str, Any]]:
    return [player for player in chat_state["players"] if player.get("kind") == "player"]


def is_full(chat_state: Dict[str, Any]) -> bool:
    limit = int(chat_state["game"].get("limit", 0) or 0)
    return bool(limit and len(chat_state["players"]) >= limit)


def parse_date(value: str) -> str:
    cleaned = value.strip().replace(".", "/").replace("-", "/")
    for fmt in ("%d/%m/%y", "%d/%m/%Y"):
        try:
            return datetime.strptime(cleaned, fmt).strftime("%d/%m/%y")
        except ValueError:
            pass
    raise ValueError("Дата должна быть в формате 24/04/26")


def parse_time_range(value: str) -> Tuple[str, str]:
    match = re.match(r"^\s*(\d{2}:\d{2})\s*-\s*(\d{2}:\d{2})\s*$", value)
    if not match:
        raise ValueError("Время должно быть в формате 20:30-22:30")
    return match.group(1), match.group(2)


def parse_game_input(value: str) -> Dict[str, Any]:
    parts = value.strip().split()
    if len(parts) < 4:
        raise ValueError("Формат: 24/04/26 20:30-22:30 18 Горизонт-арена")

    date = parse_date(parts[0])
    start, end = parse_time_range(parts[1])
    try:
        limit = max(0, int(parts[2]))
    except ValueError as exc:
        raise ValueError("Количество участников должно быть числом") from exc
    field = " ".join(parts[3:]).strip()
    if not field:
        raise ValueError("Укажи поле")

    return {
        "date": date,
        "start": start,
        "end": end,
        "limit": limit,
        "field": field,
        "open": False,
        "reminder_sent": False,
        "winners_prompt_message_id": None,
    }


def game_end_datetime(game: Dict[str, Any]) -> Optional[datetime]:
    try:
        date = datetime.strptime(game["date"], "%d/%m/%y")
        end_time = datetime.strptime(game["end"], "%H:%M").time()
        return datetime.combine(date.date(), end_time, tzinfo=TZ)
    except (KeyError, ValueError):
        return None


def format_game(chat_state: Dict[str, Any]) -> str:
    game = chat_state["game"]
    status = "Открыта ✅" if game.get("open") else "Закрыта ⛔️"
    limit = int(game.get("limit", 0) or 0)
    count = len(chat_state["players"])
    limit_text = f"{count}/{limit}" if limit else str(count)

    if chat_state["players"]:
        rows = [f"{index + 1}. {player_label(player)}" for index, player in enumerate(chat_state["players"])]
        body = "\n".join(rows)
    else:
        body = "Пока пусто."

    return (
        f"📅 {game['date']}\n"
        f"🏟 Поле: {game['field']}\n"
        f"⏰ Время: {game['start']}-{game['end']}\n\n"
        f"Статус: {status}\n"
        f"Участников: {limit_text}\n\n"
        f"Список:\n{body}"
    )


def admin_keyboard(chat_state: Dict[str, Any]) -> InlineKeyboardMarkup:
    game = chat_state["game"]
    open_label = "Закрыть список" if game.get("open") else "Открыть список"
    open_action = "admin:close" if game.get("open") else "admin:open"
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton(open_label, callback_data=open_action)],
            [
                InlineKeyboardButton("Создать игру", callback_data="admin:new_game"),
                InlineKeyboardButton("Список", callback_data="admin:list"),
            ],
            [
                InlineKeyboardButton("Поле", callback_data="admin:field"),
                InlineKeyboardButton("Время", callback_data="admin:time"),
                InlineKeyboardButton("Лимит", callback_data="admin:limit"),
            ],
            [
                InlineKeyboardButton("Завершить игру", callback_data="admin:finish"),
                InlineKeyboardButton("Статистика", callback_data="admin:stats"),
            ],
        ]
    )


def stats_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("Месяц", callback_data="stats:month"),
                InlineKeyboardButton("Квартал", callback_data="stats:quarter"),
            ],
            [
                InlineKeyboardButton("Год", callback_data="stats:year"),
                InlineKeyboardButton("Всё время", callback_data="stats:all"),
            ],
            [InlineKeyboardButton("Назад", callback_data="admin:back")],
        ]
    )


async def is_admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    if update.effective_chat is None or update.effective_user is None:
        return False
    try:
        member = await context.bot.get_chat_member(update.effective_chat.id, update.effective_user.id)
    except TelegramError:
        return False
    return member.status in ("administrator", "creator", "owner")


def set_prompt(context: ContextTypes.DEFAULT_TYPE, chat_id: int, mode: str, message_id: int):
    context.user_data["prompt"] = {"chat_id": chat_id, "mode": mode, "message_id": message_id}


def pop_prompt(context: ContextTypes.DEFAULT_TYPE, chat_id: int, reply_to_message_id: Optional[int]) -> Optional[str]:
    prompt = context.user_data.get("prompt")
    if not isinstance(prompt, dict):
        return None
    if prompt.get("chat_id") != chat_id:
        return None
    if prompt.get("message_id") != reply_to_message_id:
        return None
    context.user_data.pop("prompt", None)
    return prompt.get("mode")


def help_text() -> str:
    return (
        "Я веду список на футбол.\n\n"
        "Игрокам:\n"
        "+ — записаться\n"
        "- — выйти из списка\n"
        "+1...+5 — добавить гостей\n"
        "-1...-5 — убрать гостей\n\n"
        "Админам:\n"
        "/menu или текстом «меню» — открыть панель."
    )


def post_json_sync(url: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    request = urllib.request.Request(
        url,
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        headers={"Content-Type": "application/json; charset=utf-8"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        body = response.read().decode("utf-8")
    return json.loads(body) if body else {"ok": True}


async def send_to_sheets(payload: Dict[str, Any]) -> Tuple[bool, str]:
    url = os.getenv(GOOGLE_SHEETS_WEBHOOK_URL_ENV, "").strip()
    if not url:
        return False, "not_configured"
    try:
        response = await asyncio.to_thread(post_json_sync, url, payload)
    except (urllib.error.URLError, TimeoutError, OSError, ValueError) as exc:
        return False, str(exc)
    if response.get("ok") is False:
        return False, response.get("error", "unknown_error")
    return True, "ok"


def attendance_rows(chat_state: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows = []
    for player in main_players(chat_state):
        rows.append(
            {
                "key": f"user:{player['user_id']}",
                "display_name": player["name"],
                "username": player.get("username", ""),
            }
        )
    return rows


async def export_finished_game(chat_state: Dict[str, Any], chat_id: int, chat_title: str) -> Tuple[bool, str]:
    game = chat_state["game"]
    present = attendance_rows(chat_state)
    guests = [player_label(player) for player in chat_state["players"] if player.get("kind") == "guest"]
    payload = {
        "action": "event",
        "chat_id": chat_id,
        "chat_title": chat_title,
        "event": {
            "date": game["date"],
            "time": f"{game['start']}-{game['end']}",
            "field": game["field"],
            "present": present,
            "no_shows": [],
            "reserve": [],
            "guests": guests,
        },
    }
    return await send_to_sheets(payload)


def finish_game(chat_state: Dict[str, Any]) -> Dict[str, Any]:
    game = dict(chat_state["game"])
    present = attendance_rows(chat_state)
    guests = [player_label(player) for player in chat_state["players"] if player.get("kind") == "guest"]
    event = {
        "date": game["date"],
        "time": f"{game['start']}-{game['end']}",
        "field": game["field"],
        "present": present,
        "guests": guests,
        "finished_at": now_iso(),
    }
    chat_state["history"].append(event)
    chat_state["players"] = []
    chat_state["game"]["open"] = False
    save_state()
    return event


def in_period(date_text: str, period: str) -> bool:
    try:
        event_date = datetime.strptime(date_text, "%d/%m/%y").replace(tzinfo=TZ)
    except ValueError:
        return False
    now = datetime.now(TZ)
    if period == "all":
        return True
    if period == "month":
        return event_date.year == now.year and event_date.month == now.month
    if period == "year":
        return event_date.year == now.year
    if period == "quarter":
        return event_date.year == now.year and (event_date.month - 1) // 3 == (now.month - 1) // 3
    return False


def period_title(period: str) -> str:
    return {
        "month": "за месяц",
        "quarter": "за квартал",
        "year": "за год",
        "all": "за всё время",
    }.get(period, "")


def build_stats(chat_state: Dict[str, Any], period: str) -> Tuple[str, List[Dict[str, Any]], int]:
    counters: Dict[str, Dict[str, Any]] = {}
    games = 0
    for event in chat_state.get("history", []):
        if not in_period(event.get("date", ""), period):
            continue
        games += 1
        for person in event.get("present", []):
            key = person.get("key") or person.get("display_name")
            row = counters.setdefault(
                key,
                {
                    "display_name": person.get("display_name", "Игрок"),
                    "username": person.get("username", ""),
                    "visits": 0,
                    "no_shows": 0,
                    "reserve": 0,
                },
            )
            row["visits"] += 1
    rows = sorted(counters.values(), key=lambda row: (-row["visits"], row["display_name"]))
    if not rows:
        return f"Статистика {period_title(period)} пока пустая.", [], games

    lines = [f"Посещаемость {period_title(period)}", f"Игр учтено: {games}", ""]
    for index, row in enumerate(rows, start=1):
        username = f" (@{row['username']})" if row.get("username") else ""
        lines.append(f"{index}. {row['display_name']}{username}: {row['visits']}")
    return "\n".join(lines), rows, games


async def export_stats(chat_state: Dict[str, Any], period: str, rows: List[Dict[str, Any]], games: int):
    payload = {
        "action": "report",
        "period": period_title(period),
        "generated_at": datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S"),
        "games_count": games,
        "rows": rows,
    }
    return await send_to_sheets(payload)


async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message:
        await update.message.reply_text(help_text(), reply_markup=ReplyKeyboardRemove())


async def menu_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat is None or update.effective_chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        return
    if not await is_admin(update, context):
        if update.message:
            await update.message.reply_text("Панель доступна только админам.", reply_markup=ReplyKeyboardRemove())
        return
    chat_state = ensure_chat(update.effective_chat.id, update.effective_chat.title or "", update.effective_chat.type)
    if update.message:
        await update.message.reply_text(
            "Панель управления:",
            reply_markup=admin_keyboard(chat_state),
        )


async def plus_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat is None or update.effective_user is None or update.message is None:
        return
    if update.effective_chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        return

    chat_state = ensure_chat(update.effective_chat.id, update.effective_chat.title or "", update.effective_chat.type)
    if not chat_state["game"].get("open"):
        await update.message.reply_text("Запись сейчас закрыта.", reply_markup=ReplyKeyboardRemove())
        return

    match = PLUS_RE.match(update.message.text or "")
    guest_count = int(match.group("count") or 0) if match else 0
    existing_index = user_player_index(chat_state, update.effective_user.id)

    if existing_index is None:
        if is_full(chat_state):
            await update.message.reply_text("Список заполнен.", reply_markup=ReplyKeyboardRemove())
            return
        chat_state["players"].append(
            {
                "kind": "player",
                "user_id": update.effective_user.id,
                "username": update.effective_user.username or "",
                "name": full_name(update.effective_user),
                "owner_id": update.effective_user.id,
                "owner_name": full_name(update.effective_user),
                "joined_at": now_iso(),
            }
        )

    current_guests = len(guest_indexes(chat_state, update.effective_user.id))
    delta = guest_count - current_guests
    if delta > 0:
        for _ in range(delta):
            if is_full(chat_state):
                break
            chat_state["players"].append(
                {
                    "kind": "guest",
                    "user_id": None,
                    "username": "",
                    "name": "",
                    "owner_id": update.effective_user.id,
                    "owner_name": full_name(update.effective_user),
                    "owner_username": update.effective_user.username or "",
                    "joined_at": now_iso(),
                }
            )
    elif delta < 0:
        for index in reversed(guest_indexes(chat_state, update.effective_user.id)[delta:]):
            chat_state["players"].pop(index)

    save_state()
    await update.message.reply_text("Записал ✅\n\n" + format_game(chat_state), reply_markup=ReplyKeyboardRemove())


async def minus_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat is None or update.effective_user is None or update.message is None:
        return
    if update.effective_chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        return

    chat_state = ensure_chat(update.effective_chat.id, update.effective_chat.title or "", update.effective_chat.type)
    match = MINUS_RE.match(update.message.text or "")
    count = int(match.group("count") or 0) if match and match.group("count") else 0

    if count:
        indexes = guest_indexes(chat_state, update.effective_user.id)
        if not indexes:
            await update.message.reply_text("У тебя нет гостей в списке.", reply_markup=ReplyKeyboardRemove())
            return
        for index in reversed(indexes[-count:]):
            chat_state["players"].pop(index)
        save_state()
        await update.message.reply_text("Убрал гостей ✅\n\n" + format_game(chat_state), reply_markup=ReplyKeyboardRemove())
        return

    index = user_player_index(chat_state, update.effective_user.id)
    if index is None:
        await update.message.reply_text("Тебя нет в списке.", reply_markup=ReplyKeyboardRemove())
        return
    chat_state["players"].pop(index)
    save_state()
    await update.message.reply_text("Убрал тебя ✅\n\n" + format_game(chat_state), reply_markup=ReplyKeyboardRemove())


async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat is None or update.message is None:
        return

    text = (update.message.text or "").strip().lower()
    if text in {"меню", "menu", "панель"}:
        await menu_cmd(update, context)
        return

    reply_id = update.message.reply_to_message.message_id if update.message.reply_to_message else None
    mode = pop_prompt(context, update.effective_chat.id, reply_id)
    if not mode:
        return
    if not await is_admin(update, context):
        return

    chat_state = ensure_chat(update.effective_chat.id, update.effective_chat.title or "", update.effective_chat.type)
    raw = update.message.text or ""
    try:
        if mode == "new_game":
            chat_state["game"] = parse_game_input(raw)
            chat_state["players"] = []
            response = "Игра создана ✅\n\n" + format_game(chat_state)
        elif mode == "field":
            chat_state["game"]["field"] = raw.strip()
            response = "Поле обновлено ✅\n\n" + format_game(chat_state)
        elif mode == "time":
            start, end = parse_time_range(raw)
            chat_state["game"]["start"] = start
            chat_state["game"]["end"] = end
            chat_state["game"]["reminder_sent"] = False
            response = "Время обновлено ✅\n\n" + format_game(chat_state)
        elif mode == "limit":
            chat_state["game"]["limit"] = max(0, int(raw.strip()))
            response = "Лимит обновлён ✅\n\n" + format_game(chat_state)
        else:
            return
    except (ValueError, TypeError) as exc:
        await update.message.reply_text(f"Ошибка: {exc}", reply_markup=ReplyKeyboardRemove())
        return

    save_state()
    await update.message.reply_text(response, reply_markup=admin_keyboard(chat_state))


async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if query is None or update.effective_chat is None:
        return
    await query.answer()
    if not await is_admin(update, context):
        await query.answer("Только для админов", show_alert=True)
        return

    chat_state = ensure_chat(update.effective_chat.id, update.effective_chat.title or "", update.effective_chat.type)
    data = query.data or ""

    if data == "admin:back":
        await query.edit_message_text("Панель управления:", reply_markup=admin_keyboard(chat_state))
        return
    if data == "admin:list":
        await query.edit_message_text(format_game(chat_state), reply_markup=admin_keyboard(chat_state))
        return
    if data == "admin:open":
        chat_state["game"]["open"] = True
        save_state()
        await query.edit_message_text("Запись открыта ✅\n\n" + format_game(chat_state), reply_markup=admin_keyboard(chat_state))
        return
    if data == "admin:close":
        chat_state["game"]["open"] = False
        save_state()
        await query.edit_message_text("Запись закрыта ⛔️\n\n" + format_game(chat_state), reply_markup=admin_keyboard(chat_state))
        return
    if data == "admin:new_game":
        msg = await query.message.reply_text(
            "Ответь на это сообщение новой игрой:\n24/04/26 20:30-22:30 18 Горизонт-арена"
        )
        set_prompt(context, update.effective_chat.id, "new_game", msg.message_id)
        return
    if data == "admin:field":
        msg = await query.message.reply_text("Ответь на это сообщение новым названием поля.")
        set_prompt(context, update.effective_chat.id, "field", msg.message_id)
        return
    if data == "admin:time":
        msg = await query.message.reply_text("Ответь на это сообщение новым временем: 20:30-22:30")
        set_prompt(context, update.effective_chat.id, "time", msg.message_id)
        return
    if data == "admin:limit":
        msg = await query.message.reply_text("Ответь на это сообщение новым лимитом участников, например: 18")
        set_prompt(context, update.effective_chat.id, "limit", msg.message_id)
        return
    if data == "admin:finish":
        if not chat_state["players"]:
            await query.edit_message_text("Список пуст, завершать нечего.", reply_markup=admin_keyboard(chat_state))
            return
        sheets_ok, sheets_status = await export_finished_game(chat_state, update.effective_chat.id, update.effective_chat.title or "")
        event = finish_game(chat_state)
        text = (
            "Игра завершена ✅\n"
            f"Посещений: {len(event['present'])}\n"
            f"Гостей: {len(event['guests'])}"
        )
        if os.getenv(GOOGLE_SHEETS_WEBHOOK_URL_ENV, "").strip():
            text += "\nGoogle Sheets: записано ✅" if sheets_ok else f"\nGoogle Sheets: ошибка ({sheets_status})"
        await query.edit_message_text(text, reply_markup=admin_keyboard(chat_state))
        return
    if data == "admin:stats":
        await query.edit_message_text("Выбери период:", reply_markup=stats_keyboard())
        return
    if data.startswith("stats:"):
        period = data.split(":", 1)[1]
        text, rows, games = build_stats(chat_state, period)
        sheets_ok, sheets_status = await export_stats(chat_state, period, rows, games)
        if os.getenv(GOOGLE_SHEETS_WEBHOOK_URL_ENV, "").strip():
            text += "\n\nGoogle Sheets: записано ✅" if sheets_ok else f"\n\nGoogle Sheets: ошибка ({sheets_status})"
        await query.edit_message_text(text, reply_markup=stats_keyboard())


async def reminder_loop(app: Application):
    while True:
        try:
            now = datetime.now(TZ)
            for chat_id, chat_state in list(state.items()):
                game = chat_state.get("game", {})
                if game.get("reminder_sent"):
                    continue
                end_dt = game_end_datetime(game)
                if end_dt is None:
                    continue
                if now < end_dt + timedelta(hours=1):
                    continue

                try:
                    msg = await app.bot.send_message(
                        chat_id=int(chat_id),
                        text="Отправьте фотографию победителей 🏆",
                    )
                    try:
                        await app.bot.pin_chat_message(
                            chat_id=int(chat_id),
                            message_id=msg.message_id,
                            disable_notification=True,
                        )
                    except TelegramError as exc:
                        logger.warning("Could not pin winners prompt in %s: %s", chat_id, exc)
                    game["winners_prompt_message_id"] = msg.message_id
                    game["reminder_sent"] = True
                    save_state()
                except TelegramError as exc:
                    logger.warning("Could not send winners prompt in %s: %s", chat_id, exc)
        except Exception:
            logger.exception("Reminder loop error")
        await asyncio.sleep(60)


async def post_init(app: Application):
    app.create_task(reminder_loop(app))


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    logger.exception("Unhandled update error", exc_info=context.error)


async def member_update(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat is None or update.my_chat_member is None:
        return
    chat_state = ensure_chat(
        update.effective_chat.id,
        update.effective_chat.title or update.effective_chat.full_name or "",
        update.effective_chat.type,
    )
    status = update.my_chat_member.new_chat_member.status
    chat_state["active"] = status not in ("left", "kicked")
    save_state()


def main():
    load_state()
    token = os.getenv("BOT_TOKEN")
    if not token:
        raise RuntimeError("Set BOT_TOKEN environment variable")

    app = ApplicationBuilder().token(token).post_init(post_init).build()
    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("help", start_cmd))
    app.add_handler(CommandHandler("menu", menu_cmd))
    app.add_handler(CallbackQueryHandler(callback_handler))
    app.add_handler(MessageHandler(filters.TEXT & filters.Regex(PLUS_RE), plus_handler))
    app.add_handler(MessageHandler(filters.TEXT & filters.Regex(MINUS_RE), minus_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))
    app.add_handler(ChatMemberHandler(member_update, ChatMemberHandler.MY_CHAT_MEMBER))
    app.add_error_handler(error_handler)

    logger.info("Bot is running")
    app.run_polling()


if __name__ == "__main__":
    try:
        import dotenv

        dotenv.load_dotenv()
    except Exception:
        pass
    main()
