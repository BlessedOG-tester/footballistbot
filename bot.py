"""Telegram bot for weekly football sign-up lists.

Players write "+" / "+N" / "-" / "-N" in a group chat, the bot keeps the main
list and the reserve list, promotes from the reserve automatically and keeps a
single list message up to date. Admins manage the game with commands or the
inline panel (/menu).
"""

import asyncio
import html
import json
import logging
import os
import re
import tempfile
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
    Update,
    User,
)
from telegram.constants import ChatType, ParseMode, ReactionEmoji
from telegram.error import BadRequest, TelegramError
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


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

STATE_FILE = os.getenv("STATE_FILE", "state.json")
DEFAULT_FIELD = os.getenv("DEFAULT_FIELD", "Горизонт-арена")
DEFAULT_START = os.getenv("DEFAULT_START", "20:30")
DEFAULT_END = os.getenv("DEFAULT_END", "22:30")
DEFAULT_LIMIT = int(os.getenv("DEFAULT_LIMIT", "18"))
TZ_OFFSET_HOURS = int(os.getenv("TZ_OFFSET_HOURS", "5"))
TZ = timezone(timedelta(hours=TZ_OFFSET_HOURS))
GOOGLE_SHEETS_WEBHOOK_URL_ENV = "GOOGLE_SHEETS_WEBHOOK_URL"
MAX_GUESTS = 5
WINNERS_PROMPT_DELAY = timedelta(hours=1)
GROUP_ANONYMOUS_BOT_ID = 1087968824

PLUS_RE = re.compile(r"^\s*\+\s*(?P<count>[1-5])?\s*$")
MINUS_RE = re.compile(r"^\s*(?:-\s*(?P<count>[1-5])?|минус|не\s+смогу|не\s+буду)\s*$", re.IGNORECASE)
MENU_WORDS = {"меню", "menu", "панель"}

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger("footballistbot")

state: Dict[str, Dict[str, Any]] = {}


# --------------------------------------------------------------------------- #
# Dates and parsing
# --------------------------------------------------------------------------- #

WEEKDAYS_RU = ["понедельник", "вторник", "среда", "четверг", "пятница", "суббота", "воскресенье"]


def today() -> datetime:
    return datetime.now(TZ)


def today_str() -> str:
    return today().strftime("%d/%m/%y")


def now_iso() -> str:
    return today().isoformat(timespec="seconds")


def parse_date(value: str) -> str:
    cleaned = value.strip().replace(".", "/").replace("-", "/")
    for fmt in ("%d/%m/%y", "%d/%m/%Y", "%d/%m"):
        try:
            parsed = datetime.strptime(cleaned, fmt)
            if fmt == "%d/%m":
                parsed = parsed.replace(year=today().year)
            return parsed.strftime("%d/%m/%y")
        except ValueError:
            continue
    raise ValueError("Дата должна быть в формате 24/04/26")


def parse_time_range(value: str) -> Tuple[str, str]:
    match = re.match(r"^\s*(\d{1,2}[:.]\d{2})\s*[-–]\s*(\d{1,2}[:.]\d{2})\s*$", value)
    if not match:
        raise ValueError("Время должно быть в формате 20:30-22:30")
    start = datetime.strptime(match.group(1).replace(".", ":"), "%H:%M").strftime("%H:%M")
    end = datetime.strptime(match.group(2).replace(".", ":"), "%H:%M").strftime("%H:%M")
    return start, end


def parse_limit(value: str) -> int:
    try:
        limit = int(value.strip())
    except ValueError as exc:
        raise ValueError("Лимит должен быть числом, 0 = без лимита") from exc
    if limit < 0:
        raise ValueError("Лимит не может быть отрицательным")
    return limit


def parse_game_input(value: str) -> Dict[str, Any]:
    """'24/04/26 20:30-22:30 18 Горизонт-арена' -> game dict."""
    parts = value.strip().split()
    if len(parts) < 4:
        raise ValueError("Формат: 24/04/26 20:30-22:30 18 Горизонт-арена")
    date = parse_date(parts[0])
    start, end = parse_time_range(parts[1])
    limit = parse_limit(parts[2])
    field = " ".join(parts[3:]).strip()
    if not field:
        raise ValueError("Укажи поле")
    game = default_game()
    game.update({"date": date, "start": start, "end": end, "limit": limit, "field": field})
    return game


def game_date(game: Dict[str, Any]) -> Optional[datetime]:
    try:
        return datetime.strptime(game["date"], "%d/%m/%y").replace(tzinfo=TZ)
    except (KeyError, ValueError):
        return None


def game_end_datetime(game: Dict[str, Any]) -> Optional[datetime]:
    date = game_date(game)
    if date is None:
        return None
    try:
        end_time = datetime.strptime(game["end"], "%H:%M").time()
    except (KeyError, ValueError):
        return None
    return datetime.combine(date.date(), end_time, tzinfo=TZ)


def roll_date_forward(game: Dict[str, Any], weeks: int = 1) -> None:
    """Move the game date forward by N weeks (same weekday)."""
    date = game_date(game) or today()
    game["date"] = (date + timedelta(weeks=weeks)).strftime("%d/%m/%y")


def roll_date_to_future(game: Dict[str, Any]) -> bool:
    """If the game date is in the past, move it forward week by week until it is not.

    Returns True if the date was changed.
    """
    date = game_date(game)
    if date is None:
        game["date"] = today_str()
        return True
    changed = False
    while date.date() < today().date():
        date += timedelta(weeks=1)
        changed = True
    if changed:
        game["date"] = date.strftime("%d/%m/%y")
    return changed


def date_label(game: Dict[str, Any]) -> str:
    date = game_date(game)
    if date is None:
        return game.get("date", "")
    return f"{game['date']} ({WEEKDAYS_RU[date.weekday()]})"


# --------------------------------------------------------------------------- #
# State
# --------------------------------------------------------------------------- #


def default_game() -> Dict[str, Any]:
    return {
        "date": today_str(),
        "field": DEFAULT_FIELD,
        "start": DEFAULT_START,
        "end": DEFAULT_END,
        "limit": DEFAULT_LIMIT,
        "open": False,
        "was_opened": False,  # set when the list is opened; gates the winners reminder
        "reminder_sent": False,
        "winners_prompt_message_id": None,
        "list_message_id": None,  # the single list message the bot keeps editing
    }


def default_chat_state(chat_title: str = "", chat_type: str = "") -> Dict[str, Any]:
    return {
        "chat_title": chat_title,
        "chat_type": chat_type,
        "active": True,
        "game": default_game(),
        "players": [],
        "reserve": [],
        "history": [],
    }


def normalize_entry(item: Any) -> Optional[Dict[str, Any]]:
    if isinstance(item, str):
        return {
            "kind": "player",
            "user_id": None,
            "username": "",
            "name": item,
            "owner_id": None,
            "owner_name": item,
            "owner_username": "",
            "joined_at": "",
            "no_show": False,
        }
    if not isinstance(item, dict):
        return None
    kind = "guest" if item.get("kind") == "guest" else "player"
    return {
        "kind": kind,
        "user_id": item.get("user_id") if kind == "player" else None,
        "username": (item.get("username") or "") if kind == "player" else "",
        "name": item.get("name") or item.get("display_name") or "",
        "owner_id": item.get("owner_id") or item.get("owner_user_id") or item.get("user_id"),
        "owner_name": item.get("owner_name") or item.get("owner_display_name") or item.get("name") or "",
        "owner_username": item.get("owner_username") or (item.get("username") or "" if kind == "player" else ""),
        "joined_at": item.get("joined_at", ""),
        "no_show": bool(item.get("no_show", False)),
    }


def normalize_players(raw_players: Any) -> List[Dict[str, Any]]:
    if not isinstance(raw_players, list):
        return []
    result = []
    for item in raw_players:
        entry = normalize_entry(item)
        if entry is not None:
            result.append(entry)
    return result


def normalize_chat(chat_state: Any) -> Dict[str, Any]:
    normalized = default_chat_state()
    if isinstance(chat_state, dict):
        normalized.update(chat_state)

    game = default_game()
    if isinstance(normalized.get("game"), dict):
        game.update(normalized["game"])
    # Legacy flat fields from very old state files.
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
        if isinstance(chat_state.get("time"), str) and "-" in chat_state["time"]:
            try:
                game["start"], game["end"] = parse_time_range(chat_state["time"])
            except ValueError:
                pass
    try:
        game["limit"] = max(0, int(game.get("limit") or 0))
    except (TypeError, ValueError):
        game["limit"] = DEFAULT_LIMIT
    normalized["game"] = game

    normalized["players"] = normalize_players(normalized.get("players", []))
    normalized["reserve"] = normalize_players(normalized.get("reserve", []))
    if not isinstance(normalized.get("history"), list):
        normalized["history"] = []
    return normalized


def load_state() -> None:
    global state
    if not os.path.exists(STATE_FILE):
        state = {}
        logger.info("State file %s not found, starting empty", STATE_FILE)
        return
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as file:
            raw = json.load(file)
    except (OSError, ValueError) as exc:
        logger.error("Could not read %s (%s), starting empty", STATE_FILE, exc)
        state = {}
        return
    if not isinstance(raw, dict):
        raw = {}
    state = {str(chat_id): normalize_chat(chat_state) for chat_id, chat_state in raw.items()}
    logger.info("Loaded state for %d chat(s) from %s", len(state), STATE_FILE)


def save_state() -> None:
    """Write state atomically so a crash mid-write cannot corrupt the file."""
    directory = os.path.dirname(os.path.abspath(STATE_FILE))
    os.makedirs(directory, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(prefix=".state-", suffix=".json", dir=directory)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as file:
            json.dump(state, file, ensure_ascii=False, indent=2)
        os.replace(tmp_path, STATE_FILE)
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def ensure_chat(chat_id: int, chat_title: str = "", chat_type: str = "") -> Dict[str, Any]:
    cid = str(chat_id)
    if cid not in state:
        state[cid] = default_chat_state(chat_title, chat_type)
    if chat_title:
        state[cid]["chat_title"] = chat_title
    if chat_type:
        state[cid]["chat_type"] = chat_type
    return state[cid]


# --------------------------------------------------------------------------- #
# Players, reserve, promotion
# --------------------------------------------------------------------------- #


def full_name(user: User) -> str:
    parts = [part for part in [user.first_name, user.last_name] if part]
    return " ".join(parts) if parts else (user.username or str(user.id))


def make_player(user: User) -> Dict[str, Any]:
    return {
        "kind": "player",
        "user_id": user.id,
        "username": user.username or "",
        "name": full_name(user),
        "owner_id": user.id,
        "owner_name": full_name(user),
        "owner_username": user.username or "",
        "joined_at": now_iso(),
        "no_show": False,
    }


def make_guest(user: User) -> Dict[str, Any]:
    return {
        "kind": "guest",
        "user_id": None,
        "username": "",
        "name": "",
        "owner_id": user.id,
        "owner_name": full_name(user),
        "owner_username": user.username or "",
        "joined_at": now_iso(),
        "no_show": False,
    }


def player_label(entry: Dict[str, Any]) -> str:
    if entry.get("kind") == "guest":
        owner = entry.get("owner_name") or "игрока"
        username = entry.get("owner_username", "")
        suffix = f" (@{username})" if username else ""
        return f"+1 от {owner}{suffix}"
    username = entry.get("username", "")
    suffix = f" (@{username})" if username else ""
    mark = " ❌" if entry.get("no_show") else ""
    return f"{entry.get('name') or 'Игрок'}{suffix}{mark}"


def mention_html(entry: Dict[str, Any]) -> str:
    """Clickable mention of the owner of an entry (player or guest)."""
    name = html.escape(entry.get("owner_name") or entry.get("name") or "Игрок")
    user_id = entry.get("owner_id") or entry.get("user_id")
    if user_id:
        return f'<a href="tg://user?id={user_id}">{name}</a>'
    return name


def limit_of(chat_state: Dict[str, Any]) -> int:
    return int(chat_state["game"].get("limit", 0) or 0)


def is_full(chat_state: Dict[str, Any]) -> bool:
    limit = limit_of(chat_state)
    return bool(limit and len(chat_state["players"]) >= limit)


def find_player(entries: List[Dict[str, Any]], user_id: int) -> Optional[int]:
    for index, entry in enumerate(entries):
        if entry.get("kind") == "player" and entry.get("user_id") == user_id:
            return index
    return None


def guest_indexes(entries: List[Dict[str, Any]], user_id: int) -> List[int]:
    return [i for i, e in enumerate(entries) if e.get("kind") == "guest" and e.get("owner_id") == user_id]


def user_in_any_list(chat_state: Dict[str, Any], user_id: int) -> bool:
    return find_player(chat_state["players"], user_id) is not None or find_player(chat_state["reserve"], user_id) is not None


def total_guests(chat_state: Dict[str, Any], user_id: int) -> int:
    return len(guest_indexes(chat_state["players"], user_id)) + len(guest_indexes(chat_state["reserve"], user_id))


def add_entry(chat_state: Dict[str, Any], entry: Dict[str, Any]) -> str:
    """Append an entry to the main list, or to the reserve if the main list is full.

    Returns "main" or "reserve".
    """
    if is_full(chat_state):
        chat_state["reserve"].append(entry)
        return "reserve"
    chat_state["players"].append(entry)
    return "main"


def promote_from_reserve(chat_state: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Move reserve entries into the main list while there is room. Returns promoted entries."""
    promoted: List[Dict[str, Any]] = []
    while chat_state["reserve"] and not is_full(chat_state):
        entry = chat_state["reserve"].pop(0)
        chat_state["players"].append(entry)
        promoted.append(entry)
    return promoted


def remove_guests(chat_state: Dict[str, Any], user_id: int, count: int) -> int:
    """Remove up to `count` guests of a user, reserve first, then from the end of the main list."""
    removed = 0
    for list_name in ("reserve", "players"):
        entries = chat_state[list_name]
        for index in reversed(guest_indexes(entries, user_id)):
            if removed >= count:
                break
            entries.pop(index)
            removed += 1
    return removed


def remove_user_completely(chat_state: Dict[str, Any], user_id: int) -> bool:
    """Remove the user and all their guests from both lists."""
    removed = False
    for list_name in ("players", "reserve"):
        entries = chat_state[list_name]
        keep = [e for e in entries if not (e.get("user_id") == user_id or e.get("owner_id") == user_id)]
        if len(keep) != len(entries):
            removed = True
            chat_state[list_name] = keep
    return removed


def apply_plus(chat_state: Dict[str, Any], user: User, guests: Optional[int]) -> Dict[str, Any]:
    """Handle '+' (guests=None) or '+N' (guests=N). Returns a result dict for messaging."""
    result: Dict[str, Any] = {"joined": None, "guests_added": 0, "guests_removed": 0, "reserve_added": 0, "already": False}

    if not user_in_any_list(chat_state, user.id):
        where = add_entry(chat_state, make_player(user))
        result["joined"] = where
    elif guests is None:
        result["already"] = True

    if guests is not None:
        current = total_guests(chat_state, user.id)
        delta = guests - current
        if delta > 0:
            for _ in range(delta):
                where = add_entry(chat_state, make_guest(user))
                result["guests_added"] += 1
                if where == "reserve":
                    result["reserve_added"] += 1
        elif delta < 0:
            result["guests_removed"] = remove_guests(chat_state, user.id, -delta)

    result["promoted"] = promote_from_reserve(chat_state)
    return result


def apply_minus(chat_state: Dict[str, Any], user: User, guests: Optional[int]) -> Dict[str, Any]:
    """Handle '-' (leave with all guests) or '-N' (drop N guests)."""
    result: Dict[str, Any] = {"removed_self": False, "guests_removed": 0, "promoted": []}
    if guests:
        result["guests_removed"] = remove_guests(chat_state, user.id, guests)
    else:
        result["removed_self"] = remove_user_completely(chat_state, user.id)
    result["promoted"] = promote_from_reserve(chat_state)
    return result


def find_entry_by_query(chat_state: Dict[str, Any], query: str) -> Optional[Tuple[str, int]]:
    """Find a player by @username, name fragment or list number. Returns (list_name, index)."""
    q = query.strip().lstrip("@").lower()
    if not q:
        return None
    if q.isdigit():
        number = int(q)
        if 1 <= number <= len(chat_state["players"]):
            return "players", number - 1
        return None
    for list_name in ("players", "reserve"):
        for index, entry in enumerate(chat_state[list_name]):
            if entry.get("kind") != "player":
                continue
            if (entry.get("username") or "").lower() == q:
                return list_name, index
    for list_name in ("players", "reserve"):
        for index, entry in enumerate(chat_state[list_name]):
            if entry.get("kind") != "player":
                continue
            if q in (entry.get("name") or "").lower():
                return list_name, index
    return None


# --------------------------------------------------------------------------- #
# Rendering
# --------------------------------------------------------------------------- #


def format_game(chat_state: Dict[str, Any]) -> str:
    game = chat_state["game"]
    status = "запись открыта ✅" if game.get("open") else "запись закрыта ⛔️"
    limit = limit_of(chat_state)
    count = len(chat_state["players"])
    limit_text = f"{count}/{limit}" if limit else str(count)

    if chat_state["players"]:
        body = "\n".join(f"{i + 1}. {player_label(p)}" for i, p in enumerate(chat_state["players"]))
    else:
        body = "Пока пусто."

    text = (
        f"⚽️ Футбол — {date_label(game)}\n"
        f"🏟 {game['field']}\n"
        f"⏰ {game['start']}–{game['end']}\n\n"
        f"Статус: {status}\n"
        f"Основа ({limit_text}):\n{body}"
    )
    if chat_state["reserve"]:
        reserve = "\n".join(f"{i + 1}. {player_label(p)}" for i, p in enumerate(chat_state["reserve"]))
        text += f"\n\nРезерв ({len(chat_state['reserve'])}):\n{reserve}"
    if game.get("open"):
        text += "\n\n+ записаться · - выйти · +1…+5 гости"
    return text


def help_text() -> str:
    return (
        "Я веду список на футбол.\n\n"
        "Игрокам:\n"
        "+ — записаться\n"
        "- — выйти из списка (вместе со своими гостями)\n"
        "+1…+5 — записаться с гостями (число = сколько гостей)\n"
        "-1…-5 — убрать часть гостей\n"
        "/list — показать список\n"
        "/mystats — моя статистика\n\n"
        "Если основа заполнена, попадаешь в резерв. Когда кто-то выходит, "
        "первый из резерва поднимается автоматически, бот напишет об этом.\n\n"
        "Админам: /menu — панель, /admin — список команд."
    )


def admin_help_text() -> str:
    return (
        "Команды админов:\n"
        "/open [ДД/ММ/ГГ] [ЧЧ:ММ-ЧЧ:ММ] — открыть запись\n"
        "/close — закрыть запись\n"
        "/list — показать и закрепить список\n"
        "/newgame ДД/ММ/ГГ ЧЧ:ММ-ЧЧ:ММ ЛИМИТ ПОЛЕ — новая игра (список очищается)\n"
        "/setdate ДД/ММ/ГГ — дата\n"
        "/settime ЧЧ:ММ-ЧЧ:ММ — время\n"
        "/setfield НАЗВАНИЕ — поле\n"
        "/setlimit N — лимит мест (0 = без лимита)\n"
        "/add Имя — добавить игрока вручную\n"
        "/remove @username|Имя|№ — убрать игрока\n"
        "/noshow @username|Имя|№ — отметить, что не пришёл\n"
        "/showup @username|Имя|№ — снять отметку no-show\n"
        "/finish — завершить игру, записать статистику, перенести дату на неделю\n"
        "/stats [month|quarter|year|all] [Имя] — статистика\n"
        "/reset — очистить список\n"
        "/menu — inline-панель"
    )


def admin_keyboard(chat_state: Dict[str, Any]) -> InlineKeyboardMarkup:
    game = chat_state["game"]
    open_label = "⛔️ Закрыть запись" if game.get("open") else "✅ Открыть запись"
    open_action = "admin:close" if game.get("open") else "admin:open"
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton(open_label, callback_data=open_action)],
            [
                InlineKeyboardButton("📋 Список", callback_data="admin:list"),
                InlineKeyboardButton("🆕 Новая игра", callback_data="admin:new_game"),
            ],
            [
                InlineKeyboardButton("📅 Дата", callback_data="admin:date"),
                InlineKeyboardButton("⏰ Время", callback_data="admin:time"),
            ],
            [
                InlineKeyboardButton("🏟 Поле", callback_data="admin:field"),
                InlineKeyboardButton("👥 Лимит", callback_data="admin:limit"),
            ],
            [
                InlineKeyboardButton("🏁 Завершить игру", callback_data="admin:finish"),
                InlineKeyboardButton("📊 Статистика", callback_data="admin:stats"),
            ],
            [InlineKeyboardButton("🗑 Очистить список", callback_data="admin:reset")],
        ]
    )


def confirm_keyboard(action: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("Да, подтверждаю", callback_data=f"confirm:{action}"),
                InlineKeyboardButton("Отмена", callback_data="admin:back"),
            ]
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
            [InlineKeyboardButton("« Назад", callback_data="admin:back")],
        ]
    )


# --------------------------------------------------------------------------- #
# Statistics and Google Sheets
# --------------------------------------------------------------------------- #


def person_row(entry: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "key": f"user:{entry['user_id']}" if entry.get("user_id") else f"name:{entry.get('name', '')}",
        "display_name": entry.get("name", ""),
        "username": entry.get("username", ""),
    }


def build_event(chat_state: Dict[str, Any]) -> Dict[str, Any]:
    game = chat_state["game"]
    players = [p for p in chat_state["players"] if p.get("kind") == "player"]
    return {
        "date": game["date"],
        "time": f"{game['start']}-{game['end']}",
        "field": game["field"],
        "present": [person_row(p) for p in players if not p.get("no_show")],
        "no_shows": [person_row(p) for p in players if p.get("no_show")],
        "reserve": [person_row(p) for p in chat_state["reserve"] if p.get("kind") == "player"],
        "guests": [player_label(p) for p in chat_state["players"] if p.get("kind") == "guest"],
        "finished_at": now_iso(),
    }


def finish_game(chat_state: Dict[str, Any]) -> Dict[str, Any]:
    """Record the game into history, clear the lists, roll the date one week forward."""
    event = build_event(chat_state)
    chat_state["history"].append(event)
    chat_state["players"] = []
    chat_state["reserve"] = []
    game = chat_state["game"]
    game["open"] = False
    game["list_message_id"] = None
    roll_date_forward(game)
    game["was_opened"] = False
    game["reminder_sent"] = False
    game["winners_prompt_message_id"] = None
    return event


def in_period(date_text: str, period: str) -> bool:
    try:
        event_date = datetime.strptime(date_text, "%d/%m/%y")
    except ValueError:
        return False
    now = today()
    if period == "all":
        return True
    if period == "month":
        return (event_date.year, event_date.month) == (now.year, now.month)
    if period == "year":
        return event_date.year == now.year
    if period == "quarter":
        return event_date.year == now.year and (event_date.month - 1) // 3 == (now.month - 1) // 3
    return False


def period_title(period: str) -> str:
    return {"month": "за месяц", "quarter": "за квартал", "year": "за год", "all": "за всё время"}.get(period, "")


def build_stats(chat_state: Dict[str, Any], period: str) -> Tuple[str, List[Dict[str, Any]], int]:
    counters: Dict[str, Dict[str, Any]] = {}
    games = 0

    def row_for(person: Dict[str, Any]) -> Dict[str, Any]:
        key = person.get("key") or person.get("display_name")
        return counters.setdefault(
            key,
            {
                "display_name": person.get("display_name") or "Игрок",
                "username": person.get("username", ""),
                "visits": 0,
                "no_shows": 0,
                "reserve": 0,
            },
        )

    for event in chat_state.get("history", []):
        if not in_period(event.get("date", ""), period):
            continue
        games += 1
        for person in event.get("present", []):
            row_for(person)["visits"] += 1
        for person in event.get("no_shows", []):
            row_for(person)["no_shows"] += 1
        for person in event.get("reserve", []):
            row_for(person)["reserve"] += 1

    rows = sorted(counters.values(), key=lambda r: (-r["visits"], r["no_shows"], r["display_name"]))
    if not rows:
        return f"Статистика {period_title(period)} пока пустая.", [], games

    lines = [f"📊 Посещаемость {period_title(period)}", f"Игр учтено: {games}", ""]
    for index, row in enumerate(rows, start=1):
        username = f" (@{row['username']})" if row.get("username") else ""
        extra = []
        if row["no_shows"]:
            extra.append(f"no-show {row['no_shows']}")
        if row["reserve"]:
            extra.append(f"резерв {row['reserve']}")
        extra_text = f" · {', '.join(extra)}" if extra else ""
        lines.append(f"{index}. {row['display_name']}{username}: {row['visits']}{extra_text}")
    return "\n".join(lines), rows, games


def player_stats_text(chat_state: Dict[str, Any], user_id: Optional[int], name_query: str = "") -> str:
    visits = no_shows = reserve = 0
    display = ""
    q = name_query.strip().lstrip("@").lower()
    for event in chat_state.get("history", []):
        for bucket, attr in (("present", "visits"), ("no_shows", "no_shows"), ("reserve", "reserve")):
            for person in event.get(bucket, []):
                matches = (user_id is not None and person.get("key") == f"user:{user_id}") or (
                    q and (q == (person.get("username") or "").lower() or q in (person.get("display_name") or "").lower())
                )
                if matches:
                    display = display or person.get("display_name", "")
                    if attr == "visits":
                        visits += 1
                    elif attr == "no_shows":
                        no_shows += 1
                    else:
                        reserve += 1
    if not display:
        return "Статистики пока нет."
    return f"📊 {display}\nПосещений: {visits}\nNo-show: {no_shows}\nОставался в резерве: {reserve}"


def post_json_sync(url: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    request = urllib.request.Request(
        url,
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        headers={"Content-Type": "application/json; charset=utf-8"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=15) as response:
        body = response.read().decode("utf-8")
    return json.loads(body) if body else {"ok": True}


def sheets_configured() -> bool:
    return bool(os.getenv(GOOGLE_SHEETS_WEBHOOK_URL_ENV, "").strip())


async def send_to_sheets(payload: Dict[str, Any]) -> Tuple[bool, str]:
    url = os.getenv(GOOGLE_SHEETS_WEBHOOK_URL_ENV, "").strip()
    if not url:
        return False, "not_configured"
    try:
        response = await asyncio.to_thread(post_json_sync, url, payload)
    except (urllib.error.URLError, TimeoutError, OSError, ValueError) as exc:
        logger.warning("Google Sheets export failed: %s", exc)
        return False, str(exc)
    if isinstance(response, dict) and response.get("ok") is False:
        return False, str(response.get("error", "unknown_error"))
    return True, "ok"


async def export_event(event: Dict[str, Any], chat_id: int, chat_title: str) -> Tuple[bool, str]:
    return await send_to_sheets({"action": "event", "chat_id": chat_id, "chat_title": chat_title, "event": event})


async def export_stats(period: str, rows: List[Dict[str, Any]], games: int) -> Tuple[bool, str]:
    return await send_to_sheets(
        {
            "action": "report",
            "period": period_title(period),
            "generated_at": today().strftime("%Y-%m-%d %H:%M:%S"),
            "games_count": games,
            "rows": rows,
        }
    )


def sheets_note(ok: bool, status: str) -> str:
    if not sheets_configured():
        return ""
    return "\nGoogle Sheets: записано ✅" if ok else f"\nGoogle Sheets: ошибка ({status})"


# --------------------------------------------------------------------------- #
# Telegram helpers
# --------------------------------------------------------------------------- #


def is_group(update: Update) -> bool:
    return update.effective_chat is not None and update.effective_chat.type in (ChatType.GROUP, ChatType.SUPERGROUP)


async def is_admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    chat = update.effective_chat
    user = update.effective_user
    if chat is None or user is None:
        return False
    # Admins posting anonymously on behalf of the group.
    message = update.effective_message
    if message is not None and message.sender_chat is not None and message.sender_chat.id == chat.id:
        return True
    if user.id == GROUP_ANONYMOUS_BOT_ID:
        return True
    try:
        member = await context.bot.get_chat_member(chat.id, user.id)
    except TelegramError:
        return False
    return member.status in ("administrator", "creator", "owner")


async def require_admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    if not is_group(update):
        if update.effective_message:
            await update.effective_message.reply_text("Эта команда работает только в групповом чате.")
        return False
    if not await is_admin(update, context):
        if update.effective_message:
            await update.effective_message.reply_text("Эта команда доступна только админам чата.")
        return False
    return True


def chat_state_for(update: Update) -> Dict[str, Any]:
    chat = update.effective_chat
    assert chat is not None
    return ensure_chat(chat.id, chat.title or chat.full_name or "", chat.type)


async def refresh_list_message(context: ContextTypes.DEFAULT_TYPE, chat_id: int, chat_state: Dict[str, Any], force_new: bool = False) -> Optional[Message]:
    """Keep a single list message up to date: edit it if possible, otherwise send a fresh one."""
    game = chat_state["game"]
    text = format_game(chat_state)
    message_id = game.get("list_message_id")
    if message_id and not force_new:
        try:
            await context.bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=text)
            return None
        except BadRequest as exc:
            if "not modified" in str(exc).lower():
                return None
            logger.info("List message %s in %s could not be edited (%s), sending new one", message_id, chat_id, exc)
        except TelegramError as exc:
            logger.warning("List message edit failed in %s: %s", chat_id, exc)
    msg = await context.bot.send_message(chat_id=chat_id, text=text)
    game["list_message_id"] = msg.message_id
    if game.get("open"):
        try:
            await context.bot.pin_chat_message(chat_id=chat_id, message_id=msg.message_id, disable_notification=True)
        except TelegramError as exc:
            logger.info("Could not pin list message in %s: %s", chat_id, exc)
    save_state()
    return msg


async def react(message: Message, emoji: str) -> bool:
    try:
        await message.set_reaction(emoji)
        return True
    except TelegramError as exc:
        logger.debug("Reaction failed: %s", exc)
        return False


async def announce_promotions(context: ContextTypes.DEFAULT_TYPE, chat_id: int, promoted: List[Dict[str, Any]]) -> None:
    if not promoted:
        return
    lines = []
    for entry in promoted:
        if entry.get("kind") == "guest":
            lines.append(f"{mention_html(entry)}: твой гость (+1) поднялся из резерва в основу.")
        else:
            lines.append(f"{mention_html(entry)}, ты в игре! Приходи! ⚽️")
    await context.bot.send_message(chat_id=chat_id, text="\n".join(lines), parse_mode=ParseMode.HTML)
    # Also try to notify in private (works only if the user has started the bot).
    for entry in promoted:
        if entry.get("kind") != "player" or not entry.get("user_id"):
            continue
        try:
            await context.bot.send_message(chat_id=entry["user_id"], text="Ты поднялся из резерва в основу. Ты в игре! Приходи! ⚽️")
        except TelegramError:
            pass


def set_prompt(context: ContextTypes.DEFAULT_TYPE, chat_id: int, mode: str, message_id: int) -> None:
    context.chat_data["prompt"] = {"chat_id": chat_id, "mode": mode, "message_id": message_id}


def pop_prompt(context: ContextTypes.DEFAULT_TYPE, chat_id: int, reply_to_message_id: Optional[int]) -> Optional[str]:
    prompt = context.chat_data.get("prompt")
    if not isinstance(prompt, dict):
        return None
    if prompt.get("chat_id") != chat_id or prompt.get("message_id") != reply_to_message_id:
        return None
    context.chat_data.pop("prompt", None)
    return prompt.get("mode")


# --------------------------------------------------------------------------- #
# Player handlers
# --------------------------------------------------------------------------- #


async def plus_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_group(update) or update.effective_user is None or update.message is None:
        return
    chat_state = chat_state_for(update)
    message = update.message
    if not chat_state["game"].get("open"):
        await message.reply_text("Запись сейчас закрыта.")
        return

    match = PLUS_RE.match(message.text or "")
    guests = int(match.group("count")) if match and match.group("count") else None
    user = update.effective_user

    result = apply_plus(chat_state, user, guests)
    save_state()

    notes: List[str] = []
    if result["already"]:
        notes.append("Ты уже в списке.")
    elif result["joined"] == "reserve":
        position = find_player(chat_state["reserve"], user.id)
        notes.append(f"Основа заполнена — ты в резерве под №{(position or 0) + 1}. Если кто-то выйдет, подниму тебя автоматически.")
    if result["reserve_added"]:
        notes.append(f"Гости в резерве: {result['reserve_added']}.")
    if result["guests_removed"]:
        notes.append(f"Убрал гостей: {result['guests_removed']}.")

    changed = result["joined"] is not None or result["guests_added"] or result["guests_removed"]
    if changed:
        await react(message, ReactionEmoji.THUMBS_UP)
        await refresh_list_message(context, update.effective_chat.id, chat_state)
    if notes:
        await message.reply_text(" ".join(notes))
    await announce_promotions(context, update.effective_chat.id, result["promoted"])


async def minus_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_group(update) or update.effective_user is None or update.message is None:
        return
    chat_state = chat_state_for(update)
    message = update.message
    match = MINUS_RE.match(message.text or "")
    guests = int(match.group("count")) if match and match.group("count") else None
    user = update.effective_user

    if not user_in_any_list(chat_state, user.id) and total_guests(chat_state, user.id) == 0:
        await message.reply_text("Тебя нет в списке.")
        return

    result = apply_minus(chat_state, user, guests)
    save_state()

    if guests and not result["guests_removed"]:
        await message.reply_text("У тебя нет гостей в списке.")
        return

    await react(message, ReactionEmoji.THUMBS_UP)
    await refresh_list_message(context, update.effective_chat.id, chat_state)
    await announce_promotions(context, update.effective_chat.id, result["promoted"])


async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_chat is None or update.message is None:
        return
    text = (update.message.text or "").strip().lower()
    if text in MENU_WORDS:
        await menu_cmd(update, context)
        return

    reply_id = update.message.reply_to_message.message_id if update.message.reply_to_message else None
    if reply_id is None:
        return
    prompt = context.chat_data.get("prompt")
    if not isinstance(prompt, dict) or prompt.get("message_id") != reply_id:
        return
    if not await is_admin(update, context):
        return
    mode = pop_prompt(context, update.effective_chat.id, reply_id)
    if not mode:
        return

    chat_state = chat_state_for(update)
    game = chat_state["game"]
    raw = (update.message.text or "").strip()
    try:
        if mode == "new_game":
            chat_state["game"] = parse_game_input(raw)
            chat_state["players"] = []
            chat_state["reserve"] = []
            response = "Игра создана ✅ Теперь открой запись."
        elif mode == "field":
            if not raw:
                raise ValueError("Название поля пустое")
            game["field"] = raw
            response = "Поле обновлено ✅"
        elif mode == "time":
            game["start"], game["end"] = parse_time_range(raw)
            game["reminder_sent"] = False
            response = "Время обновлено ✅"
        elif mode == "date":
            game["date"] = parse_date(raw)
            game["reminder_sent"] = False
            response = "Дата обновлена ✅"
        elif mode == "limit":
            game["limit"] = parse_limit(raw)
            promoted = promote_from_reserve(chat_state)
            response = "Лимит обновлён ✅"
            await announce_promotions(context, update.effective_chat.id, promoted)
        else:
            return
    except ValueError as exc:
        await update.message.reply_text(f"Ошибка: {exc}")
        return

    save_state()
    await update.message.reply_text(response + "\n\n" + format_game(chat_state), reply_markup=admin_keyboard(chat_state))
    if chat_state["game"].get("list_message_id"):
        await refresh_list_message(context, update.effective_chat.id, chat_state)


# --------------------------------------------------------------------------- #
# Commands
# --------------------------------------------------------------------------- #


async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message:
        await update.message.reply_text(help_text())


async def admin_help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message:
        await update.message.reply_text(admin_help_text())


async def menu_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_admin(update, context):
        return
    chat_state = chat_state_for(update)
    if update.message:
        await update.message.reply_text("Панель управления:\n\n" + format_game(chat_state), reply_markup=admin_keyboard(chat_state))


async def list_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_group(update) or update.message is None:
        return
    chat_state = chat_state_for(update)
    await refresh_list_message(context, update.effective_chat.id, chat_state, force_new=True)


async def open_list(context: ContextTypes.DEFAULT_TYPE, chat_id: int, chat_state: Dict[str, Any]) -> str:
    game = chat_state["game"]
    rolled = roll_date_to_future(game)
    game["open"] = True
    game["was_opened"] = True
    game["reminder_sent"] = False
    save_state()
    await refresh_list_message(context, chat_id, chat_state, force_new=True)
    note = "Запись открыта ✅"
    if rolled:
        note += f"\nДата была в прошлом — перенёс на {date_label(game)}."
    return note


async def open_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_admin(update, context):
        return
    chat_state = chat_state_for(update)
    game = chat_state["game"]
    try:
        for arg in context.args or []:
            if "-" in arg and ":" in arg:
                game["start"], game["end"] = parse_time_range(arg)
            else:
                game["date"] = parse_date(arg)
    except ValueError as exc:
        await update.message.reply_text(f"Ошибка: {exc}\nПример: /open 17/10/26 20:30-22:30")
        return
    note = await open_list(context, update.effective_chat.id, chat_state)
    if "перенёс" in note:
        await update.message.reply_text(note)


async def close_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_admin(update, context):
        return
    chat_state = chat_state_for(update)
    chat_state["game"]["open"] = False
    save_state()
    await refresh_list_message(context, update.effective_chat.id, chat_state)
    await update.message.reply_text("Запись закрыта ⛔️")


async def newgame_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_admin(update, context):
        return
    chat_state = chat_state_for(update)
    try:
        chat_state["game"] = parse_game_input(" ".join(context.args or []))
    except ValueError as exc:
        await update.message.reply_text(f"Ошибка: {exc}")
        return
    chat_state["players"] = []
    chat_state["reserve"] = []
    save_state()
    await update.message.reply_text("Игра создана ✅ Открыть запись: /open\n\n" + format_game(chat_state))


async def _set_game_field(update: Update, context: ContextTypes.DEFAULT_TYPE, key: str) -> None:
    if not await require_admin(update, context):
        return
    chat_state = chat_state_for(update)
    game = chat_state["game"]
    raw = " ".join(context.args or []).strip()
    promoted: List[Dict[str, Any]] = []
    try:
        if key == "date":
            if not raw:
                raise ValueError("Укажи дату: /setdate 17/10/26")
            game["date"] = parse_date(raw)
            game["reminder_sent"] = False
            note = f"Дата: {date_label(game)} ✅"
        elif key == "time":
            if not raw:
                raise ValueError("Укажи время: /settime 20:30-22:30")
            game["start"], game["end"] = parse_time_range(raw)
            game["reminder_sent"] = False
            note = f"Время: {game['start']}–{game['end']} ✅"
        elif key == "field":
            if not raw:
                raise ValueError("Укажи поле: /setfield Горизонт-арена")
            game["field"] = raw
            note = f"Поле: {raw} ✅"
        else:
            if not raw:
                raise ValueError("Укажи лимит: /setlimit 18")
            game["limit"] = parse_limit(raw)
            promoted = promote_from_reserve(chat_state)
            note = f"Лимит: {game['limit'] or 'без лимита'} ✅"
    except ValueError as exc:
        await update.message.reply_text(f"Ошибка: {exc}")
        return
    save_state()
    await update.message.reply_text(note)
    if game.get("list_message_id"):
        await refresh_list_message(context, update.effective_chat.id, chat_state)
    await announce_promotions(context, update.effective_chat.id, promoted)


async def setdate_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _set_game_field(update, context, "date")


async def settime_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _set_game_field(update, context, "time")


async def setfield_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _set_game_field(update, context, "field")


async def setlimit_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _set_game_field(update, context, "limit")


async def add_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Manually add a player who is not in Telegram (or cannot write himself)."""
    if not await require_admin(update, context):
        return
    name = " ".join(context.args or []).strip()
    if not name:
        await update.message.reply_text("Укажи имя: /add Иван")
        return
    chat_state = chat_state_for(update)
    entry = normalize_entry(name)
    assert entry is not None
    entry["joined_at"] = now_iso()
    where = add_entry(chat_state, entry)
    save_state()
    await update.message.reply_text(f"Добавил {name} в {'основу' if where == 'main' else 'резерв'} ✅")
    await refresh_list_message(context, update.effective_chat.id, chat_state)


async def remove_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_admin(update, context):
        return
    query = " ".join(context.args or []).strip()
    if not query:
        await update.message.reply_text("Укажи кого убрать: /remove @username, имя или номер в списке")
        return
    chat_state = chat_state_for(update)
    found = find_entry_by_query(chat_state, query)
    if found is None:
        await update.message.reply_text("Не нашёл такого игрока в списке.")
        return
    list_name, index = found
    entry = chat_state[list_name][index]
    if entry.get("user_id"):
        remove_user_completely(chat_state, entry["user_id"])
    else:
        chat_state[list_name].pop(index)
    promoted = promote_from_reserve(chat_state)
    save_state()
    await update.message.reply_text(f"Убрал {player_label(entry)} ✅")
    await refresh_list_message(context, update.effective_chat.id, chat_state)
    await announce_promotions(context, update.effective_chat.id, promoted)


async def _mark_noshow(update: Update, context: ContextTypes.DEFAULT_TYPE, value: bool) -> None:
    if not await require_admin(update, context):
        return
    query = " ".join(context.args or []).strip()
    if not query:
        await update.message.reply_text("Укажи игрока: @username, имя или номер в списке")
        return
    chat_state = chat_state_for(update)
    found = find_entry_by_query(chat_state, query)
    if found is None or found[0] != "players":
        await update.message.reply_text("Не нашёл такого игрока в основе.")
        return
    entry = chat_state["players"][found[1]]
    entry["no_show"] = value
    save_state()
    label = player_label({**entry, "no_show": False})
    await update.message.reply_text(f"{label}: {'отмечен как no-show ❌' if value else 'отметка no-show снята ✅'}")
    await refresh_list_message(context, update.effective_chat.id, chat_state)


async def noshow_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _mark_noshow(update, context, True)


async def showup_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _mark_noshow(update, context, False)


async def do_finish(context: ContextTypes.DEFAULT_TYPE, chat_id: int, chat_title: str, chat_state: Dict[str, Any]) -> str:
    event = finish_game(chat_state)
    save_state()
    ok, status = await export_event(event, chat_id, chat_title)
    text = (
        "Игра завершена ✅\n"
        f"Пришли: {len(event['present'])}\n"
        f"No-show: {len(event['no_shows'])}\n"
        f"Остались в резерве: {len(event['reserve'])}\n"
        f"Гостей: {len(event['guests'])}\n"
        f"Следующая игра: {date_label(chat_state['game'])} — открыть запись: /open"
    )
    return text + sheets_note(ok, status)


async def finish_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_admin(update, context):
        return
    chat_state = chat_state_for(update)
    if not chat_state["players"]:
        await update.message.reply_text("Список пуст, завершать нечего.")
        return
    chat = update.effective_chat
    await update.message.reply_text(await do_finish(context, chat.id, chat.title or "", chat_state))


async def stats_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_group(update) or update.message is None:
        return
    chat_state = chat_state_for(update)
    args = list(context.args or [])
    period = "all"
    if args and args[0].lower() in ("month", "quarter", "year", "all"):
        period = args.pop(0).lower()
    if args:
        await update.message.reply_text(player_stats_text(chat_state, None, " ".join(args)))
        return
    text, rows, games = build_stats(chat_state, period)
    await update.message.reply_text(text)


async def mystats_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_group(update) or update.message is None or update.effective_user is None:
        return
    chat_state = chat_state_for(update)
    await update.message.reply_text(player_stats_text(chat_state, update.effective_user.id))


async def reset_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_admin(update, context):
        return
    chat_state = chat_state_for(update)
    if not chat_state["players"] and not chat_state["reserve"]:
        await update.message.reply_text("Список уже пуст.")
        return
    await update.message.reply_text(
        "Очистить список без записи статистики? (для завершения игры с учётом статистики используй /finish)",
        reply_markup=confirm_keyboard("reset"),
    )


# --------------------------------------------------------------------------- #
# Inline panel
# --------------------------------------------------------------------------- #


async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if query is None or update.effective_chat is None or query.message is None:
        return
    if not await is_admin(update, context):
        await query.answer("Только для админов", show_alert=True)
        return
    await query.answer()

    chat = update.effective_chat
    chat_state = chat_state_for(update)
    game = chat_state["game"]
    data = query.data or ""

    async def show_panel(prefix: str = "") -> None:
        text = (prefix + "\n\n" if prefix else "") + "Панель управления:\n\n" + format_game(chat_state)
        try:
            await query.edit_message_text(text, reply_markup=admin_keyboard(chat_state))
        except BadRequest as exc:
            if "not modified" not in str(exc).lower():
                raise

    if data in ("admin:back", "admin:list"):
        await show_panel()
    elif data == "admin:open":
        note = await open_list(context, chat.id, chat_state)
        await show_panel(note)
    elif data == "admin:close":
        game["open"] = False
        save_state()
        await refresh_list_message(context, chat.id, chat_state)
        await show_panel("Запись закрыта ⛔️")
    elif data == "admin:new_game":
        msg = await query.message.reply_text("Ответь на это сообщение данными новой игры:\n24/04/26 20:30-22:30 18 Горизонт-арена")
        set_prompt(context, chat.id, "new_game", msg.message_id)
    elif data == "admin:field":
        msg = await query.message.reply_text("Ответь на это сообщение названием поля.")
        set_prompt(context, chat.id, "field", msg.message_id)
    elif data == "admin:time":
        msg = await query.message.reply_text("Ответь на это сообщение временем, например: 20:30-22:30")
        set_prompt(context, chat.id, "time", msg.message_id)
    elif data == "admin:date":
        msg = await query.message.reply_text("Ответь на это сообщение датой, например: 17/10/26")
        set_prompt(context, chat.id, "date", msg.message_id)
    elif data == "admin:limit":
        msg = await query.message.reply_text("Ответь на это сообщение лимитом участников, например: 18 (0 = без лимита)")
        set_prompt(context, chat.id, "limit", msg.message_id)
    elif data == "admin:finish":
        if not chat_state["players"]:
            await show_panel("Список пуст, завершать нечего.")
            return
        await query.edit_message_text(
            "Завершить игру? Статистика запишется, список очистится, дата перенесётся на неделю вперёд.\n\n"
            "Перед этим отметь тех, кто не пришёл: /noshow @username",
            reply_markup=confirm_keyboard("finish"),
        )
    elif data == "confirm:finish":
        if not chat_state["players"]:
            await show_panel("Список пуст, завершать нечего.")
            return
        text = await do_finish(context, chat.id, chat.title or "", chat_state)
        await show_panel(text)
    elif data == "admin:reset":
        if not chat_state["players"] and not chat_state["reserve"]:
            await show_panel("Список уже пуст.")
            return
        await query.edit_message_text(
            "Очистить список без записи статистики?",
            reply_markup=confirm_keyboard("reset"),
        )
    elif data == "confirm:reset":
        chat_state["players"] = []
        chat_state["reserve"] = []
        save_state()
        if game.get("list_message_id"):
            await refresh_list_message(context, chat.id, chat_state)
        await show_panel("Список очищен 🗑")
    elif data == "admin:stats":
        await query.edit_message_text("Выбери период:", reply_markup=stats_keyboard())
    elif data.startswith("stats:"):
        period = data.split(":", 1)[1]
        text, rows, games = build_stats(chat_state, period)
        if rows and sheets_configured():
            ok, status = await export_stats(period, rows, games)
            text += "\n" + sheets_note(ok, status)
        try:
            await query.edit_message_text(text, reply_markup=stats_keyboard())
        except BadRequest as exc:
            if "not modified" not in str(exc).lower():
                raise


# --------------------------------------------------------------------------- #
# Background jobs
# --------------------------------------------------------------------------- #


async def winners_reminder_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    """One hour after the game ends, ask for a photo of the winners (only if the list was used)."""
    now = today()
    for chat_id, chat_state in list(state.items()):
        game = chat_state.get("game", {})
        if game.get("reminder_sent") or not game.get("was_opened") or not chat_state.get("active", True):
            continue
        if not chat_state["players"]:
            continue
        end_dt = game_end_datetime(game)
        if end_dt is None or now < end_dt + WINNERS_PROMPT_DELAY:
            continue
        try:
            msg = await context.bot.send_message(chat_id=int(chat_id), text="Отправьте фотографию победителей 🏆")
            try:
                await context.bot.pin_chat_message(chat_id=int(chat_id), message_id=msg.message_id, disable_notification=True)
            except TelegramError as exc:
                logger.info("Could not pin winners prompt in %s: %s", chat_id, exc)
            game["winners_prompt_message_id"] = msg.message_id
        except TelegramError as exc:
            logger.warning("Could not send winners prompt in %s: %s", chat_id, exc)
        game["reminder_sent"] = True
        save_state()


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.error("Unhandled error while processing update", exc_info=context.error)


async def member_update(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_chat is None or update.my_chat_member is None:
        return
    chat_state = chat_state_for(update)
    status = update.my_chat_member.new_chat_member.status
    chat_state["active"] = status not in ("left", "kicked")
    save_state()


async def post_init(app: Application) -> None:
    await app.bot.set_my_commands(
        [
            ("list", "Показать список"),
            ("open", "Открыть запись (админ)"),
            ("close", "Закрыть запись (админ)"),
            ("menu", "Панель управления (админ)"),
            ("finish", "Завершить игру (админ)"),
            ("stats", "Статистика посещений"),
            ("mystats", "Моя статистика"),
            ("help", "Как пользоваться"),
            ("admin", "Команды админов"),
        ]
    )
    if app.job_queue is not None:
        app.job_queue.run_repeating(winners_reminder_job, interval=60, first=10)
    else:
        logger.warning("JobQueue is not available; install python-telegram-bot[job-queue]")


def main() -> None:
    token = os.getenv("BOT_TOKEN", "").strip()
    if not token:
        raise RuntimeError("Set BOT_TOKEN environment variable")
    load_state()

    app = ApplicationBuilder().token(token).post_init(post_init).build()

    app.add_handler(CommandHandler(["start", "help"], start_cmd))
    app.add_handler(CommandHandler("admin", admin_help_cmd))
    app.add_handler(CommandHandler("menu", menu_cmd))
    app.add_handler(CommandHandler("list", list_cmd))
    app.add_handler(CommandHandler("open", open_cmd))
    app.add_handler(CommandHandler("close", close_cmd))
    app.add_handler(CommandHandler("newgame", newgame_cmd))
    app.add_handler(CommandHandler("setdate", setdate_cmd))
    app.add_handler(CommandHandler("settime", settime_cmd))
    app.add_handler(CommandHandler("setfield", setfield_cmd))
    app.add_handler(CommandHandler("setlimit", setlimit_cmd))
    app.add_handler(CommandHandler("add", add_cmd))
    app.add_handler(CommandHandler("remove", remove_cmd))
    app.add_handler(CommandHandler("noshow", noshow_cmd))
    app.add_handler(CommandHandler("showup", showup_cmd))
    app.add_handler(CommandHandler("finish", finish_cmd))
    app.add_handler(CommandHandler("stats", stats_cmd))
    app.add_handler(CommandHandler("mystats", mystats_cmd))
    app.add_handler(CommandHandler("reset", reset_cmd))
    app.add_handler(CallbackQueryHandler(callback_handler))
    app.add_handler(MessageHandler(filters.TEXT & filters.Regex(PLUS_RE), plus_handler))
    app.add_handler(MessageHandler(filters.TEXT & filters.Regex(MINUS_RE), minus_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))
    app.add_handler(ChatMemberHandler(member_update, ChatMemberHandler.MY_CHAT_MEMBER))
    app.add_error_handler(error_handler)

    logger.info("Bot is running, state file: %s", STATE_FILE)
    app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)


if __name__ == "__main__":
    try:
        import dotenv

        dotenv.load_dotenv()
    except Exception:
        pass
    main()
