"""Telegram bot for weekly football sign-up lists.

Players write "+" / "+N" / "-" / "-N" in a group chat, the bot keeps the main
list and the reserve list, promotes from the reserve automatically and replies
with the current list after every action. After the game is finished the bot
keeps track of who took the bibs home to wash ("я взял"). Admins manage the
game with commands or the inline panel (/menu).
"""

import html
import json
import logging
import os
import re
import sys
import asyncio
import tempfile
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
import ai
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
MAX_GUESTS = 5
WINNERS_PROMPT_DELAY = timedelta(hours=1)
GROUP_ANONYMOUS_BOT_ID = 1087968824

PLUS_RE = re.compile(r"^\s*\+\s*(?P<count>[1-5])?\s*$")
MINUS_RE = re.compile(r"^\s*(?:-\s*(?P<count>[1-5])?|минус|не\s+смогу|не\s+буду)\s*$", re.IGNORECASE)
# "я взял", "взял", "беру", "я забрал манишки", "манишки у меня", "взял манишки стирать"
BIBS_TAKE_RE = re.compile(
    r"^\s*(?:я\s+)?(?:взял|взяла|беру|забрал|забрала|забираю|заберу|возьму)"
    r"(?:\s+манишки)?(?:\s+(?:домой|стирать|постирать))*\s*[!.)]*\s*$"
    r"|^\s*манишки\s+(?:у\s+меня|взял|беру|забрал)\s*[!.)]*\s*$",
    re.IGNORECASE,
)
MENU_WORDS = {"меню", "menu", "панель"}
BIBS_WORDS = {"манишки", "манишка"}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    stream=sys.stdout,
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("apscheduler").setLevel(logging.WARNING)
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
    """If the game date is in the past, move it forward week by week until it is not."""
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
        "list_message_id": None,  # the pinned list message the bot keeps editing
    }


def default_bibs() -> Dict[str, Any]:
    return {
        "active": False,  # True between /finish and the admin's reset
        "takers": [],  # [{user_id, name, username, taken_at}]
        "message_id": None,  # the bibs message the bot keeps editing
        "game_date": "",  # the game after which this round started
    }


def default_chat_state(chat_title: str = "", chat_type: str = "") -> Dict[str, Any]:
    return {
        "chat_title": chat_title,
        "chat_type": chat_type,
        "active": True,
        "game": default_game(),
        "players": [],
        "reserve": [],
        "bibs": default_bibs(),
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


def normalize_bibs(raw: Any) -> Dict[str, Any]:
    bibs = default_bibs()
    if isinstance(raw, dict):
        bibs["active"] = bool(raw.get("active", False))
        bibs["message_id"] = raw.get("message_id")
        bibs["game_date"] = raw.get("game_date", "") or ""
        takers = raw.get("takers", [])
        if isinstance(takers, list):
            for item in takers:
                if isinstance(item, dict) and (item.get("user_id") or item.get("name")):
                    bibs["takers"].append(
                        {
                            "user_id": item.get("user_id"),
                            "name": item.get("name") or "",
                            "username": item.get("username") or "",
                            "taken_at": item.get("taken_at") or "",
                        }
                    )
    return bibs


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
    normalized["bibs"] = normalize_bibs(normalized.get("bibs"))
    normalized.pop("history", None)  # attendance statistics were dropped
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
    }


def player_label(entry: Dict[str, Any]) -> str:
    if entry.get("kind") == "guest":
        owner = entry.get("owner_name") or "игрока"
        username = entry.get("owner_username", "")
        suffix = f" (@{username})" if username else ""
        return f"+1 от {owner}{suffix}"
    username = entry.get("username", "")
    suffix = f" (@{username})" if username else ""
    return f"{entry.get('name') or 'Игрок'}{suffix}"


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
    """Append an entry to the main list, or to the reserve if the main list is full."""
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
            if entry.get("kind") == "player" and (entry.get("username") or "").lower() == q:
                return list_name, index
    for list_name in ("players", "reserve"):
        for index, entry in enumerate(chat_state[list_name]):
            if entry.get("kind") == "player" and q in (entry.get("name") or "").lower():
                return list_name, index
    return None


# --------------------------------------------------------------------------- #
# Bibs (манишки)
# --------------------------------------------------------------------------- #


def start_bibs_round(chat_state: Dict[str, Any], game_date_text: str) -> None:
    """Called on /finish: a new round of 'who took the bibs' starts, the old list is dropped."""
    chat_state["bibs"] = default_bibs()
    chat_state["bibs"]["active"] = True
    chat_state["bibs"]["game_date"] = game_date_text


def add_bibs_taker(chat_state: Dict[str, Any], user: User) -> bool:
    """Record that the user took the bibs. Returns False if already recorded."""
    bibs = chat_state["bibs"]
    if any(t.get("user_id") == user.id for t in bibs["takers"]):
        return False
    bibs["takers"].append(
        {"user_id": user.id, "name": full_name(user), "username": user.username or "", "taken_at": now_iso()}
    )
    return True


def remove_bibs_taker(chat_state: Dict[str, Any], user_id: int) -> bool:
    bibs = chat_state["bibs"]
    before = len(bibs["takers"])
    bibs["takers"] = [t for t in bibs["takers"] if t.get("user_id") != user_id]
    return len(bibs["takers"]) != before


def format_bibs(chat_state: Dict[str, Any]) -> str:
    bibs = chat_state["bibs"]
    date_text = f" после игры {bibs['game_date']}" if bibs.get("game_date") else ""
    if bibs["takers"]:
        rows = []
        for i, t in enumerate(bibs["takers"], start=1):
            username = f" (@{t['username']})" if t.get("username") else ""
            rows.append(f"{i}. {t['name']}{username}")
        body = "\n".join(rows)
    else:
        body = "Пока никто не отметился."
    return (
        f"🧺 Манишки{date_text}\n"
        "Кто забрал манишки стирать — напиши «я взял».\n\n"
        f"Взяли стирать:\n{body}"
    )


def bibs_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("🔄 Обновить список (манишки вернули)", callback_data="admin:bibs_reset"),
            ],
            [InlineKeyboardButton("« Назад", callback_data="admin:back")],
        ]
    )


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
        "«я взял» — после игры отметить, что забрал манишки стирать\n"
        "/bibs или слово «манишки» — у кого сейчас манишки\n\n"
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
        "/finish — завершить игру: очистить список, перенести дату на неделю и открыть учёт манишек\n"
        "/bibs — список, кто взял манишки\n"
        "/bibs reset — обновить список манишек (манишки вернули)\n"
        "/bibs remove @username|Имя — убрать человека из списка манишек\n"
        "/reset — очистить список игроков\n"
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
                InlineKeyboardButton("🧺 Манишки", callback_data="admin:bibs"),
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


async def edit_or_ignore(context: ContextTypes.DEFAULT_TYPE, chat_id: int, message_id: Optional[int], text: str, reply_markup=None) -> bool:
    """Edit a message the bot sent earlier; returns False if it could not be edited."""
    if not message_id:
        return False
    try:
        await context.bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=text, reply_markup=reply_markup)
        return True
    except BadRequest as exc:
        if "not modified" in str(exc).lower():
            return True
        logger.info("Message %s in %s could not be edited: %s", message_id, chat_id, exc)
    except TelegramError as exc:
        logger.warning("Message edit failed in %s: %s", chat_id, exc)
    return False


async def refresh_list_message(context: ContextTypes.DEFAULT_TYPE, chat_id: int, chat_state: Dict[str, Any], force_new: bool = False) -> Optional[Message]:
    """Keep the pinned list message up to date: edit it if possible, otherwise send a fresh one."""
    game = chat_state["game"]
    text = format_game(chat_state)
    if not force_new and await edit_or_ignore(context, chat_id, game.get("list_message_id"), text):
        return None
    msg = await context.bot.send_message(chat_id=chat_id, text=text)
    game["list_message_id"] = msg.message_id
    if game.get("open"):
        try:
            await context.bot.pin_chat_message(chat_id=chat_id, message_id=msg.message_id, disable_notification=True)
        except TelegramError as exc:
            logger.info("Could not pin list message in %s: %s", chat_id, exc)
    save_state()
    return msg


async def post_list_update(context: ContextTypes.DEFAULT_TYPE, chat_id: int, chat_state: Dict[str, Any], message: Message, header: str) -> None:
    """After a player's action: keep the pinned list current and reply with a fresh copy of the list."""
    await refresh_list_message(context, chat_id, chat_state)
    await message.reply_text(header + "\n\n" + format_game(chat_state))


async def post_bibs_message(context: ContextTypes.DEFAULT_TYPE, chat_id: int, chat_state: Dict[str, Any]) -> Message:
    """Send a fresh bibs message and remember it for later edits."""
    msg = await context.bot.send_message(chat_id=chat_id, text=format_bibs(chat_state))
    chat_state["bibs"]["message_id"] = msg.message_id
    save_state()
    return msg


async def refresh_bibs_message(context: ContextTypes.DEFAULT_TYPE, chat_id: int, chat_state: Dict[str, Any]) -> None:
    await edit_or_ignore(context, chat_id, chat_state["bibs"].get("message_id"), format_bibs(chat_state))


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


async def do_join(update: Update, context: ContextTypes.DEFAULT_TYPE, guests: Optional[int]) -> None:
    """Sign the author up (guests=None keeps the current guest count, N sets it)."""
    chat_state = chat_state_for(update)
    message = update.message
    user = update.effective_user
    if not chat_state["game"].get("open"):
        await message.reply_text("Запись сейчас закрыта.")
        return

    result = apply_plus(chat_state, user, guests)
    save_state()

    notes: List[str] = []
    if result["already"]:
        notes.append("Ты уже в списке.")
    elif result["joined"] == "reserve":
        position = find_player(chat_state["reserve"], user.id)
        notes.append(f"Основа заполнена — ты в резерве под №{(position or 0) + 1}. Если кто-то выйдет, подниму тебя автоматически.")
    elif result["joined"] == "main":
        notes.append("Записал ✅")
    if result["guests_added"]:
        notes.append(f"Гостей добавил: {result['guests_added']}.")
    if result["reserve_added"]:
        notes.append(f"Гости в резерве: {result['reserve_added']}.")
    if result["guests_removed"]:
        notes.append(f"Убрал гостей: {result['guests_removed']}.")

    changed = result["joined"] is not None or result["guests_added"] or result["guests_removed"]
    if changed:
        await react(message, ReactionEmoji.THUMBS_UP)
    await post_list_update(context, update.effective_chat.id, chat_state, message, " ".join(notes))
    await announce_promotions(context, update.effective_chat.id, result["promoted"])


async def do_leave(update: Update, context: ContextTypes.DEFAULT_TYPE, guests: Optional[int]) -> None:
    """Remove the author (guests=None: with all guests; N: only N guests)."""
    chat_state = chat_state_for(update)
    message = update.message
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
    header = f"Убрал гостей: {result['guests_removed']} ✅" if guests else "Убрал тебя из списка ✅"
    await post_list_update(context, update.effective_chat.id, chat_state, message, header)
    await announce_promotions(context, update.effective_chat.id, result["promoted"])


async def do_bibs_take(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Record the author as holding the bibs. Returns False when no bibs round is open."""
    chat_state = chat_state_for(update)
    message = update.message
    user = update.effective_user
    if not chat_state["bibs"].get("active"):
        # Bibs are tracked only between /finish and the admin's reset; otherwise ignore.
        return False
    added = add_bibs_taker(chat_state, user)
    save_state()
    if not added:
        await message.reply_text("Ты уже в списке манишек 🧺")
        return True
    await react(message, ReactionEmoji.THUMBS_UP)
    await refresh_bibs_message(context, update.effective_chat.id, chat_state)
    await message.reply_text(f"Записал: {full_name(user)} забрал манишки стирать 🧺\n\n" + format_bibs(chat_state))
    return True


def _group_text_update(update: Update) -> bool:
    return is_group(update) and update.effective_user is not None and update.message is not None


async def plus_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _group_text_update(update):
        return
    match = PLUS_RE.match(update.message.text or "")
    guests = int(match.group("count")) if match and match.group("count") else None
    await do_join(update, context, guests)


async def minus_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _group_text_update(update):
        return
    match = MINUS_RE.match(update.message.text or "")
    guests = int(match.group("count")) if match and match.group("count") else None
    await do_leave(update, context, guests)


async def bibs_take_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Someone wrote 'я взял' — record them as holding the bibs."""
    if not _group_text_update(update):
        return
    await do_bibs_take(update, context)


async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_chat is None or update.message is None:
        return
    text = (update.message.text or "").strip().lower().rstrip("?!.")
    if text in MENU_WORDS:
        await menu_cmd(update, context)
        return
    if text in BIBS_WORDS:
        await bibs_cmd(update, context)
        return

    reply_id = update.message.reply_to_message.message_id if update.message.reply_to_message else None
    prompt = context.chat_data.get("prompt")
    if reply_id is None or not isinstance(prompt, dict) or prompt.get("message_id") != reply_id:
        await ai_handler(update, context)
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
# AI layer (optional, needs OPENAI_API_KEY)
# --------------------------------------------------------------------------- #

AI_HISTORY_LIMIT = 20
AI_MIN_CONFIDENCE = 0.7


def remember_message(context: ContextTypes.DEFAULT_TYPE, author: str, text: str) -> None:
    history: List[str] = context.chat_data.setdefault("history", [])
    history.append(f"{author}: {text.strip()[:300]}")
    del history[:-AI_HISTORY_LIMIT]


async def history_recorder(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Runs for every group text message (own handler group) and only records it for AI context."""
    if not _group_text_update(update) or not ai.enabled():
        return
    remember_message(context, full_name(update.effective_user), update.message.text or "")


def date_table() -> str:
    rows = []
    for offset in range(0, 8):
        day = today() + timedelta(days=offset)
        label = "сегодня" if offset == 0 else ("завтра" if offset == 1 else "")
        rows.append(f"{day.strftime('%d/%m/%y')} — {WEEKDAYS_RU[day.weekday()]}{' (' + label + ')' if label else ''}")
    return "\n".join(rows) + f"\nСейчас {today().strftime('%H:%M')}."


async def ai_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Free-form message: ask the model what the author means and act through the normal code paths."""
    if not ai.enabled() or not _group_text_update(update):
        return
    message = update.message
    text = (message.text or "").strip()
    if not text or len(text) > ai.MAX_MESSAGE_CHARS or update.effective_user.is_bot:
        return

    chat_state = chat_state_for(update)
    admin = await is_admin(update, context)
    history: List[str] = list(context.chat_data.get("history", []))
    # The recorder already appended the current message; show the model the earlier ones only.
    if history and history[-1].endswith(text[:300]):
        history = history[:-1]

    intent = await asyncio.to_thread(
        ai.classify_sync,
        text,
        full_name(update.effective_user),
        admin,
        format_game(chat_state),
        format_bibs(chat_state),
        date_table(),
        history,
    )
    if not intent or intent["intent"] == "none":
        return
    logger.info("AI intent in %s: %s", update.effective_chat.id, intent)
    if intent["confidence"] < AI_MIN_CONFIDENCE:
        return

    kind = intent["intent"]
    if kind == "join":
        await do_join(update, context, intent["guests"] if intent["guests"] else None)
    elif kind == "leave":
        await do_leave(update, context, intent["guests"] or None)
    elif kind == "bibs_taken":
        await do_bibs_take(update, context)
    elif kind == "question":
        if intent["reply"]:
            reply = await message.reply_text(intent["reply"])
            remember_message(context, "Бот", reply.text or "")
    elif kind == "admin":
        await ai_admin_action(update, context, intent)


async def ai_admin_action(update: Update, context: ContextTypes.DEFAULT_TYPE, intent: Dict[str, Any]) -> None:
    action = intent.get("action")
    args = intent.get("args") or {}
    if not action:
        return
    if not await is_admin(update, context):
        await update.message.reply_text("Это может сделать только админ чата.")
        return

    def arg(name: str) -> str:
        value = args.get(name)
        return str(value).strip() if value not in (None, "") else ""

    if action == "open":
        context.args = [a for a in (arg("date"), arg("time")) if a]
        await open_cmd(update, context)
    elif action == "close":
        await close_cmd(update, context)
    elif action == "list":
        await list_cmd(update, context)
    elif action == "setlimit":
        context.args = [arg("limit")]
        await setlimit_cmd(update, context)
    elif action == "setfield":
        context.args = arg("field").split()
        await setfield_cmd(update, context)
    elif action == "setdate":
        context.args = [arg("date")]
        await setdate_cmd(update, context)
    elif action == "settime":
        context.args = [arg("time")]
        await settime_cmd(update, context)
    elif action == "finish":
        chat_state = chat_state_for(update)
        if not chat_state["players"] and not chat_state["reserve"]:
            await update.message.reply_text("Список пуст, завершать нечего.")
            return
        await update.message.reply_text(
            "Завершить игру? Список очистится, дата перенесётся на неделю вперёд, бот спросит, кто забрал манишки.",
            reply_markup=confirm_keyboard("finish"),
        )
    elif action == "bibs_reset":
        context.args = ["reset"]
        await bibs_cmd(update, context)


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


def finish_game(chat_state: Dict[str, Any]) -> str:
    """Clear the lists, roll the date one week forward and open a new bibs round. Returns the finished game's date."""
    game = chat_state["game"]
    finished_date = game["date"]
    chat_state["players"] = []
    chat_state["reserve"] = []
    game["open"] = False
    game["list_message_id"] = None
    roll_date_forward(game)
    game["was_opened"] = False
    game["reminder_sent"] = False
    game["winners_prompt_message_id"] = None
    start_bibs_round(chat_state, finished_date)
    return finished_date


async def do_finish(context: ContextTypes.DEFAULT_TYPE, chat_id: int, chat_state: Dict[str, Any]) -> str:
    finish_game(chat_state)
    save_state()
    await post_bibs_message(context, chat_id, chat_state)
    return (
        "Игра завершена ✅ Список очищен.\n"
        f"Следующая игра: {date_label(chat_state['game'])} — открыть запись: /open"
    )


async def finish_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_admin(update, context):
        return
    chat_state = chat_state_for(update)
    if not chat_state["players"] and not chat_state["reserve"]:
        await update.message.reply_text("Список пуст, завершать нечего.")
        return
    await update.message.reply_text(await do_finish(context, update.effective_chat.id, chat_state))


async def bibs_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/bibs — show who has the bibs; /bibs reset and /bibs remove X are for admins."""
    if not is_group(update) or update.message is None:
        return
    chat_state = chat_state_for(update)
    args = list(context.args or [])
    sub = args[0].lower() if args else ""

    if sub in ("reset", "сброс", "обновить", "new"):
        if not await require_admin(update, context):
            return
        start_bibs_round(chat_state, "")
        save_state()
        await post_bibs_message(context, update.effective_chat.id, chat_state)
        return

    if sub in ("remove", "убрать", "del"):
        if not await require_admin(update, context):
            return
        q = " ".join(args[1:]).strip().lstrip("@").lower()
        takers = chat_state["bibs"]["takers"]
        target = next((t for t in takers if (t.get("username") or "").lower() == q), None) or next(
            (t for t in takers if q and q in (t.get("name") or "").lower()), None
        )
        if target is None:
            await update.message.reply_text("Не нашёл такого в списке манишек.")
            return
        remove_bibs_taker(chat_state, target["user_id"])
        save_state()
        await refresh_bibs_message(context, update.effective_chat.id, chat_state)
        await update.message.reply_text(f"Убрал {target['name']} из списка манишек ✅\n\n" + format_bibs(chat_state))
        return

    await post_bibs_message(context, update.effective_chat.id, chat_state)


async def reset_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_admin(update, context):
        return
    chat_state = chat_state_for(update)
    if not chat_state["players"] and not chat_state["reserve"]:
        await update.message.reply_text("Список уже пуст.")
        return
    await update.message.reply_text(
        "Очистить список игроков? (чтобы завершить игру и открыть учёт манишек, используй /finish)",
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

    async def show_bibs(prefix: str = "") -> None:
        text = (prefix + "\n\n" if prefix else "") + format_bibs(chat_state)
        try:
            await query.edit_message_text(text, reply_markup=bibs_keyboard())
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
        if not chat_state["players"] and not chat_state["reserve"]:
            await show_panel("Список пуст, завершать нечего.")
            return
        await query.edit_message_text(
            "Завершить игру? Список очистится, дата перенесётся на неделю вперёд, "
            "бот спросит, кто забрал манишки.",
            reply_markup=confirm_keyboard("finish"),
        )
    elif data == "confirm:finish":
        if not chat_state["players"] and not chat_state["reserve"]:
            await show_panel("Список пуст, завершать нечего.")
            return
        text = await do_finish(context, chat.id, chat_state)
        await show_panel(text)
    elif data == "admin:reset":
        if not chat_state["players"] and not chat_state["reserve"]:
            await show_panel("Список уже пуст.")
            return
        await query.edit_message_text("Очистить список игроков?", reply_markup=confirm_keyboard("reset"))
    elif data == "confirm:reset":
        chat_state["players"] = []
        chat_state["reserve"] = []
        save_state()
        if game.get("list_message_id"):
            await refresh_list_message(context, chat.id, chat_state)
        await show_panel("Список очищен 🗑")
    elif data == "admin:bibs":
        await show_bibs()
    elif data == "admin:bibs_reset":
        start_bibs_round(chat_state, "")
        save_state()
        await post_bibs_message(context, chat.id, chat_state)
        await show_bibs("Список манишек обновлён 🔄")


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
            ("bibs", "Кто взял манишки"),
            ("open", "Открыть запись (админ)"),
            ("close", "Закрыть запись (админ)"),
            ("menu", "Панель управления (админ)"),
            ("finish", "Завершить игру (админ)"),
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

    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, history_recorder), group=-1)
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
    app.add_handler(CommandHandler("finish", finish_cmd))
    app.add_handler(CommandHandler(["bibs", "manishki"], bibs_cmd))
    app.add_handler(CommandHandler("reset", reset_cmd))
    app.add_handler(CallbackQueryHandler(callback_handler))
    app.add_handler(MessageHandler(filters.TEXT & filters.Regex(PLUS_RE), plus_handler))
    app.add_handler(MessageHandler(filters.TEXT & filters.Regex(MINUS_RE), minus_handler))
    app.add_handler(MessageHandler(filters.TEXT & filters.Regex(BIBS_TAKE_RE), bibs_take_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))
    app.add_handler(ChatMemberHandler(member_update, ChatMemberHandler.MY_CHAT_MEMBER))
    app.add_error_handler(error_handler)

    logger.info("Bot is running, state file: %s, AI: %s", STATE_FILE, f"on ({ai.OPENAI_MODEL})" if ai.enabled() else "off")
    app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)


if __name__ == "__main__":
    try:
        import dotenv

        dotenv.load_dotenv()
    except Exception:
        pass
    main()
