import json
import os
import re
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from telegram import (
    ChatMember,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ReplyKeyboardMarkup,
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


DEFAULT_FIELD = "Горизонт-арена"
DEFAULT_TIME = "20:00-22:00"
DEFAULT_TIME_PRESETS = ["18:00-20:00", "20:00-22:00", "21:00-23:00"]
STATE_FILE = "state.json"

BUTTON_PLUS = "+"
BUTTON_MINUS = "-"
BUTTON_LIST = "Список"
BUTTON_OPEN = "Открыть запись"
BUTTON_CLOSE = "Закрыть запись"
BUTTON_SET_DATETIME = "Изменить дату/время"
BUTTON_SET_FIELD = "Изменить поле"
BUTTON_SHOW_SCHEDULE = "Расписания"

WEEKDAY_RU = [
    "Понедельник",
    "Вторник",
    "Среда",
    "Четверг",
    "Пятница",
    "Суббота",
    "Воскресенье",
]

WEEKDAY_ALIASES = {
    "пн": 0,
    "понедельник": 0,
    "monday": 0,
    "mon": 0,
    "вт": 1,
    "вторник": 1,
    "tuesday": 1,
    "tue": 1,
    "ср": 2,
    "среда": 2,
    "wednesday": 2,
    "wed": 2,
    "чт": 3,
    "четверг": 3,
    "thursday": 3,
    "thu": 3,
    "пт": 4,
    "пятница": 4,
    "friday": 4,
    "fri": 4,
    "сб": 5,
    "суббота": 5,
    "saturday": 5,
    "sat": 5,
    "вс": 6,
    "воскресенье": 6,
    "sunday": 6,
    "sun": 6,
}

PLUS_PATTERN = re.compile(r"^\s*\+(?:[1-5])?\s*$", re.IGNORECASE)
MINUS_PATTERN = re.compile(
    r"^\s*(?:-.*|минус\b.*|не\s+смогу\b.*|не\s+получится\b.*)$",
    re.IGNORECASE,
)

state: Dict[str, Dict[str, Any]] = {}


def owner_ids() -> set[int]:
    raw = os.getenv("BOT_OWNER_IDS", "")
    result: set[int] = set()
    for chunk in raw.split(","):
        chunk = chunk.strip()
        if chunk.isdigit():
            result.add(int(chunk))
    return result


def now_date_str() -> str:
    return datetime.now().strftime("%d/%m/%y")


def safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def build_default_state() -> Dict[str, Any]:
    return {
        "open": False,
        "date": now_date_str(),
        "time": DEFAULT_TIME,
        "field": DEFAULT_FIELD,
        "field_options": [DEFAULT_FIELD],
        "limit": 0,
        "players": [],
        "reserve": [],
        "noshow": [],
        "stats": {
            "events_completed": 0,
            "participants": {},
        },
        "schedules": [],
        "active_schedule_id": None,
        "schedule": {
            "enabled": False,
            "weekday": None,
            "time": DEFAULT_TIME,
            "field": DEFAULT_FIELD,
        },
        "ads_enabled": True,
        "chat_title": "",
        "chat_type": "",
        "active": True,
    }


def create_guest_entry(
    owner_user_id: Optional[int],
    owner_display_name: str,
    owner_username: str,
    sort_key: str,
    joined_at: str,
) -> Dict[str, Any]:
    return {
        "kind": "guest",
        "user_id": None,
        "username": "",
        "display_name": "",
        "owner_user_id": owner_user_id,
        "owner_display_name": owner_display_name,
        "owner_username": owner_username,
        "sort_key": sort_key,
        "joined_at": joined_at,
    }


def make_schedule_entry(name: str, weekday: int, time_value: str, field: str, schedule_id: Optional[str] = None) -> Dict[str, Any]:
    normalized_name = name.strip() or f"{WEEKDAY_RU[weekday]} {time_value}"
    return {
        "id": schedule_id or f"schedule:{weekday}:{time_value}:{normalized_name.lower()}",
        "name": normalized_name,
        "enabled": True,
        "weekday": weekday,
        "time": time_value,
        "field": field.strip() or DEFAULT_FIELD,
    }


def legacy_participant(name: str, position: int) -> Dict[str, Any]:
    return {
        "kind": "owner",
        "user_id": None,
        "username": "",
        "display_name": name,
        "owner_user_id": None,
        "owner_display_name": name,
        "owner_username": "",
        "sort_key": f"legacy:{position}:{name.lower()}",
        "joined_at": "",
    }


def participant_key(participant: Dict[str, Any]) -> str:
    user_id = participant.get("user_id")
    if user_id is not None:
        return f"user:{user_id}"
    custom_key = participant.get("sort_key")
    if custom_key:
        return custom_key
    return f"legacy:{participant.get('display_name', '').lower()}"


def normalize_participants(raw_items: Any) -> List[Dict[str, Any]]:
    participants: List[Dict[str, Any]] = []
    seen: set[str] = set()

    if not isinstance(raw_items, list):
        return participants

    for index, raw in enumerate(raw_items):
        if isinstance(raw, str):
            participant = legacy_participant(raw, index)
            extras: List[Dict[str, Any]] = []
        elif isinstance(raw, dict):
            participant = {
                "kind": raw.get("kind") or "owner",
                "user_id": raw.get("user_id"),
                "username": raw.get("username") or "",
                "display_name": raw.get("display_name")
                or raw.get("name")
                or raw.get("title")
                or "Без имени",
                "owner_user_id": raw.get("owner_user_id"),
                "owner_display_name": raw.get("owner_display_name") or raw.get("display_name") or raw.get("name") or "",
                "owner_username": raw.get("owner_username") or raw.get("username") or "",
                "sort_key": raw.get("sort_key") or raw.get("key") or "",
                "joined_at": raw.get("joined_at") or "",
            }
            extras = []
            if participant["kind"] != "guest":
                participant["kind"] = "owner"
                participant["owner_user_id"] = participant.get("user_id")
                participant["owner_display_name"] = participant.get("display_name", "")
                participant["owner_username"] = participant.get("username", "")
                guest_count = max(0, min(5, safe_int(raw.get("guest_count", 0), 0)))
                for guest_index in range(guest_count):
                    extras.append(
                        create_guest_entry(
                            participant.get("user_id"),
                            participant.get("display_name", ""),
                            participant.get("username", ""),
                            f"{participant.get('sort_key') or participant_key(participant)}:guest:{guest_index}",
                            participant.get("joined_at", ""),
                        )
                    )
            else:
                participant["display_name"] = ""
                participant["username"] = ""
        else:
            continue

        key = participant_key(participant)
        if key in seen:
            continue
        seen.add(key)
        participants.append(participant)
        for extra in extras:
            extra_key = participant_key(extra)
            if extra_key in seen:
                continue
            seen.add(extra_key)
            participants.append(extra)

    return participants


def normalize_chat_state(chat_state: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(chat_state, dict):
        chat_state = {}

    normalized = build_default_state()
    normalized.update(chat_state)

    if "players" not in chat_state:
        normalized["players"] = normalize_participants(chat_state.get("users", []))
    else:
        normalized["players"] = normalize_participants(chat_state.get("players", []))

    normalized["reserve"] = normalize_participants(chat_state.get("reserve", []))
    player_keys = {participant_key(item) for item in normalized["players"]}
    normalized["reserve"] = [
        item for item in normalized["reserve"] if participant_key(item) not in player_keys
    ]
    normalized["noshow"] = [
        item for item in normalized.get("noshow", [])
        if isinstance(item, str) and item.strip()
    ]
    normalized["stats"] = normalized.get("stats", {})
    if not isinstance(normalized["stats"], dict):
        normalized["stats"] = {}
    normalized["stats"]["events_completed"] = int(normalized["stats"].get("events_completed", 0))
    participants_stats = normalized["stats"].get("participants", {})
    if not isinstance(participants_stats, dict):
        participants_stats = {}
    normalized["stats"]["participants"] = participants_stats
    schedule = normalized.get("schedule", {})
    if not isinstance(schedule, dict):
        schedule = {}
    normalized["schedule"] = {
        "enabled": bool(schedule.get("enabled", False)),
        "weekday": schedule.get("weekday"),
        "time": schedule.get("time") or DEFAULT_TIME,
        "field": schedule.get("field") or normalized.get("field") or DEFAULT_FIELD,
    }
    try:
        weekday = normalized["schedule"]["weekday"]
        normalized["schedule"]["weekday"] = int(weekday) if weekday is not None else None
    except (TypeError, ValueError):
        normalized["schedule"]["weekday"] = None

    schedules_raw = normalized.get("schedules", [])
    schedules: List[Dict[str, Any]] = []
    if isinstance(schedules_raw, list):
        for index, item in enumerate(schedules_raw):
            if not isinstance(item, dict):
                continue
            try:
                weekday_value = int(item.get("weekday"))
            except (TypeError, ValueError):
                continue
            schedules.append(
                make_schedule_entry(
                    item.get("name") or f"Расписание {index + 1}",
                    weekday_value,
                    item.get("time") or DEFAULT_TIME,
                    item.get("field") or DEFAULT_FIELD,
                    schedule_id=item.get("id") or f"schedule:{index}",
                )
            )

    if not schedules and normalized["schedule"].get("enabled") and normalized["schedule"].get("weekday") is not None:
        schedules.append(
            make_schedule_entry(
                "Основное расписание",
                int(normalized["schedule"]["weekday"]),
                normalized["schedule"].get("time", DEFAULT_TIME),
                normalized["schedule"].get("field", DEFAULT_FIELD),
                schedule_id="default",
            )
        )

    normalized["schedules"] = schedules[:10]
    active_schedule_id = normalized.get("active_schedule_id")
    if active_schedule_id is None and normalized["schedules"]:
        active_schedule_id = normalized["schedules"][0]["id"]
    if active_schedule_id and not any(item["id"] == active_schedule_id for item in normalized["schedules"]):
        active_schedule_id = normalized["schedules"][0]["id"] if normalized["schedules"] else None
    normalized["active_schedule_id"] = active_schedule_id

    active_schedule = next(
        (item for item in normalized["schedules"] if item["id"] == normalized["active_schedule_id"]),
        None,
    )
    if active_schedule:
        normalized["schedule"] = {
            "enabled": True,
            "weekday": active_schedule["weekday"],
            "time": active_schedule["time"],
            "field": active_schedule["field"],
        }
    else:
        normalized["schedule"]["enabled"] = False
        normalized["schedule"]["weekday"] = None
    normalized["field_options"] = [
        item.strip()
        for item in normalized.get("field_options", [])
        if isinstance(item, str) and item.strip()
    ][:3]

    field = normalized.get("field") or DEFAULT_FIELD
    if not normalized["field_options"]:
        normalized["field_options"] = [field]
    if field not in normalized["field_options"]:
        normalized["field_options"].insert(0, field)
        normalized["field_options"] = normalized["field_options"][:3]
    normalized["field"] = normalized["field_options"][0] if field not in normalized["field_options"] else field

    try:
        normalized["limit"] = max(0, int(normalized.get("limit", 0)))
    except (TypeError, ValueError):
        normalized["limit"] = 0

    if not normalized.get("date"):
        normalized["date"] = now_date_str()
    if not normalized.get("time"):
        normalized["time"] = DEFAULT_TIME

    rebalance_lists(normalized)
    normalized.pop("users", None)
    return normalized


def load_state():
    global state
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r", encoding="utf-8") as file:
            raw_state = json.load(file)
    else:
        raw_state = {}

    state = {str(chat_id): normalize_chat_state(chat_state) for chat_id, chat_state in raw_state.items()}


def save_state():
    with open(STATE_FILE, "w", encoding="utf-8") as file:
        json.dump(state, file, ensure_ascii=False, indent=2)


def ensure_chat(chat_id: int, chat_title: str = "", chat_type: str = ""):
    cid = str(chat_id)
    if cid not in state:
        state[cid] = build_default_state()
    state[cid] = normalize_chat_state(state[cid])
    if chat_title:
        state[cid]["chat_title"] = chat_title
    if chat_type:
        state[cid]["chat_type"] = chat_type


def is_admin_member(member: ChatMember) -> bool:
    return member.status in ("administrator", "creator", "owner")


async def is_admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    if update.effective_chat is None or update.effective_user is None:
        return False
    try:
        member = await context.bot.get_chat_member(update.effective_chat.id, update.effective_user.id)
        return is_admin_member(member)
    except TelegramError:
        return False


def is_owner(update: Update) -> bool:
    user = update.effective_user
    return bool(user and user.id in owner_ids())


def parse_date(value: str) -> str:
    cleaned = value.strip().replace(".", "/").replace("-", "/")
    for fmt in ("%d/%m/%y", "%d/%m/%Y"):
        try:
            return datetime.strptime(cleaned, fmt).strftime("%d/%m/%y")
        except ValueError:
            continue
    raise ValueError("Неверный формат даты. Пример: 25/04/26")


def parse_time(value: str) -> str:
    if re.match(r"^\d{2}:\d{2}-\d{2}:\d{2}$", value):
        return value
    raise ValueError("Неверный формат времени. Пример: 20:00-22:00")


def parse_datetime_input(value: str) -> Tuple[str, str]:
    cleaned = " ".join(value.strip().split())
    match = re.match(r"^(\d{1,2}[./-]\d{1,2}[./-]\d{2,4})\s+(\d{2}:\d{2}-\d{2}:\d{2})$", cleaned)
    if not match:
        raise ValueError("Формат: ДД/ММ/ГГ ЧЧ:ММ-ЧЧ:ММ")
    return parse_date(match.group(1)), parse_time(match.group(2))


def parse_plus_guest_count(value: str) -> int:
    match = re.match(r"^\s*\+(?P<count>[1-5])?\s*$", value)
    if not match:
        raise ValueError("Используй +, +1, +2, +3, +4 или +5")
    count = match.group("count")
    return int(count) if count else 0


def parse_minus_action(value: str) -> Tuple[str, int]:
    cleaned = value.strip().lower().replace("ё", "е")
    exact = re.match(r"^-(?P<count>[1-5])?$", cleaned)
    if exact:
        count = exact.group("count")
        if count:
            return "guests", int(count)
        return "remove", 0

    if cleaned.startswith("минус") or cleaned.startswith("не смогу") or cleaned.startswith("не получится"):
        return "remove", 0

    raise ValueError("Используй -, -1, -2, -3, -4, -5 или 'минус'")


def parse_weekday(value: str) -> int:
    normalized = value.strip().lower()
    if normalized not in WEEKDAY_ALIASES:
        raise ValueError("День недели не распознан. Пример: пятница")
    return WEEKDAY_ALIASES[normalized]


def parse_schedule_input(value: str) -> Dict[str, Any]:
    cleaned = " ".join(value.strip().split())
    parts = cleaned.split(" ", 2)
    if len(parts) < 3:
        raise ValueError("Формат: пятница 20:30-22:30 Горизонт-арена")
    return {
        "enabled": True,
        "weekday": parse_weekday(parts[0]),
        "time": parse_time(parts[1]),
        "field": parts[2].strip(),
    }


def parse_named_schedule_input(value: str) -> Dict[str, Any]:
    cleaned = value.strip()
    if "|" in cleaned:
        name_part, body_part = [part.strip() for part in cleaned.split("|", 1)]
        if not name_part:
            raise ValueError("Нужно указать имя расписания до символа |")
        payload = parse_schedule_input(body_part)
        payload["name"] = name_part
        return payload

    payload = parse_schedule_input(cleaned)
    payload["name"] = f"{WEEKDAY_RU[payload['weekday']]} {payload['time']}"
    return payload


def get_active_schedule(chat_state: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    active_schedule_id = chat_state.get("active_schedule_id")
    for schedule in chat_state.get("schedules", []):
        if schedule.get("id") == active_schedule_id:
            return schedule
    return chat_state.get("schedules", [None])[0]


def next_weekday_date(target_weekday: int, base_date: Optional[datetime] = None) -> str:
    current = base_date or datetime.now()
    delta = (target_weekday - current.weekday()) % 7
    return (current + timedelta(days=delta)).strftime("%d/%m/%y")


def apply_schedule(chat_state: Dict[str, Any], base_date: Optional[datetime] = None) -> bool:
    schedule = get_active_schedule(chat_state)
    if not schedule:
        return False
    weekday = schedule.get("weekday")
    if weekday is None:
        return False
    chat_state["active_schedule_id"] = schedule.get("id")
    chat_state["schedule"] = {
        "enabled": True,
        "weekday": weekday,
        "time": schedule.get("time", DEFAULT_TIME),
        "field": schedule.get("field", DEFAULT_FIELD),
    }
    chat_state["date"] = next_weekday_date(int(weekday), base_date)
    chat_state["time"] = schedule.get("time", DEFAULT_TIME)
    schedule_field = schedule.get("field") or chat_state.get("field") or DEFAULT_FIELD
    chat_state["field"] = schedule_field
    if schedule_field not in chat_state.get("field_options", []):
        chat_state["field_options"] = [schedule_field] + chat_state.get("field_options", [])
        chat_state["field_options"] = chat_state["field_options"][:3]
    return True


def schedule_text(chat_state: Dict[str, Any]) -> str:
    schedule = get_active_schedule(chat_state)
    if not schedule or schedule.get("weekday") is None:
        return "Расписание не настроено."
    weekday_name = WEEKDAY_RU[int(schedule["weekday"])]
    next_date = next_weekday_date(int(schedule["weekday"]))
    return (
        f"{schedule.get('name', 'Расписание')}\n"
        f"Каждую {weekday_name.lower()}\n"
        f"Следующая игра: {next_date}\n"
        f"Поле: {schedule.get('field', DEFAULT_FIELD)}\n"
        f"Время: {schedule.get('time', DEFAULT_TIME)}"
    )


def shift_date(date_str: str, days: int) -> str:
    current = datetime.strptime(date_str, "%d/%m/%y")
    return (current + timedelta(days=days)).strftime("%d/%m/%y")


def format_header(chat_state: Dict[str, Any]) -> str:
    try:
        current_date = datetime.strptime(chat_state["date"], "%d/%m/%y")
        weekday = WEEKDAY_RU[current_date.weekday()]
    except Exception:
        weekday = "?"

    return (
        f"📅 {chat_state['date']} ({weekday})\n"
        f"🏟️ Поле: {chat_state.get('field', DEFAULT_FIELD)}\n"
        f"⏰ Время: {chat_state.get('time', DEFAULT_TIME)}"
    )


def format_participants(title: str, items: List[Dict[str, Any]]) -> str:
    if not items:
        return f"{title}\nПока пусто."
    rows = [f"{index + 1}. {participant_label(participant)}" for index, participant in enumerate(items)]
    return f"{title}\n" + "\n".join(rows)


def format_list(chat_state: Dict[str, Any]) -> str:
    header = format_header(chat_state)
    players = chat_state.get("players", [])
    reserve = chat_state.get("reserve", [])
    limit = chat_state.get("limit", 0)
    status = "Открыто ✅" if chat_state.get("open") else "Закрыто ⛔️"
    main_total = total_people(players)
    reserve_total = total_people(reserve)

    if limit:
        slots_line = f"Основной состав: {main_total}/{limit}"
    else:
        slots_line = f"Основной состав: {main_total}"

    if reserve:
        slots_line += f" | Резерв: {reserve_total}"

    parts = [
        header,
        "",
        f"Статус: {status}",
        slots_line,
        "",
        format_participants("Основа:", players),
    ]

    if reserve:
        parts.extend(["", format_participants("Резерв:", reserve)])
    elif limit and main_total >= limit:
        parts.extend(["", "Резерв:\nПока пусто, но места в основе уже закончились."])

    return "\n".join(parts)


def full_name_from_user(user) -> str:
    parts = [part for part in [user.first_name, user.last_name] if part]
    if parts:
        return " ".join(parts)
    return user.username or str(user.id)


def participant_display_name(user) -> str:
    name = full_name_from_user(user)
    if user.username:
        return f"{name} (@{user.username})"
    return name


def participant_notify_name(participant: Dict[str, Any]) -> str:
    if participant.get("kind") == "guest":
        return f"+1 от {participant.get('owner_display_name', 'Игрок')}"
    username = participant.get("username")
    if username:
        return f"@{username}"
    return participant.get("display_name", "Игрок")


def participant_size(participant: Dict[str, Any]) -> int:
    return 1


def total_people(participants: List[Dict[str, Any]]) -> int:
    return sum(participant_size(participant) for participant in participants)


def participant_label(participant: Dict[str, Any]) -> str:
    if participant.get("kind") == "guest":
        return f"+1 от {participant.get('owner_display_name', 'Игрок')}"
    return participant["display_name"]


def stats_display_name(participant: Dict[str, Any]) -> str:
    username = participant.get("username")
    if username:
        return f"{participant.get('display_name', 'Игрок')} [{username}]"
    return participant.get("display_name", "Игрок")


def participant_from_update(update: Update) -> Dict[str, Any]:
    user = update.effective_user
    if user is None:
        return legacy_participant("Без имени", 0)
    return {
        "kind": "owner",
        "user_id": user.id,
        "username": user.username or "",
        "display_name": participant_display_name(user),
        "owner_user_id": user.id,
        "owner_display_name": participant_display_name(user),
        "owner_username": user.username or "",
        "sort_key": f"user:{user.id}",
        "joined_at": datetime.utcnow().isoformat(timespec="seconds"),
    }


def ensure_stats_entry(chat_state: Dict[str, Any], participant: Dict[str, Any]) -> Dict[str, Any]:
    stats = chat_state.setdefault("stats", {"events_completed": 0, "participants": {}})
    participants = stats.setdefault("participants", {})
    key = participant_key(participant)
    entry = participants.get(key)

    if not isinstance(entry, dict):
        entry = {
            "display_name": participant.get("display_name", "Игрок"),
            "username": participant.get("username", ""),
            "visits": 0,
            "no_shows": 0,
            "reserve_games": 0,
            "promotions": 0,
            "captured_games": 0,
        }
        participants[key] = entry

    if participant.get("display_name"):
        entry["display_name"] = participant["display_name"]
    if participant.get("username"):
        entry["username"] = participant["username"]

    for metric in ("visits", "no_shows", "reserve_games", "promotions", "captured_games"):
        entry[metric] = int(entry.get(metric, 0))

    return entry


def clear_event_marks(chat_state: Dict[str, Any]):
    chat_state["noshow"] = []


def mark_no_show(chat_state: Dict[str, Any], participant: Dict[str, Any]) -> bool:
    key = participant_key(participant)
    if key in chat_state["noshow"]:
        return False
    chat_state["noshow"].append(key)
    return True


def unmark_no_show(chat_state: Dict[str, Any], participant: Dict[str, Any]) -> bool:
    key = participant_key(participant)
    if key not in chat_state["noshow"]:
        return False
    chat_state["noshow"] = [item for item in chat_state["noshow"] if item != key]
    return True


def is_no_show(chat_state: Dict[str, Any], participant: Dict[str, Any]) -> bool:
    return participant_key(participant) in chat_state.get("noshow", [])


def record_promotion_stats(chat_state: Dict[str, Any], promotions: List[Dict[str, Any]]):
    for participant in promotions:
        if participant.get("kind") == "guest":
            continue
        entry = ensure_stats_entry(chat_state, participant)
        entry["promotions"] += 1


def finalize_event_stats(chat_state: Dict[str, Any]) -> Dict[str, int]:
    players = list(chat_state.get("players", []))
    reserve = list(chat_state.get("reserve", []))
    no_show_keys = set(chat_state.get("noshow", []))

    visits = 0
    no_shows = 0
    reserve_only = 0

    for participant in players:
        if participant.get("kind") == "guest":
            continue
        entry = ensure_stats_entry(chat_state, participant)
        entry["captured_games"] += 1
        if participant_key(participant) in no_show_keys:
            entry["no_shows"] += 1
            no_shows += 1
        else:
            entry["visits"] += 1
            visits += 1

    player_keys = {
        participant_key(participant)
        for participant in players
        if participant.get("kind") != "guest"
    }
    for participant in reserve:
        if participant.get("kind") == "guest":
            continue
        if participant_key(participant) in player_keys:
            continue
        entry = ensure_stats_entry(chat_state, participant)
        entry["reserve_games"] += 1
        reserve_only += 1

    chat_state["stats"]["events_completed"] = int(chat_state["stats"].get("events_completed", 0)) + 1
    return {
        "visits": visits,
        "no_shows": no_shows,
        "reserve_only": reserve_only,
    }


def format_stats_line(index: int, entry: Dict[str, Any]) -> str:
    games = int(entry.get("visits", 0)) + int(entry.get("no_shows", 0))
    base = (
        f"{index}. {stats_display_name(entry)} — посещений: {entry.get('visits', 0)}, "
        f"no-show: {entry.get('no_shows', 0)}, резерв: {entry.get('reserve_games', 0)}"
    )
    if games > 0:
        reliability = round((int(entry.get("visits", 0)) / games) * 100)
        base += f", надежность: {reliability}%"
    return base


def build_stats_summary(chat_state: Dict[str, Any], query: str = "") -> str:
    stats = chat_state.get("stats", {})
    participants = list(stats.get("participants", {}).values())
    events_completed = int(stats.get("events_completed", 0))

    if query:
        query_lower = query.lower()
        participants = [
            item
            for item in participants
            if query_lower in item.get("display_name", "").lower()
            or query_lower in item.get("username", "").lower()
        ]
        participants.sort(
            key=lambda item: (
                -int(item.get("visits", 0)),
                -int(item.get("no_shows", 0)),
                item.get("display_name", ""),
            )
        )
        if not participants:
            return f"По запросу '{query}' статистика не найдена."
        if len(participants) == 1:
            entry = participants[0]
            games = int(entry.get("visits", 0)) + int(entry.get("no_shows", 0))
            reliability = round((int(entry.get("visits", 0)) / games) * 100) if games else 0
            return (
                f"{stats_display_name(entry)}\n"
                f"Посещений: {entry.get('visits', 0)}\n"
                f"No-show: {entry.get('no_shows', 0)}\n"
                f"Резервных игр: {entry.get('reserve_games', 0)}\n"
                f"Повышений из резерва: {entry.get('promotions', 0)}\n"
                f"Надежность: {reliability}%"
            )

    participants.sort(
        key=lambda item: (
            -int(item.get("visits", 0)),
            int(item.get("no_shows", 0)),
            item.get("display_name", ""),
        )
    )

    if not participants:
        return "Статистика пока пустая. После первой завершенной игры она появится."

    lines = [f"Завершённых игр: {events_completed}", "", "Топ по посещениям:"]
    for index, entry in enumerate(participants[:10], start=1):
        lines.append(format_stats_line(index, entry))
    return "\n".join(lines)


def find_participant(chat_state: Dict[str, Any], user_id: int) -> Tuple[Optional[str], Optional[int]]:
    for list_name in ("players", "reserve"):
        for index, participant in enumerate(chat_state[list_name]):
            if participant.get("kind") != "guest" and participant.get("user_id") == user_id:
                return list_name, index
    return None, None


def find_players_by_text(participants: List[Dict[str, Any]], query: str) -> List[Dict[str, Any]]:
    query_lower = query.lower()
    return [
        participant
        for participant in participants
        if participant.get("kind") != "guest"
        if query_lower in participant.get("display_name", "").lower()
        or query_lower in participant.get("username", "").lower()
    ]


def owner_identity(participant: Dict[str, Any]) -> str:
    user_id = participant.get("user_id") or participant.get("owner_user_id")
    if user_id is not None:
        return f"user:{user_id}"
    return participant.get("owner_display_name") or participant.get("display_name") or participant_key(participant)


def get_owner_participant(chat_state: Dict[str, Any], user_id: int) -> Optional[Dict[str, Any]]:
    for list_name in ("players", "reserve"):
        for participant in chat_state[list_name]:
            if participant.get("kind") != "guest" and participant.get("user_id") == user_id:
                return participant
    return None


def get_guest_entries(chat_state: Dict[str, Any], user_id: int) -> List[Tuple[str, int, Dict[str, Any]]]:
    matches: List[Tuple[str, int, Dict[str, Any]]] = []
    for list_name in ("players", "reserve"):
        for index, participant in enumerate(chat_state[list_name]):
            if participant.get("kind") == "guest" and participant.get("owner_user_id") == user_id:
                matches.append((list_name, index, participant))
    return matches


def create_guest_entries_for_owner(owner: Dict[str, Any], count: int) -> List[Dict[str, Any]]:
    timestamp = datetime.utcnow().isoformat(timespec="seconds")
    return [
        create_guest_entry(
            owner.get("user_id"),
            owner.get("display_name", "Игрок"),
            owner.get("username", ""),
            f"{owner.get('sort_key', participant_key(owner))}:guest:{timestamp}:{index}",
            timestamp,
        )
        for index in range(count)
    ]


def add_entries_with_limit(chat_state: Dict[str, Any], entries: List[Dict[str, Any]]) -> None:
    limit = chat_state.get("limit", 0)
    for entry in entries:
        if limit and len(chat_state["players"]) >= limit:
            chat_state["reserve"].append(entry)
        else:
            chat_state["players"].append(entry)


def remove_latest_guests(chat_state: Dict[str, Any], user_id: int, count: int) -> int:
    removed = 0
    for list_name in ("reserve", "players"):
        indexes = [
            index
            for index, participant in enumerate(chat_state[list_name])
            if participant.get("kind") == "guest" and participant.get("owner_user_id") == user_id
        ]
        for index in reversed(indexes):
            if removed >= count:
                return removed
            chat_state[list_name].pop(index)
            removed += 1
    return removed


def remove_owner_and_guests(chat_state: Dict[str, Any], user_id: int) -> Tuple[Optional[Dict[str, Any]], int]:
    removed_owner: Optional[Dict[str, Any]] = None
    removed_total = 0
    for list_name in ("players", "reserve"):
        new_items: List[Dict[str, Any]] = []
        for participant in chat_state[list_name]:
            is_owner = participant.get("kind") != "guest" and participant.get("user_id") == user_id
            is_guest = participant.get("kind") == "guest" and participant.get("owner_user_id") == user_id
            if is_owner:
                removed_owner = participant
                removed_total += 1
                continue
            if is_guest:
                removed_total += 1
                continue
            new_items.append(participant)
        chat_state[list_name] = new_items
    return removed_owner, removed_total


def remove_owner_only(chat_state: Dict[str, Any], user_id: int) -> Optional[Dict[str, Any]]:
    for list_name in ("players", "reserve"):
        for index, participant in enumerate(chat_state[list_name]):
            if participant.get("kind") != "guest" and participant.get("user_id") == user_id:
                return chat_state[list_name].pop(index)
    return None


def rebalance_lists(chat_state: Dict[str, Any]) -> List[Dict[str, Any]]:
    players = chat_state["players"]
    reserve = chat_state["reserve"]
    limit = chat_state.get("limit", 0)
    promoted: List[Dict[str, Any]] = []

    if limit:
        overflow: List[Dict[str, Any]] = []
        while players and total_people(players) > limit:
            overflow.insert(0, players.pop())
        if overflow:
            chat_state["reserve"] = overflow + reserve
            reserve = chat_state["reserve"]

    while reserve:
        promoted_player = reserve[0]
        if limit and total_people(players) + participant_size(promoted_player) > limit:
            break
        reserve.pop(0)
        players.append(promoted_player)
        promoted.append(promoted_player)

    return promoted


async def notify_promotions(
    chat_id: int,
    context: ContextTypes.DEFAULT_TYPE,
    promotions: List[Dict[str, Any]],
):
    if not promotions:
        return

    lines = [f"{participant_notify_name(player)}, ты в игре! Приходи!" for player in promotions]
    await context.bot.send_message(chat_id=chat_id, text="\n".join(lines))

    for player in promotions:
        user_id = player.get("user_id")
        if not user_id:
            continue
        try:
            await context.bot.send_message(chat_id=user_id, text="Ты в игре! Приходи!")
        except Forbidden:
            continue
        except TelegramError:
            continue


def build_reply_keyboard(chat_state: Dict[str, Any], admin: bool) -> ReplyKeyboardMarkup:
    if admin:
        rows: List[List[str]] = [[BUTTON_LIST]]
        rows.append([BUTTON_OPEN, BUTTON_CLOSE])
        rows.append([BUTTON_SET_DATETIME, BUTTON_SET_FIELD])
        rows.append([BUTTON_SHOW_SCHEDULE])
        return ReplyKeyboardMarkup(rows, resize_keyboard=True, is_persistent=True, selective=True)
    raise ValueError("Reply keyboard is available only for admins.")


def build_admin_panel(chat_state: Dict[str, Any]) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = [
        [
            InlineKeyboardButton("Сегодня", callback_data="date:today"),
            InlineKeyboardButton("Завтра", callback_data="date:tomorrow"),
        ],
        [
            InlineKeyboardButton("-1 день", callback_data="date:-1"),
            InlineKeyboardButton("+1 день", callback_data="date:+1"),
        ],
        [
            InlineKeyboardButton(DEFAULT_TIME_PRESETS[0], callback_data=f"time:{DEFAULT_TIME_PRESETS[0]}"),
            InlineKeyboardButton(DEFAULT_TIME_PRESETS[1], callback_data=f"time:{DEFAULT_TIME_PRESETS[1]}"),
        ],
        [
            InlineKeyboardButton(DEFAULT_TIME_PRESETS[2], callback_data=f"time:{DEFAULT_TIME_PRESETS[2]}"),
        ],
    ]

    field_buttons = [
        InlineKeyboardButton(field_name, callback_data=f"field:{index}")
        for index, field_name in enumerate(chat_state.get("field_options", [])[:3])
    ]
    if field_buttons:
        rows.append(field_buttons)

    if chat_state.get("open"):
        rows.append([InlineKeyboardButton("Закрыть запись", callback_data="toggle:close")])
    else:
        rows.append([InlineKeyboardButton("Открыть запись", callback_data="toggle:open")])

    rows.append(
        [
            InlineKeyboardButton("Изменить дату/время", callback_data="prompt:datetime"),
            InlineKeyboardButton("Изменить поле", callback_data="prompt:field"),
        ]
    )
    rows.append([InlineKeyboardButton("Расписания", callback_data="schedule:hub")])
    rows.append([InlineKeyboardButton("Обновить список", callback_data="show:list")])
    return InlineKeyboardMarkup(rows)


def set_admin_prompt(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    mode: str,
    prompt_message_id: Optional[int] = None,
):
    context.user_data["admin_prompt"] = {
        "chat_id": chat_id,
        "mode": mode,
        "prompt_message_id": prompt_message_id,
    }


def get_admin_prompt(context: ContextTypes.DEFAULT_TYPE, chat_id: int) -> Optional[Dict[str, Any]]:
    prompt = context.user_data.get("admin_prompt")
    if not isinstance(prompt, dict):
        return None
    if prompt.get("chat_id") != chat_id:
        return None
    return prompt


def clear_admin_prompt(context: ContextTypes.DEFAULT_TYPE):
    context.user_data.pop("admin_prompt", None)


def build_schedule_hub_markup(chat_state: Dict[str, Any]) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    for index, schedule in enumerate(chat_state.get("schedules", [])[:8]):
        active_marker = "✅ " if schedule.get("id") == chat_state.get("active_schedule_id") else ""
        rows.append(
            [InlineKeyboardButton(f"{active_marker}{schedule.get('name', f'Расписание {index + 1}')}", callback_data=f"schedule:view:{index}")]
        )

    rows.append([InlineKeyboardButton("Создать новое", callback_data="schedule:create")])
    return InlineKeyboardMarkup(rows)


def build_schedule_detail_markup(index: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("Применить", callback_data=f"schedule:apply:{index}"),
                InlineKeyboardButton("Изменить", callback_data=f"schedule:edit:{index}"),
            ],
            [
                InlineKeyboardButton("Удалить", callback_data=f"schedule:delete:{index}"),
                InlineKeyboardButton("Назад", callback_data="schedule:hub"),
            ],
        ]
    )


def is_service_button_text(text: str, chat_state: Dict[str, Any]) -> bool:
    button_texts = {
        BUTTON_PLUS,
        BUTTON_MINUS,
        BUTTON_LIST,
        BUTTON_OPEN,
        BUTTON_CLOSE,
        BUTTON_SET_DATETIME,
        BUTTON_SET_FIELD,
        BUTTON_SHOW_SCHEDULE,
        "+1",
        "+2",
        "+3",
        "+4",
        "+5",
    }
    return text in button_texts


async def reply_in_chat(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    text: str,
    *,
    chat_state: Optional[Dict[str, Any]] = None,
    admin: Optional[bool] = None,
    reply_markup=None,
):
    message = update.effective_message
    if message is None:
        return

    markup = reply_markup
    if markup is None and update.effective_chat is not None and chat_state is not None:
        if admin is None:
            admin = await is_admin(update, context)
        if admin:
            markup = build_reply_keyboard(chat_state, admin)

    await message.reply_text(text, reply_markup=markup)


def help_text() -> str:
    return (
        "Я веду список на футбол.\n\n"
        "Игрокам:\n"
        "+ / +1...+5 — записаться\n"
        "- / минус — выйти из списка\n"
        "Список — показать текущий состав\n\n"
        "Админам:\n"
        "/open [ДД/ММ/ГГ] [ЧЧ:ММ-ЧЧ:ММ]\n"
        "/close\n"
        "/list\n"
        "/setdate ДД/ММ/ГГ\n"
        "/settime ЧЧ:ММ-ЧЧ:ММ\n"
        "/setdatetime ДД/ММ/ГГ ЧЧ:ММ-ЧЧ:ММ\n"
        "/setfield НАЗВАНИЕ\n"
        "/setfields Поле 1 | Поле 2 | Поле 3\n"
        "/schedule [Название | пятница 20:30-22:30 Горизонт-арена|off]\n"
        "/setlimit N\n"
        "/remove @username|Имя\n"
        "/noshow @username|Имя\n"
        "/showup @username|Имя\n"
        "/finish\n"
        "/stats [имя]\n"
        "/mystats\n"
        "/reset\n"
        "/ads on|off\n\n"
        "Владельцу бота:\n"
        "/broadcast ТЕКСТ"
    )


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat is None:
        return
    ensure_chat(
        update.effective_chat.id,
        chat_title=update.effective_chat.title or update.effective_chat.full_name or "",
        chat_type=update.effective_chat.type,
    )
    chat_state = state[str(update.effective_chat.id)]
    admin = await is_admin(update, context)
    await reply_in_chat(update, context, help_text(), chat_state=chat_state, admin=admin)


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await start(update, context)


async def show_schedule_hub_message(update: Update, context: ContextTypes.DEFAULT_TYPE, chat_state: Dict[str, Any]):
    message = update.effective_message
    if message is None:
        return
    await message.reply_text(
        "Расписания:",
        reply_markup=build_schedule_hub_markup(chat_state),
    )


async def menu_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat is None:
        return

    ensure_chat(
        update.effective_chat.id,
        chat_title=update.effective_chat.title or update.effective_chat.full_name or "",
        chat_type=update.effective_chat.type,
    )
    chat_state = state[str(update.effective_chat.id)]
    admin = await is_admin(update, context)

    await reply_in_chat(
        update,
        context,
        "Клавиатура подключена.",
        chat_state=chat_state,
        admin=admin,
    )

    if admin and update.effective_message is not None:
        await update.effective_message.reply_text(
            "Панель управления записью:",
            reply_markup=build_admin_panel(chat_state),
        )


async def open_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat is None:
        return
    if not await is_admin(update, context):
        return

    ensure_chat(
        update.effective_chat.id,
        chat_title=update.effective_chat.title or update.effective_chat.full_name or "",
        chat_type=update.effective_chat.type,
    )
    chat_state = state[str(update.effective_chat.id)]
    args = context.args

    try:
        if len(args) >= 1:
            chat_state["date"] = parse_date(args[0])
        elif apply_schedule(chat_state):
            pass
        if len(args) >= 2:
            chat_state["time"] = parse_time(args[1])
        chat_state["open"] = True
        clear_event_marks(chat_state)
        save_state()
        await reply_in_chat(
            update,
            context,
            "Запись открыта ✅\n\n" + format_list(chat_state),
            chat_state=chat_state,
            admin=True,
        )
    except ValueError as exc:
        await reply_in_chat(update, context, f"Ошибка: {exc}")


async def setdatetime_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat is None:
        return
    if not await is_admin(update, context):
        return

    ensure_chat(
        update.effective_chat.id,
        chat_title=update.effective_chat.title or update.effective_chat.full_name or "",
        chat_type=update.effective_chat.type,
    )
    chat_state = state[str(update.effective_chat.id)]
    raw_text = " ".join(context.args).strip()

    if not raw_text:
        await reply_in_chat(
            update,
            context,
            "Укажите дату и время: /setdatetime 25/04/26 20:30-22:30",
            chat_state=chat_state,
            admin=True,
        )
        return

    try:
        chat_state["date"], chat_state["time"] = parse_datetime_input(raw_text)
        save_state()
        await reply_in_chat(
            update,
            context,
            "Дата и время обновлены ✅\n\n" + format_list(chat_state),
            chat_state=chat_state,
            admin=True,
        )
    except ValueError as exc:
        await reply_in_chat(update, context, f"Ошибка: {exc}", chat_state=chat_state, admin=True)


async def setdate_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat is None:
        return
    if not await is_admin(update, context):
        return

    ensure_chat(
        update.effective_chat.id,
        chat_title=update.effective_chat.title or update.effective_chat.full_name or "",
        chat_type=update.effective_chat.type,
    )
    chat_state = state[str(update.effective_chat.id)]

    if not context.args:
        await reply_in_chat(
            update,
            context,
            "Укажите дату: /setdate ДД/ММ/ГГ",
            chat_state=chat_state,
            admin=True,
        )
        return

    try:
        chat_state["date"] = parse_date(context.args[0])
        save_state()
        await reply_in_chat(
            update,
            context,
            "Дата обновлена ✅\n\n" + format_list(chat_state),
            chat_state=chat_state,
            admin=True,
        )
    except ValueError as exc:
        await reply_in_chat(update, context, f"Ошибка: {exc}", chat_state=chat_state, admin=True)


async def settime_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat is None:
        return
    if not await is_admin(update, context):
        return

    ensure_chat(
        update.effective_chat.id,
        chat_title=update.effective_chat.title or update.effective_chat.full_name or "",
        chat_type=update.effective_chat.type,
    )
    chat_state = state[str(update.effective_chat.id)]

    if not context.args:
        await reply_in_chat(
            update,
            context,
            "Укажите время: /settime ЧЧ:ММ-ЧЧ:ММ",
            chat_state=chat_state,
            admin=True,
        )
        return

    try:
        chat_state["time"] = parse_time(context.args[0])
        save_state()
        await reply_in_chat(
            update,
            context,
            "Время обновлено ✅\n\n" + format_list(chat_state),
            chat_state=chat_state,
            admin=True,
        )
    except ValueError as exc:
        await reply_in_chat(update, context, f"Ошибка: {exc}", chat_state=chat_state, admin=True)


async def setfield_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat is None:
        return
    if not await is_admin(update, context):
        return

    ensure_chat(
        update.effective_chat.id,
        chat_title=update.effective_chat.title or update.effective_chat.full_name or "",
        chat_type=update.effective_chat.type,
    )
    chat_state = state[str(update.effective_chat.id)]
    field_name = " ".join(context.args).strip()

    if not field_name:
        await reply_in_chat(
            update,
            context,
            "Укажите название поля: /setfield Горизонт-арена",
            chat_state=chat_state,
            admin=True,
        )
        return

    chat_state["field"] = field_name
    if field_name not in chat_state["field_options"]:
        chat_state["field_options"] = [field_name] + chat_state["field_options"]
        chat_state["field_options"] = chat_state["field_options"][:3]
    save_state()
    await reply_in_chat(
        update,
        context,
        "Поле обновлено ✅\n\n" + format_list(chat_state),
        chat_state=chat_state,
        admin=True,
    )


async def setfields_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat is None:
        return
    if not await is_admin(update, context):
        return

    ensure_chat(
        update.effective_chat.id,
        chat_title=update.effective_chat.title or update.effective_chat.full_name or "",
        chat_type=update.effective_chat.type,
    )
    chat_state = state[str(update.effective_chat.id)]
    raw_text = " ".join(context.args).strip()

    if not raw_text:
        await reply_in_chat(
            update,
            context,
            "Укажите до 3 полей через | : /setfields Поле 1 | Поле 2 | Поле 3",
            chat_state=chat_state,
            admin=True,
        )
        return

    field_options = [item.strip() for item in raw_text.split("|") if item.strip()][:3]
    if not field_options:
        await reply_in_chat(
            update,
            context,
            "Не получилось прочитать названия полей.",
            chat_state=chat_state,
            admin=True,
        )
        return

    chat_state["field_options"] = field_options
    if chat_state["field"] not in field_options:
        chat_state["field"] = field_options[0]
    save_state()
    await reply_in_chat(
        update,
        context,
        "Список полей обновлён ✅\n\n" + format_list(chat_state),
        chat_state=chat_state,
        admin=True,
    )


async def schedule_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat is None:
        return
    if not await is_admin(update, context):
        return

    ensure_chat(
        update.effective_chat.id,
        chat_title=update.effective_chat.title or update.effective_chat.full_name or "",
        chat_type=update.effective_chat.type,
    )
    chat_state = state[str(update.effective_chat.id)]
    raw_text = " ".join(context.args).strip()

    if not raw_text:
        await show_schedule_hub_message(update, context, chat_state)
        return

    if raw_text.lower() == "off":
        chat_state["schedules"] = []
        chat_state["active_schedule_id"] = None
        chat_state["schedule"] = {
            "enabled": False,
            "weekday": None,
            "time": chat_state.get("time", DEFAULT_TIME),
            "field": chat_state.get("field", DEFAULT_FIELD),
        }
        save_state()
        await reply_in_chat(
            update,
            context,
            "Расписание отключено.",
            chat_state=chat_state,
            admin=True,
        )
        return

    try:
        payload = parse_named_schedule_input(raw_text)
        schedule_entry = make_schedule_entry(
            payload["name"],
            payload["weekday"],
            payload["time"],
            payload["field"],
            schedule_id=f"schedule:{len(chat_state.get('schedules', [])) + 1}",
        )
        chat_state["schedules"].append(schedule_entry)
        chat_state["active_schedule_id"] = schedule_entry["id"]
        chat_state["schedule"] = {
            "enabled": True,
            "weekday": schedule_entry["weekday"],
            "time": schedule_entry["time"],
            "field": schedule_entry["field"],
        }
        schedule_field = schedule_entry["field"]
        if schedule_field not in chat_state.get("field_options", []):
            chat_state["field_options"] = [schedule_field] + chat_state.get("field_options", [])
            chat_state["field_options"] = chat_state["field_options"][:3]
        save_state()
        await reply_in_chat(
            update,
            context,
            "Расписание сохранено ✅\n\n" + schedule_text(chat_state),
            chat_state=chat_state,
            admin=True,
        )
    except ValueError as exc:
        await reply_in_chat(update, context, f"Ошибка: {exc}", chat_state=chat_state, admin=True)


async def setlimit_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat is None:
        return
    if not await is_admin(update, context):
        return

    ensure_chat(
        update.effective_chat.id,
        chat_title=update.effective_chat.title or update.effective_chat.full_name or "",
        chat_type=update.effective_chat.type,
    )
    chat_state = state[str(update.effective_chat.id)]

    if not context.args:
        await reply_in_chat(
            update,
            context,
            "Укажите лимит: /setlimit 28 (0 = без лимита)",
            chat_state=chat_state,
            admin=True,
        )
        return

    try:
        chat_state["limit"] = max(0, int(context.args[0]))
    except ValueError:
        await reply_in_chat(
            update,
            context,
            "Неверное значение. Пример: /setlimit 28",
            chat_state=chat_state,
            admin=True,
        )
        return

    promotions = rebalance_lists(chat_state)
    record_promotion_stats(chat_state, promotions)
    save_state()
    await reply_in_chat(
        update,
        context,
        "Лимит обновлён ✅\n\n" + format_list(chat_state),
        chat_state=chat_state,
        admin=True,
    )
    await notify_promotions(update.effective_chat.id, context, promotions)


async def remove_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat is None:
        return
    if not await is_admin(update, context):
        return

    ensure_chat(
        update.effective_chat.id,
        chat_title=update.effective_chat.title or update.effective_chat.full_name or "",
        chat_type=update.effective_chat.type,
    )
    chat_state = state[str(update.effective_chat.id)]
    key = " ".join(context.args).strip().lower()

    if not key:
        await reply_in_chat(
            update,
            context,
            "Кого убрать? /remove @username или /remove Имя",
            chat_state=chat_state,
            admin=True,
        )
        return

    owners_to_remove = {
        participant.get("user_id")
        for participant in chat_state["players"] + chat_state["reserve"]
        if participant.get("kind") != "guest"
        and (
            key in participant.get("display_name", "").lower()
            or key in participant.get("username", "").lower()
        )
        and participant.get("user_id") is not None
    }
    removed_total = 0
    removed_keys: set[str] = set()
    for owner_user_id in owners_to_remove:
        removed_owner, removed_count = remove_owner_and_guests(chat_state, int(owner_user_id))
        removed_total += removed_count
        if removed_owner is not None:
            removed_keys.add(participant_key(removed_owner))

    if removed_keys:
        chat_state["noshow"] = [item for item in chat_state.get("noshow", []) if item not in removed_keys]

    promotions = rebalance_lists(chat_state)
    record_promotion_stats(chat_state, promotions)
    save_state()

    await reply_in_chat(
        update,
        context,
        f"Убрано: {removed_total}\n\n" + format_list(chat_state),
        chat_state=chat_state,
        admin=True,
    )
    await notify_promotions(update.effective_chat.id, context, promotions)


async def noshow_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat is None:
        return
    if not await is_admin(update, context):
        return

    ensure_chat(
        update.effective_chat.id,
        chat_title=update.effective_chat.title or update.effective_chat.full_name or "",
        chat_type=update.effective_chat.type,
    )
    chat_state = state[str(update.effective_chat.id)]
    query = " ".join(context.args).strip()

    if not query:
        await reply_in_chat(
            update,
            context,
            "Кого отметить как no-show? /noshow @username или /noshow Имя",
            chat_state=chat_state,
            admin=True,
        )
        return

    matches = find_players_by_text(chat_state["players"], query)
    if not matches:
        await reply_in_chat(
            update,
            context,
            "Игрок в основном составе не найден.",
            chat_state=chat_state,
            admin=True,
        )
        return

    changed = 0
    for participant in matches:
        if mark_no_show(chat_state, participant):
            changed += 1

    save_state()
    names = ", ".join(participant_notify_name(player) for player in matches)
    await reply_in_chat(
        update,
        context,
        f"Отметил no-show: {names}. Эти игроки не попадут в посещения после /finish.",
        chat_state=chat_state,
        admin=True,
    )


async def showup_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat is None:
        return
    if not await is_admin(update, context):
        return

    ensure_chat(
        update.effective_chat.id,
        chat_title=update.effective_chat.title or update.effective_chat.full_name or "",
        chat_type=update.effective_chat.type,
    )
    chat_state = state[str(update.effective_chat.id)]
    query = " ".join(context.args).strip()

    if not query:
        await reply_in_chat(
            update,
            context,
            "Кого вернуть из no-show? /showup @username или /showup Имя",
            chat_state=chat_state,
            admin=True,
        )
        return

    matches = find_players_by_text(chat_state["players"], query)
    if not matches:
        await reply_in_chat(
            update,
            context,
            "Игрок в основном составе не найден.",
            chat_state=chat_state,
            admin=True,
        )
        return

    changed = 0
    for participant in matches:
        if unmark_no_show(chat_state, participant):
            changed += 1

    save_state()
    if changed == 0:
        text = "У этих игроков не было отметки no-show."
    else:
        names = ", ".join(participant_notify_name(player) for player in matches)
        text = f"Снял no-show: {names}."

    await reply_in_chat(update, context, text, chat_state=chat_state, admin=True)


async def finish_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat is None:
        return
    if not await is_admin(update, context):
        return

    ensure_chat(
        update.effective_chat.id,
        chat_title=update.effective_chat.title or update.effective_chat.full_name or "",
        chat_type=update.effective_chat.type,
    )
    chat_state = state[str(update.effective_chat.id)]

    if not chat_state["players"] and not chat_state["reserve"]:
        await reply_in_chat(
            update,
            context,
            "Завершать нечего: список пуст.",
            chat_state=chat_state,
            admin=True,
        )
        return

    summary = finalize_event_stats(chat_state)
    chat_state["open"] = False
    chat_state["players"] = []
    chat_state["reserve"] = []
    clear_event_marks(chat_state)
    save_state()

    await reply_in_chat(
        update,
        context,
        (
            "Игра завершена и записана в статистику ✅\n\n"
            f"Посещений: {summary['visits']}\n"
            f"No-show: {summary['no_shows']}\n"
            f"Остались в резерве: {summary['reserve_only']}\n\n"
            f"{build_stats_summary(chat_state)}"
        ),
        chat_state=chat_state,
        admin=True,
    )


async def list_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat is None:
        return

    ensure_chat(
        update.effective_chat.id,
        chat_title=update.effective_chat.title or update.effective_chat.full_name or "",
        chat_type=update.effective_chat.type,
    )
    chat_state = state[str(update.effective_chat.id)]
    admin = await is_admin(update, context)
    await reply_in_chat(update, context, format_list(chat_state), chat_state=chat_state, admin=admin)


async def stats_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat is None:
        return

    ensure_chat(
        update.effective_chat.id,
        chat_title=update.effective_chat.title or update.effective_chat.full_name or "",
        chat_type=update.effective_chat.type,
    )
    chat_state = state[str(update.effective_chat.id)]
    admin = await is_admin(update, context)
    query = " ".join(context.args).strip()
    await reply_in_chat(
        update,
        context,
        build_stats_summary(chat_state, query),
        chat_state=chat_state,
        admin=admin,
    )


async def mystats_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat is None or update.effective_user is None:
        return

    ensure_chat(
        update.effective_chat.id,
        chat_title=update.effective_chat.title or update.effective_chat.full_name or "",
        chat_type=update.effective_chat.type,
    )
    chat_state = state[str(update.effective_chat.id)]
    admin = await is_admin(update, context)
    key = f"user:{update.effective_user.id}"
    entry = chat_state.get("stats", {}).get("participants", {}).get(key)

    if not entry:
        text = "Твоей статистики пока нет. Она появится после первой завершенной игры с твоим участием."
    else:
        games = int(entry.get("visits", 0)) + int(entry.get("no_shows", 0))
        reliability = round((int(entry.get("visits", 0)) / games) * 100) if games else 0
        text = (
            f"{stats_display_name(entry)}\n"
            f"Посещений: {entry.get('visits', 0)}\n"
            f"No-show: {entry.get('no_shows', 0)}\n"
            f"Резервных игр: {entry.get('reserve_games', 0)}\n"
            f"Повышений из резерва: {entry.get('promotions', 0)}\n"
            f"Надежность: {reliability}%"
        )

    await reply_in_chat(update, context, text, chat_state=chat_state, admin=admin)


async def reset_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat is None:
        return
    if not await is_admin(update, context):
        return

    ensure_chat(
        update.effective_chat.id,
        chat_title=update.effective_chat.title or update.effective_chat.full_name or "",
        chat_type=update.effective_chat.type,
    )
    chat_state = state[str(update.effective_chat.id)]
    chat_state["players"] = []
    chat_state["reserve"] = []
    clear_event_marks(chat_state)
    save_state()
    await reply_in_chat(
        update,
        context,
        "Список очищен 🧹\n\n" + format_list(chat_state),
        chat_state=chat_state,
        admin=True,
    )


async def close_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat is None:
        return
    if not await is_admin(update, context):
        return

    ensure_chat(
        update.effective_chat.id,
        chat_title=update.effective_chat.title or update.effective_chat.full_name or "",
        chat_type=update.effective_chat.type,
    )
    chat_state = state[str(update.effective_chat.id)]
    chat_state["open"] = False
    clear_event_marks(chat_state)
    save_state()
    await reply_in_chat(
        update,
        context,
        "Запись закрыта ⛔️\n\n" + format_list(chat_state),
        chat_state=chat_state,
        admin=True,
    )


async def ads_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat is None:
        return
    if not await is_admin(update, context):
        return

    ensure_chat(
        update.effective_chat.id,
        chat_title=update.effective_chat.title or update.effective_chat.full_name or "",
        chat_type=update.effective_chat.type,
    )
    chat_state = state[str(update.effective_chat.id)]

    if not context.args or context.args[0].lower() not in {"on", "off"}:
        status = "on" if chat_state.get("ads_enabled", True) else "off"
        await reply_in_chat(
            update,
            context,
            f"Текущий режим рекламы: {status}\nИспользование: /ads on или /ads off",
            chat_state=chat_state,
            admin=True,
        )
        return

    chat_state["ads_enabled"] = context.args[0].lower() == "on"
    save_state()
    current = "включена" if chat_state["ads_enabled"] else "выключена"
    await reply_in_chat(
        update,
        context,
        f"Рассылка для этого чата {current}.",
        chat_state=chat_state,
        admin=True,
    )


async def broadcast_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update):
        return
    if update.effective_message is None:
        return

    text = " ".join(context.args).strip()
    if not text:
        await update.effective_message.reply_text("Использование: /broadcast ТЕКСТ")
        return

    success = 0
    failed = 0

    for chat_id, chat_state in state.items():
        if chat_state.get("chat_type") not in (ChatType.GROUP, ChatType.SUPERGROUP):
            continue
        if not chat_state.get("active", True):
            continue
        if not chat_state.get("ads_enabled", True):
            continue
        try:
            await context.bot.send_message(chat_id=int(chat_id), text=f"📢 {text}")
            success += 1
        except TelegramError:
            failed += 1

    await update.effective_message.reply_text(
        f"Рассылка завершена. Успешно: {success}, ошибок: {failed}."
    )


async def plus_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat is None or update.effective_message is None:
        return
    if update.effective_chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        return

    ensure_chat(
        update.effective_chat.id,
        chat_title=update.effective_chat.title or "",
        chat_type=update.effective_chat.type,
    )
    chat_state = state[str(update.effective_chat.id)]
    admin = await is_admin(update, context)

    if not chat_state.get("open", False):
        await reply_in_chat(
            update,
            context,
            "Запись сейчас закрыта.",
            chat_state=chat_state,
            admin=admin,
        )
        return

    if update.effective_user is None:
        return

    guest_count = parse_plus_guest_count(update.effective_message.text or "")
    existing_list, _ = find_participant(chat_state, update.effective_user.id)
    owner = get_owner_participant(chat_state, update.effective_user.id)
    current_guest_total = len(get_guest_entries(chat_state, update.effective_user.id))

    if existing_list == "players":
        if guest_count > 0:
            assert owner is not None
            delta = guest_count - current_guest_total
            if delta > 0:
                add_entries_with_limit(chat_state, create_guest_entries_for_owner(owner, delta))
            elif delta < 0:
                remove_latest_guests(chat_state, update.effective_user.id, -delta)
            promotions = rebalance_lists(chat_state)
            record_promotion_stats(chat_state, promotions)
            save_state()
            await reply_in_chat(
                update,
                context,
                "Обновил количество твоих плюсов ✅\n\n" + format_list(chat_state),
                chat_state=chat_state,
                admin=admin,
            )
            await notify_promotions(update.effective_chat.id, context, promotions)
            return

        await reply_in_chat(
            update,
            context,
            "Ты уже в основном составе ✅",
            chat_state=chat_state,
            admin=admin,
        )
        return

    if existing_list == "reserve":
        if guest_count > 0:
            assert owner is not None
            delta = guest_count - current_guest_total
            if delta > 0:
                chat_state["reserve"].extend(create_guest_entries_for_owner(owner, delta))
            elif delta < 0:
                remove_latest_guests(chat_state, update.effective_user.id, -delta)
            promotions = rebalance_lists(chat_state)
            record_promotion_stats(chat_state, promotions)
            save_state()
            current_list_name, _ = find_participant(chat_state, update.effective_user.id)
            status_text = (
                "Ты уже в основном составе ✅"
                if current_list_name == "players"
                else "Обновил запись в резерве ⏳"
            )
            await reply_in_chat(
                update,
                context,
                f"{status_text}\n\n" + format_list(chat_state),
                chat_state=chat_state,
                admin=admin,
            )
            await notify_promotions(update.effective_chat.id, context, promotions)
            return

        await reply_in_chat(
            update,
            context,
            "Ты уже в резерве ⏳",
            chat_state=chat_state,
            admin=admin,
        )
        return

    participant = participant_from_update(update)
    limit = chat_state.get("limit", 0)

    if limit and len(chat_state["players"]) >= limit:
        chat_state["reserve"].append(participant)
        response = "Основа уже заполнена. Добавил тебя в резерв ⏳\n\n"
    else:
        chat_state["players"].append(participant)
        response = "Записал! ✅\n\n"

    if guest_count > 0:
        add_entries_with_limit(chat_state, create_guest_entries_for_owner(participant, guest_count))

    save_state()
    await reply_in_chat(
        update,
        context,
        response + format_list(chat_state),
        chat_state=chat_state,
        admin=admin,
    )


async def minus_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat is None or update.effective_message is None or update.effective_user is None:
        return
    if update.effective_chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        return

    ensure_chat(
        update.effective_chat.id,
        chat_title=update.effective_chat.title or "",
        chat_type=update.effective_chat.type,
    )
    chat_state = state[str(update.effective_chat.id)]
    admin = await is_admin(update, context)
    action, count = parse_minus_action(update.effective_message.text or "-")
    guest_entries = get_guest_entries(chat_state, update.effective_user.id)

    list_name, index = find_participant(chat_state, update.effective_user.id)
    if action == "guests" and not guest_entries:
        await reply_in_chat(
            update,
            context,
            "У тебя нет такого количества гостей, чтобы уменьшить запись.",
            chat_state=chat_state,
            admin=admin,
        )
        return

    if action != "guests" and (list_name is None or index is None):
        await reply_in_chat(
            update,
            context,
            "Тебя нет в списке.",
            chat_state=chat_state,
            admin=admin,
        )
        return

    participant = chat_state[list_name][index]

    if action == "guests":
        removed = remove_latest_guests(chat_state, update.effective_user.id, count)

        promotions = rebalance_lists(chat_state)
        record_promotion_stats(chat_state, promotions)
        save_state()
        await reply_in_chat(
            update,
            context,
            "Обновил количество твоих плюсов ✅\n\n" + format_list(chat_state),
            chat_state=chat_state,
            admin=admin,
        )
        await notify_promotions(update.effective_chat.id, context, promotions)
        return

    removed_participant = remove_owner_only(chat_state, update.effective_user.id)
    if list_name == "players":
        chat_state["noshow"] = [
            item for item in chat_state.get("noshow", []) if item != participant_key(participant)
        ]
    promotions = rebalance_lists(chat_state)
    record_promotion_stats(chat_state, promotions)
    save_state()

    label = "Убрал тебя из основного состава." if list_name == "players" else "Убрал тебя из резерва."
    await reply_in_chat(
        update,
        context,
        label + "\n\n" + format_list(chat_state),
        chat_state=chat_state,
        admin=admin,
    )
    await notify_promotions(update.effective_chat.id, context, promotions)


async def handle_pending_admin_input(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    chat_state: Dict[str, Any],
) -> bool:
    if update.effective_chat is None or update.effective_message is None:
        return False

    prompt = get_admin_prompt(context, update.effective_chat.id)
    if not prompt:
        return False

    text = (update.effective_message.text or "").strip()
    mode = prompt.get("mode")
    prompt_message_id = prompt.get("prompt_message_id")

    replied_message = update.effective_message.reply_to_message
    if prompt_message_id and (replied_message is None or replied_message.message_id != prompt_message_id):
        return False
    if is_service_button_text(text, chat_state):
        return False

    try:
        if mode == "datetime":
            chat_state["date"], chat_state["time"] = parse_datetime_input(text)
            reply_text = "Дата и время обновлены ✅\n\n" + format_list(chat_state)
        elif mode == "field":
            if not text:
                raise ValueError("Название поля не может быть пустым.")
            chat_state["field"] = text
            if text not in chat_state.get("field_options", []):
                chat_state["field_options"] = [text] + chat_state.get("field_options", [])
                chat_state["field_options"] = chat_state["field_options"][:3]
            reply_text = "Поле обновлено ✅\n\n" + format_list(chat_state)
        elif mode == "schedule_new":
            payload = parse_named_schedule_input(text)
            schedule_entry = make_schedule_entry(
                payload["name"],
                payload["weekday"],
                payload["time"],
                payload["field"],
                schedule_id=f"schedule:{len(chat_state.get('schedules', [])) + 1}",
            )
            chat_state["schedules"].append(schedule_entry)
            chat_state["active_schedule_id"] = schedule_entry["id"]
            chat_state["schedule"] = {
                "enabled": True,
                "weekday": schedule_entry["weekday"],
                "time": schedule_entry["time"],
                "field": schedule_entry["field"],
            }
            schedule_field = schedule_entry["field"]
            if schedule_field not in chat_state.get("field_options", []):
                chat_state["field_options"] = [schedule_field] + chat_state.get("field_options", [])
                chat_state["field_options"] = chat_state["field_options"][:3]
            reply_text = "Расписание сохранено ✅\n\n" + schedule_text(chat_state)
        elif isinstance(mode, str) and mode.startswith("schedule_edit:"):
            schedule_index = int(mode.split(":", 1)[1])
            payload = parse_named_schedule_input(text)
            if not (0 <= schedule_index < len(chat_state.get("schedules", []))):
                raise ValueError("Это расписание уже не найдено.")
            existing_schedule = chat_state["schedules"][schedule_index]
            updated_schedule = make_schedule_entry(
                payload["name"],
                payload["weekday"],
                payload["time"],
                payload["field"],
                schedule_id=existing_schedule["id"],
            )
            chat_state["schedules"][schedule_index] = updated_schedule
            chat_state["active_schedule_id"] = updated_schedule["id"]
            chat_state["schedule"] = {
                "enabled": True,
                "weekday": updated_schedule["weekday"],
                "time": updated_schedule["time"],
                "field": updated_schedule["field"],
            }
            schedule_field = updated_schedule["field"]
            if schedule_field not in chat_state.get("field_options", []):
                chat_state["field_options"] = [schedule_field] + chat_state.get("field_options", [])
                chat_state["field_options"] = chat_state["field_options"][:3]
            reply_text = "Расписание обновлено ✅\n\n" + schedule_text(chat_state)
        else:
            clear_admin_prompt(context)
            return False
    except ValueError as exc:
        await reply_in_chat(update, context, f"Ошибка: {exc}", chat_state=chat_state, admin=True)
        return True

    clear_admin_prompt(context)
    save_state()
    await reply_in_chat(update, context, reply_text, chat_state=chat_state, admin=True)
    return True


async def button_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat is None or update.effective_message is None:
        return
    if update.effective_chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        return

    text = (update.effective_message.text or "").strip()
    ensure_chat(
        update.effective_chat.id,
        chat_title=update.effective_chat.title or "",
        chat_type=update.effective_chat.type,
    )
    chat_state = state[str(update.effective_chat.id)]
    admin = await is_admin(update, context)

    if admin and await handle_pending_admin_input(update, context, chat_state):
        return

    if admin and is_service_button_text(text, chat_state):
        clear_admin_prompt(context)

    if text == BUTTON_LIST:
        await list_cmd(update, context)
        return

    if not admin:
        return

    if text == BUTTON_OPEN:
        apply_schedule(chat_state)
        chat_state["open"] = True
        clear_event_marks(chat_state)
        save_state()
        await reply_in_chat(
            update,
            context,
            "Запись открыта ✅\n\n" + format_list(chat_state),
            chat_state=chat_state,
            admin=True,
        )
        return

    if text == BUTTON_CLOSE:
        chat_state["open"] = False
        clear_event_marks(chat_state)
        save_state()
        await reply_in_chat(
            update,
            context,
            "Запись закрыта ⛔️\n\n" + format_list(chat_state),
            chat_state=chat_state,
            admin=True,
        )
        return

    if text == BUTTON_SET_DATETIME:
        prompt_message = await update.effective_message.reply_text(
            "Отправь дату и время ответом на это сообщение.\nПример: 25/04/26 20:30-22:30"
        )
        set_admin_prompt(
            context,
            update.effective_chat.id,
            "datetime",
            prompt_message_id=prompt_message.message_id,
        )
        return

    if text == BUTTON_SET_FIELD:
        prompt_message = await update.effective_message.reply_text(
            "Напиши новое название поля ответом на это сообщение."
        )
        set_admin_prompt(
            context,
            update.effective_chat.id,
            "field",
            prompt_message_id=prompt_message.message_id,
        )
        return

    if text == BUTTON_SHOW_SCHEDULE:
        await show_schedule_hub_message(update, context, chat_state)
        return


async def admin_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if query is None or update.effective_chat is None:
        return

    await query.answer()
    if not await is_admin(update, context):
        await query.answer("Только для админов", show_alert=True)
        return

    ensure_chat(
        update.effective_chat.id,
        chat_title=update.effective_chat.title or update.effective_chat.full_name or "",
        chat_type=update.effective_chat.type,
    )
    chat_state = state[str(update.effective_chat.id)]
    data = query.data or ""
    promotions: List[Dict[str, Any]] = []

    if not data.startswith("prompt:"):
        clear_admin_prompt(context)

    if data == "date:today":
        chat_state["date"] = now_date_str()
    elif data == "date:tomorrow":
        chat_state["date"] = (datetime.now() + timedelta(days=1)).strftime("%d/%m/%y")
    elif data.startswith("date:"):
        chat_state["date"] = shift_date(chat_state["date"], int(data.split(":", 1)[1]))
    elif data.startswith("time:"):
        chat_state["time"] = parse_time(data.split(":", 1)[1])
    elif data.startswith("field:"):
        field_index = int(data.split(":", 1)[1])
        options = chat_state.get("field_options", [])
        if 0 <= field_index < len(options):
            chat_state["field"] = options[field_index]
    elif data == "toggle:open":
        apply_schedule(chat_state)
        chat_state["open"] = True
    elif data == "toggle:close":
        chat_state["open"] = False
    elif data == "prompt:datetime":
        prompt_message = await query.message.reply_text(
            "Отправь дату и время ответом на это сообщение: 25/04/26 20:30-22:30"
        )
        set_admin_prompt(
            context,
            update.effective_chat.id,
            "datetime",
            prompt_message_id=prompt_message.message_id,
        )
    elif data == "prompt:field":
        prompt_message = await query.message.reply_text(
            "Напиши новое название поля ответом на это сообщение."
        )
        set_admin_prompt(
            context,
            update.effective_chat.id,
            "field",
            prompt_message_id=prompt_message.message_id,
        )
    elif data == "show:list":
        pass
    elif data == "schedule:hub":
        await query.edit_message_text(
            "Расписания:",
            reply_markup=build_schedule_hub_markup(chat_state),
        )
        return
    elif data == "schedule:create":
        prompt_message = await query.message.reply_text(
            "Отправь новое расписание ответом на это сообщение.\nФормат:\nНазвание | пятница 20:30-22:30 Горизонт-арена"
        )
        set_admin_prompt(
            context,
            update.effective_chat.id,
            "schedule_new",
            prompt_message_id=prompt_message.message_id,
        )
        return
    elif data.startswith("schedule:view:"):
        schedule_index = int(data.rsplit(":", 1)[1])
        if not (0 <= schedule_index < len(chat_state.get("schedules", []))):
            await query.answer("Расписание не найдено", show_alert=True)
            return
        schedule = chat_state["schedules"][schedule_index]
        detail_text = (
            f"{schedule.get('name', 'Расписание')}\n"
            f"Каждую {WEEKDAY_RU[int(schedule['weekday'])].lower()}\n"
            f"Поле: {schedule.get('field', DEFAULT_FIELD)}\n"
            f"Время: {schedule.get('time', DEFAULT_TIME)}"
        )
        await query.edit_message_text(detail_text, reply_markup=build_schedule_detail_markup(schedule_index))
        return
    elif data.startswith("schedule:apply:"):
        schedule_index = int(data.rsplit(":", 1)[1])
        if not (0 <= schedule_index < len(chat_state.get("schedules", []))):
            await query.answer("Расписание не найдено", show_alert=True)
            return
        selected = chat_state["schedules"][schedule_index]
        chat_state["active_schedule_id"] = selected["id"]
        apply_schedule(chat_state)
        save_state()
        await query.edit_message_text(
            "Расписание применено ✅\n\n" + schedule_text(chat_state),
            reply_markup=build_schedule_detail_markup(schedule_index),
        )
        return
    elif data.startswith("schedule:edit:"):
        schedule_index = int(data.rsplit(":", 1)[1])
        if not (0 <= schedule_index < len(chat_state.get("schedules", []))):
            await query.answer("Расписание не найдено", show_alert=True)
            return
        prompt_message = await query.message.reply_text(
            "Ответь на это сообщение обновлённым расписанием.\nФормат:\nНазвание | пятница 20:30-22:30 Горизонт-арена"
        )
        set_admin_prompt(
            context,
            update.effective_chat.id,
            f"schedule_edit:{schedule_index}",
            prompt_message_id=prompt_message.message_id,
        )
        return
    elif data.startswith("schedule:delete:"):
        schedule_index = int(data.rsplit(":", 1)[1])
        if not (0 <= schedule_index < len(chat_state.get("schedules", []))):
            await query.answer("Расписание не найдено", show_alert=True)
            return
        removed = chat_state["schedules"].pop(schedule_index)
        if chat_state.get("active_schedule_id") == removed.get("id"):
            chat_state["active_schedule_id"] = chat_state["schedules"][0]["id"] if chat_state["schedules"] else None
        apply_schedule(chat_state) if chat_state.get("schedules") else None
        if not chat_state.get("schedules"):
            chat_state["schedule"] = {
                "enabled": False,
                "weekday": None,
                "time": chat_state.get("time", DEFAULT_TIME),
                "field": chat_state.get("field", DEFAULT_FIELD),
            }
        save_state()
        await query.edit_message_text("Расписания:", reply_markup=build_schedule_hub_markup(chat_state))
        return

    promotions.extend(rebalance_lists(chat_state))
    record_promotion_stats(chat_state, promotions)
    if data in {"toggle:open", "toggle:close"}:
        clear_event_marks(chat_state)
    save_state()

    try:
        await query.edit_message_text(
            "Панель управления записью\n\n" + format_list(chat_state),
            reply_markup=build_admin_panel(chat_state),
        )
    except BadRequest as exc:
        if "Message is not modified" not in str(exc):
            raise

    await notify_promotions(update.effective_chat.id, context, promotions)


async def handle_member_update(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat is None or update.my_chat_member is None:
        return

    chat = update.effective_chat
    ensure_chat(chat.id, chat_title=chat.title or chat.full_name or "", chat_type=chat.type)
    chat_state = state[str(chat.id)]
    new_status = update.my_chat_member.new_chat_member.status
    chat_state["active"] = new_status not in ("left", "kicked")
    save_state()


def main():
    load_state()
    token = os.getenv("BOT_TOKEN")
    if not token:
        raise RuntimeError("Установите переменную окружения BOT_TOKEN")

    app: Application = ApplicationBuilder().token(token).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("menu", menu_cmd))
    app.add_handler(CommandHandler("open", open_cmd))
    app.add_handler(CommandHandler("setdate", setdate_cmd))
    app.add_handler(CommandHandler("settime", settime_cmd))
    app.add_handler(CommandHandler("setdatetime", setdatetime_cmd))
    app.add_handler(CommandHandler("setfield", setfield_cmd))
    app.add_handler(CommandHandler("setfields", setfields_cmd))
    app.add_handler(CommandHandler("schedule", schedule_cmd))
    app.add_handler(CommandHandler("setlimit", setlimit_cmd))
    app.add_handler(CommandHandler("remove", remove_cmd))
    app.add_handler(CommandHandler("noshow", noshow_cmd))
    app.add_handler(CommandHandler("showup", showup_cmd))
    app.add_handler(CommandHandler("finish", finish_cmd))
    app.add_handler(CommandHandler("list", list_cmd))
    app.add_handler(CommandHandler("stats", stats_cmd))
    app.add_handler(CommandHandler("mystats", mystats_cmd))
    app.add_handler(CommandHandler("reset", reset_cmd))
    app.add_handler(CommandHandler("close", close_cmd))
    app.add_handler(CommandHandler("ads", ads_cmd))
    app.add_handler(CommandHandler("broadcast", broadcast_cmd))

    app.add_handler(CallbackQueryHandler(admin_callback, pattern=r"^(date|time|field|toggle|show|prompt|schedule):"))
    app.add_handler(MessageHandler(filters.TEXT & filters.Regex(PLUS_PATTERN), plus_message))
    app.add_handler(MessageHandler(filters.TEXT & filters.Regex(MINUS_PATTERN), minus_message))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, button_message))
    app.add_handler(ChatMemberHandler(handle_member_update, ChatMemberHandler.MY_CHAT_MEMBER))

    print("Bot is running. Press Ctrl+C to stop.")
    app.run_polling()


if __name__ == "__main__":
    try:
        import dotenv

        dotenv.load_dotenv()
    except Exception:
        pass

    main()
