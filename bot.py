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
BUTTON_MENU = "Меню"
BUTTON_OPEN = "Открыть запись"
BUTTON_CLOSE = "Закрыть запись"
BUTTON_TODAY = "Сегодня"
BUTTON_TOMORROW = "Завтра"
BUTTON_SET_DATETIME = "Изменить дату/время"
BUTTON_SET_FIELD = "Изменить поле"
BUTTON_SET_SCHEDULE = "Настроить расписание"
BUTTON_SHOW_SCHEDULE = "Расписание"

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


def legacy_participant(name: str, position: int) -> Dict[str, Any]:
    return {
        "user_id": None,
        "username": "",
        "display_name": name,
        "guest_count": 0,
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
        elif isinstance(raw, dict):
            participant = {
                "user_id": raw.get("user_id"),
                "username": raw.get("username") or "",
                "display_name": raw.get("display_name")
                or raw.get("name")
                or raw.get("title")
                or "Без имени",
                "guest_count": max(0, min(5, safe_int(raw.get("guest_count", 0), 0))),
                "sort_key": raw.get("sort_key") or raw.get("key") or "",
                "joined_at": raw.get("joined_at") or "",
            }
        else:
            continue

        key = participant_key(participant)
        if key in seen:
            continue
        seen.add(key)
        participants.append(participant)

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


def next_weekday_date(target_weekday: int, base_date: Optional[datetime] = None) -> str:
    current = base_date or datetime.now()
    delta = (target_weekday - current.weekday()) % 7
    return (current + timedelta(days=delta)).strftime("%d/%m/%y")


def apply_schedule(chat_state: Dict[str, Any], base_date: Optional[datetime] = None) -> bool:
    schedule = chat_state.get("schedule", {})
    weekday = schedule.get("weekday")
    if not schedule.get("enabled") or weekday is None:
        return False
    chat_state["date"] = next_weekday_date(int(weekday), base_date)
    chat_state["time"] = schedule.get("time", DEFAULT_TIME)
    schedule_field = schedule.get("field") or chat_state.get("field") or DEFAULT_FIELD
    chat_state["field"] = schedule_field
    if schedule_field not in chat_state.get("field_options", []):
        chat_state["field_options"] = [schedule_field] + chat_state.get("field_options", [])
        chat_state["field_options"] = chat_state["field_options"][:3]
    return True


def schedule_text(chat_state: Dict[str, Any]) -> str:
    schedule = chat_state.get("schedule", {})
    if not schedule.get("enabled") or schedule.get("weekday") is None:
        return "Расписание не настроено."
    weekday_name = WEEKDAY_RU[int(schedule["weekday"])]
    next_date = next_weekday_date(int(schedule["weekday"]))
    return (
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
    username = participant.get("username")
    if username:
        return f"@{username}"
    return participant.get("display_name", "Игрок")


def participant_size(participant: Dict[str, Any]) -> int:
    return 1 + max(0, int(participant.get("guest_count", 0) or 0))


def total_people(participants: List[Dict[str, Any]]) -> int:
    return sum(participant_size(participant) for participant in participants)


def participant_label(participant: Dict[str, Any]) -> str:
    guests = max(0, int(participant.get("guest_count", 0) or 0))
    if guests:
        return f"{participant['display_name']} (+{guests})"
    return participant["display_name"]


def stats_display_name(participant: Dict[str, Any]) -> str:
    username = participant.get("username")
    if username:
        return f"{participant.get('display_name', 'Игрок')} [{username}]"
    return participant.get("display_name", "Игрок")


def participant_from_update(update: Update, guest_count: int = 0) -> Dict[str, Any]:
    user = update.effective_user
    if user is None:
        return legacy_participant("Без имени", 0)
    return {
        "user_id": user.id,
        "username": user.username or "",
        "display_name": participant_display_name(user),
        "guest_count": max(0, min(5, int(guest_count))),
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
        entry = ensure_stats_entry(chat_state, participant)
        entry["captured_games"] += 1
        if participant_key(participant) in no_show_keys:
            entry["no_shows"] += 1
            no_shows += 1
        else:
            entry["visits"] += 1
            visits += 1

    player_keys = {participant_key(participant) for participant in players}
    for participant in reserve:
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
            if participant.get("user_id") == user_id:
                return list_name, index
    return None, None


def find_players_by_text(participants: List[Dict[str, Any]], query: str) -> List[Dict[str, Any]]:
    query_lower = query.lower()
    return [
        participant
        for participant in participants
        if query_lower in participant.get("display_name", "").lower()
        or query_lower in participant.get("username", "").lower()
    ]


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
    rows: List[List[str]] = [
        [BUTTON_PLUS, "+1", "+2"],
        ["+3", "+4", "+5"],
        [BUTTON_MINUS, BUTTON_LIST],
        [BUTTON_MENU],
    ]

    if admin:
        rows.append([BUTTON_OPEN, BUTTON_CLOSE])
        rows.append([BUTTON_TODAY, BUTTON_TOMORROW])
        rows.append([BUTTON_SET_DATETIME, BUTTON_SET_FIELD])
        rows.append([BUTTON_SET_SCHEDULE, BUTTON_SHOW_SCHEDULE])
        field_buttons = chat_state.get("field_options", [])[:3]
        if field_buttons:
            rows.append(field_buttons)

    return ReplyKeyboardMarkup(rows, resize_keyboard=True, is_persistent=True)


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
    rows.append(
        [
            InlineKeyboardButton("Настроить расписание", callback_data="prompt:schedule"),
        ]
    )
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


def is_service_button_text(text: str, chat_state: Dict[str, Any]) -> bool:
    button_texts = {
        BUTTON_PLUS,
        BUTTON_MINUS,
        BUTTON_LIST,
        BUTTON_MENU,
        BUTTON_OPEN,
        BUTTON_CLOSE,
        BUTTON_TODAY,
        BUTTON_TOMORROW,
        BUTTON_SET_DATETIME,
        BUTTON_SET_FIELD,
        BUTTON_SET_SCHEDULE,
        BUTTON_SHOW_SCHEDULE,
        "+1",
        "+2",
        "+3",
        "+4",
        "+5",
    }
    button_texts.update(chat_state.get("field_options", []))
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
        "/menu\n"
        "/setdate ДД/ММ/ГГ\n"
        "/settime ЧЧ:ММ-ЧЧ:ММ\n"
        "/setdatetime ДД/ММ/ГГ ЧЧ:ММ-ЧЧ:ММ\n"
        "/setfield НАЗВАНИЕ\n"
        "/setfields Поле 1 | Поле 2 | Поле 3\n"
        "/schedule [пятница 20:30-22:30 Горизонт-арена|off]\n"
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
        "Клавиатура подключена. Можно записываться кнопками, а админам доступна панель ниже.",
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
        await reply_in_chat(
            update,
            context,
            schedule_text(chat_state),
            chat_state=chat_state,
            admin=True,
        )
        return

    if raw_text.lower() == "off":
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
        chat_state["schedule"] = parse_schedule_input(raw_text)
        schedule_field = chat_state["schedule"]["field"]
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

    removed_from_main = [
        participant
        for participant in chat_state["players"]
        if key in participant.get("display_name", "").lower()
        or key in participant.get("username", "").lower()
    ]
    removed_from_reserve = [
        participant
        for participant in chat_state["reserve"]
        if key in participant.get("display_name", "").lower()
        or key in participant.get("username", "").lower()
    ]

    chat_state["players"] = [
        participant
        for participant in chat_state["players"]
        if participant not in removed_from_main
    ]
    chat_state["reserve"] = [
        participant
        for participant in chat_state["reserve"]
        if participant not in removed_from_reserve
    ]
    removed_keys = {participant_key(item) for item in removed_from_main}
    if removed_keys:
        chat_state["noshow"] = [
            item for item in chat_state.get("noshow", []) if item not in removed_keys
        ]

    promotions = rebalance_lists(chat_state)
    record_promotion_stats(chat_state, promotions)
    save_state()

    removed_total = len(removed_from_main) + len(removed_from_reserve)
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

    existing_list, _ = find_participant(chat_state, update.effective_user.id)
    guest_count = parse_plus_guest_count(update.effective_message.text or "")

    if existing_list == "players":
        if guest_count > 0:
            existing = next(
                participant
                for participant in chat_state["players"]
                if participant.get("user_id") == update.effective_user.id
            )
            existing["guest_count"] = max(0, min(5, int(existing.get("guest_count", 0) or 0) + guest_count))
            promotions = rebalance_lists(chat_state)
            record_promotion_stats(chat_state, promotions)
            save_state()
            await reply_in_chat(
                update,
                context,
                f"Обновил запись: теперь {participant_label(existing)} ✅\n\n" + format_list(chat_state),
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
            existing = next(
                participant
                for participant in chat_state["reserve"]
                if participant.get("user_id") == update.effective_user.id
            )
            existing["guest_count"] = max(0, min(5, int(existing.get("guest_count", 0) or 0) + guest_count))
            promotions = rebalance_lists(chat_state)
            record_promotion_stats(chat_state, promotions)
            save_state()
            current_list_name, _ = find_participant(chat_state, update.effective_user.id)
            status_text = "Ты уже в основном составе ✅" if current_list_name == "players" else "Обновил запись в резерве ⏳"
            await reply_in_chat(
                update,
                context,
                f"{status_text}: теперь {participant_label(existing)}\n\n" + format_list(chat_state),
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

    participant = participant_from_update(update, guest_count=guest_count)
    group_size = participant_size(participant)
    limit = chat_state.get("limit", 0)

    if limit and total_people(chat_state["players"]) + group_size > limit:
        chat_state["reserve"].append(participant)
        response = "Основа уже заполнена или группа не помещается. Добавил тебя в резерв ⏳\n\n"
    else:
        chat_state["players"].append(participant)
        response = "Записал! ✅\n\n"

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

    list_name, index = find_participant(chat_state, update.effective_user.id)
    if list_name is None or index is None:
        await reply_in_chat(
            update,
            context,
            "Тебя нет в списке.",
            chat_state=chat_state,
            admin=admin,
        )
        return

    removed_participant = chat_state[list_name].pop(index)
    if list_name == "players":
        chat_state["noshow"] = [
            item for item in chat_state.get("noshow", []) if item != participant_key(removed_participant)
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
        elif mode == "schedule":
            if text.lower() == "off":
                chat_state["schedule"] = {
                    "enabled": False,
                    "weekday": None,
                    "time": chat_state.get("time", DEFAULT_TIME),
                    "field": chat_state.get("field", DEFAULT_FIELD),
                }
                reply_text = "Расписание отключено."
            else:
                chat_state["schedule"] = parse_schedule_input(text)
                schedule_field = chat_state["schedule"]["field"]
                if schedule_field not in chat_state.get("field_options", []):
                    chat_state["field_options"] = [schedule_field] + chat_state.get("field_options", [])
                    chat_state["field_options"] = chat_state["field_options"][:3]
                reply_text = "Расписание сохранено ✅\n\n" + schedule_text(chat_state)
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

    if text == BUTTON_LIST:
        await list_cmd(update, context)
        return
    if text == BUTTON_MENU:
        await menu_cmd(update, context)
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

    if text == BUTTON_TODAY:
        chat_state["date"] = now_date_str()
        save_state()
        await reply_in_chat(
            update,
            context,
            "Дата переключена на сегодня ✅\n\n" + format_list(chat_state),
            chat_state=chat_state,
            admin=True,
        )
        return

    if text == BUTTON_TOMORROW:
        chat_state["date"] = (datetime.now() + timedelta(days=1)).strftime("%d/%m/%y")
        save_state()
        await reply_in_chat(
            update,
            context,
            "Дата переключена на завтра ✅\n\n" + format_list(chat_state),
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

    if text == BUTTON_SET_SCHEDULE:
        prompt_message = await update.effective_message.reply_text(
            "Напиши расписание ответом на это сообщение:\nпятница 20:30-22:30 Горизонт-арена\n\nЧтобы выключить расписание, отправь: off"
        )
        set_admin_prompt(
            context,
            update.effective_chat.id,
            "schedule",
            prompt_message_id=prompt_message.message_id,
        )
        return

    if text == BUTTON_SHOW_SCHEDULE:
        await reply_in_chat(
            update,
            context,
            schedule_text(chat_state),
            chat_state=chat_state,
            admin=True,
        )
        return

    if text in chat_state.get("field_options", []):
        chat_state["field"] = text
        save_state()
        await reply_in_chat(
            update,
            context,
            "Поле обновлено ✅\n\n" + format_list(chat_state),
            chat_state=chat_state,
            admin=True,
        )


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
    elif data == "prompt:schedule":
        prompt_message = await query.message.reply_text(
            "Напиши расписание ответом на это сообщение:\nпятница 20:30-22:30 Горизонт-арена\n\nЧтобы выключить расписание, отправь: off"
        )
        set_admin_prompt(
            context,
            update.effective_chat.id,
            "schedule",
            prompt_message_id=prompt_message.message_id,
        )
    elif data == "show:list":
        pass

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

    app.add_handler(CallbackQueryHandler(admin_callback, pattern=r"^(date|time|field|toggle|show|prompt):"))
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
