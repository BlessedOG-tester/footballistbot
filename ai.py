"""Optional OpenAI layer: turns free-form group messages into structured intents.

The bot works without it. Set OPENAI_API_KEY to enable. The model never acts on
its own — it only returns an intent, and bot.py executes it with the same code
the slash commands use.
"""

import json
import logging
import os
import urllib.error
import urllib.request
from typing import Any, Dict, List, Optional

logger = logging.getLogger("footballistbot.ai")

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini").strip() or "gpt-4o-mini"
OPENAI_URL = "https://api.openai.com/v1/chat/completions"
MAX_MESSAGE_CHARS = 400
HISTORY_LIMIT = 15

INTENTS = {"none", "join", "leave", "bibs_taken", "question", "admin"}
ADMIN_ACTIONS = {"open", "close", "setlimit", "setfield", "setdate", "settime", "finish", "bibs_reset", "list"}

SYSTEM_PROMPT = """Ты — помощник бота, который ведёт запись на еженедельный любительский футбол в групповом чате Telegram.
Тебе дают одно новое сообщение из чата и контекст. Определи, что хочет автор сообщения, и верни ТОЛЬКО JSON.

Возможные intent:
- "join" — автор записывается на игру. guests = сколько человек он приводит С СОБОЙ помимо себя (0, если один). "я с другом" → guests 1, "нас трое" → guests 2. Если автор уже в списке и просто говорит, что придёт, — всё равно "join" с guests null.
- "leave" — автор не придёт / снимается. guests = null, если уходит сам (со всеми гостями); число N — если убирает только N гостей ("друг не придёт" → guests 1).
- "bibs_taken" — автор сообщает, что ОН забрал манишки (стирать/домой). Только про себя и только утверждение, не вопрос.
- "question" — вопрос про игру, список, резерв, время, поле, манишки, правила бота. Заполни "reply": короткий ответ по-русски на основе контекста (2–4 строки, без markdown). Если ответа в контексте нет — честно скажи, что не знаешь.
- "admin" — автор просит бота что-то изменить: открыть/закрыть запись, поменять дату/время/поле/лимит, завершить игру, обновить список манишек, показать список. Заполни "action" и "args":
  open: args {"date": "ДД/ММ/ГГ" или null, "time": "ЧЧ:ММ-ЧЧ:ММ" или null}; close: {}; setlimit: {"limit": N}; setfield: {"field": "..."}; setdate: {"date": "ДД/ММ/ГГ"}; settime: {"time": "ЧЧ:ММ-ЧЧ:ММ"}; finish: {}; bibs_reset: {}; list: {}.
  Даты считай по таблице ближайших дней из контекста. "в пятницу" = ближайшая пятница (если сегодня пятница и время игры ещё не прошло — сегодня).
- "none" — всё остальное: болтовня, обсуждение матча, шутки, сообщения не боту, неоднозначные фразы. Если сомневаешься — "none".

Правила:
- Не выдумывай действий. Реагируй только на явные намерения, обращённые к записи/игре. Обычный разговор между людьми — "none".
- Сообщение вида "+", "-", "+2" уже обработано кодом, ты их не увидишь.
- "confidence" — число от 0 до 1, насколько ты уверен.
- Формат ответа строго: {"intent": "...", "guests": null|число, "reply": "..."|null, "action": "..."|null, "args": {...}|null, "confidence": 0.0}
"""


def enabled() -> bool:
    return bool(OPENAI_API_KEY)


def _post(payload: Dict[str, Any]) -> Dict[str, Any]:
    request = urllib.request.Request(
        OPENAI_URL,
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        headers={
            "Content-Type": "application/json; charset=utf-8",
            "Authorization": f"Bearer {OPENAI_API_KEY}",
        },
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=20) as response:
        return json.loads(response.read().decode("utf-8"))


def classify_sync(
    text: str,
    author: str,
    is_admin: bool,
    game_context: str,
    bibs_context: str,
    date_table: str,
    history: List[str],
) -> Optional[Dict[str, Any]]:
    """Blocking call to OpenAI. Returns a parsed intent dict or None on any failure."""
    if not enabled():
        return None
    text = (text or "").strip()
    if not text or len(text) > MAX_MESSAGE_CHARS:
        return None

    context_block = (
        f"Сегодня и ближайшие дни:\n{date_table}\n\n"
        f"Текущее состояние игры:\n{game_context}\n\n"
        f"Манишки:\n{bibs_context}\n\n"
        f"Автор нового сообщения: {author} ({'админ чата' if is_admin else 'не админ'})\n\n"
        "Последние сообщения чата (старые → новые):\n" + ("\n".join(history[-HISTORY_LIMIT:]) or "(пусто)")
    )
    payload = {
        "model": OPENAI_MODEL,
        "temperature": 0,
        "max_tokens": 300,
        "response_format": {"type": "json_object"},
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": context_block + f"\n\nНовое сообщение от {author}:\n{text}"},
        ],
    }
    try:
        data = _post(payload)
        content = data["choices"][0]["message"]["content"]
        parsed = json.loads(content)
    except (urllib.error.URLError, TimeoutError, OSError, ValueError, KeyError, IndexError, TypeError) as exc:
        logger.warning("OpenAI call failed: %s", exc)
        return None
    return normalize_intent(parsed)


def normalize_intent(parsed: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(parsed, dict):
        return None
    intent = str(parsed.get("intent") or "none").strip().lower()
    if intent not in INTENTS:
        intent = "none"
    guests = parsed.get("guests")
    try:
        guests = None if guests in (None, "", "null") else max(0, min(5, int(guests)))
    except (TypeError, ValueError):
        guests = None
    try:
        confidence = float(parsed.get("confidence") or 0)
    except (TypeError, ValueError):
        confidence = 0.0
    action = parsed.get("action")
    action = str(action).strip().lower() if action else None
    if action not in ADMIN_ACTIONS:
        action = None
    args = parsed.get("args") if isinstance(parsed.get("args"), dict) else {}
    reply = parsed.get("reply")
    reply = str(reply).strip() if reply else None
    return {
        "intent": intent,
        "guests": guests,
        "reply": reply,
        "action": action,
        "args": args,
        "confidence": max(0.0, min(1.0, confidence)),
    }
