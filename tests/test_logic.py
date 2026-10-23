"""Unit tests for the pure list/reserve/stats logic in bot.py (no Telegram needed).

Run: python -m pytest -q
"""

import os
import sys
from datetime import timedelta
from types import SimpleNamespace

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import bot  # noqa: E402


def user(uid, name="User", username=None):
    return SimpleNamespace(id=uid, first_name=f"{name}{uid}", last_name=None, username=username)


def fresh(limit=3):
    cs = bot.default_chat_state("Test", "supergroup")
    cs["game"]["limit"] = limit
    cs["game"]["open"] = True
    return cs


def names(entries):
    return [bot.player_label(e) for e in entries]


def test_plus_adds_player_once():
    cs = fresh()
    r1 = bot.apply_plus(cs, user(1), None)
    r2 = bot.apply_plus(cs, user(1), None)
    assert r1["joined"] == "main" and r2["already"] is True
    assert len(cs["players"]) == 1


def test_plain_plus_does_not_drop_guests():
    cs = fresh(limit=10)
    bot.apply_plus(cs, user(1), 2)
    assert bot.total_guests(cs, 1) == 2
    bot.apply_plus(cs, user(1), None)  # the old bug removed guests here
    assert bot.total_guests(cs, 1) == 2


def test_plus_n_sets_guest_count_up_and_down():
    cs = fresh(limit=10)
    bot.apply_plus(cs, user(1), 3)
    assert bot.total_guests(cs, 1) == 3
    bot.apply_plus(cs, user(1), 1)
    assert bot.total_guests(cs, 1) == 1
    assert len(cs["players"]) == 2


def test_overflow_goes_to_reserve_and_promotes():
    cs = fresh(limit=2)
    bot.apply_plus(cs, user(1), None)
    bot.apply_plus(cs, user(2), None)
    r3 = bot.apply_plus(cs, user(3), None)
    assert r3["joined"] == "reserve"
    assert [e["user_id"] for e in cs["reserve"]] == [3]

    r = bot.apply_minus(cs, user(1), None)
    assert r["removed_self"] is True
    assert [e["user_id"] for e in r["promoted"]] == [3]
    assert [e["user_id"] for e in cs["players"]] == [2, 3]
    assert cs["reserve"] == []
    assert cs["bibs"]["takers"] == [] and cs["bibs"]["active"] is False


def test_guests_overflow_into_reserve_and_promote_in_order():
    cs = fresh(limit=2)
    bot.apply_plus(cs, user(1), 2)  # player + 1 guest in main, 1 guest in reserve
    assert len(cs["players"]) == 2 and len(cs["reserve"]) == 1
    bot.apply_plus(cs, user(2), None)  # reserve #2
    bot.apply_minus(cs, user(1), 1)  # drop one guest: reserve guest removed first
    assert bot.total_guests(cs, 1) == 1
    # user 2 is promoted into the freed slot? No: guest removed from reserve, main still full
    assert [e["user_id"] for e in cs["reserve"]] == [2]
    bot.apply_minus(cs, user(1), 1)  # drop the main-list guest -> user 2 promoted
    assert [e.get("user_id") for e in cs["players"]] == [1, 2]


def test_minus_removes_player_with_all_guests():
    cs = fresh(limit=10)
    bot.apply_plus(cs, user(1), 3)
    bot.apply_plus(cs, user(2), None)
    bot.apply_minus(cs, user(1), None)
    assert [e["user_id"] for e in cs["players"]] == [2]


def test_raising_limit_promotes():
    cs = fresh(limit=1)
    bot.apply_plus(cs, user(1), None)
    bot.apply_plus(cs, user(2), None)
    cs["game"]["limit"] = 0
    promoted = bot.promote_from_reserve(cs)
    assert [e["user_id"] for e in promoted] == [2]


def test_find_by_username_name_number():
    cs = fresh(limit=10)
    bot.apply_plus(cs, user(1, username="alpha"), None)
    bot.apply_plus(cs, user(2, name="Boris"), None)
    assert bot.find_entry_by_query(cs, "@Alpha") == ("players", 0)
    assert bot.find_entry_by_query(cs, "boris") == ("players", 1)
    assert bot.find_entry_by_query(cs, "2") == ("players", 1)
    assert bot.find_entry_by_query(cs, "nobody") is None


def test_finish_clears_lists_rolls_date_and_opens_bibs():
    cs = fresh(limit=2)
    cs["game"]["date"] = "17/10/26"
    bot.apply_plus(cs, user(1), 1)
    bot.apply_plus(cs, user(2), None)  # reserve
    finished = bot.finish_game(cs)
    assert finished == "17/10/26"
    assert cs["players"] == [] and cs["reserve"] == []
    assert cs["game"]["date"] == "24/10/26"
    assert cs["game"]["open"] is False and cs["game"]["was_opened"] is False
    assert cs["bibs"]["active"] is True and cs["bibs"]["takers"] == [] and cs["bibs"]["game_date"] == "17/10/26"


def test_bibs_takers_dedupe_remove_and_reset():
    cs = fresh()
    bot.start_bibs_round(cs, "17/10/26")
    assert bot.add_bibs_taker(cs, user(1, username="alpha")) is True
    assert bot.add_bibs_taker(cs, user(1, username="alpha")) is False
    assert bot.add_bibs_taker(cs, user(2)) is True
    text = bot.format_bibs(cs)
    assert "1. User1 (@alpha)" in text and "2. User2" in text and "17/10/26" in text
    assert bot.add_bibs_taker_by_name(cs, "Алишер") is True
    assert bot.add_bibs_taker_by_name(cs, "алишер") is False  # case-insensitive dedupe
    assert bot.split_names("Фаррух, Далер и @sher") == ["Фаррух", "Далер", "sher"]
    assert bot.find_bibs_taker(cs, "@ALPHA") == 0
    assert bot.find_bibs_taker(cs, "али") == 2
    assert bot.find_bibs_taker(cs, "3") == 2
    assert bot.find_bibs_taker(cs, "nobody") is None
    removed = bot.remove_bibs_taker(cs, bot.find_bibs_taker(cs, "alpha"))
    assert removed["user_id"] == 1
    assert [t["name"] for t in cs["bibs"]["takers"]] == ["User2", "Алишер"]
    bot.start_bibs_round(cs, "")
    assert cs["bibs"]["takers"] == [] and "Пока никто" in bot.format_bibs(cs)


def test_bibs_take_regex():
    yes = ["я взял", "Я взял!", "взял", "беру", "я забрал манишки", "Взял манишки стирать", "манишки у меня", "я взяла", "заберу"]
    no = ["я взял мяч", "взял бы", "кто взял?", "+", "манишки"]
    for t in yes:
        assert bot.BIBS_TAKE_RE.match(t), t
    for t in no:
        assert not bot.BIBS_TAKE_RE.match(t), t


def test_roll_date_to_future():
    game = bot.default_game()
    past = (bot.today() - timedelta(days=15)).strftime("%d/%m/%y")
    game["date"] = past
    assert bot.roll_date_to_future(game) is True
    assert bot.game_date(game).date() >= bot.today().date()
    assert bot.game_date(game).weekday() == bot.game_date({"date": past}).weekday()
    assert bot.roll_date_to_future(game) is False


def test_parsers():
    assert bot.parse_date("17.10.2026") == "17/10/26"
    assert bot.parse_time_range("20.30 – 22:30") == ("20:30", "22:30")
    g = bot.parse_game_input("17/10/26 20:30-22:30 18 Горизонт арена")
    assert g["field"] == "Горизонт арена" and g["limit"] == 18 and g["open"] is False
    assert bot.PLUS_RE.match("+ 2") and bot.PLUS_RE.match("+") and not bot.PLUS_RE.match("+6")
    assert bot.MINUS_RE.match("не смогу") and bot.MINUS_RE.match("-1") and bot.MINUS_RE.match("Минус")


def test_state_roundtrip(tmp_path):
    bot.STATE_FILE = str(tmp_path / "data" / "state.json")
    bot.state.clear()
    cs = bot.ensure_chat(-100, "Chat", "supergroup")
    cs["game"]["open"] = True
    bot.apply_plus(cs, user(1, username="alpha"), 1)
    bot.save_state()
    bot.state.clear()
    bot.load_state()
    loaded = bot.state["-100"]
    assert loaded["game"]["open"] is True
    assert names(loaded["players"]) == ["User1 (@alpha)", "+1 от User1 (@alpha)"]


def test_legacy_state_is_normalized():
    legacy = {"players": ["Иван", {"user_id": 5, "name": "Пётр", "username": "p"}], "limit": "4", "time": "19:00-21:00"}
    cs = bot.normalize_chat(legacy)
    assert cs["game"]["limit"] == 4 and cs["game"]["start"] == "19:00"
    assert names(cs["players"]) == ["Иван", "Пётр (@p)"]
    assert cs["reserve"] == []
    assert cs["bibs"]["takers"] == [] and cs["bibs"]["active"] is False
