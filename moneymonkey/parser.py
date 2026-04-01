from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from moneymonkey.sheets import SubcategoryDef

RU_MONTHS = {
    "января": 1,
    "февраля": 2,
    "марта": 3,
    "апреля": 4,
    "мая": 5,
    "июня": 6,
    "июля": 7,
    "августа": 8,
    "сентября": 9,
    "октября": 10,
    "ноября": 11,
    "декабря": 12,
    "январь": 1,
    "февраль": 2,
    "март": 3,
    "апрель": 4,
    "май": 5,
    "июнь": 6,
    "июль": 7,
    "август": 8,
    "сентябрь": 9,
    "октябрь": 10,
    "ноябрь": 11,
    "декабрь": 12,
}

UNKNOWN_CATEGORY = "Неизвестно"


class ParseError(Exception):
    def __init__(self, reason: str) -> None:
        self.reason = reason
        super().__init__(reason)


@dataclass(frozen=True)
class ParsedTransaction:
    amount: float
    parent_category: str
    subcategory_name: str
    is_income: bool
    tx_date: date
    comment: str
    matched_synonym_span: str
    requires_type_choice: bool
    tag: str | None


_DATE_DD_MM_YYYY = re.compile(
    r"(?<!\d)(\d{1,2})\s*[.\-_ ]\s*(\d{1,2})\s*[.\-_ ]\s*(\d{2,4})(?!\d)"
)
_DATE_DD_MM = re.compile(r"(?<!\d)(\d{1,2})\s*[.\-_ ]\s*(\d{1,2})(?!\d)(?!\s*[.\-_ ]\s*\d{2,4})")
_DATE_START = re.compile(
    r"^(\d{1,2})\s*[.\-_ ]\s*(\d{1,2})(?:\s*[.\-_ ]\s*(\d{2,4}))?"
)

_HASHTAG = re.compile(r"#([^\s#]+)", re.UNICODE)


def _clamp_year(y: int) -> int:
    if y < 100:
        return 2000 + y if y < 70 else 1900 + y
    return y


def _spans_overlap(a: tuple[int, int], b: tuple[int, int]) -> bool:
    return not (a[1] <= b[0] or b[1] <= a[0])


def _try_parse_numeric_date_fragment(
    message: str,
) -> tuple[date, int, int] | None:
    """Return (date, match_start, match_end) for first DD.MM.YYYY / DD.MM / lone DD."""
    candidates: list[tuple[int, int, int, date]] = []

    for m in _DATE_DD_MM_YYYY.finditer(message):
        d, mo, y = int(m.group(1)), int(m.group(2)), _clamp_year(int(m.group(3)))
        try:
            dt = date(y, mo, d)
        except ValueError:
            raise ParseError("неверный формат даты (используйте ДД.ММ.ГГГГ)") from None
        candidates.append((m.start(), m.end(), 0, dt))

    for m in _DATE_DD_MM.finditer(message):
        d, mo = int(m.group(1)), int(m.group(2))
        y = date.today().year
        try:
            dt = date(y, mo, d)
        except ValueError:
            raise ParseError("неверный формат даты (используйте ДД.ММ.ГГГГ)") from None
        candidates.append((m.start(), m.end(), 1, dt))

    blocked = [(s, e) for s, e, _, _ in candidates]

    for m in re.finditer(r"(?<!\d)(\d{1,2})(?!\d)", message):
        span = (m.start(), m.end())
        if any(_spans_overlap(span, b) for b in blocked):
            continue
        d = int(m.group(1))
        if m.start() == 0 and _DATE_START.match(message):
            continue
        if not (1 <= d <= 31):
            continue
        today = date.today()
        try:
            dt = date(today.year, today.month, d)
        except ValueError:
            continue
        candidates.append((m.start(), m.end(), 2, dt))

    if not candidates:
        return None
    candidates.sort(key=lambda x: (x[0], x[2], -(x[1] - x[0])))
    s, e, _, dt = candidates[0]
    return dt, s, e


def _try_parse_ru_words(message: str) -> tuple[date, int, int] | None:
    low = message.lower()
    best: tuple[int, int, date] | None = None

    pat_day_month_year = re.compile(
        r"(\d{1,2})\s+([а-яё]+)\s+(\d{2,4})\b",
        re.IGNORECASE,
    )
    for m in pat_day_month_year.finditer(low):
        day = int(m.group(1))
        mon_w = m.group(2)
        y = _clamp_year(int(m.group(3)))
        mo = RU_MONTHS.get(mon_w)
        if mo is None:
            continue
        try:
            dt = date(y, mo, day)
        except ValueError:
            raise ParseError("неверный формат даты (используйте ДД.ММ.ГГГГ)") from None
        if best is None or m.start() < best[0]:
            best = (m.start(), m.end(), dt)

    pat_month_day_year = re.compile(
        r"([а-яё]+)\s+(\d{1,2})\s+(\d{2,4})\b",
        re.IGNORECASE,
    )
    for m in pat_month_day_year.finditer(low):
        mon_w = m.group(1)
        day = int(m.group(2))
        y = _clamp_year(int(m.group(3)))
        mo = RU_MONTHS.get(mon_w)
        if mo is None:
            continue
        try:
            dt = date(y, mo, day)
        except ValueError:
            raise ParseError("неверный формат даты (используйте ДД.ММ.ГГГГ)") from None
        if best is None or m.start() < best[0]:
            best = (m.start(), m.end(), dt)

    pat_day_month = re.compile(r"(\d{1,2})\s+([а-яё]+)\b", re.IGNORECASE)
    for m in pat_day_month.finditer(low):
        day = int(m.group(1))
        mon_w = m.group(2)
        mo = RU_MONTHS.get(mon_w)
        if mo is None:
            continue
        y = date.today().year
        try:
            dt = date(y, mo, day)
        except ValueError:
            raise ParseError("неверный формат даты (используйте ДД.ММ.ГГГГ)") from None
        if best is None or m.start() < best[0]:
            best = (m.start(), m.end(), dt)

    if best is None:
        return None
    start, end, dt = best
    return dt, start, end


def _extract_date(message: str) -> tuple[date | None, str]:
    """Returns (date or None, literal substring to strip from message for category/comment)."""
    num = _try_parse_numeric_date_fragment(message)
    ru = _try_parse_ru_words(message)
    if num is None and ru is None:
        return None, ""
    if num is None:
        d, s, e = ru
        return d, message[s:e]
    if ru is None:
        d, s, e = num
        return d, message[s:e]
    ns = num[1]
    rs = ru[1]
    if ns < rs or (ns == rs and (num[2] - num[1]) >= (ru[2] - ru[1])):
        d, s, e = num
        return d, message[s:e]
    d, s, e = ru
    return d, message[s:e]


def _find_amount_span(message: str) -> tuple[int, int, float]:
    msg = message.strip()
    offset = 0
    dm = _DATE_START.match(msg)
    if dm:
        offset = dm.end()

    pat = re.compile(r"\d{1,3}(?:[ \u00A0\u202F]\d{3})+(?:[.,]\d+)?|\d+(?:[.,]\d+)?")
    pos = offset
    while True:
        m = pat.search(msg, pos)
        if m is None:
            raise ParseError("не указана сумма")
        raw = (
            m.group()
            .replace("\u00A0", " ")
            .replace("\u202F", " ")
            .replace(" ", "")
            .replace(",", ".")
        )
        try:
            val = float(raw)
        except ValueError:
            raise ParseError("сумма должна быть числом") from None
        if val < 0:
            raise ParseError("сумма должна быть числом")
        s, e = m.span()
        tail = msg[e : e + 24]
        looks_like_date_continuation = bool(
            re.match(r"^\s*[.\-_ ]\s*\d{1,2}\b", tail)
            and val < 100
            and ("." in raw or "," in raw)
        )
        if looks_like_date_continuation:
            pos = s + 1
            continue
        return s, e, val


def _normalize_ws(t: str) -> str:
    return re.sub(r"\s+", " ", t).strip()


def _strip_hashtags(message: str, *, tags_allowed: bool) -> tuple[str, str | None]:
    """Убирает хэштеги из текста для разбора суммы/категории; при подписке возвращает первый тег."""
    if not tags_allowed:
        cleaned = _HASHTAG.sub(" ", message)
        return _normalize_ws(cleaned), None
    first: str | None = None
    for m in _HASHTAG.finditer(message):
        raw_t = (m.group(1) or "").strip()
        if raw_t:
            first = raw_t
            break
    cleaned = _HASHTAG.sub(" ", message)
    return _normalize_ws(cleaned), first


def category_mode_from_sub(s: SubcategoryDef) -> str:
    is_inc = s.is_income and not s.is_expense
    is_exp = s.is_expense and not s.is_income
    if is_inc:
        return "income"
    if is_exp:
        return "expense"
    if s.is_income and s.is_expense:
        return "both"
    return "expense"


def _category_mode_from_sub(s: SubcategoryDef) -> str:
    return category_mode_from_sub(s)


def resolve_income_from_subcategory(
    sub: SubcategoryDef,
    explicit_is_income: bool | None,
) -> tuple[bool, bool]:
    """(is_income, requires_type_choice) — как при разборе транзакции."""
    mode = category_mode_from_sub(sub)
    if mode == "income":
        return True, False
    if mode == "expense":
        return False, False
    if mode == "both":
        if explicit_is_income is None:
            return False, True
        return bool(explicit_is_income), False
    return False, False


def extract_hashtag_tags(text: str) -> list[str]:
    """Все теги из хэштегов в порядке появления, без дубликатов (сохраняем регистр первого вхождения)."""
    seen: set[str] = set()
    out: list[str] = []
    for m in _HASHTAG.finditer(text or ""):
        raw = (m.group(1) or "").strip()
        if not raw:
            continue
        key = raw.casefold()
        if key in seen:
            continue
        seen.add(key)
        out.append(raw)
    return out


def _unknown_subcategory_pair(
    category_names: list[str],
    subcategories: list[SubcategoryDef],
) -> tuple[str, str]:
    if UNKNOWN_CATEGORY.casefold() in {c.casefold() for c in category_names}:
        for s in subcategories:
            if s.nadkat_name.casefold() == UNKNOWN_CATEGORY.casefold():
                return s.name, s.nadkat_name
    return UNKNOWN_CATEGORY, UNKNOWN_CATEGORY


def _match_subcategory(
    text_for_cat: str,
    category_names: list[str],
    subcategories: list[SubcategoryDef],
) -> tuple[str, str, str, str]:
    """(sub_name, parent_category, mode, matched_span)"""
    low = text_for_cat.lower().strip()
    if not low:
        unk_sub, unk_par = _unknown_subcategory_pair(category_names, subcategories)
        return unk_sub, unk_par, "expense", ""

    def sort_key(s: SubcategoryDef) -> tuple[int, int]:
        defer = 1 if s.nadkat_name.casefold() == UNKNOWN_CATEGORY.casefold() else 0
        return (defer, s.sub_id)

    subs = sorted(subcategories, key=sort_key)

    tokens = low.split()
    max_n = min(len(tokens), 6)
    for n in range(max_n, 0, -1):
        phrase = " ".join(tokens[:n])
        for s in subs:
            if s.name.strip().lower() == phrase:
                return s.name, s.nadkat_name, _category_mode_from_sub(s), phrase
            for syn in s.synonyms:
                if syn == phrase:
                    return s.name, s.nadkat_name, _category_mode_from_sub(s), phrase
    w1 = tokens[0]
    for s in subs:
        if s.name.strip().lower() == w1:
            return s.name, s.nadkat_name, _category_mode_from_sub(s), w1
        for syn in s.synonyms:
            if syn == w1:
                return s.name, s.nadkat_name, _category_mode_from_sub(s), w1
    unk_sub, unk_par = _unknown_subcategory_pair(category_names, subcategories)
    return unk_sub, unk_par, "expense", w1


def parse_transaction(
    message: str,
    category_names: list[str],
    subcategories: list[SubcategoryDef],
    *,
    tags_allowed: bool = False,
) -> ParsedTransaction:
    raw, tag = _strip_hashtags(message, tags_allowed=tags_allowed)
    if not raw:
        raise ParseError("не указана сумма")

    parts = raw.split(" ")
    explicit_type: str | None = None
    if parts and parts[0].lower() in ("доход", "+"):
        explicit_type = "income"
        parts = parts[1:]
    elif parts and parts[0].lower() in ("расход", "-"):
        explicit_type = "expense"
        parts = parts[1:]
    raw = _normalize_ws(" ".join(parts))
    parts = raw.split(" ") if raw else []
    if len(parts) < 2:
        raise ParseError("не распознана категория (напишите /help)")

    amount_raw = parts[0].replace(",", ".")
    if " " in amount_raw:
        raise ParseError("сумма должна быть числом")
    if not re.fullmatch(r"\d+(?:[.]\d+)?", amount_raw):
        raise ParseError("сумма должна быть числом")
    if re.fullmatch(r"\d+", parts[1]) and len(parts) >= 3:
        raise ParseError("сумма должна быть числом (без пробелов в числе)")
    amount = float(amount_raw)

    category_token = parts[1].lower()
    sub_name, parent_cat, category_mode, span = _match_subcategory(
        category_token, category_names, subcategories
    )
    requires_choice = category_mode == "both" and explicit_type is None
    if category_mode == "income":
        is_income = True
    elif category_mode == "expense":
        is_income = False
    elif category_mode == "both":
        is_income = explicit_type == "income"
    else:
        is_income = False

    rest = parts[2:]
    tx_date = date.today()
    consumed = 0
    if rest:
        d, consumed = _parse_optional_date_prefix(rest)
        if d is not None:
            tx_date = d
    comment = _normalize_ws(" ".join(rest[consumed:]))

    return ParsedTransaction(
        amount=amount,
        parent_category=parent_cat,
        subcategory_name=sub_name,
        is_income=is_income,
        tx_date=tx_date,
        comment=comment,
        matched_synonym_span=span,
        requires_type_choice=requires_choice,
        tag=tag,
    )


def _parse_optional_date_prefix(tokens: list[str]) -> tuple[date | None, int]:
    if not tokens:
        return None, 0

    t0 = tokens[0]
    m_full = re.fullmatch(r"(\d{1,2})[.\-_/](\d{1,2})[.\-_/](\d{2,4})", t0)
    if m_full:
        d, mo, y = int(m_full.group(1)), int(m_full.group(2)), _clamp_year(int(m_full.group(3)))
        try:
            return date(y, mo, d), 1
        except ValueError:
            raise ParseError("неверный формат даты (используйте ДД.ММ.ГГГГ)") from None

    m_dm = re.fullmatch(r"(\d{1,2})[.\-_/](\d{1,2})", t0)
    if m_dm:
        d, mo = int(m_dm.group(1)), int(m_dm.group(2))
        try:
            return date(date.today().year, mo, d), 1
        except ValueError:
            raise ParseError("неверный формат даты (используйте ДД.ММ.ГГГГ)") from None

    if re.fullmatch(r"\d{1,2}", t0):
        day = int(t0)
        if len(tokens) >= 2 and tokens[1].lower() in RU_MONTHS:
            month = RU_MONTHS[tokens[1].lower()]
            year = date.today().year
            consumed = 2
            if len(tokens) >= 3 and re.fullmatch(r"\d{2,4}", tokens[2]):
                year = _clamp_year(int(tokens[2]))
                consumed = 3
            try:
                return date(year, month, day), consumed
            except ValueError:
                raise ParseError("неверный формат даты (используйте ДД.ММ.ГГГГ)") from None
        try:
            return date(date.today().year, date.today().month, day), 1
        except ValueError:
            return None, 0

    if len(tokens) >= 2 and tokens[0].lower() in RU_MONTHS and re.fullmatch(r"\d{1,2}", tokens[1]):
        month = RU_MONTHS[tokens[0].lower()]
        day = int(tokens[1])
        year = date.today().year
        consumed = 2
        if len(tokens) >= 3 and re.fullmatch(r"\d{2,4}", tokens[2]):
            year = _clamp_year(int(tokens[2]))
            consumed = 3
        try:
            return date(year, month, day), consumed
        except ValueError:
            raise ParseError("неверный формат даты (используйте ДД.ММ.ГГГГ)") from None

    return None, 0


def strip_optional_table_suffix(message: str, table_names: list[str]) -> tuple[str, str | None]:
    """Если строка заканчивается на имя одной из таблиц (после пробела), отрезает его."""
    raw = _normalize_ws(message)
    if not raw or not table_names:
        return raw, None
    uniq = sorted({n.strip() for n in table_names if n and str(n).strip()}, key=len, reverse=True)
    low = raw.casefold()
    for name in uniq:
        suf = " " + name
        if low.endswith(suf.casefold()):
            return raw[: -len(suf)].strip(), name
        if low == name.casefold():
            return "", name
    return raw, None


def format_tx_date(d: date) -> str:
    return d.strftime("%d.%m.%Y")


def format_added_at() -> str:
    return datetime.now().strftime("%d.%m.%Y %H:%M")


def parse_tool_tx_date_arg(value: str | None) -> date:
    """Дата из аргумента инструмента (YYYY-MM-DD или ДД.ММ.ГГГГ); пусто — сегодня."""
    if value is None or not str(value).strip():
        return date.today()
    s = str(value).strip()
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
        try:
            return date.fromisoformat(s)
        except ValueError:
            return date.today()
    m = re.fullmatch(r"(\d{1,2})[.\-/](\d{1,2})[.\-/](\d{2,4})", s)
    if m:
        d, mo, y = int(m.group(1)), int(m.group(2)), _clamp_year(int(m.group(3)))
        try:
            return date(y, mo, d)
        except ValueError:
            return date.today()
    return date.today()
