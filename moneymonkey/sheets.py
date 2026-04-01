from __future__ import annotations

import contextlib
import logging
import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

log = logging.getLogger(__name__)

SCOPES = (
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
)


class SheetsSetupHttpError(Exception):
    def __init__(self, step: str, http_error: HttpError) -> None:
        self.step = step
        self.http_error = http_error
        msg = (http_error.reason or "").strip() or str(http_error)
        super().__init__(f"{step}: {msg}")


class SheetStructureError(Exception):
    pass


def _setup_step(step: str, fn: Callable[[], Any]) -> Any:
    try:
        return fn()
    except HttpError as exc:
        raise SheetsSetupHttpError(step, exc) from exc


def _close_http_transport(http: Any) -> None:
    if http is None:
        return
    seen: set[int] = set()
    stack: list[Any] = [http]
    while stack:
        h = stack.pop()
        hid = id(h)
        if hid in seen:
            continue
        seen.add(hid)
        inner = getattr(h, "http", None)
        if inner is not None and id(inner) not in seen:
            stack.append(inner)
        conns = getattr(h, "connections", None)
        if isinstance(conns, dict):
            for sock in list(conns.values()):
                if sock is None:
                    continue
                with contextlib.suppress(Exception):
                    sock.close()
            with contextlib.suppress(Exception):
                conns.clear()
        with contextlib.suppress(Exception):
            h.close()


def _col_letter(idx: int) -> str:
    n = idx + 1
    s = ""
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


EXPENSES_TITLE = "Расходы"
INCOMES_TITLE = "Доходы"
CATEGORIES_TITLE = "Категории"
NADKAT_TITLE = "Надкатегории"
TAGS_TITLE = "Теги"

EXPENSE_HEADERS = [
    "ID",
    "Дата Транзакции",
    "Тип",
    "Надкатегория",
    "Категория",
    "Сумма",
    "Человек",
    "Дата добавления",
    "Команда",
    "Примечание",
    "Тег",
]


def parse_transaction_date(raw: str) -> datetime | None:
    raw = (raw or "").strip()
    if not raw:
        return None
    normalized = (
        raw.replace("\u00A0", " ")
        .replace("\u202F", " ")
        .replace("-", ".")
        .replace("/", ".")
        .strip()
    )
    for fmt in (
        "%d.%m.%Y",
        "%d.%m.%y",
        "%d.%m.%Y %H:%M",
        "%d.%m.%Y %H:%M:%S",
        "%Y-%m-%d",
    ):
        try:
            return datetime.strptime(normalized, fmt)
        except ValueError:
            continue
    return None


def parse_transaction_amount(raw: str | float | int | None) -> float | None:
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        return float(raw)
    normalized = (
        str(raw)
        .replace("\u00A0", " ")
        .replace("\u202F", " ")
        .strip()
        .replace(" ", "")
    )
    m = re.search(r"-?\d+(?:[.,]\d+)?", normalized)
    if m is None:
        return None
    try:
        return float(m.group(0).replace(",", "."))
    except ValueError:
        return None


NADKAT_HEADERS = [["Надкатегория"]]
CATEGORIES_SHEET_HEADERS = [["Категория", "Надкатегория", "Доход", "Расход", "Синонимы"]]
TAGS_HEADERS = [["ID тега", "Название"]]

DEFAULT_NADKAT_ROWS = [
    ["Зарплата"],
    ["Продукты"],
    ["Транспорт"],
    ["Здоровье"],
    ["Кафе и Рестораны"],
    ["Неизвестно"],
]

DEFAULT_CATEGORY_ROWS = [
    ["Оклад", "Зарплата", 1, 1, "оклад, зарплатный перевод, аванс, зп"],
    ["Премия", "Зарплата", 1, 0, "премия, бонус, премиальные"],
    ["Такси", "Транспорт", 0, 1, "такси, taxi, убер, яндекс"],
    ["Метро", "Транспорт", 0, 1, "метро, метрополитен, подземка"],
    ["Автобус", "Транспорт", 0, 1, "автобус, маршрутка, автобус"],
    ["Лекарства", "Здоровье", 0, 1, "лекарства, аптека, таблетки, лекарство"],
    ["СПА", "Здоровье", 0, 1, "спа, массаж, салон, wellness"],
    ["Спорт", "Здоровье", 0, 1, "спорт, зал, фитнес, тренировка, бассейн"],
    ["Продукты", "Продукты", 0, 1, "продукты, еда, супермаркет, магазин, овощи"],
    ["Кофе", "Кафе и Рестораны", 0, 1, "кофе, кофейня, латте, капучино, бариста"],
    ["Обед", "Кафе и Рестораны", 0, 1, "обед, ланч, бизнес-ланч"],
    ["Ресторан", "Кафе и Рестораны", 0, 1, "ресторан, ужин, поужинать, бар"],
    ["Неизвестно", "Неизвестно", 1, 1, ""],
]


@dataclass(frozen=True)
class SubcategoryDef:
    sub_id: int
    nadkat_name: str
    name: str
    is_income: bool
    is_expense: bool
    synonyms: tuple[str, ...]


@dataclass(frozen=True)
class TxContext:
    cats: list[str]
    subs: list[SubcategoryDef]
    next_expense_id: int
    next_income_id: int


@dataclass(frozen=True)
class LastTransactionRef:
    sheet_title: str
    sheet_id: int
    row_1based: int
    numeric_id: int


class SheetsClient:
    def __init__(self, creds: Credentials | None = None) -> None:
        self._sheets = None
        self._drive = None
        if creds is not None:
            c = creds.with_scopes(SCOPES) if hasattr(creds, "with_scopes") else creds
            self._sheets = build("sheets", "v4", credentials=c, cache_discovery=False)
            self._drive = build("drive", "v3", credentials=c, cache_discovery=False)

    def from_credentials(self, creds: Credentials) -> "SheetsClient":
        return SheetsClient(creds)

    def close(self) -> None:
        for svc in (self._sheets, self._drive):
            if svc is None:
                continue
            http = getattr(svc, "_http", None) or getattr(svc, "http", None)
            _close_http_transport(http)
            if hasattr(svc, "close"):
                with contextlib.suppress(Exception):
                    svc.close()
        self._sheets = None
        self._drive = None

    def create_spreadsheet_for_user(self, title: str) -> tuple[str, str]:
        body: dict[str, Any] = {
            "properties": {"title": title},
            "sheets": [
                {"properties": {"title": EXPENSES_TITLE}},
                {"properties": {"title": INCOMES_TITLE}},
                {"properties": {"title": CATEGORIES_TITLE}},
                {"properties": {"title": NADKAT_TITLE}},
                {"properties": {"title": TAGS_TITLE}},
            ],
        }
        created = _setup_step(
            "create_spreadsheet",
            lambda: self._sheets.spreadsheets()
            .create(body=body, fields="spreadsheetId,spreadsheetUrl,sheets(properties(sheetId,title))")
            .execute(),
        )
        spreadsheet_id = created["spreadsheetId"]
        url = created.get("spreadsheetUrl") or f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}"

        expense_headers = [EXPENSE_HEADERS]
        income_headers = [EXPENSE_HEADERS]
        detail_block = CATEGORIES_SHEET_HEADERS + DEFAULT_CATEGORY_ROWS
        nadkat_block = NADKAT_HEADERS + DEFAULT_NADKAT_ROWS

        _setup_step(
            "batch_update_values",
            lambda: self._sheets.spreadsheets()
            .values()
            .batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={
                    "valueInputOption": "USER_ENTERED",
                    "data": [
                        {"range": f"'{EXPENSES_TITLE}'!A1", "values": expense_headers},
                        {"range": f"'{INCOMES_TITLE}'!A1", "values": income_headers},
                        {"range": f"'{CATEGORIES_TITLE}'!A1", "values": detail_block},
                        {"range": f"'{NADKAT_TITLE}'!A1", "values": nadkat_block},
                        {"range": f"'{TAGS_TITLE}'!A1", "values": TAGS_HEADERS},
                    ],
                },
            )
            .execute(),
        )
        self._apply_category_nadkat_validation(spreadsheet_id)
        self._apply_transaction_sheets_validation(spreadsheet_id)

        return spreadsheet_id, url

    def _apply_category_nadkat_validation(self, spreadsheet_id: str) -> None:
        """Выпадающий список в колонке «Надкатегория» на листе «Категории» = значения с листа «Надкатегории»."""
        meta = self._sheet_meta(spreadsheet_id)
        nad_sid = meta.get(NADKAT_TITLE)
        cat_sid = meta.get(CATEGORIES_TITLE)
        if nad_sid is None or cat_sid is None:
            return
        try:
            self._sheets.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={
                    "requests": [
                        {
                            "setDataValidation": {
                                "range": {
                                    "sheetId": cat_sid,
                                    "startRowIndex": 1,
                                    "endRowIndex": 2000,
                                    "startColumnIndex": 1,
                                    "endColumnIndex": 2,
                                },
                                "rule": {
                                    "condition": {
                                        "type": "ONE_OF_RANGE",
                                        "values": [
                                            {
                                                "userEnteredValue": (
                                                    f"='{NADKAT_TITLE}'!$A$2:$A$100"
                                                )
                                            }
                                        ],
                                    },
                                    "strict": True,
                                    "showCustomUi": True,
                                },
                            }
                        }
                    ]
                },
            ).execute()
        except HttpError:
            log.exception("setDataValidation for categories failed")

    def _apply_transaction_sheets_validation(self, spreadsheet_id: str) -> None:
        """Выпадающие списки на «Расходы»/«Доходы»: D = надкатегории, E = категории (как на листе «Категории»)."""
        meta = self._sheet_meta(spreadsheet_id)
        nad_sid = meta.get(NADKAT_TITLE)
        cat_sid = meta.get(CATEGORIES_TITLE)
        exp_sid = meta.get(EXPENSES_TITLE)
        inc_sid = meta.get(INCOMES_TITLE)
        if nad_sid is None or cat_sid is None:
            return
        nad_range = f"='{NADKAT_TITLE}'!$A$2:$A$500"
        leaf_range = f"='{CATEGORIES_TITLE}'!$A$2:$A$500"
        idx_nad = EXPENSE_HEADERS.index("Надкатегория")
        idx_cat = EXPENSE_HEADERS.index("Категория")
        end_row = 10000

        def _dv(sheet_id: int, col_idx: int, formula: str) -> dict[str, Any]:
            return {
                "setDataValidation": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 1,
                        "endRowIndex": end_row,
                        "startColumnIndex": col_idx,
                        "endColumnIndex": col_idx + 1,
                    },
                    "rule": {
                        "condition": {
                            "type": "ONE_OF_RANGE",
                            "values": [{"userEnteredValue": formula}],
                        },
                        "strict": True,
                        "showCustomUi": True,
                    },
                }
            }

        requests: list[dict[str, Any]] = []
        for tx_sid in (exp_sid, inc_sid):
            if tx_sid is None:
                continue
            requests.append(_dv(tx_sid, idx_nad, nad_range))
            requests.append(_dv(tx_sid, idx_cat, leaf_range))
        if not requests:
            return
        try:
            self._sheets.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={"requests": requests},
            ).execute()
        except HttpError:
            log.exception("setDataValidation for transaction sheets failed")

    def _reorder_sheets_nadkat_tags_last(self, spreadsheet_id: str) -> None:
        """Порядок: Расходы, Доходы, Категории, прочие листы, Надкатегории, Теги."""
        resp = (
            self._sheets.spreadsheets()
            .get(spreadsheetId=spreadsheet_id, fields="sheets(properties(sheetId,title,index))")
            .execute()
        )
        sheets = resp.get("sheets", [])
        title_to_id: dict[str, int] = {}
        by_index: list[tuple[int, str, int]] = []
        for s in sheets:
            p = s.get("properties", {})
            t = p.get("title")
            sid = p.get("sheetId")
            idx = int(p.get("index", 0))
            if t is not None and sid is not None:
                tt = str(t)
                sid_i = int(sid)
                by_index.append((idx, tt, sid_i))
                title_to_id[tt] = sid_i
        by_index.sort(key=lambda x: x[0])
        titles_in_order = [x[1] for x in by_index]

        head = [EXPENSES_TITLE, INCOMES_TITLE, CATEGORIES_TITLE]
        tail = [NADKAT_TITLE, TAGS_TITLE]
        mid = [t for t in titles_in_order if t not in head + tail]
        final: list[str] = []
        for t in head:
            if t in title_to_id:
                final.append(t)
        final.extend(mid)
        for t in tail:
            if t in title_to_id:
                final.append(t)

        requests: list[dict[str, Any]] = []
        for new_index, title in enumerate(final):
            requests.append(
                {
                    "updateSheetProperties": {
                        "properties": {"sheetId": title_to_id[title], "index": new_index},
                        "fields": "index",
                    }
                }
            )
        if requests:
            self._sheets.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={"requests": requests},
            ).execute()

    def ensure_support_sheets(self, spreadsheet_id: str) -> None:
        """Добавляет при отсутствии листы «Категории», «Надкатегории», «Теги»; порядок вкладок и валидация."""
        meta = self._sheet_meta(spreadsheet_id)
        requests: list[dict[str, Any]] = []
        if CATEGORIES_TITLE not in meta:
            requests.append({"addSheet": {"properties": {"title": CATEGORIES_TITLE}}})
        if NADKAT_TITLE not in meta:
            requests.append({"addSheet": {"properties": {"title": NADKAT_TITLE}}})
        if TAGS_TITLE not in meta:
            requests.append({"addSheet": {"properties": {"title": TAGS_TITLE}}})
        if requests:
            self._sheets.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={"requests": requests},
            ).execute()

        tag_hdr = (
            self._sheets.spreadsheets()
            .values()
            .get(spreadsheetId=spreadsheet_id, range=f"'{TAGS_TITLE}'!A1:B1")
            .execute()
        )
        trow = (tag_hdr.get("values") or [[]])[0]
        if not trow or not str(trow[0]).strip():
            self._sheets.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=f"'{TAGS_TITLE}'!A1",
                valueInputOption="USER_ENTERED",
                body={"values": TAGS_HEADERS},
            ).execute()

        self._reorder_sheets_nadkat_tags_last(spreadsheet_id)
        self._apply_category_nadkat_validation(spreadsheet_id)
        self._apply_transaction_sheets_validation(spreadsheet_id)

    def fetch_categories(self, spreadsheet_id: str) -> list[str]:
        ctx = self.fetch_tx_context(spreadsheet_id)
        return ctx.cats

    def fetch_category_bundle(self, spreadsheet_id: str) -> tuple[list[str], list[SubcategoryDef]]:
        ctx = self.fetch_tx_context(spreadsheet_id)
        return ctx.cats, ctx.subs

    def fetch_tx_context(self, spreadsheet_id: str) -> "TxContext":
        """Один batchGet: надкатегории + категории + ID расходов + ID доходов."""
        try:
            result = (
                self._sheets.spreadsheets()
                .values()
                .batchGet(
                    spreadsheetId=spreadsheet_id,
                    ranges=[
                        f"'{NADKAT_TITLE}'!A2:A500",
                        f"'{CATEGORIES_TITLE}'!A2:E500",
                        f"'{EXPENSES_TITLE}'!A2:A50000",
                        f"'{INCOMES_TITLE}'!A2:A50000",
                    ],
                )
                .execute()
            )
        except HttpError as exc:
            if exc.resp.status == 400:
                raise SheetStructureError(
                    f"В таблице отсутствуют обязательные листы."
                ) from exc
            raise

        vr = result.get("valueRanges", [])
        nadkat_rows = vr[0].get("values", []) if len(vr) > 0 else []
        cat_rows = vr[1].get("values", []) if len(vr) > 1 else []
        exp_ids = vr[2].get("values", []) if len(vr) > 2 else []
        inc_ids = vr[3].get("values", []) if len(vr) > 3 else []

        cats: list[str] = []
        for row in nadkat_rows:
            if not row or not str(row[0]).strip():
                continue
            cats.append(str(row[0]).strip())

        subs: list[SubcategoryDef] = []
        for i, row in enumerate(cat_rows):
            if len(row) < 3:
                continue
            sname = str(row[0]).strip()
            nadkat = str(row[1]).strip()
            if not sname:
                continue
            try:
                inc = int(str(row[2]).strip() or "0")
                exp = int(str(row[3]).strip() or "0")
            except ValueError:
                inc, exp = 1, 1
            syn_raw = str(row[4]).strip() if len(row) > 4 else ""
            syns = tuple(s.strip().lower() for s in syn_raw.split(",") if s.strip())
            subs.append(
                SubcategoryDef(
                    sub_id=i + 1,
                    nadkat_name=nadkat,
                    name=sname,
                    is_income=bool(inc),
                    is_expense=bool(exp),
                    synonyms=syns,
                )
            )

        return TxContext(
            cats=cats,
            subs=subs,
            next_expense_id=self._max_id_from_rows(exp_ids) + 1,
            next_income_id=self._max_id_from_rows(inc_ids) + 1,
        )

    @staticmethod
    def _max_id_from_rows(rows: list[list]) -> int:
        max_id = 0
        for r in rows:
            if not r or not r[0]:
                continue
            try:
                max_id = max(max_id, int(str(r[0]).strip()))
            except ValueError:
                continue
        return max_id

    def _fetch_tag_map(self, spreadsheet_id: str) -> dict[str, str]:
        res = (
            self._sheets.spreadsheets()
            .values()
            .get(spreadsheetId=spreadsheet_id, range=f"'{TAGS_TITLE}'!A2:B5000")
            .execute()
        )
        rows = res.get("values") or []
        m: dict[str, str] = {}
        for row in rows:
            if len(row) < 2:
                continue
            raw = str(row[1]).strip()
            if not raw:
                continue
            m[raw.casefold()] = raw
        return m

    def ensure_tag(self, spreadsheet_id: str, tag_display: str) -> str:
        """Гарантирует наличие тега на листе «Теги»; возвращает каноническое написание."""
        t = tag_display.strip().lstrip("#").strip()
        if not t:
            return ""
        key = t.casefold()
        try:
            existing = self._fetch_tag_map(spreadsheet_id)
        except HttpError as exc:
            if exc.resp.status == 400:
                raise SheetStructureError(
                    f"В таблице отсутствует лист «{TAGS_TITLE}»."
                ) from exc
            raise
        if key in existing:
            return existing[key]
        new_id = self._next_id(spreadsheet_id, TAGS_TITLE)
        self._sheets.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id,
            range=f"'{TAGS_TITLE}'!A:B",
            valueInputOption="USER_ENTERED",
            insertDataOption="INSERT_ROWS",
            body={"values": [[new_id, t]]},
        ).execute()
        return t

    def _sheet_meta(self, spreadsheet_id: str) -> dict[str, int]:
        meta = (
            self._sheets.spreadsheets()
            .get(spreadsheetId=spreadsheet_id, fields="sheets(properties(sheetId,title))")
            .execute()
        )
        m: dict[str, int] = {}
        for s in meta.get("sheets", []):
            props = s.get("properties", {})
            t = props.get("title")
            sid = props.get("sheetId")
            if t is not None and sid is not None:
                m[t] = int(sid)
        return m

    def _next_id(self, spreadsheet_id: str, sheet_title: str) -> int:
        rng = f"'{sheet_title}'!A2:A50000"
        result = (
            self._sheets.spreadsheets()
            .values()
            .get(spreadsheetId=spreadsheet_id, range=rng)
            .execute()
        )
        rows = result.get("values") or []
        max_id = 0
        for r in rows:
            if not r or not r[0]:
                continue
            try:
                max_id = max(max_id, int(str(r[0]).strip()))
            except ValueError:
                continue
        return max_id + 1

    def _read_header_row(self, spreadsheet_id: str, sheet_title: str) -> list[str]:
        header_res = (
            self._sheets.spreadsheets()
            .values()
            .get(spreadsheetId=spreadsheet_id, range=f"'{sheet_title}'!A1:Z1")
            .execute()
        )
        return [str(h).strip() for h in (header_res.get("values") or [[]])[0]]

    def append_transaction(
        self,
        spreadsheet_id: str,
        sheet_title: str,
        *,
        tx_date: str,
        type_label: str,
        parent_category: str,
        subcategory: str,
        amount: float,
        person_name: str,
        added_at: str,
        command: str,
        note: str,
        tag: str = "",
        next_id: int | None = None,
    ) -> int | None:
        """Добавляет строку. Возвращает 1-based номер строки (для /del) или None."""
        row_id = next_id if next_id is not None else self._next_id(spreadsheet_id, sheet_title)
        row = [
            row_id,
            tx_date,
            type_label,
            parent_category,
            subcategory,
            amount,
            person_name,
            added_at,
            command,
            note,
            tag or "",
        ]
        resp = self._sheets.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id,
            range=f"'{sheet_title}'!A:K",
            valueInputOption="USER_ENTERED",
            insertDataOption="INSERT_ROWS",
            body={"values": [row]},
        ).execute()
        return self._parse_appended_row(resp)

    def append_category_row(
        self,
        spreadsheet_id: str,
        *,
        category_name: str,
        nadkat_name: str = "Неизвестно",
        is_income: int = 1,
        is_expense: int = 1,
        synonyms: str = "",
    ) -> None:
        """Добавляет строку на лист «Категории»: имя, надкатегория, флаги доход/расход, синонимы."""
        row = [
            category_name.strip(),
            nadkat_name.strip(),
            int(is_income),
            int(is_expense),
            synonyms or "",
        ]
        self._sheets.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id,
            range=f"'{CATEGORIES_TITLE}'!A:E",
            valueInputOption="USER_ENTERED",
            insertDataOption="INSERT_ROWS",
            body={"values": [row]},
        ).execute()

    @staticmethod
    def _parse_appended_row(resp: dict[str, Any]) -> int | None:
        """Извлекает номер строки из ответа values().append (например 'Расходы'!A6:K6 → 6)."""
        updated_range = resp.get("updates", {}).get("updatedRange", "")
        m = re.search(r"!.*?(\d+)(?::\w+\d+)?$", updated_range)
        if m:
            return int(m.group(1))
        return None

    def _parse_added_at(self, raw: str) -> datetime | None:
        raw = raw.strip()
        for fmt in ("%d.%m.%Y %H:%M", "%d.%m.%Y %H:%M:%S", "%d.%m.%Y"):
            try:
                return datetime.strptime(raw, fmt)
            except ValueError:
                continue
        return None

    def _parse_tx_date(self, raw: str) -> datetime | None:
        return parse_transaction_date(raw)

    def _parse_amount_cell(self, raw: str) -> float | None:
        return parse_transaction_amount(raw)

    def find_last_transaction(self, spreadsheet_id: str, person_name: str) -> LastTransactionRef | None:
        meta = self._sheet_meta(spreadsheet_id)
        best: tuple[datetime, int, str, int, int] | None = None
        pn = person_name.strip()

        for title in (EXPENSES_TITLE, INCOMES_TITLE):
            sid = meta.get(title)
            if sid is None:
                continue
            headers = self._read_header_row(spreadsheet_id, title)
            if "Человек" not in headers or "Дата добавления" not in headers:
                continue
            pi = headers.index("Человек")
            ai = headers.index("Дата добавления")
            rng = f"'{title}'!A2:Z100000"
            result = (
                self._sheets.spreadsheets()
                .values()
                .get(spreadsheetId=spreadsheet_id, range=rng)
                .execute()
            )
            rows = result.get("values") or []
            for i, row in enumerate(rows):
                if len(row) <= max(pi, ai):
                    continue
                if str(row[pi]).strip() != pn:
                    continue
                dt = self._parse_added_at(str(row[ai]))
                if dt is None:
                    continue
                nid = 0
                try:
                    nid = int(str(row[0]).strip())
                except ValueError:
                    pass
                tup = (dt, i, title, sid, nid)
                if best is None or tup[0] > best[0] or (tup[0] == best[0] and tup[1] > best[1]):
                    best = tup

        if best is None:
            return None
        _, row_i, title, sid, nid = best
        return LastTransactionRef(
            sheet_title=title,
            sheet_id=sid,
            row_1based=row_i + 2,
            numeric_id=nid,
        )

    def delete_row(self, spreadsheet_id: str, sheet_id: int, row_index_1based: int) -> None:
        idx = row_index_1based - 1
        self._sheets.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={
                "requests": [
                    {
                        "deleteDimension": {
                            "range": {
                                "sheetId": sheet_id,
                                "dimension": "ROWS",
                                "startIndex": idx,
                                "endIndex": idx + 1,
                            }
                        }
                    }
                ]
            },
        ).execute()

    def delete_row_by_title(self, spreadsheet_id: str, sheet_title: str, row_index_1based: int) -> None:
        meta = self._sheet_meta(spreadsheet_id)
        sheet_id = meta.get(sheet_title)
        if sheet_id is None:
            return
        self.delete_row(spreadsheet_id, sheet_id, row_index_1based)

    def delete_spreadsheet(self, spreadsheet_id: str) -> None:
        self._drive.files().delete(fileId=spreadsheet_id).execute()

    def share_with_email(self, spreadsheet_id: str, email: str, *, role: str = "writer") -> str:
        """Share file with a Google account email. Returns Drive permission id."""
        body = {"type": "user", "role": role, "emailAddress": email.strip()}
        perm = (
            self._drive.permissions()
            .create(fileId=spreadsheet_id, body=body, fields="id", sendNotificationEmail=False)
            .execute()
        )
        return str(perm["id"])

    def remove_permission(self, spreadsheet_id: str, permission_id: str) -> None:
        self._drive.permissions().delete(fileId=spreadsheet_id, permissionId=permission_id).execute()

    def get_sheet_grid_row_count(self, spreadsheet_id: str, sheet_title: str) -> int | None:
        """Число строк в сетке листа (включая строку заголовка), или None если лист не найден."""
        resp = (
            self._sheets.spreadsheets()
            .get(
                spreadsheetId=spreadsheet_id,
                fields="sheets(properties(sheetId,title,gridProperties(rowCount)))",
            )
            .execute()
        )
        for s in resp.get("sheets", []):
            p = s.get("properties", {})
            if p.get("title") == sheet_title:
                gp = p.get("gridProperties") or {}
                return int(gp.get("rowCount") or 0)
        return None

    def fetch_transaction_values(
        self,
        spreadsheet_id: str,
        sheet_title: str,
        *,
        first_row_1based: int = 2,
        last_row_1based: int | None = None,
        max_rows_default: int = 50000,
    ) -> tuple[list[str], list[list[Any]]]:
        """
        Сырые строки листа «Расходы»/«Доходы» (колонки A–K).
        first_row_1based ≥ 2 (под заголовком). last_row_1based включительно; если None — читается
        не более max_rows_default строк подряд.
        """
        first_row_1based = max(2, int(first_row_1based))
        if last_row_1based is None:
            last_row_1based = first_row_1based + max_rows_default - 1
        else:
            last_row_1based = max(first_row_1based, int(last_row_1based))
        last_row_1based = min(last_row_1based, first_row_1based + max_rows_default - 1)
        rng = f"'{sheet_title}'!A{first_row_1based}:K{last_row_1based}"
        result = (
            self._sheets.spreadsheets()
            .values()
            .get(spreadsheetId=spreadsheet_id, range=rng)
            .execute()
        )
        rows = result.get("values") or []
        headers = self._read_header_row(spreadsheet_id, sheet_title)
        return headers, rows

    def fetch_transaction_values_tail(
        self,
        spreadsheet_id: str,
        sheet_title: str,
        *,
        tail_data_rows: int,
    ) -> tuple[list[str], list[list[Any]]]:
        """Последние tail_data_rows строк данных (экономия запроса при запросе «свежих» операций)."""
        headers = self._read_header_row(spreadsheet_id, sheet_title)
        rc = self.get_sheet_grid_row_count(spreadsheet_id, sheet_title)
        if rc is None or rc < 2:
            return headers, []
        n_data = rc - 1
        take = max(1, min(int(tail_data_rows), n_data))
        start = max(2, rc - take + 1)
        return self.fetch_transaction_values(
            spreadsheet_id,
            sheet_title,
            first_row_1based=start,
            last_row_1based=rc,
            max_rows_default=max(take + 100, 1000),
        )

    def fetch_transaction_tail_selected_columns(
        self,
        spreadsheet_id: str,
        sheet_title: str,
        *,
        tail_data_rows: int,
        column_names: list[str],
    ) -> tuple[list[str], list[list[Any]]]:
        """
        Последние tail_data_rows строк, только указанные колонки (один batchGet по диапазонам).
        column_names — подмножество фактических заголовков листа (порядок как в column_names).
        """
        headers_full = self._read_header_row(spreadsheet_id, sheet_title)
        rc = self.get_sheet_grid_row_count(spreadsheet_id, sheet_title)
        if rc is None or rc < 2 or not column_names:
            return [], []
        n_data = rc - 1
        take = max(1, min(int(tail_data_rows), n_data))
        start = max(2, rc - take + 1)
        end = rc
        indices: list[int] = []
        selected_headers: list[str] = []
        hset = set(headers_full)
        for name in column_names:
            if name not in hset:
                continue
            idx = headers_full.index(name)
            if idx not in indices:
                indices.append(idx)
                selected_headers.append(name)
        if not indices:
            return [], []
        ranges = [
            f"'{sheet_title}'!{_col_letter(i)}{start}:{_col_letter(i)}{end}" for i in indices
        ]
        result = (
            self._sheets.spreadsheets()
            .values()
            .batchGet(spreadsheetId=spreadsheet_id, ranges=ranges)
            .execute()
        )
        vrs = result.get("valueRanges", [])
        columns_data: list[list[Any]] = []
        for vr in vrs:
            vals = vr.get("values") or []
            col_vals = [row[0] if row else "" for row in vals]
            columns_data.append(col_vals)
        if not columns_data:
            return selected_headers, []
        max_len = max(len(c) for c in columns_data)
        rows: list[list[Any]] = []
        for ri in range(max_len):
            rows.append(
                [
                    columns_data[ci][ri] if ri < len(columns_data[ci]) else ""
                    for ci in range(len(columns_data))
                ]
            )
        return selected_headers, rows

    def monthly_totals(
        self,
        spreadsheet_id: str,
        month_start: datetime,
        now: datetime,
    ) -> tuple[float, float]:
        start_d = month_start.date()
        end_d = now.date()
        income = 0.0
        expense = 0.0
        for title, kind in ((INCOMES_TITLE, "income"), (EXPENSES_TITLE, "expense")):
            headers = self._read_header_row(spreadsheet_id, title)
            if "Дата Транзакции" not in headers or "Сумма" not in headers:
                continue
            bi = headers.index("Дата Транзакции")
            si = headers.index("Сумма")
            rng = f"'{title}'!A2:Z50000"
            result = (
                self._sheets.spreadsheets()
                .values()
                .get(spreadsheetId=spreadsheet_id, range=rng)
                .execute()
            )
            rows = result.get("values") or []
            for row in rows:
                if len(row) <= max(bi, si):
                    continue
                d = self._parse_tx_date(str(row[bi]))
                if d is None:
                    continue
                if not (start_d <= d.date() <= end_d):
                    continue
                val = self._parse_amount_cell(str(row[si]))
                if val is None:
                    continue
                if kind == "income":
                    income += val
                else:
                    expense += val
        return income, expense


def is_valid_email(email: str) -> bool:
    email = email.strip()
    if len(email) > 254:
        return False
    return bool(re.match(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$", email))
