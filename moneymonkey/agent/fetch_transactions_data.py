from __future__ import annotations

import json
import logging
import re
from datetime import date, datetime
from typing import Any

import pandas as pd
from pandasql import sqldf

from moneymonkey.agent.context import AgentRunContext
from moneymonkey.sheets import (
    EXPENSE_HEADERS,
    EXPENSES_TITLE,
    INCOMES_TITLE,
    parse_transaction_amount,
    parse_transaction_date,
)

log = logging.getLogger(__name__)

MAX_JSON_CHARS = 120_000
TAIL_LIMIT = 50_000

ALLOWED_AGG = frozenset({"sum", "mean", "min", "max", "count", "nunique"})

_CF_PERSON = "_cf_person"
_CF_CATEGORY = "_cf_category"
_CF_NADKAT = "_cf_nadkat"
_CF_TAG = "_cf_tag"
_INTERNAL_SQL_COLS = frozenset(
    {"tx_date_iso", "amount_num", _CF_PERSON, _CF_CATEGORY, _CF_NADKAT, _CF_TAG}
)


def _clamp_year(y: int) -> int:
    if y < 100:
        return 2000 + y if y < 70 else 1900 + y
    return y


def _parse_date_bound(value: str | None) -> date | None:
    if value is None or not str(value).strip():
        return None
    s = str(value).strip()
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
        try:
            return date.fromisoformat(s)
        except ValueError:
            return None
    m = re.fullmatch(r"(\d{1,2})[.\-/](\d{1,2})[.\-/](\d{2,4})", s)
    if m:
        d, mo, y = int(m.group(1)), int(m.group(2)), _clamp_year(int(m.group(3)))
        try:
            return date(y, mo, d)
        except ValueError:
            return None
    return None


def _sql_literal(s: str) -> str:
    return "'" + str(s).replace("'", "''") + "'"


def _quote_ident(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def resolve_table_id(ctx: AgentRunContext, table: str) -> tuple[str | None, str | None]:
    raw = (table or "").strip()
    if not raw:
        return None, "Укажи table: spreadsheet_id или имя таблицы из описания инструмента."
    allowed_ids = {str(t["spreadsheet_id"]) for t in ctx.targets}
    if raw in allowed_ids:
        return raw, None
    raw_cf = raw.casefold()
    for t in ctx.targets:
        if str(t.get("name") or "").strip().casefold() == raw_cf:
            return str(t["spreadsheet_id"]), None
    return None, "Таблица не найдена среди доступных (смотри список в описании инструмента)."


def _sheet_title(sheet_type: str) -> tuple[str | None, str | None]:
    st = (sheet_type or "").strip().lower()
    if st in ("expenses", "expense", "расходы"):
        return EXPENSES_TITLE, None
    if st in ("incomes", "income", "доходы"):
        return INCOMES_TITLE, None
    return None, "sheet_type должен быть expenses или incomes (один лист за вызов)."


def _normalize_str_list(val: list[str] | None) -> list[str] | None:
    if not val:
        return None
    out = [str(x).strip() for x in val if str(x).strip()]
    return out or None


def _normalize_columns_arg(columns: list[str] | None) -> list[str] | None:
    """None или только пустые строки → все колонки (None). Иначе список имён без пустых."""
    if columns is None:
        return None
    stripped = [str(c).strip() for c in columns if str(c).strip()]
    return stripped or None


def _columns_to_fetch(
    columns: list[str] | None,
    *,
    date_from: str | None,
    date_to: str | None,
    users: list[str] | None,
    categories: list[str] | None,
    nadcategories: list[str] | None,
    tags: list[str] | None,
    group_by: str | None,
    aggregates: list[str] | None,
) -> list[str] | None:
    """None = полный хвост A:K. Иначе — подмножество колонок в порядке EXPENSE_HEADERS."""
    need: set[str] = set()
    if date_from or date_to:
        need.add("Дата Транзакции")
    if users:
        need.add("Человек")
    if categories:
        need.add("Категория")
    if nadcategories:
        need.add("Надкатегория")
    if tags:
        need.add("Тег")
    if group_by:
        g = str(group_by).strip()
        if g in EXPENSE_HEADERS:
            need.add(g)
    if aggregates:
        for a in aggregates:
            if str(a).strip().lower() in ("sum", "mean", "min", "max", "nunique"):
                need.add("Сумма")
                break

    if columns is None:
        return None if not need else [h for h in EXPENSE_HEADERS if h in need]

    user_set = set(columns)
    merged = user_set | need
    return [h for h in EXPENSE_HEADERS if h in merged]


def _str_col_casefold(series: pd.Series) -> pd.Series:
    return series.fillna("").astype(str).str.strip().str.casefold()


def _prepare_df_for_sql(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "Дата Транзакции" in out.columns:
        def _iso(x: Any) -> str | None:
            d = parse_transaction_date(str(x)) if pd.notna(x) and str(x).strip() else None
            return d.date().isoformat() if isinstance(d, datetime) else None

        out["tx_date_iso"] = out["Дата Транзакции"].map(_iso)
    else:
        out["tx_date_iso"] = None
    if "Сумма" in out.columns:
        out["amount_num"] = out["Сумма"].map(parse_transaction_amount)
    else:
        out["amount_num"] = None
    out[_CF_PERSON] = _str_col_casefold(out["Человек"]) if "Человек" in out.columns else ""
    out[_CF_CATEGORY] = _str_col_casefold(out["Категория"]) if "Категория" in out.columns else ""
    out[_CF_NADKAT] = _str_col_casefold(out["Надкатегория"]) if "Надкатегория" in out.columns else ""
    out[_CF_TAG] = _str_col_casefold(out["Тег"]) if "Тег" in out.columns else ""
    return out


def _build_where_sql(
    *,
    d_from: date | None,
    d_to: date | None,
    users: list[str] | None,
    categories: list[str] | None,
    nadcategories: list[str] | None,
    tags: list[str] | None,
) -> str:
    parts: list[str] = ["1=1"]
    if d_from is not None:
        parts.append(f"tx_date_iso IS NOT NULL AND tx_date_iso >= {_sql_literal(d_from.isoformat())}")
    if d_to is not None:
        parts.append(f"tx_date_iso IS NOT NULL AND tx_date_iso <= {_sql_literal(d_to.isoformat())}")
    if users:
        vs = ",".join(_sql_literal(u.strip().casefold()) for u in users)
        parts.append(f"{_quote_ident(_CF_PERSON)} IN ({vs})")
    if categories:
        vs = ",".join(_sql_literal(c.strip().casefold()) for c in categories)
        parts.append(f"{_quote_ident(_CF_CATEGORY)} IN ({vs})")
    if nadcategories:
        vs = ",".join(_sql_literal(n.strip().casefold()) for n in nadcategories)
        parts.append(f"{_quote_ident(_CF_NADKAT)} IN ({vs})")
    if tags:
        vs = ",".join(_sql_literal(t.strip().casefold()) for t in tags)
        parts.append(f"{_quote_ident(_CF_TAG)} IN ({vs})")
    return " AND ".join(parts)


def _build_agg_select(aggregates: list[str] | None, alias_offset: int = 0) -> list[str]:
    if not aggregates:
        return []
    out: list[str] = []
    seen_count = 0
    for i, raw in enumerate(aggregates):
        fn = str(raw).strip().lower()
        if fn not in ALLOWED_AGG:
            continue
        if fn == "sum":
            out.append(f"SUM(amount_num) AS agg_sum_{alias_offset + i}")
        elif fn == "mean":
            out.append(f"AVG(amount_num) AS agg_mean_{alias_offset + i}")
        elif fn == "min":
            out.append(f"MIN(amount_num) AS agg_min_{alias_offset + i}")
        elif fn == "max":
            out.append(f"MAX(amount_num) AS agg_max_{alias_offset + i}")
        elif fn == "count":
            out.append(f"COUNT(*) AS agg_count_{seen_count}")
            seen_count += 1
        elif fn == "nunique":
            out.append(f"COUNT(DISTINCT amount_num) AS agg_nunique_{alias_offset + i}")
    return out


def _run_pandasql(df: pd.DataFrame, query: str) -> pd.DataFrame:
    return sqldf(query, env={"df": df})


def sync_fetch_transactions_data(
    ctx: AgentRunContext,
    *,
    table: str,
    sheet_type: str,
    columns: list[str] | None = None,
    group_by: str | None = None,
    aggregates: list[str] | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    users: list[str] | None = None,
    categories: list[str] | None = None,
    nadcategories: list[str] | None = None,
    tags: list[str] | None = None,
) -> str:
    sid, err = resolve_table_id(ctx, table)
    if err:
        return json.dumps({"error": err}, ensure_ascii=False)

    title, err = _sheet_title(sheet_type)
    if err or title is None:
        return json.dumps({"error": err or "sheet_type"}, ensure_ascii=False)

    d_from = _parse_date_bound(date_from)
    d_to = _parse_date_bound(date_to)
    if date_from and d_from is None:
        return json.dumps({"error": f"Неверный date_from: {date_from!r}"}, ensure_ascii=False)
    if date_to and d_to is None:
        return json.dumps({"error": f"Неверный date_to: {date_to!r}"}, ensure_ascii=False)

    users_l = _normalize_str_list(users)
    cat_l = _normalize_str_list(categories)
    nad_l = _normalize_str_list(nadcategories)
    tag_l = _normalize_str_list(tags)
    aggs_l = [str(a).strip().lower() for a in (aggregates or []) if str(a).strip()]
    aggs_l = [a for a in aggs_l if a in ALLOWED_AGG]

    gb = (group_by or "").strip() or None
    if gb and gb not in EXPENSE_HEADERS:
        return json.dumps({"error": f"group_by должна быть одной из колонок листа: {EXPENSE_HEADERS}"}, ensure_ascii=False)

    columns_norm = _normalize_columns_arg(columns)
    if columns is not None and columns_norm is not None:
        bad = [c for c in columns_norm if c not in EXPENSE_HEADERS]
        if bad:
            return json.dumps({"error": f"Неизвестные columns: {bad}"}, ensure_ascii=False)

    cols_fetch = _columns_to_fetch(
        columns_norm,
        date_from=date_from,
        date_to=date_to,
        users=users_l,
        categories=cat_l,
        nadcategories=nad_l,
        tags=tag_l,
        group_by=gb,
        aggregates=aggs_l,
    )

    client = ctx.user_sheets
    try:
        if cols_fetch is None:
            headers, rows = client.fetch_transaction_values_tail(
                sid, title, tail_data_rows=TAIL_LIMIT
            )
            meta_mode = "tail_full_row"
        else:
            headers, rows = client.fetch_transaction_tail_selected_columns(
                sid, title, tail_data_rows=TAIL_LIMIT, column_names=cols_fetch
            )
            meta_mode = "tail_selected_columns"
    except Exception as e:
        log.exception("fetch transactions")
        return json.dumps({"error": f"Ошибка чтения листа: {e!s}"}, ensure_ascii=False)

    if not rows:
        return json.dumps(
            {
                "rows_json": "[]",
                "row_count": 0,
                "meta": {"spreadsheet_id": sid, "sheet": title, "fetch_mode": meta_mode, "fetched_rows": 0},
            },
            ensure_ascii=False,
        )

    hdr = headers if headers else list(cols_fetch or EXPENSE_HEADERS)
    if cols_fetch is None and not set(EXPENSE_HEADERS).issubset(set(hdr)):
        missing = set(EXPENSE_HEADERS) - set(hdr)
        return json.dumps({"error": f"На листе отсутствуют колонки: {sorted(missing)}"}, ensure_ascii=False)

    w = len(hdr)
    padded = [list(r) + [""] * (w - len(r)) for r in rows]
    df = pd.DataFrame(padded, columns=hdr[:w])

    for h in EXPENSE_HEADERS:
        if h not in df.columns:
            df[h] = None

    df = _prepare_df_for_sql(df)
    where_sql = _build_where_sql(
        d_from=d_from,
        d_to=d_to,
        users=users_l,
        categories=cat_l,
        nadcategories=nad_l,
        tags=tag_l,
    )

    query = ""
    try:
        if gb and aggs_l:
            agg_sel = _build_agg_select(aggs_l)
            if not agg_sel:
                agg_sel = ["COUNT(*) AS agg_count_0"]
            sel = f'{_quote_ident(gb)}, {", ".join(agg_sel)}'
            query = f"SELECT {sel} FROM df WHERE {where_sql} GROUP BY {_quote_ident(gb)}"
            result_df = _run_pandasql(df, query)
        elif gb:
            query = f"""SELECT {_quote_ident(gb)}, COUNT(*) AS row_count FROM df WHERE {where_sql} GROUP BY {_quote_ident(gb)}"""
            result_df = _run_pandasql(df, query)
        elif aggs_l:
            agg_sel = _build_agg_select(aggs_l)
            if not agg_sel:
                agg_sel = ["COUNT(*) AS agg_count_0"]
            query = f"SELECT {', '.join(agg_sel)} FROM df WHERE {where_sql}"
            result_df = _run_pandasql(df, query)
        else:
            query = f"SELECT * FROM df WHERE {where_sql}"
            result_df = _run_pandasql(df, query)
    except Exception as e:
        log.exception("pandasql")
        return json.dumps({"error": f"Ошибка pandasql: {e!s}", "sql": query}, ensure_ascii=False)

    drop_internal = [c for c in _INTERNAL_SQL_COLS if c in result_df.columns]
    result_df = result_df.drop(columns=drop_internal, errors="ignore")
    drop_cols = [c for c in result_df.columns if str(c).startswith("__")]
    result_df = result_df.drop(columns=[c for c in drop_cols if c in result_df.columns], errors="ignore")

    rows_json = _df_to_json_payload(result_df)
    payload = {
        "rows_json": rows_json,
        "row_count": int(len(result_df)),
        "sql": query,
        "meta": {
            "spreadsheet_id": sid,
            "sheet": title,
            "fetch_mode": meta_mode,
            "fetched_rows": len(rows),
            "tail_limit": TAIL_LIMIT,
        },
    }
    return json.dumps(payload, ensure_ascii=False)


def _df_to_json_payload(df: pd.DataFrame) -> str:
    if df.empty:
        return "[]"
    dfc = df.copy()
    for c in dfc.columns:
        if pd.api.types.is_datetime64_any_dtype(dfc[c]):
            dfc[c] = dfc[c].dt.strftime("%Y-%m-%d %H:%M:%S")
        elif dfc[c].dtype == object:
            dfc[c] = dfc[c].apply(
                lambda x: x.isoformat() if isinstance(x, (datetime, date)) else x
            )
    txt = dfc.to_json(orient="records", force_ascii=False, default_handler=str)
    if len(txt) > MAX_JSON_CHARS:
        return txt[: MAX_JSON_CHARS - 80] + "\n… [обрезано]"
    return txt


def build_fetch_transactions_description(available_tables_lines: str) -> str:
    cols = "\n".join(f"• {h}" for h in EXPENSE_HEADERS)
    return f"""\
Чтение последних {TAIL_LIMIT} транзакций с одного листа («Расходы» или «Доходы»), фильтры и агрегация через SQL (pandasql / SQLite) по DataFrame.

Доступные таблицы (укажи spreadsheet_id или имя):
{available_tables_lines}

Колонки листа (первая строка — заголовки):
{cols}

Параметры выборки данных:
• table — spreadsheet_id или имя таблицы из списка выше (обязательно).
• sheet_type — expenses (лист «Расходы») или incomes (лист «Доходы»); за один вызов только один лист.
• columns — список имён колонок для загрузки из API; пустой список или отсутствие — все колонки A–K. К колонкам автоматически добавляются поля, нужные для фильтров и агрегатов.

Фильтры (подставляются в WHERE):
• date_from, date_to — границы по дате транзакции (ГГГГ-ММ-ДД или ДД.ММ.ГГГГ).
• users — список имён «Человек» (регистронезависимо).
• categories — значения колонки «Категория».
• nadcategories — значения «Надкатегория».
• tags — значения «Тег».

Группировка и агрегаты (pandasql):
• group_by — одна колонка для GROUP BY; можно пусто.
• aggregates — список функций по полю amount_num (из «Сумма»): sum, mean, min, max, count (COUNT(*)), nunique (COUNT(DISTINCT amount_num)). Если задан group_by без aggregates — считается COUNT(*) по группам.

Ответ: JSON с rows_json (массив записей строкой), row_count, sql (выполненный запрос), meta.""".strip()
