from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Literal

from html import escape

from langchain_core.tools import StructuredTool
from pydantic import BaseModel, ConfigDict, Field

from moneymonkey.agent.context import get_agent_ctx
from moneymonkey.agent.fetch_transactions_data import (
    build_fetch_transactions_description,
    sync_fetch_transactions_data,
)
from moneymonkey.db import list_person_names_for_spreadsheet
from moneymonkey.parser import (
    UNKNOWN_CATEGORY,
    ParsedTransaction,
    extract_hashtag_tags,
    format_tx_date,
    parse_tool_tx_date_arg,
    resolve_income_from_subcategory,
)
from moneymonkey.reply_format import parse_query_reply_html
from moneymonkey.sheets import EXPENSES_TITLE, INCOMES_TITLE, SubcategoryDef
from moneymonkey.transaction_append import append_parsed_batch

log = logging.getLogger(__name__)


class RecordTransactionArgs(BaseModel):
    model_config = ConfigDict(extra="forbid")

    original_query: str = Field(..., description="Дословный запрос пользователя")
    amount: float = Field(..., gt=0, description="Сумма в рублях")
    predicted_subcategory_name: str = Field(
        ...,
        description="Как ты понял категорию (своя формулировка)",
    )
    extracted_subcategory_name: str = Field(
        "",
        description="ТОЧНО одно имя из списка доступных категорий в описании инструмента; пустая строка если ни одно не подходит",
    )
    is_income: bool | None = Field(
        None,
        description="True/False только если пользователь явно указал доход/расход; иначе None — тип из флагов категории",
    )
    predicted_expense_or_income: str | bool | None = Field(
        None,
        description=(
            "Твое предположение, является ли транзакция доходом (true — доход, false — расход)"
        ),
    )
    tx_date: str | None = Field(
        None,
        description="Дата YYYY-MM-DD или ДД.ММ.ГГГГ; None = сегодня",
    )
    comment: str | None = Field(None, description="Комментарий без суммы и категории")
    tag: str | None = Field(None, description="Тег без #; хэштеги из original_query учитываются отдельно")


class GetCategoryTreeArgs(BaseModel):
    model_config = ConfigDict(extra="forbid")

    spreadsheet_id: str | None = Field(
        None,
        description="ID Google таблицы (spreadsheetId). Пусто — таблица текущего контекста сообщения",
    )


class GetTableUsersArgs(BaseModel):
    model_config = ConfigDict(extra="forbid")

    spreadsheet_id: str | None = Field(
        None,
        description="ID Google таблицы. Пусто — таблица текущего контекста. Участники из БД бота, без запроса к Sheets",
    )


class FetchTransactionsDataArgs(BaseModel):
    model_config = ConfigDict(extra="forbid")

    table: str = Field(
        ...,
        description="spreadsheet_id или имя таблицы из списка в описании инструмента",
    )
    sheet_type: str = Field(
        ...,
        description="Только один лист: expenses (Расходы) или incomes (Доходы)",
    )
    columns: list[str] | None = Field(
        None,
        description="Колонки для загрузки из Sheets; пусто или null — все A–K. Для фильтров нужные поля подставляются автоматически",
    )
    group_by: str | None = Field(
        None,
        description="Одна колонка для GROUP BY в pandasql; пусто — без группировки",
    )
    aggregates: list[str] | None = Field(
        None,
        description="Функции по сумме: sum, mean, min, max, count (COUNT(*)), nunique",
    )
    date_from: str | None = Field(None, description="Нижняя граница даты, ГГГГ-ММ-ДД или ДД.ММ.ГГГГ")
    date_to: str | None = Field(None, description="Верхняя граница даты")
    users: list[str] | None = Field(None, description="Фильтр по колонке «Человек» (без учёта регистра)")
    categories: list[str] | None = Field(None, description="Фильтр по «Категория»")
    nadcategories: list[str] | None = Field(None, description="Фильтр по «Надкатегория»")
    tags: list[str] | None = Field(None, description="Фильтр по «Тег»")


def _coerce_json_list(val: Any) -> list[Any] | None:
    if val is None:
        return None
    if isinstance(val, str):
        try:
            val = json.loads(val)
        except json.JSONDecodeError:
            return None
    if not isinstance(val, list):
        return None
    return val


def _allowed_spreadsheet_ids(ctx) -> set[str]:
    return {str(t["spreadsheet_id"]) for t in ctx.targets}


def _resolve_tool_spreadsheet_id(ctx, spreadsheet_id: str | None) -> tuple[str | None, str | None]:
    raw = (spreadsheet_id or "").strip()
    allowed = _allowed_spreadsheet_ids(ctx)
    if not raw:
        sid = str(ctx.cat_sid).strip()
        if sid in allowed:
            return sid, None
        return None, "Не удалось определить таблицу контекста."
    if raw not in allowed:
        return None, "Указанная таблица не входит в доступные пользователю (используй таблицу из текущего контекста или списка /tables)."
    return raw, None


def category_tree_from_subs(subs: list[SubcategoryDef]) -> dict[str, list[str]]:
    groups: dict[str, list[str]] = {"empty": []}
    unk = UNKNOWN_CATEGORY.casefold()
    for s in subs:
        name = s.name.strip()
        if not name:
            continue
        n = (s.nadkat_name or "").strip()
        if not n or n.casefold() == unk:
            groups["empty"].append(name)
        else:
            groups.setdefault(n, []).append(name)
    return groups


def _find_sub_by_name(subs: list, name: str):
    n = name.strip().casefold()
    if not n:
        return None
    for s in subs:
        if s.name.strip().casefold() == n:
            return s
    return None


def _normalize_predicted_expense_or_income(val: Any) -> Literal["income", "expense"] | None:
    if val is None:
        return None
    if isinstance(val, bool):
        return "income" if val else "expense"
    s = str(val).strip().casefold()
    if s in ("income", "доход", "inc", "i", "true", "1"):
        return "income"
    if s in ("expense", "расход", "exp", "e", "false", "0"):
        return "expense"
    return None


def _merge_tags(original_query: str, tag_arg: str | None, paid: bool) -> str | None:
    if not paid:
        return None
    tags = extract_hashtag_tags(original_query)
    if tag_arg and str(tag_arg).strip():
        t = str(tag_arg).strip().lstrip("#")
        if t and t.casefold() not in {x.casefold() for x in tags}:
            tags.insert(0, t)
    if not tags:
        return None
    return tags[0]


async def _record_transaction_impl(
    original_query: str,
    amount: float,
    predicted_subcategory_name: str,
    extracted_subcategory_name: str = "",
    is_income: bool | None = None,
    predicted_expense_or_income: str | bool | None = None,
    tx_date: str | None = None,
    comment: str | None = None,
    tag: str | None = None,
) -> str:
    ctx = get_agent_ctx()
    if ctx is None:
        return "Внутренняя ошибка: нет контекста бота."

    pred = (predicted_subcategory_name or "").strip() or "Новая категория"
    ext = (extracted_subcategory_name or "").strip()
    tx_d = parse_tool_tx_date_arg(tx_date)
    note = (comment or "").strip()
    primary_tag = _merge_tags(original_query, tag, ctx.paid)

    if ext:
        sub = _find_sub_by_name(ctx.subs, ext)
        if sub is None:
            return (
                f"Ошибка: «{ext}» нет среди доступных категорий. "
                "Передай пустой extracted_subcategory_name или точное имя из списка."
            )
        inc, need_choice = resolve_income_from_subcategory(sub, is_income)
        if need_choice:
            parsed_partial = ParsedTransaction(
                amount=amount,
                parent_category=sub.nadkat_name,
                subcategory_name=sub.name,
                is_income=False,
                tx_date=tx_d,
                comment=note,
                matched_synonym_span="",
                requires_type_choice=True,
                tag=primary_tag,
            )
            await ctx.on_requires_type_choice(parsed_partial, ctx.tx_ctx)
            ctx.transaction_completed = True
            return "Запрошен выбор доход/расход у пользователя (кнопки)."

        parsed = ParsedTransaction(
            amount=amount,
            parent_category=sub.nadkat_name,
            subcategory_name=sub.name,
            is_income=inc,
            tx_date=tx_d,
            comment=note,
            matched_synonym_span="",
            requires_type_choice=False,
            tag=primary_tag,
        )
        try:
            await append_parsed_batch(
                ctx.uid,
                ctx.user_sheets,
                ctx.targets,
                ctx.person_name,
                ctx.paid,
                ctx.command_text,
                parsed,
                ctx.tx_ctx,
            )
        except Exception as e:
            log.exception("record_transaction append")
            return f"Не удалось записать в таблицу: {e!s}"

        sheet_title = INCOMES_TITLE if parsed.is_income else EXPENSES_TITLE
        type_label = "Доходы" if parsed.is_income else "Расходы"
        txd_s = format_tx_date(parsed.tx_date)
        html = parse_query_reply_html(
            type_label,
            parsed.parent_category,
            parsed.subcategory_name,
            parsed.amount,
            txd_s,
            parsed.comment,
            tag=primary_tag,
        )
        if len(ctx.targets) > 1:
            html += f"\n\n<i>📋 Записано в таблиц: {len(ctx.targets)}</i>"
        await ctx.message.answer(html, parse_mode="HTML")
        ctx.transaction_completed = True
        return "Транзакция успешно записана в Google Таблицу."

    pie = _normalize_predicted_expense_or_income(predicted_expense_or_income)
    draft = {
        "predicted_subcategory_name": pred,
        "amount": amount,
        "is_income_opt": is_income,
        "predicted_expense_or_income": pie,
        "tx_date": tx_d,
        "comment": note,
        "tag": primary_tag,
        "targets": ctx.targets,
        "person_name": ctx.person_name,
        "paid": ctx.paid,
        "command_text": ctx.command_text,
        "cat_sid": ctx.cat_sid,
        "ask_mode": ctx.ask_mode,
        "batch_refs": ctx.batch_refs,
    }
    ctx.note_pending_new_category(draft)

    from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="Оставить",
                    callback_data=f"ai_cat:pred:{ctx.uid}",
                )
            ],
            [
                InlineKeyboardButton(
                    text="Ввести своё",
                    callback_data=f"ai_cat:custom:{ctx.uid}",
                )
            ],
        ]
    )
    if pie == "income":
        create_phrase = f"Предлагаем создать категорию доходов «{escape(pred)}»"
    elif pie == "expense":
        create_phrase = f"Предлагаем создать категорию расходов «{escape(pred)}»"
    else:
        create_phrase = f"Предлагаем создать категорию «{escape(pred)}»"
    await ctx.message.answer(
        "<b>Категория не определилась</b>\n\n"
        f"{create_phrase} "
        f"с суммой <b>{amount:g} ₽</b>.\n\n"
        "Нажми <b>Оставить</b>, чтобы добавить её в таблицу, "
        "или <b>Ввести своё</b> и отправь другое название одним сообщением.",
        parse_mode="HTML",
        reply_markup=kb,
    )
    ctx.category_prompt_sent = True
    return (
        "Сообщение с выбором категории отправлено пользователю. "
        "Не дублируй ответ; дождись действия пользователя."
    )


async def _get_category_tree_impl(spreadsheet_id: str | None = None) -> str:
    ctx = get_agent_ctx()
    if ctx is None:
        return "Внутренняя ошибка: нет контекста бота."
    sid, err = _resolve_tool_spreadsheet_id(ctx, spreadsheet_id)
    if err:
        return err
    assert sid is not None
    try:
        if sid == str(ctx.cat_sid).strip():
            subs = ctx.subs
        else:
            tx_ctx = await asyncio.to_thread(ctx.user_sheets.fetch_tx_context, sid)
            subs = tx_ctx.subs
        tree = category_tree_from_subs(subs)
        return json.dumps(tree, ensure_ascii=False)
    except Exception as e:
        log.exception("get_category_tree")
        return f"Не удалось прочитать категории: {e!s}"


async def _fetch_transactions_data_impl(
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
    ctx = get_agent_ctx()
    if ctx is None:
        return "Внутренняя ошибка: нет контекста бота."
    cols = _coerce_json_list(columns) if columns is not None else None
    if columns is not None and cols is None and isinstance(columns, list):
        cols = columns
    ag = _coerce_json_list(aggregates) if aggregates is not None else None
    if aggregates is not None and ag is None and isinstance(aggregates, list):
        ag = aggregates
    if ag is not None:
        ag = [str(x).strip().lower() for x in ag if str(x).strip()]
    u = _coerce_json_list(users) if users is not None else None
    if users is not None and u is None and isinstance(users, list):
        u = users
    cat = _coerce_json_list(categories) if categories is not None else None
    if categories is not None and cat is None and isinstance(categories, list):
        cat = categories
    nad = _coerce_json_list(nadcategories) if nadcategories is not None else None
    if nadcategories is not None and nad is None and isinstance(nadcategories, list):
        nad = nadcategories
    tg = _coerce_json_list(tags) if tags is not None else None
    if tags is not None and tg is None and isinstance(tags, list):
        tg = tags
    try:
        return await asyncio.to_thread(
            sync_fetch_transactions_data,
            ctx,
            table=table,
            sheet_type=sheet_type,
            columns=cols,
            group_by=group_by,
            aggregates=ag,
            date_from=date_from,
            date_to=date_to,
            users=u,
            categories=cat,
            nadcategories=nad,
            tags=tg,
        )
    except Exception as e:
        log.exception("fetch_transactions_data")
        return json.dumps({"error": str(e)}, ensure_ascii=False)


async def _get_table_users_impl(spreadsheet_id: str | None = None) -> str:
    ctx = get_agent_ctx()
    if ctx is None:
        return "Внутренняя ошибка: нет контекста бота."
    sid, err = _resolve_tool_spreadsheet_id(ctx, spreadsheet_id)
    if err:
        return err
    assert sid is not None
    try:
        rows = await list_person_names_for_spreadsheet(sid)
        return json.dumps(rows, ensure_ascii=False)
    except Exception as e:
        log.exception("get_table_users")
        return f"Не удалось прочитать участников: {e!s}"


def build_get_category_tree_tool() -> StructuredTool:
    return StructuredTool.from_function(
        name="get_category_tree",
        description=(
            "Получить дерево категорий из Google Таблицы: для каждой надкатегории — список подкатегорий; "
            "ключ \"empty\" — подкатегории без надкатегории или с пометкой «Неизвестно». "
            "Возвращает JSON-объект. spreadsheet_id опционален (по умолчанию таблица контекста)."
        ),
        coroutine=_get_category_tree_impl,
        args_schema=GetCategoryTreeArgs,
        infer_schema=False,
    )


def build_fetch_transactions_data_tool(available_tables_lines: str) -> StructuredTool:
    return StructuredTool.from_function(
        name="fetch_transactions_data",
        description=build_fetch_transactions_description(available_tables_lines),
        coroutine=_fetch_transactions_data_impl,
        args_schema=FetchTransactionsDataArgs,
        infer_schema=False,
    )


def build_get_table_users_tool() -> StructuredTool:
    return StructuredTool.from_function(
        name="get_table_users",
        description=(
            "Список участников таблицы: telegram_id и person_name из базы бота (владелец и приглашённые по "
            "user_spreadsheets). Запрос к Google Sheets не выполняется. JSON-массив объектов. "
            "spreadsheet_id опционален."
        ),
        coroutine=_get_table_users_impl,
        args_schema=GetTableUsersArgs,
        infer_schema=False,
    )


def build_agent_tools(categories_lines: str, available_tables_lines: str) -> list[StructuredTool]:
    return [
        build_record_transaction_tool(categories_lines),
        build_fetch_transactions_data_tool(available_tables_lines),
        build_get_category_tree_tool(),
        build_get_table_users_tool(),
    ]


def build_record_transaction_tool(categories_lines: str) -> StructuredTool:
    description = (
        "Записать транзакцию по запросу пользователя. "
        "Используй, когда из текста можно выделить сумму и смысл траты/дохода.\n\n"
        "Если категория не из списка (<code>extracted_subcategory_name</code> пустой), "
        "укажи <code>predicted_expense_or_income</code>: income или expense — "
        "так в таблице выставятся флаги дохода/расхода для новой категории.\n\n"
        "<b>Доступные категории</b> (поле «Категория» в таблице) — для "
        "<code>extracted_subcategory_name</code> нужно ТОЧНОЕ совпадение с одной строкой "
        "или пустая строка, если ни одна не подходит:\n"
        f"{categories_lines}"
    )
    return StructuredTool.from_function(
        name="record_transaction",
        description=description,
        coroutine=_record_transaction_impl,
        args_schema=RecordTransactionArgs,
        infer_schema=False,
    )
