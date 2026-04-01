from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from typing import Literal

from html import escape

from aiogram import F
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandObject, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Message

from moneymonkey.auth import OAuthService
from moneymonkey.db import (
    clear_last_transaction_batch,
    clear_oauth_tokens,
    find_spreadsheet_by_name,
    get_last_transaction_batch,
    get_user,
    list_user_spreadsheets,
    save_last_transaction_batch,
    set_person_name,
    set_person_name_if_empty,
)
from moneymonkey.agent import AgentRunContext, run_financial_assistant_async
from moneymonkey.parser import (
    ParseError,
    ParsedTransaction,
    format_added_at,
    format_tx_date,
    parse_transaction,
    resolve_income_from_subcategory,
    strip_optional_table_suffix,
)
from moneymonkey.reply_format import markdown_to_telegram_html
from moneymonkey.transaction_append import append_parsed_batch
from moneymonkey.sheets import EXPENSES_TITLE, INCOMES_TITLE, SheetStructureError, SheetsClient

from moneymonkey.handlers.common import (
    CreateTableStates,
    NameStates,
    RedoStates,
    ShareStates,
    _PENDING_TYPE_CHOICE,
    ask_type_choice,
    authorized_sheets,
    format_balance_block,
    handle_invalid_grant,
    name_prompt_markup,
    name_prompt_text,
    parse_error_reply,
    ref_from_append,
    resolve_transaction_targets,
    targets_from_batch,
    user_ai_mode_enabled,
    user_paid,
    parse_query_reply_html,
    router,
)

log = logging.getLogger(__name__)

_PENDING_AI_NEW_CAT: dict[int, dict] = {}
_PENDING_AI_CUSTOM_CAT: dict[int, dict] = {}


async def _finish_ai_new_category(
    uid: int,
    user: dict,
    user_sheets: SheetsClient,
    oauth: OAuthService,
    message: Message,
    category_label: str,
    draft: dict,
    *,
    ask_mode: str = "add",
    batch_refs: list | None = None,
) -> None:
    name = category_label.strip()
    if not name:
        await message.answer("<b>⚠️</b> Пустое название.", parse_mode=ParseMode.HTML)
        return
    cat_sid = str(draft["cat_sid"])
    pie = draft.get("predicted_expense_or_income")
    if pie == "income":
        row_income, row_expense = 1, 0
    elif pie == "expense":
        row_income, row_expense = 0, 1
    else:
        row_income, row_expense = 1, 1
    try:
        await asyncio.to_thread(
            user_sheets.append_category_row,
            cat_sid,
            category_name=name,
            is_income=row_income,
            is_expense=row_expense,
        )
        tx_ctx2 = await asyncio.to_thread(user_sheets.fetch_tx_context, cat_sid)
        sub = next(
            (s for s in tx_ctx2.subs if s.name.strip().casefold() == name.casefold()),
            None,
        )
        if sub is None:
            await message.answer(
                "<b>😕</b> Не удалось найти добавленную категорию. Проверь таблицу.",
                parse_mode=ParseMode.HTML,
            )
            return
        inc, need = resolve_income_from_subcategory(sub, draft.get("is_income_opt"))
        parsed = ParsedTransaction(
            amount=float(draft["amount"]),
            parent_category=sub.nadkat_name,
            subcategory_name=sub.name,
            is_income=inc,
            tx_date=draft["tx_date"],
            comment=str(draft.get("comment") or ""),
            matched_synonym_span="",
            requires_type_choice=need,
            tag=draft.get("tag"),
        )
        if need:
            added = format_added_at()
            txd = format_tx_date(parsed.tx_date)
            br = batch_refs if batch_refs is not None else draft.get("batch_refs")
            await ask_type_choice(
                message,
                user_id=uid,
                mode=ask_mode,
                spreadsheet_targets=draft["targets"],
                batch_refs=br or [],
                text=str(draft["command_text"]),
                tx_date=txd,
                parent_category=parsed.parent_category,
                subcategory=parsed.subcategory_name,
                amount=parsed.amount,
                person_name=str(draft["person_name"]),
                added_at=added,
                note=parsed.comment,
                tag=parsed.tag,
                next_expense_id=tx_ctx2.next_expense_id,
                next_income_id=tx_ctx2.next_income_id,
            )
            return
        await append_parsed_batch(
            uid,
            user_sheets,
            draft["targets"],
            str(draft["person_name"]),
            bool(draft["paid"]),
            str(draft["command_text"]),
            parsed,
            tx_ctx2,
        )
        txd = format_tx_date(parsed.tx_date)
        type_label = "Доходы" if parsed.is_income else "Расходы"
        reply = parse_query_reply_html(
            type_label,
            parsed.parent_category,
            parsed.subcategory_name,
            parsed.amount,
            txd,
            parsed.comment,
            tag=parsed.tag,
        )
        if len(draft["targets"]) > 1:
            reply += f"\n\n<i>📋 Записано в таблиц: {len(draft['targets'])}</i>"
        await message.answer(reply, parse_mode=ParseMode.HTML)
    except Exception as exc:
        if oauth.is_invalid_grant(exc):
            await handle_invalid_grant(message, uid)
            return
        log.exception("_finish_ai_new_category")
        await message.answer(
            "<b>😕 Не удалось сохранить</b>\n\nПопробуй позже.",
            parse_mode=ParseMode.HTML,
        )


async def _run_premium_ai_assistant(
    message: Message,
    uid: int,
    user: dict,
    user_sheets: SheetsClient,
    oauth: OAuthService,
    user_text: str,
    targets: list[dict],
    person_name: str,
    paid: bool,
    cat_sid: str,
    tx_ctx,
    *,
    ask_mode: str = "add",
    batch_refs: list | None = None,
) -> str | None:
    async def on_requires_type_choice(parsed: ParsedTransaction, tx_ctx2) -> None:
        added = format_added_at()
        txd = format_tx_date(parsed.tx_date)
        await ask_type_choice(
            message,
            user_id=uid,
            mode=ask_mode,
            spreadsheet_targets=targets,
            batch_refs=batch_refs or [],
            text=user_text,
            tx_date=txd,
            parent_category=parsed.parent_category,
            subcategory=parsed.subcategory_name,
            amount=parsed.amount,
            person_name=person_name,
            added_at=added,
            note=parsed.comment,
            tag=parsed.tag,
            next_expense_id=tx_ctx2.next_expense_id,
            next_income_id=tx_ctx2.next_income_id,
        )

    ctx = AgentRunContext(
        uid=uid,
        user=user,
        message=message,
        user_sheets=user_sheets,
        oauth=oauth,
        targets=targets,
        person_name=person_name,
        paid=paid,
        command_text=user_text,
        cat_sid=cat_sid,
        tx_ctx=tx_ctx,
        subs=list(tx_ctx.subs),
        cats=list(tx_ctx.cats),
        note_pending_new_category=lambda d: _PENDING_AI_NEW_CAT.__setitem__(uid, d),
        on_requires_type_choice=on_requires_type_choice,
        ask_mode=ask_mode,
        batch_refs=batch_refs,
    )
    return await run_financial_assistant_async(ctx, user_text)


@router.callback_query(F.data.startswith("ai_cat:"))
async def ai_cat_callback(callback: CallbackQuery, oauth: OAuthService, sheets: SheetsClient) -> None:
    parts = (callback.data or "").split(":")
    if len(parts) != 3:
        await callback.answer("⚠️ Ошибка")
        return
    _, kind, uid_s = parts
    try:
        req_uid = int(uid_s)
    except ValueError:
        await callback.answer("⚠️ Ошибка")
        return
    uid = callback.from_user.id if callback.from_user else 0
    if uid != req_uid:
        await callback.answer("🚫 Чужая кнопка")
        return
    draft = _PENDING_AI_NEW_CAT.pop(req_uid, None)
    if not draft:
        await callback.answer("⏳ Устарело", show_alert=True)
        return
    if kind == "custom":
        _PENDING_AI_CUSTOM_CAT[req_uid] = draft
        await callback.answer()
        if callback.message:
            await callback.message.answer(
                "<b>Новая категория</b>\n\nНапиши название одним сообщением.",
                parse_mode=ParseMode.HTML,
            )
        return
    if kind != "pred":
        await callback.answer("⚠️ Ошибка")
        return
    pred_name = str(draft.get("predicted_subcategory_name") or "").strip()
    if not pred_name:
        await callback.answer("Нет названия", show_alert=True)
        return
    auth = await authorized_sheets(uid, oauth, sheets)
    if auth is None:
        await callback.answer("Сначала /start", show_alert=True)
        return
    user, user_sheets = auth
    try:
        await _finish_ai_new_category(
            uid,
            user,
            user_sheets,
            oauth,
            callback.message,
            pred_name,
            draft,
            ask_mode=str(draft.get("ask_mode") or "add"),
            batch_refs=draft.get("batch_refs"),
        )
        await callback.answer("✅ Готово")
    except Exception:
        log.exception("ai_cat_callback pred")
        await callback.answer("😕 Ошибка", show_alert=True)
    finally:
        user_sheets.close()


async def _delete_ref_row(user_sheets: SheetsClient, r: dict) -> None:
    sid = str(r["spreadsheet_id"])
    row = int(r["row_1based"])
    if "sheet_id" in r:
        await asyncio.to_thread(user_sheets.delete_row, sid, int(r["sheet_id"]), row)
    elif "sheet_title" in r:
        await asyncio.to_thread(user_sheets.delete_row_by_title, sid, str(r["sheet_title"]), row)


async def _tag_cell_for_append(
    user_sheets: SheetsClient,
    spreadsheet_id: str,
    raw_tag: str | None,
    paid: bool,
) -> str:
    if not paid or not (raw_tag or "").strip():
        return ""
    return await asyncio.to_thread(user_sheets.ensure_tag, spreadsheet_id, raw_tag.strip())


@router.callback_query(F.data.startswith("tx_type:"))
async def on_choose_tx_type(callback: CallbackQuery, oauth: OAuthService, sheets: SheetsClient) -> None:
    uid = callback.from_user.id if callback.from_user else 0
    data = (callback.data or "").split(":")
    if len(data) != 3:
        await callback.answer("⚠️ Ошибка запроса")
        return
    chosen = data[1]
    try:
        requested_uid = int(data[2])
    except ValueError:
        await callback.answer("⚠️ Ошибка запроса")
        return
    if uid != requested_uid:
        await callback.answer("🚫 Чужая кнопка")
        return

    pending = _PENDING_TYPE_CHOICE.pop(uid, None)
    if pending is None:
        await callback.answer("⏳ Устарело — отправь транзакцию снова")
        return

    auth = await authorized_sheets(uid, oauth, sheets)
    if auth is None:
        await callback.answer("👉 Сначала /start")
        return
    _user, user_sheets = auth
    targets = pending["spreadsheet_targets"]
    batch_refs = pending.get("batch_refs") or []
    text = pending["text"]
    tx_date = pending["tx_date"]
    parent_category = str(pending.get("parent_category") or "").strip()
    subcategory = str(pending.get("subcategory") or pending.get("category") or "").strip()
    amount = float(pending["amount"])
    person_name = pending["person_name"]
    added_at = pending["added_at"]
    note = pending["note"]
    mode = pending["mode"]
    pending_tag = (pending.get("tag") or "").strip() or None
    paid = user_paid(_user)
    is_income = chosen == "income"
    sheet_title = INCOMES_TITLE if is_income else EXPENSES_TITLE
    type_label = "Доходы" if is_income else "Расходы"
    next_id = int(pending.get("next_income_id") or 0) if is_income else int(pending.get("next_expense_id") or 0)

    try:
        if mode == "redo":
            to_del = list(batch_refs)
            if not to_del and targets:
                ref = await asyncio.to_thread(
                    user_sheets.find_last_transaction,
                    str(targets[0]["spreadsheet_id"]),
                    person_name,
                )
                if ref is None:
                    await callback.answer("📭 Нечего менять")
                    return
                to_del = [
                    {
                        "spreadsheet_id": str(targets[0]["spreadsheet_id"]),
                        "sheet_id": ref.sheet_id,
                        "row_1based": ref.row_1based,
                    }
                ]
            if not to_del:
                await callback.answer("📭 Нечего менять")
                return
            for r in to_del:
                await _delete_ref_row(user_sheets, r)
        tag_reply = ""
        refs: list[dict] = []
        for t in targets:
            sid = str(t["spreadsheet_id"])
            tag_cell = await _tag_cell_for_append(user_sheets, sid, pending_tag, paid)
            if tag_cell and not tag_reply:
                tag_reply = tag_cell
            row_1based = await asyncio.to_thread(
                user_sheets.append_transaction,
                sid,
                sheet_title,
                tx_date=tx_date,
                type_label=type_label,
                parent_category=parent_category,
                subcategory=subcategory,
                amount=amount,
                person_name=person_name,
                added_at=added_at,
                command=text,
                note=note,
                tag=tag_cell,
                next_id=next_id or None,
            )
            r = ref_from_append(sid, sheet_title, row_1based)
            if r:
                refs.append(r)
            if next_id:
                next_id += 1
        await save_last_transaction_batch(uid, refs)
    except SheetStructureError:
        await callback.answer("⚠️ Структура таблицы нарушена", show_alert=True)
        return
    except Exception as exc:
        if oauth.is_invalid_grant(exc):
            if callback.message:
                await handle_invalid_grant(callback.message, uid)
            else:
                await clear_oauth_tokens(uid)
            await callback.answer("🔐 Нужна повторная авторизация")
            return
        log.exception("on_choose_tx_type")
        await callback.answer("😕 Не удалось сохранить")
        return
    finally:
        user_sheets.close()

    await callback.answer("✅ Сохранено!")
    if callback.message:
        txt = parse_query_reply_html(
            type_label,
            parent_category,
            subcategory,
            amount,
            tx_date,
            note,
            tag=tag_reply or None,
        )
        await callback.message.edit_text(txt, parse_mode=ParseMode.HTML)


_BalancePeriod = Literal["month", "week", "day"]


async def _cmd_balance_period(
    message: Message,
    command: CommandObject,
    oauth: OAuthService,
    sheets: SheetsClient,
    *,
    period: _BalancePeriod,
    cmd: str,
) -> None:
    uid = message.from_user.id if message.from_user else 0
    auth = await authorized_sheets(uid, oauth, sheets)
    if auth is None:
        await message.answer(
            "<b>🔐 Сначала подключи Google</b>\n\nВыполни <code>/start</code>.",
            parse_mode=ParseMode.HTML,
        )
        return
    user, user_sheets = auth
    all_sheets = await list_user_spreadsheets(uid)
    if not all_sheets:
        user_sheets.close()
        await message.answer(
            "<b>📭 Таблицы не найдены</b>\n\nНажми <code>/start</code>.",
            parse_mode=ParseMode.HTML,
        )
        return
    arg = (command.args or "").strip()
    now = datetime.now()
    period_hint = {"month": "текущий месяц", "week": "последние 7 дней", "day": "сегодня"}[period]
    try:
        if arg:
            row = await find_spreadsheet_by_name(uid, arg)
            if row is None:
                await message.answer(
                    f"<b>❌ Не найдено</b>\n\nТаблицы «{escape(arg)}» нет в списке.",
                    parse_mode=ParseMode.HTML,
                )
                return
            block = await format_balance_block(
                user_sheets,
                str(row["spreadsheet_id"]),
                table_title=str(row["name"]),
                now=now,
                period=period,
            )
            await message.answer(block, parse_mode=ParseMode.HTML)
            return
        targets = [s for s in all_sheets if s["is_enabled"]]
        if not targets:
            if not user_paid(user) and all_sheets:
                targets = [all_sheets[0]]
            else:
                await message.answer(
                    "<b>⚠️ Нет включённых таблиц</b>\n\n"
                    f"Включи хотя бы одну в /tables или укажи имя в <code>/{cmd} Имя</code>.",
                    parse_mode=ParseMode.HTML,
                )
                return
        parts: list[str] = []
        for s in targets:
            block = await format_balance_block(
                user_sheets,
                str(s["spreadsheet_id"]),
                table_title=str(s["name"]),
                now=now,
                period=period,
            )
            parts.append(block)
        header = (
            f"<b>💰 Сводка по включённым таблицам</b> <i>({period_hint})</i>\n\n"
            if len(parts) > 1
            else ""
        )
        await message.answer(header + "\n\n".join(parts), parse_mode=ParseMode.HTML)
    except Exception as exc:
        if oauth.is_invalid_grant(exc):
            await handle_invalid_grant(message, uid)
            return
        log.exception("cmd_balance_period %s", period)
        await message.answer(
            "<b>😕 Не удалось прочитать таблицы</b>\n\nПопробуй позже.",
            parse_mode=ParseMode.HTML,
        )
        return
    finally:
        user_sheets.close()


@router.message(Command("month"))
async def cmd_month(message: Message, command: CommandObject, oauth: OAuthService, sheets: SheetsClient) -> None:
    await _cmd_balance_period(message, command, oauth, sheets, period="month", cmd="month")


@router.message(Command("week"))
async def cmd_week(message: Message, command: CommandObject, oauth: OAuthService, sheets: SheetsClient) -> None:
    await _cmd_balance_period(message, command, oauth, sheets, period="week", cmd="week")


@router.message(Command("day"))
async def cmd_day(message: Message, command: CommandObject, oauth: OAuthService, sheets: SheetsClient) -> None:
    await _cmd_balance_period(message, command, oauth, sheets, period="day", cmd="day")

@router.message(Command("del"))
async def cmd_del(message: Message, oauth: OAuthService, sheets: SheetsClient) -> None:
    uid = message.from_user.id if message.from_user else 0
    auth = await authorized_sheets(uid, oauth, sheets)
    if auth is None:
        await message.answer(
            "<b>🔐 Сначала Google</b>\n\n<code>/start</code>",
            parse_mode=ParseMode.HTML,
        )
        return
    user_row, user_sheets = auth
    sheets_list = await list_user_spreadsheets(uid)
    if not sheets_list:
        user_sheets.close()
        await message.answer(
            "<b>🔐 Сначала Google</b>\n\n<code>/start</code>",
            parse_mode=ParseMode.HTML,
        )
        return
    person_name = str(user_row.get("person_name") or "").strip()
    if not person_name:
        user_sheets.close()
        await message.answer(
            "<b>⚠️ Нет имени для поиска строк</b>\n\n"
            "Имя в колонке «Человек» задаётся при первом запуске. Если его нет — напиши в поддержку.",
            parse_mode=ParseMode.HTML,
        )
        return
    batch = await get_last_transaction_batch(uid)
    try:
        if batch:
            for r in batch:
                await _delete_ref_row(user_sheets, r)
            await clear_last_transaction_batch(uid)
        else:
            sid = str(sheets_list[0]["spreadsheet_id"])
            ref = await asyncio.to_thread(user_sheets.find_last_transaction, sid, person_name)
            if ref is None:
                await message.answer(
                    "<b>📭 Нечего удалять</b>\n\nНет твоих транзакций для отката.",
                    parse_mode=ParseMode.HTML,
                )
                return
            await asyncio.to_thread(user_sheets.delete_row, sid, ref.sheet_id, ref.row_1based)
            await clear_last_transaction_batch(uid)
    except Exception as exc:
        if oauth.is_invalid_grant(exc):
            await handle_invalid_grant(message, uid)
            return
        log.exception("cmd_del")
        await message.answer(
            "<b>😕 Не удалось удалить</b>\n\nПопробуй позже.",
            parse_mode=ParseMode.HTML,
        )
        return
    finally:
        user_sheets.close()
    await message.answer(
        "<b>🗑 Готово</b>\n\nПоследняя операция удалена из таблицы.",
        parse_mode=ParseMode.HTML,
    )


@router.message(Command("red"))
async def cmd_red(message: Message, state: FSMContext) -> None:
    uid = message.from_user.id if message.from_user else 0
    row = await get_user(uid)
    if row is None or not row.get("refresh_token_enc"):
        await message.answer(
            "<b>🔐 Сначала Google</b>\n\n<code>/start</code>",
            parse_mode=ParseMode.HTML,
        )
        return
    await state.set_state(RedoStates.waiting_tx)
    await message.answer(
        "<b>✏️ Замена последней операции</b>\n\n"
        "Отправь <b>новую</b> транзакцию в том же формате, что и обычно.",
        parse_mode=ParseMode.HTML,
    )


@router.message(Command("help"))
async def cmd_guide(message: Message) -> None:
    await message.answer(
        "<b>📘 Как пользоваться MoneyMonkey</b>\n\n"
        "<b>Запись транзакции</b>\n"
        "• Сумма и <b>категория</b> (по синонимам с листа «Категории»): "
        "<code>500 такси</code>, <code>30000 зарплата</code>\n"
        "• Доход/расход задаётся <b>категорией</b> на листе «Категории» (колонки «Доход»/«Расход»)\n"
        "• Дата: <code>500 такси 15.04</code>; комментарий — как удобно\n"
        "• <b>Тег</b> (⭐): слово с <code>#</code> в любом месте, например <code>500 кафе #отпуск</code>\n"
        "• В конце можно указать имя таблицы — запись только в неё\n\n"
        "<b>📌 Команды</b>\n"
        "• <code>/month</code>, <code>/week</code>, <code>/day</code> — баланс за текущий месяц, "
        "последние 7 дней или сегодня; к команде можно добавить имя таблицы\n"
        "• <code>/tarifs</code> — тарифы\n"
        "• <code>/tables</code> — ссылка на таблицу (бесплатно) или полное меню и несколько таблиц (⭐ платно)\n"
        "• <code>/on</code> / <code>/off</code> Имя — вкл/выкл таблицы\n"
        "• Удалить свою таблицу: <code>/tables</code> → кнопка «Удалить»\n"
        "• <code>/settings</code> — настройки (имя; AI для Premium — все сообщения или парсер как в Pro)\n"
        "• <code>/del</code> — отменить последнюю операцию\n"
        "• <code>/red</code> — изменить последнюю операцию\n"
        "• <code>/help</code> — эта справка",
        parse_mode=ParseMode.HTML,
    )


@router.message(NameStates.waiting_new_name, F.text)
async def on_new_name(message: Message, state: FSMContext) -> None:
    uid = message.from_user.id if message.from_user else 0
    text = (message.text or "").strip()
    if not text:
        await message.answer(
            "<b>⚠️ Пустое имя</b>\n\nВведи новое имя текстом.",
            parse_mode=ParseMode.HTML,
        )
        return
    await set_person_name(uid, text)
    await state.clear()
    await message.answer(
        f"<b>✅ Имя обновлено</b>\n\nТеперь ты — <b>{escape(text)}</b>.",
        parse_mode=ParseMode.HTML,
    )


@router.message(RedoStates.waiting_tx, F.text)
async def redo_text(message: Message, state: FSMContext, oauth: OAuthService, sheets: SheetsClient) -> None:
    uid = message.from_user.id if message.from_user else 0
    auth = await authorized_sheets(uid, oauth, sheets)
    if auth is None:
        await state.clear()
        await message.answer(
            "<b>🔐 Сначала Google</b>\n\n<code>/start</code>",
            parse_mode=ParseMode.HTML,
        )
        return
    user, user_sheets = auth
    person_name = str(user.get("person_name") or "").strip()
    if not person_name:
        user_sheets.close()
        await state.clear()
        await message.answer(
            name_prompt_text(),
            reply_markup=name_prompt_markup(),
            parse_mode=ParseMode.HTML,
        )
        return
    sheets_list = await list_user_spreadsheets(uid)
    if not sheets_list:
        user_sheets.close()
        await state.clear()
        await message.answer(
            "<b>🔐 Сначала Google</b>\n\n<code>/start</code>",
            parse_mode=ParseMode.HTML,
        )
        return
    batch = await get_last_transaction_batch(uid)
    if batch:
        targets = await targets_from_batch(uid, batch)
        if not targets:
            user_sheets.close()
            await state.clear()
            await message.answer(
                "<b>📭 Нечего менять</b>\n\nПоследняя операция ссылалась на удалённые таблицы.",
                parse_mode=ParseMode.HTML,
            )
            return
    else:
        targets = [sheets_list[0]]
    text = (message.text or "").strip()
    if text.startswith("/"):
        await state.clear()
        await message.answer(
            "<b>⚠️ Это команда</b>\n\nДля замены отправь сумму и категорию, или снова <code>/red</code>.",
            parse_mode=ParseMode.HTML,
        )
        return
    names = [str(s["name"]) for s in sheets_list]
    rest, _suffix = strip_optional_table_suffix(text, names)
    cat_sid = str(targets[0]["spreadsheet_id"])
    paid = user_paid(user)
    try:
        tx_ctx = await asyncio.to_thread(user_sheets.fetch_tx_context, cat_sid)
        if user_ai_mode_enabled(user):
            try:
                reply = await _run_premium_ai_assistant(
                    message,
                    uid,
                    user,
                    user_sheets,
                    oauth,
                    text,
                    targets,
                    person_name,
                    paid,
                    cat_sid,
                    tx_ctx,
                    ask_mode="redo",
                    batch_refs=batch if batch else [],
                )
                if reply:
                    await message.answer(
                        markdown_to_telegram_html(reply),
                        parse_mode=ParseMode.HTML,
                    )
            except Exception:
                log.exception("redo_text AI")
                await message.answer(
                    "<b>😕 Не удалось обработать запрос</b>\n\nПопробуй позже или сформулируй иначе.",
                    parse_mode=ParseMode.HTML,
                )
            else:
                await state.clear()
            finally:
                user_sheets.close()
            return
        parsed = parse_transaction(rest, tx_ctx.cats, tx_ctx.subs, tags_allowed=paid)
    except SheetStructureError:
        user_sheets.close()
        await state.clear()
        await message.answer(
            "<b>⚠️ Структура таблицы нарушена</b>\n\n"
            "Отсутствуют обязательные листы. Удалите таблицу через /tables и создайте новую.",
            parse_mode=ParseMode.HTML,
        )
        return
    except ParseError as e:
        user_sheets.close()
        await message.answer(parse_error_reply(e.reason), parse_mode=ParseMode.HTML)
        return
    added = format_added_at()
    txd = format_tx_date(parsed.tx_date)
    if parsed.requires_type_choice:
        await ask_type_choice(
            message,
            user_id=uid,
            mode="redo",
            spreadsheet_targets=targets,
            batch_refs=batch if batch else [],
            text=text,
            tx_date=txd,
            parent_category=parsed.parent_category,
            subcategory=parsed.subcategory_name,
            amount=parsed.amount,
            person_name=person_name,
            added_at=added,
            note=parsed.comment,
            tag=parsed.tag,
            next_expense_id=tx_ctx.next_expense_id,
            next_income_id=tx_ctx.next_income_id,
        )
        user_sheets.close()
        return
    sheet_title = INCOMES_TITLE if parsed.is_income else EXPENSES_TITLE
    type_label = "Доходы" if parsed.is_income else "Расходы"
    next_id = tx_ctx.next_income_id if parsed.is_income else tx_ctx.next_expense_id
    try:
        to_del = list(batch) if batch else []
        if not to_del:
            ref = await asyncio.to_thread(user_sheets.find_last_transaction, cat_sid, person_name)
            if ref is None:
                await state.clear()
                await message.answer(
                    "<b>📭 Нечего менять</b>\n\nНет последней транзакции для замены.",
                    parse_mode=ParseMode.HTML,
                )
                return
            to_del = [
                {
                    "spreadsheet_id": cat_sid,
                    "sheet_id": ref.sheet_id,
                    "row_1based": ref.row_1based,
                }
            ]
        for r in to_del:
            await _delete_ref_row(user_sheets, r)
        tag_reply = ""
        refs: list[dict] = []
        for t in targets:
            sid = str(t["spreadsheet_id"])
            tag_cell = await _tag_cell_for_append(user_sheets, sid, parsed.tag, paid)
            if tag_cell and not tag_reply:
                tag_reply = tag_cell
            row_1based = await asyncio.to_thread(
                user_sheets.append_transaction,
                sid,
                sheet_title,
                tx_date=txd,
                type_label=type_label,
                parent_category=parsed.parent_category,
                subcategory=parsed.subcategory_name,
                amount=parsed.amount,
                person_name=person_name,
                added_at=added,
                command=text,
                note=parsed.comment,
                tag=tag_cell,
                next_id=next_id,
            )
            r = ref_from_append(sid, sheet_title, row_1based)
            if r:
                refs.append(r)
            next_id += 1
        await save_last_transaction_batch(uid, refs)
    except SheetStructureError:
        await state.clear()
        await message.answer(
            "<b>⚠️ Структура таблицы нарушена</b>\n\n"
            "Отсутствуют обязательные листы. Удалите таблицу через /tables и создайте новую.",
            parse_mode=ParseMode.HTML,
        )
        return
    except Exception as exc:
        if oauth.is_invalid_grant(exc):
            await handle_invalid_grant(message, uid)
            return
        log.exception("redo_text")
        await state.clear()
        await message.answer(
            "<b>😕 Не удалось обновить</b>\n\nПопробуй позже.",
            parse_mode=ParseMode.HTML,
        )
        return
    finally:
        user_sheets.close()
    await state.clear()
    reply = parse_query_reply_html(
        type_label,
        parsed.parent_category,
        parsed.subcategory_name,
        parsed.amount,
        txd,
        parsed.comment,
        success_title="Транзакция обновлена",
        tag=tag_reply or None,
    )
    if len(targets) > 1:
        reply += f"\n\n<i>📋 Записано в таблиц: {len(targets)}</i>"
    await message.answer(reply, parse_mode=ParseMode.HTML)


@router.message(
    F.text & ~F.text.startswith("/"),
    ~StateFilter(
        RedoStates.waiting_tx,
        NameStates.waiting_new_name,
        CreateTableStates.waiting_name,
        ShareStates.waiting_grant_usernames,
        ShareStates.waiting_revoke_usernames,
    ),
)
async def plain_text_tx(message: Message, oauth: OAuthService, sheets: SheetsClient) -> None:
    uid = message.from_user.id if message.from_user else 0
    auth = await authorized_sheets(uid, oauth, sheets)
    if auth is None:
        await message.answer(
            "<b>🔐 Подключи Google</b>\n\nВыполни <code>/start</code>.",
            parse_mode=ParseMode.HTML,
        )
        return
    user, user_sheets = auth
    person_name = str(user.get("person_name") or "").strip()
    sheets_list = await list_user_spreadsheets(uid)
    if not sheets_list:
        user_sheets.close()
        await message.answer(
            "<b>🔐 Подключи Google</b>\n\nВыполни <code>/start</code>.",
            parse_mode=ParseMode.HTML,
        )
        return

    text = (message.text or "").strip()
    if not person_name:
        saved = await set_person_name_if_empty(uid, text)
        user_sheets.close()
        if saved:
            await message.answer(
                f"<b>👋 Добро пожаловать, {escape(text)}!</b>\n\n"
                "Краткая справка — <code>/help</code>.",
                parse_mode=ParseMode.HTML,
            )
        else:
            await message.answer(
                "<b>🔒 Имя уже зафиксировано</b>\n\nЕго нельзя сменить через текст.",
                parse_mode=ParseMode.HTML,
            )
        return
    if uid in _PENDING_AI_CUSTOM_CAT:
        draft = _PENDING_AI_CUSTOM_CAT.pop(uid)
        try:
            await _finish_ai_new_category(
                uid,
                user,
                user_sheets,
                oauth,
                message,
                text,
                draft,
                ask_mode=str(draft.get("ask_mode") or "add"),
                batch_refs=draft.get("batch_refs"),
            )
        finally:
            user_sheets.close()
        return
    names = [str(s["name"]) for s in sheets_list]
    rest, explicit_table = strip_optional_table_suffix(text, names)
    try:
        targets = await resolve_transaction_targets(uid, user, explicit_table)
    except ValueError as e:
        user_sheets.close()
        await message.answer(f"<b>⚠️</b> {escape(str(e))}", parse_mode=ParseMode.HTML)
        return
    cat_sid = str(targets[0]["spreadsheet_id"])
    paid = user_paid(user)
    try:
        tx_ctx = await asyncio.to_thread(user_sheets.fetch_tx_context, cat_sid)
        if user_ai_mode_enabled(user):
            try:
                reply = await _run_premium_ai_assistant(
                    message,
                    uid,
                    user,
                    user_sheets,
                    oauth,
                    text,
                    targets,
                    person_name,
                    paid,
                    cat_sid,
                    tx_ctx,
                )
                if reply:
                    await message.answer(
                        markdown_to_telegram_html(reply),
                        parse_mode=ParseMode.HTML,
                    )
            except Exception:
                log.exception("plain_text_tx AI")
                await message.answer(
                    "<b>😕 Не удалось обработать запрос</b>\n\nПопробуй позже или сформулируй иначе.",
                    parse_mode=ParseMode.HTML,
                )
            finally:
                user_sheets.close()
            return
        parsed = parse_transaction(rest, tx_ctx.cats, tx_ctx.subs, tags_allowed=paid)
    except SheetStructureError:
        user_sheets.close()
        await message.answer(
            "<b>⚠️ Структура таблицы нарушена</b>\n\n"
            "Отсутствуют обязательные листы. Удалите таблицу через /tables и создайте новую.",
            parse_mode=ParseMode.HTML,
        )
        return
    except ParseError as e:
        user_sheets.close()
        await message.answer(parse_error_reply(e.reason), parse_mode=ParseMode.HTML)
        return
    added = format_added_at()
    txd = format_tx_date(parsed.tx_date)
    if parsed.requires_type_choice:
        await ask_type_choice(
            message,
            user_id=uid,
            mode="add",
            spreadsheet_targets=targets,
            batch_refs=None,
            text=text,
            tx_date=txd,
            parent_category=parsed.parent_category,
            subcategory=parsed.subcategory_name,
            amount=parsed.amount,
            person_name=person_name,
            added_at=added,
            note=parsed.comment,
            tag=parsed.tag,
            next_expense_id=tx_ctx.next_expense_id,
            next_income_id=tx_ctx.next_income_id,
        )
        user_sheets.close()
        return
    sheet_title = INCOMES_TITLE if parsed.is_income else EXPENSES_TITLE
    type_label = "Доходы" if parsed.is_income else "Расходы"
    next_id = tx_ctx.next_income_id if parsed.is_income else tx_ctx.next_expense_id
    try:
        tag_reply = ""
        refs: list[dict] = []
        for t in targets:
            sid = str(t["spreadsheet_id"])
            tag_cell = await _tag_cell_for_append(user_sheets, sid, parsed.tag, paid)
            if tag_cell and not tag_reply:
                tag_reply = tag_cell
            row_1based = await asyncio.to_thread(
                user_sheets.append_transaction,
                sid,
                sheet_title,
                tx_date=txd,
                type_label=type_label,
                parent_category=parsed.parent_category,
                subcategory=parsed.subcategory_name,
                amount=parsed.amount,
                person_name=person_name,
                added_at=added,
                command=text,
                note=parsed.comment,
                tag=tag_cell,
                next_id=next_id,
            )
            r = ref_from_append(sid, sheet_title, row_1based)
            if r:
                refs.append(r)
            next_id += 1
        await save_last_transaction_batch(uid, refs)
    except SheetStructureError:
        await message.answer(
            "<b>⚠️ Структура таблицы нарушена</b>\n\n"
            "Отсутствуют обязательные листы. Удалите таблицу через /tables и создайте новую.",
            parse_mode=ParseMode.HTML,
        )
        return
    except Exception as exc:
        if oauth.is_invalid_grant(exc):
            await handle_invalid_grant(message, uid)
            return
        log.exception("plain_text_tx")
        await message.answer(
            "<b>😕 Не удалось записать</b>\n\nПопробуй позже.",
            parse_mode=ParseMode.HTML,
        )
        return
    finally:
        user_sheets.close()

    reply = parse_query_reply_html(
        type_label,
        parsed.parent_category,
        parsed.subcategory_name,
        parsed.amount,
        txd,
        parsed.comment,
        tag=tag_reply or None,
    )
    if len(targets) > 1:
        reply += f"\n\n<i>📋 Записано в таблиц: {len(targets)}</i>"
    await message.answer(reply, parse_mode=ParseMode.HTML)
