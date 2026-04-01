from __future__ import annotations

import asyncio
import logging
from datetime import datetime

from html import escape

from aiogram import F
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandObject
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message
from moneymonkey.auth import OAuthService
from moneymonkey.db import (
    add_user_spreadsheet,
    clear_last_transaction_batch,
    clear_oauth_tokens,
    delete_all_spreadsheet_rows_for_file,
    find_spreadsheet_by_name,
    get_user,
    get_user_spreadsheet_row,
    list_spreadsheet_collaborators,
    list_user_spreadsheets,
    set_spreadsheet_enabled,
)
from moneymonkey.sheets import SheetsClient

from moneymonkey.handlers.common import (
    CreateTableStates,
    authorized_sheets,
    buy_button_markup,
    format_balance_block,
    handle_invalid_grant,
    html_link,
    is_owned_sheet,
    spreadsheet_file_title,
    user_paid,
    router,
)

log = logging.getLogger(__name__)

def _tables_manage_keyboard(sheets: list[dict]) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = [
        [InlineKeyboardButton(text="➕ Создать новую таблицу", callback_data="tscr")],
    ]
    for s in sheets:
        rows.append(
            [InlineKeyboardButton(text=str(s["name"])[:60], callback_data=f"tspr:{s['id']}")]
        )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _table_detail_keyboard(row_id: int, enabled: bool, *, is_owner: bool) -> InlineKeyboardMarkup:
    toggle_data = f"tsof:{row_id}" if enabled else f"tson:{row_id}"
    toggle_text = "🔴 Выключить" if enabled else "🟢 Включить"
    rows: list[list[InlineKeyboardButton]] = [
        [InlineKeyboardButton(text="◀️ Назад", callback_data="tsbk")],
    ]
    if is_owner:
        rows.append(
            [
                InlineKeyboardButton(text="🔓 Выдать доступ", callback_data=f"tssg:{row_id}"),
                InlineKeyboardButton(text="🔒 Отозвать доступ", callback_data=f"tssr:{row_id}"),
            ]
        )
    rows.append([InlineKeyboardButton(text=toggle_text, callback_data=toggle_data)])
    row_bal = [InlineKeyboardButton(text="💰 Баланс", callback_data=f"tsbl:{row_id}")]
    if is_owner:
        row_bal.append(InlineKeyboardButton(text="🗑 Удалить", callback_data=f"tsdl:{row_id}"))
    rows.append(row_bal)
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _free_tables_keyboard(rows: list[dict]) -> InlineKeyboardMarkup:
    """Кнопка удаления (первая своя таблица) + тариф, как под ответом /tables без подписки."""
    owned = [s for s in rows if is_owned_sheet(s)]
    buy = buy_button_markup()
    merged: list[list[InlineKeyboardButton]] = []
    if owned:
        rid = int(owned[0]["id"])
        merged.append([InlineKeyboardButton(text="🗑 Удалить", callback_data=f"tsdl:{rid}")])
    merged.extend(list(buy.inline_keyboard))
    return InlineKeyboardMarkup(inline_keyboard=merged)


def _free_tables_message_html(rows: list[dict]) -> str:
    """Текст экрана /tables для пользователя без подписки (непустой список таблиц)."""
    first = rows[0]
    link = html_link(str(first["name"]), str(first["spreadsheet_url"]))
    extra = ""
    if len(rows) > 1:
        extra = (
            f"\n\n<i>В аккаунте числится несколько таблиц; в бесплатной версии записи идут в основную. "
            f"Управление всеми списком — в платной подписке: </i><code>/tarifs</code>"
        )
    return (
        "<b>📊 Твоя таблица</b>\n\n"
        f"{link}\n\n"
        "Открой файл в Google Таблицах — там листы с расходами, доходами и категориями.\n\n"
        "Несколько таблиц доступны в <b>⭐ платной версии</b>: <code>/tarifs</code>"
        f"{extra}"
    )


async def _tables_list_message_html(rows: list[dict]) -> str:
    if not rows:
        return (
            "<b>📭 Пока нет таблиц</b>\n\n"
            "Нажми <b>➕ Создать новую таблицу</b> ниже."
        )
    lines: list[str] = []
    for i, s in enumerate(rows, 1):
        mark = "✅" if s["is_enabled"] else "❌"
        role = "Владелец" if is_owned_sheet(s) else "участник"
        lines.append(
            f"{i}. {html_link(str(s['name']), str(s['spreadsheet_url']))} "
            f"(<b>{role}</b>) {mark}"
        )
    return "\n".join(lines) + "\n\n<b>📋 Управление таблицами</b>"


@router.message(Command("tables"))
async def cmd_tables(message: Message) -> None:
    uid = message.from_user.id if message.from_user else 0
    user = await get_user(uid)
    rows = await list_user_spreadsheets(uid)

    if not user or not user.get("refresh_token_enc"):
        await message.answer(
            "<b>🔐 Сначала подключи Google</b>\n\nВыполни <code>/start</code>.",
            parse_mode=ParseMode.HTML,
        )
        return

    if not rows:
        await message.answer(
            "<b>📭 Таблица ещё не создана</b>\n\n"
            "Выполни <code>/start</code>, чтобы создать Google Таблицу для учёта.",
            parse_mode=ParseMode.HTML,
        )
        return

    if not user_paid(user):
        await message.answer(
            _free_tables_message_html(rows),
            parse_mode=ParseMode.HTML,
            reply_markup=_free_tables_keyboard(rows),
        )
        return

    body = await _tables_list_message_html(rows)
    await message.answer(
        body,
        parse_mode=ParseMode.HTML,
        reply_markup=_tables_manage_keyboard(rows),
    )


@router.message(Command("on"))
async def cmd_on(message: Message, command: CommandObject) -> None:
    uid = message.from_user.id if message.from_user else 0
    user = await get_user(uid)
    if not user_paid(user):
        await message.answer(
            "<b>⭐ Команда <code>/on</code></b> — только в платной версии.\n👉 <code>/tarifs</code>",
            parse_mode=ParseMode.HTML,
        )
        return
    name = (command.args or "").strip()
    if not name:
        await message.answer(
            "<b>⚠️ Укажи имя таблицы</b>\n\nПример: <code>/on Моя таблица</code>",
            parse_mode=ParseMode.HTML,
        )
        return
    row = await find_spreadsheet_by_name(uid, name)
    if row is None:
        await message.answer(
            f"<b>🔍 Не найдено</b>\n\nТаблицы «{escape(name)}» нет в списке.",
            parse_mode=ParseMode.HTML,
        )
        return
    await set_spreadsheet_enabled(uid, int(row["id"]), True)
    await message.answer(
        f"<b>🟢 Включено</b>\n\n«{escape(str(row['name']))}»",
        parse_mode=ParseMode.HTML,
    )


@router.message(Command("off"))
async def cmd_off(message: Message, command: CommandObject) -> None:
    uid = message.from_user.id if message.from_user else 0
    user = await get_user(uid)
    if not user_paid(user):
        await message.answer(
            "<b>⭐ Команда <code>/off</code></b> — только в платной версии.\n👉 <code>/tarifs</code>",
            parse_mode=ParseMode.HTML,
        )
        return
    name = (command.args or "").strip()
    if not name:
        await message.answer(
            "<b>⚠️ Укажи имя таблицы</b>\n\nПример: <code>/off Моя таблица</code>",
            parse_mode=ParseMode.HTML,
        )
        return
    row = await find_spreadsheet_by_name(uid, name)
    if row is None:
        await message.answer(
            f"<b>🔍 Не найдено</b>\n\nТаблицы «{escape(name)}» нет в списке.",
            parse_mode=ParseMode.HTML,
        )
        return
    await set_spreadsheet_enabled(uid, int(row["id"]), False)
    await message.answer(
        f"<b>🔴 Выключено</b>\n\n«{escape(str(row['name']))}»",
        parse_mode=ParseMode.HTML,
    )


@router.callback_query(F.data == "tscr")
async def tables_create_prompt(callback: CallbackQuery, state: FSMContext) -> None:
    uid = callback.from_user.id if callback.from_user else 0
    user = await get_user(uid)
    if not user_paid(user):
        await callback.answer("⭐ Нужна подписка")
        return
    if not user or not user.get("refresh_token_enc"):
        await callback.answer("👉 Сначала /start")
        return
    await state.set_state(CreateTableStates.waiting_name)
    await callback.answer()
    if callback.message:
        await callback.message.answer(
            "<b>➕ Новая таблица</b>\n\n"
            "Напиши <b>короткое имя</b> одним сообщением.\n"
            "В Google оно будет: <code>MoneyMonkey - …</code>",
            parse_mode=ParseMode.HTML,
        )


@router.callback_query(F.data == "tsbk")
async def tables_back_to_list(callback: CallbackQuery) -> None:
    uid = callback.from_user.id if callback.from_user else 0
    user = await get_user(uid)
    if not user_paid(user):
        await callback.answer("⭐ Нужна подписка")
        return
    rows = await list_user_spreadsheets(uid)
    await callback.answer()
    if callback.message:
        body = await _tables_list_message_html(rows)
        await callback.message.edit_text(
            body,
            parse_mode=ParseMode.HTML,
            reply_markup=_tables_manage_keyboard(rows),
        )


@router.message(CreateTableStates.waiting_name, F.text)
async def tables_create_finish(message: Message, state: FSMContext, oauth: OAuthService, sheets: SheetsClient) -> None:
    uid = message.from_user.id if message.from_user else 0
    user = await get_user(uid)
    if not user or not user_paid(user):
        await state.clear()
        await message.answer(
            "<b>⭐ Создание второй таблицы</b> — только с подпиской.\n👉 <code>/tarifs</code>",
            parse_mode=ParseMode.HTML,
        )
        return
    display = (message.text or "").strip()
    if not display:
        await message.answer(
            "<b>⚠️ Пустое имя</b>\n\nВведи название таблицы.",
            parse_mode=ParseMode.HTML,
        )
        return
    if await find_spreadsheet_by_name(uid, display):
        await message.answer(
            "<b>📛 Такое имя уже занято</b>\n\nПридумай другое.",
            parse_mode=ParseMode.HTML,
        )
        return
    auth = await authorized_sheets(uid, oauth, sheets)
    if auth is None:
        await state.clear()
        await message.answer(
            "<b>🔐 Сначала Google</b>\n\n<code>/start</code>",
            parse_mode=ParseMode.HTML,
        )
        return
    _user, user_sheets = auth
    try:
        sid, url = await asyncio.to_thread(
            user_sheets.create_spreadsheet_for_user,
            spreadsheet_file_title(display),
        )
        await add_user_spreadsheet(uid, display, sid, url, is_enabled=True)
    except Exception as exc:
        if oauth.is_invalid_grant(exc):
            await handle_invalid_grant(message, uid)
            await state.clear()
            return
        log.exception("tables_create_finish")
        await message.answer(
            "<b>😕 Не удалось создать таблицу</b>\n\nПопробуй позже.",
            parse_mode=ParseMode.HTML,
        )
        return
    finally:
        user_sheets.close()
    await state.clear()
    await message.answer(
        f"<b>🎉 Таблица готова!</b>\n\n"
        f"«{escape(display)}» включена ✅\n"
        f"🔗 {html_link('Открыть в Google', url)}",
        parse_mode=ParseMode.HTML,
    )


async def _table_detail_message_html(uid: int, row: dict) -> str:
    user = await get_user(uid)
    so = row.get("share_owner_id")
    if so is not None:
        owner_u = await get_user(int(so))
        owner_name = escape(str(owner_u.get("person_name") or "—")) if owner_u else "—"
        you = escape(str(user.get("person_name") or "ты"))
        status = "<b>🟢 ВКЛ</b>" if row["is_enabled"] else "<b>🔴 ВЫКЛ</b>"
        link = html_link(str(row["name"]), str(row["spreadsheet_url"]))
        return (
            f"{link}\n"
            f"<b>👤 Владелец таблицы:</b> {owner_name}\n"
            f"<b>Твоя роль:</b> совместный доступ ({you})\n"
            f"<b>Статус:</b> {status}"
        )
    owner = escape(str(user.get("person_name") or "—"))
    pname = str(user.get("person_name") or "").strip()
    collabs = await list_spreadsheet_collaborators(uid, str(row["spreadsheet_id"]))
    total = 1 + len(collabs)
    if collabs:
        bits: list[str] = []
        for c in collabs:
            label = (c.get("person_name") or "").strip()
            if not label:
                un = (c.get("telegram_username") or "").strip()
                label = f"@{un}" if un else str(c.get("telegram_id"))
            bits.append(escape(label))
        collab_line = ", ".join(bits)
        participants = (
            f"{total} <i>(владелец: {escape(pname) if pname else '—'}; с доступом: {collab_line})</i>"
        )
    else:
        participants = f"{total} ({escape(pname)})" if pname else str(total)
    status = "<b>🟢 ВКЛ</b>" if row["is_enabled"] else "<b>🔴 ВЫКЛ</b>"
    link = html_link(str(row["name"]), str(row["spreadsheet_url"]))
    return (
        f"{link}\n"
        f"<b>Владелец:</b> {owner}\n"
        f"<b>Участники:</b> {participants}\n"
        f"<b>Статус:</b> {status}"
    )


@router.callback_query(F.data.startswith("tspr:"))
async def tables_pick(callback: CallbackQuery) -> None:
    uid = callback.from_user.id if callback.from_user else 0
    user = await get_user(uid)
    if not user_paid(user):
        await callback.answer("⭐ Нужна подписка")
        return
    try:
        row_id = int((callback.data or "").split(":", 1)[1])
    except (IndexError, ValueError):
        await callback.answer("⚠️ Ошибка запроса")
        return
    row = await get_user_spreadsheet_row(uid, row_id)
    if row is None:
        await callback.answer("🔍 Таблица не найдена")
        return
    is_owner = is_owned_sheet(row)
    text = await _table_detail_message_html(uid, row)
    await callback.answer()
    if callback.message:
        await callback.message.edit_text(
            text,
            parse_mode=ParseMode.HTML,
            reply_markup=_table_detail_keyboard(row_id, bool(row["is_enabled"]), is_owner=is_owner),
        )

@router.callback_query(F.data.startswith("tsof:"))
async def tables_turn_off(callback: CallbackQuery) -> None:
    uid = callback.from_user.id if callback.from_user else 0
    user = await get_user(uid)
    if not user or not user_paid(user):
        await callback.answer("⭐ Нужна подписка")
        return
    try:
        row_id = int((callback.data or "").split(":", 1)[1])
    except (IndexError, ValueError):
        await callback.answer("⚠️ Ошибка запроса")
        return
    row = await get_user_spreadsheet_row(uid, row_id)
    if row is None:
        await callback.answer("🔍 Таблица не найдена")
        return
    await set_spreadsheet_enabled(uid, row_id, False)
    row = await get_user_spreadsheet_row(uid, row_id)
    if row is None:
        await callback.answer("😕 Ошибка")
        return
    await callback.answer("🔴 Выключено")
    if callback.message:
        text = await _table_detail_message_html(uid, row)
        await callback.message.edit_text(
            text,
            parse_mode=ParseMode.HTML,
            reply_markup=_table_detail_keyboard(row_id, False, is_owner=is_owned_sheet(row)),
        )


@router.callback_query(F.data.startswith("tson:"))
async def tables_turn_on(callback: CallbackQuery) -> None:
    uid = callback.from_user.id if callback.from_user else 0
    user = await get_user(uid)
    if not user or not user_paid(user):
        await callback.answer("⭐ Нужна подписка")
        return
    try:
        row_id = int((callback.data or "").split(":", 1)[1])
    except (IndexError, ValueError):
        await callback.answer("⚠️ Ошибка запроса")
        return
    row = await get_user_spreadsheet_row(uid, row_id)
    if row is None:
        await callback.answer("🔍 Таблица не найдена")
        return
    await set_spreadsheet_enabled(uid, row_id, True)
    row = await get_user_spreadsheet_row(uid, row_id)
    if row is None:
        await callback.answer("😕 Ошибка")
        return
    await callback.answer("🟢 Включено")
    if callback.message:
        text = await _table_detail_message_html(uid, row)
        await callback.message.edit_text(
            text,
            parse_mode=ParseMode.HTML,
            reply_markup=_table_detail_keyboard(row_id, True, is_owner=is_owned_sheet(row)),
        )


@router.callback_query(F.data.startswith("tsbl:"))
async def tables_balance(callback: CallbackQuery, oauth: OAuthService, sheets: SheetsClient) -> None:
    uid = callback.from_user.id if callback.from_user else 0
    user = await get_user(uid)
    if not user or not user_paid(user):
        await callback.answer("⭐ Нужна подписка")
        return
    try:
        row_id = int((callback.data or "").split(":", 1)[1])
    except (IndexError, ValueError):
        await callback.answer("⚠️ Ошибка запроса")
        return
    row = await get_user_spreadsheet_row(uid, row_id)
    if row is None:
        await callback.answer("🔍 Таблица не найдена")
        return
    auth = await authorized_sheets(uid, oauth, sheets)
    if auth is None:
        await callback.answer("👉 Сначала /start")
        return
    _user, user_sheets = auth
    now = datetime.now()
    try:
        block = await format_balance_block(
            user_sheets,
            str(row["spreadsheet_id"]),
            table_title=str(row["name"]),
            now=now,
        )
    except Exception as exc:
        if oauth.is_invalid_grant(exc):
            if callback.message:
                await handle_invalid_grant(callback.message, uid)
            else:
                await clear_oauth_tokens(uid)
            return
        log.exception("tables_balance")
        await callback.answer("😕 Не удалось прочитать")
        return
    finally:
        user_sheets.close()
    await callback.answer()
    if callback.message:
        await callback.message.answer(block, parse_mode=ParseMode.HTML)


@router.callback_query(F.data.startswith("tsdl:"))
async def tables_delete_ask(callback: CallbackQuery) -> None:
    uid = callback.from_user.id if callback.from_user else 0
    try:
        row_id = int((callback.data or "").split(":", 1)[1])
    except (IndexError, ValueError):
        await callback.answer("⚠️ Ошибка запроса")
        return
    row = await get_user_spreadsheet_row(uid, row_id)
    if row is None:
        await callback.answer("🔍 Таблица не найдена")
        return
    if not is_owned_sheet(row):
        await callback.answer("🚫 Только владелец")
        return
    await callback.answer()
    if callback.message:
        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(text="✅ Да", callback_data=f"tsdy:{row_id}"),
                    InlineKeyboardButton(text="❌ Нет", callback_data=f"tsdn:{row_id}"),
                ]
            ]
        )
        await callback.message.edit_text(
            "<b>🗑 Удалить таблицу?</b>\n\n"
            f"«<b>{escape(str(row['name']))}</b>» — <b>без отката</b>.",
            parse_mode=ParseMode.HTML,
            reply_markup=kb,
        )


@router.callback_query(F.data.startswith("tsdn:"))
async def tables_delete_no(callback: CallbackQuery) -> None:
    uid = callback.from_user.id if callback.from_user else 0
    try:
        row_id = int((callback.data or "").split(":", 1)[1])
    except (IndexError, ValueError):
        await callback.answer("⚠️ Ошибка запроса")
        return
    row = await get_user_spreadsheet_row(uid, row_id)
    if row is None:
        await callback.answer("🔍 Таблица не найдена")
        return
    user = await get_user(uid)
    await callback.answer("👍 Отменено")
    if callback.message:
        if user and user_paid(user):
            text = await _table_detail_message_html(uid, row)
            await callback.message.edit_text(
                text,
                parse_mode=ParseMode.HTML,
                reply_markup=_table_detail_keyboard(
                    row_id, bool(row["is_enabled"]), is_owner=is_owned_sheet(row)
                ),
            )
        else:
            rows = await list_user_spreadsheets(uid)
            if not rows:
                await callback.message.edit_text(
                    "<b>📭 Таблица ещё не создана</b>\n\nВыполни <code>/start</code>.",
                    parse_mode=ParseMode.HTML,
                )
                return
            await callback.message.edit_text(
                _free_tables_message_html(rows),
                parse_mode=ParseMode.HTML,
                reply_markup=_free_tables_keyboard(rows),
            )


@router.callback_query(F.data.startswith("tsdy:"))
async def tables_delete_yes(callback: CallbackQuery, oauth: OAuthService, sheets: SheetsClient) -> None:
    uid = callback.from_user.id if callback.from_user else 0
    try:
        row_id = int((callback.data or "").split(":", 1)[1])
    except (IndexError, ValueError):
        await callback.answer("⚠️ Ошибка запроса")
        return
    row = await get_user_spreadsheet_row(uid, row_id)
    if row is None:
        await callback.answer("📭 Уже удалена")
        return
    if not is_owned_sheet(row):
        await callback.answer("🚫 Только владелец может удалить таблицу")
        return
    spreadsheet_id = str(row["spreadsheet_id"])
    auth = await authorized_sheets(uid, oauth, sheets)
    if auth is None:
        await callback.answer("👉 Сначала /start")
        return
    _user, user_sheets = auth
    try:
        await asyncio.to_thread(user_sheets.delete_spreadsheet, spreadsheet_id)
        await delete_all_spreadsheet_rows_for_file(spreadsheet_id, uid)
        await clear_last_transaction_batch(uid)
    except Exception as exc:
        if oauth.is_invalid_grant(exc):
            if callback.message:
                await handle_invalid_grant(callback.message, uid)
            else:
                await clear_oauth_tokens(uid)
            return
        log.exception("tables_delete_yes")
        await callback.answer("😕 Не удалось удалить")
        return
    finally:
        user_sheets.close()
    await callback.answer("🗑 Удалено")
    if callback.message:
        user = await get_user(uid)
        rows = await list_user_spreadsheets(uid)
        if rows and user and user_paid(user):
            rm: InlineKeyboardMarkup = _tables_manage_keyboard(rows)
        elif rows:
            rm = _free_tables_keyboard(rows)
        else:
            rm = buy_button_markup()
        await callback.message.edit_text(
            "<b>🗑 Таблица удалена</b>",
            parse_mode=ParseMode.HTML,
            reply_markup=rm,
        )
