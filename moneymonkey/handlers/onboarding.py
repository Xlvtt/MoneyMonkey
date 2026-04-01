from __future__ import annotations

import asyncio
import logging

from html import escape

from aiogram import F
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Message

from moneymonkey.auth import OAuthService
from moneymonkey.db import (
    SUBSCRIPTION_FREE,
    SUBSCRIPTION_PREMIUM,
    SUBSCRIPTION_PRO,
    add_user_spreadsheet,
    get_user,
    list_user_spreadsheets,
    set_person_name,
    set_person_name_if_empty,
    set_subscription_level,
    upsert_user,
)
from moneymonkey.sheets import SheetsClient

from moneymonkey.handlers.common import (
    NameStates,
    answer_with_buy,
    authorized_sheets,
    handle_invalid_grant,
    html_link,
    name_prompt_markup,
    name_prompt_text,
    spreadsheet_file_title,
    tarifs_keyboard,
    tarifs_text,
    router,
)

log = logging.getLogger(__name__)


@router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext, oauth: OAuthService, sheets: SheetsClient) -> None:
    await state.clear()
    uid = message.from_user.id if message.from_user else 0
    user = await get_user(uid)
    sheet_rows = await list_user_spreadsheets(uid)

    if sheet_rows:
        first_url = str(sheet_rows[0]["spreadsheet_url"])
        await message.answer(
            "<b>✅ Google уже подключён</b>\n\n"
            f"🔗 {html_link('Открыть таблицу', first_url)}\n\n"
            "<i>Новая таблица с нуля: <code>/tables</code> → «Удалить», затем снова <code>/start</code></i>",
            parse_mode=ParseMode.HTML,
        )
        if not user or not user.get("person_name"):
            await message.answer(
                name_prompt_text(),
                reply_markup=name_prompt_markup(),
                parse_mode=ParseMode.HTML,
            )
        return

    if user and user.get("refresh_token_enc"):
        try:
            auth = await authorized_sheets(uid, oauth, sheets)
            if auth is None:
                raise RuntimeError("Missing auth row")
            user_row, user_sheets = auth
            try:
                sid, url = await asyncio.to_thread(
                    user_sheets.create_spreadsheet_for_user, spreadsheet_file_title("Моя таблица")
                )
                await add_user_spreadsheet(uid, "Моя таблица", sid, url, is_enabled=True)
                await upsert_user(uid, email=user_row.get("email"))
                await message.answer(
                    "<b>🎉 Таблица создана!</b>\n\n"
                    f"🔗 {html_link('Открыть в Google Sheets', url)}",
                    parse_mode=ParseMode.HTML,
                )
                return
            finally:
                user_sheets.close()
        except Exception as exc:
            if oauth.is_invalid_grant(exc):
                await handle_invalid_grant(message, uid)
                return
            log.exception("cmd_start create table failed")
            await message.answer(
                "<b>😕 Не удалось создать таблицу</b>\n\nПопробуй позже.",
                parse_mode=ParseMode.HTML,
            )
            return

    auth_url = oauth.build_authorization_url(uid)
    msg = await message.answer(
        "<b>📎 Подключи Google</b>\n\n"
        f"👉 {html_link('Перейти к авторизации', auth_url)}\n\n"
        "<i>После входа бот сам создаст таблицу и пришлёт ссылку.</i>",
        parse_mode=ParseMode.HTML,
    )
    oauth.remember_onboarding_message_id(uid, msg.message_id)


@router.message(Command("payment"))
async def cmd_payment(message: Message) -> None:
    await answer_with_buy(
        message,
        tarifs_text(),
        include_buy=False,
        parse_mode=ParseMode.HTML,
        reply_markup=tarifs_keyboard(),
    )


@router.message(Command("tarifs"))
async def cmd_tarifs(message: Message) -> None:
    await answer_with_buy(
        message,
        tarifs_text(),
        include_buy=False,
        parse_mode=ParseMode.HTML,
        reply_markup=tarifs_keyboard(),
    )


@router.callback_query(F.data == "buy_open")
async def on_buy_open(callback: CallbackQuery) -> None:
    await callback.answer()
    if callback.message:
        await callback.message.answer(
            tarifs_text(),
            parse_mode=ParseMode.HTML,
            reply_markup=tarifs_keyboard(),
        )


@router.callback_query(F.data == "tarif_base")
async def on_tarif_base(callback: CallbackQuery) -> None:
    uid = callback.from_user.id if callback.from_user else 0
    await set_subscription_level(uid, SUBSCRIPTION_FREE)
    await callback.answer("🆓 Базовый тариф")
    if callback.message:
        await callback.message.answer(
            "<b>🆓 Базовый тариф</b>\n\nОсновные функции доступны — пользуйся ботом!",
            parse_mode=ParseMode.HTML,
        )


@router.callback_query(F.data == "tarif_pro")
async def on_tarif_pro(callback: CallbackQuery) -> None:
    uid = callback.from_user.id if callback.from_user else 0
    await set_subscription_level(uid, SUBSCRIPTION_PRO)
    await callback.answer("⭐ Pro активирован!")
    if callback.message:
        await callback.message.answer(
            "<b>⭐ Pro активирован!</b>\n\n"
            "Доступно: несколько таблиц, совместный доступ и теги <i>(режим MVP)</i>.",
            parse_mode=ParseMode.HTML,
        )


@router.callback_query(F.data == "tarif_premium")
async def on_tarif_premium(callback: CallbackQuery) -> None:
    uid = callback.from_user.id if callback.from_user else 0
    await set_subscription_level(uid, SUBSCRIPTION_PREMIUM)
    await callback.answer("💎 Premium активирован!")
    if callback.message:
        await callback.message.answer(
            "<b>💎 Premium активирован!</b>\n\n"
            "Доступно всё из Pro, плюс <b>AI-ассистент</b> в чате: если бот не распознал сообщение как транзакцию, "
            "ответит финансовый помощник.",
            parse_mode=ParseMode.HTML,
        )


@router.callback_query(F.data == "name_from_tg")
async def on_name_from_tg(callback: CallbackQuery, state: FSMContext) -> None:
    uid = callback.from_user.id if callback.from_user else 0
    tg_name = (callback.from_user.full_name or "").strip() if callback.from_user else ""
    if not tg_name:
        await callback.answer("😕 В профиле нет имени")
        return
    if await state.get_state() == NameStates.waiting_new_name.state:
        await set_person_name(uid, tg_name)
        await state.clear()
        await callback.answer("✅ Имя обновлено!")
        if callback.message:
            await callback.message.answer(
                f"<b>✅ Готово!</b>\n\nТеперь ты в системе как <b>{escape(tg_name)}</b>.",
                parse_mode=ParseMode.HTML,
            )
        return

    changed = await set_person_name_if_empty(uid, tg_name)
    if changed:
        await callback.answer("✅ Имя сохранено!")
        if callback.message:
            await callback.message.answer(
                f"<b>👋 Добро пожаловать, {escape(tg_name)}!</b>\n\n"
                "Краткая справка — команда <code>/help</code>.",
                parse_mode=ParseMode.HTML,
            )
    else:
        await callback.answer("🔒 Имя уже зафиксировано")
