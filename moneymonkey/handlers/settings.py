from __future__ import annotations

from aiogram import F
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from moneymonkey.db import get_user, set_user_ai_mode

from moneymonkey.handlers.common import (
    NameStates,
    change_name_prompt_text,
    name_prompt_markup,
    user_premium,
    router,
)


def _ai_stored_on(user: dict) -> bool:
    return int(user.get("ai_mode_enabled", 1) or 0) != 0


def _settings_text_html(user: dict) -> str:
    if user_premium(user):
        on = _ai_stored_on(user)
        status = "🟢 <b>ВКЛ</b>" if on else "🔴 <b>ВЫКЛ</b>"
        hint = "\n<i>Включено: все сообщения обрабатывает ассистент. Выключено: как в Pro — обычный парсер.</i>" if on else ""
        ai_block = f"<b>AI-режим:</b> {status}{hint}"
    else:
        ai_block = (
            "<b>AI-режим:</b> 🔴 <b>ВЫКЛ</b>\n"
            "<i>Включение в Premium: все сообщения обрабатывает ассистент вместо обычного парсера — "
            "<code>/tarifs</code>.</i>"
        )
    return f"<b>⚙️ Настройки</b>\n\n{ai_block}"


def _settings_keyboard(user: dict) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = [
        [InlineKeyboardButton(text="✏️ Изменить имя", callback_data="sett:name")],
    ]
    if user_premium(user):
        on = _ai_stored_on(user)
        toggle_text = "🔴 Выключить AI" if on else "🟢 Включить AI"
        rows.append([InlineKeyboardButton(text=toggle_text, callback_data="sett:aitog")])
    else:
        rows.append([InlineKeyboardButton(text="🤖 AI режим", callback_data="sett:aiprem")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


@router.message(Command("settings"))
async def cmd_settings(message: Message) -> None:
    uid = message.from_user.id if message.from_user else 0
    user = await get_user(uid)
    if user is None:
        await message.answer(
            "<b>👋 Сначала</b> выполни <code>/start</code>.",
            parse_mode=ParseMode.HTML,
        )
        return
    await message.answer(
        _settings_text_html(user),
        reply_markup=_settings_keyboard(user),
        parse_mode=ParseMode.HTML,
    )


@router.callback_query(F.data == "sett:name")
async def settings_change_name(callback: CallbackQuery, state: FSMContext) -> None:
    uid = callback.from_user.id if callback.from_user else 0
    user = await get_user(uid)
    if user is None or not user.get("refresh_token_enc"):
        await callback.answer("Сначала подключи Google: /start", show_alert=True)
        return
    await state.set_state(NameStates.waiting_new_name)
    await callback.answer()
    if callback.message:
        await callback.message.answer(
            change_name_prompt_text(),
            reply_markup=name_prompt_markup(),
            parse_mode=ParseMode.HTML,
        )


@router.callback_query(F.data == "sett:aiprem")
async def settings_ai_premium_only(callback: CallbackQuery) -> None:
    await callback.answer("Доступно только в Premium", show_alert=True)


@router.callback_query(F.data == "sett:aitog")
async def settings_ai_toggle(callback: CallbackQuery) -> None:
    uid = callback.from_user.id if callback.from_user else 0
    user = await get_user(uid)
    if not user or not user_premium(user):
        await callback.answer("⭐ Нужен Premium", show_alert=True)
        return
    new_on = not _ai_stored_on(user)
    await set_user_ai_mode(uid, new_on)
    user = await get_user(uid)
    if not user:
        await callback.answer("😕 Ошибка")
        return
    await callback.answer("🟢 Включено" if new_on else "🔴 Выключено")
    if callback.message:
        await callback.message.edit_text(
            _settings_text_html(user),
            parse_mode=ParseMode.HTML,
            reply_markup=_settings_keyboard(user),
        )
