from __future__ import annotations

import asyncio
import logging

from html import escape

from aiogram import F
from aiogram.enums import ParseMode
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from moneymonkey.auth import OAuthService
from moneymonkey.db import (
    add_user_spreadsheet,
    clear_oauth_tokens,
    create_sheet_invitation,
    delete_sheet_invitation,
    delete_user_spreadsheet_by_id,
    find_shared_row_for_user,
    get_sheet_invitation,
    get_telegram_id_by_username_lower,
    get_user,
    get_user_spreadsheet_row,
)
from moneymonkey.sheets import SheetsClient

from moneymonkey.handlers.common import (
    ShareStates,
    authorized_sheets,
    handle_invalid_grant,
    is_owned_sheet,
    parse_telegram_usernames,
    user_paid,
    router,
)

log = logging.getLogger(__name__)

@router.callback_query(F.data.startswith("tssg:"))
async def tables_share_grant_start(callback: CallbackQuery, state: FSMContext) -> None:
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
    if row is None or not is_owned_sheet(row):
        await callback.answer("🚫 Недоступно")
        return
    await state.set_state(ShareStates.waiting_grant_usernames)
    await state.update_data(share_row_id=row_id, share_spreadsheet_id=str(row["spreadsheet_id"]))
    await callback.answer()
    if callback.message:
        await callback.message.answer(
            "<b>🔓 Совместный доступ</b>\n\n"
            "Отправь список Telegram-никнеймов через пробел или с новой строки, например:\n"
            "<code>@friend @colleague</code>\n\n"
            "У каждого приглашённого должна быть <b>активная платная подписка</b>. "
            "Пользователь должен хотя раз написать боту, чтобы мы знали его ник.",
            parse_mode=ParseMode.HTML,
        )


@router.callback_query(F.data.startswith("tssr:"))
async def tables_share_revoke_start(callback: CallbackQuery, state: FSMContext) -> None:
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
    if row is None or not is_owned_sheet(row):
        await callback.answer("🚫 Недоступно")
        return
    await state.set_state(ShareStates.waiting_revoke_usernames)
    await state.update_data(share_row_id=row_id, share_spreadsheet_id=str(row["spreadsheet_id"]))
    await callback.answer()
    if callback.message:
        await callback.message.answer(
            "<b>🔒 Отзыв доступа</b>\n\n"
            "Перечисли никнеймы через пробел, у кого отозвать доступ к этой таблице:\n"
            "<code>@user1 @user2</code>",
            parse_mode=ParseMode.HTML,
        )


@router.message(ShareStates.waiting_grant_usernames, F.text)
async def share_grant_usernames(message: Message, state: FSMContext) -> None:
    uid = message.from_user.id if message.from_user else 0
    user = await get_user(uid)
    if not user or not user_paid(user):
        await state.clear()
        await message.answer(
            "<b>⭐ Совместный доступ</b> — только с подпиской.\n👉 <code>/tarifs</code>",
            parse_mode=ParseMode.HTML,
        )
        return
    data = await state.get_data()
    row_id = int(data.get("share_row_id") or 0)
    row = await get_user_spreadsheet_row(uid, row_id)
    if row is None or not is_owned_sheet(row):
        await state.clear()
        return

    usernames = parse_telegram_usernames(message.text or "")
    if not usernames:
        await message.answer(
            "<b>⚠️ Не нашёл никнеймов</b>\n\n"
            "Укажи через пробел, например: <code>@friend</code>",
            parse_mode=ParseMode.HTML,
        )
        return

    invited_ok: list[tuple[str, int, int]] = []
    no_sub: list[str] = []
    not_in_bot: list[str] = []

    for un in usernames:
        tid = await get_telegram_id_by_username_lower(un)
        if tid is None:
            not_in_bot.append(un)
            continue
        if tid == uid:
            continue
        invitee = await get_user(tid)
        if not user_paid(invitee):
            no_sub.append(un)
            continue
        existing = await find_shared_row_for_user(tid, uid, str(row["spreadsheet_id"]))
        if existing:
            continue
        inv_id = await create_sheet_invitation(
            uid,
            row_id,
            tid,
            str(row["spreadsheet_id"]),
            str(row["name"]),
        )
        invited_ok.append((un, tid, inv_id))

    owner = await get_user(uid)
    owner_un = (str(owner.get("telegram_username") or "").strip().lower() if owner else "")
    owner_label = f"@{owner_un}" if owner_un else escape(str(owner.get("person_name") or "Владелец"))

    bot = message.bot
    for un, tid, inv_id in invited_ok:
        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(text="✅ Принять", callback_data=f"shiny:{inv_id}"),
                    InlineKeyboardButton(text="❌ Отклонить", callback_data=f"shind:{inv_id}"),
                ]
            ]
        )
        await bot.send_message(
            tid,
            "<b>👥 Приглашение в таблицу</b>\n\n"
            f"{owner_label} предлагает совместный доступ к «<b>{escape(str(row['name']))}</b>».\n\n"
            "<b>Принять?</b>",
            parse_mode=ParseMode.HTML,
            reply_markup=kb,
        )

    parts: list[str] = []
    if invited_ok:
        ats = ", ".join(f"@{x[0]}" for x in invited_ok)
        parts.append(f"<b>✅ Отправил приглашение пользователям:</b> {ats}")
    if no_sub:
        ns = ", ".join(f"@{x}" for x in no_sub)
        parts.append(
            f"<b>⚠️ У пользователей {ns} не обнаружена подписка</b> — для совместного доступа она нужна "
            "<b>каждому</b>. Попросите их приобрести подписку и попробуйте пригласить снова! "
            "Я буду с радостью их ждать!"
        )
    if not_in_bot:
        nb = ", ".join(f"@{x}" for x in not_in_bot)
        parts.append(
            f"<b>😕 Не найдены в боте:</b> {nb}\n"
            "Пусть хотя раз напишут боту (например, <code>/start</code>), чтобы мы узнали их ник."
        )
    if parts:
        await message.answer("\n\n".join(parts), parse_mode=ParseMode.HTML)
    else:
        await message.answer(
            "<b>ℹ️ Некого пригласить</b>\n\n"
            "Проверь ники, подписку и то, что у людей ещё нет доступа к этой таблице.",
            parse_mode=ParseMode.HTML,
        )
    await state.clear()


@router.message(ShareStates.waiting_revoke_usernames, F.text)
async def share_revoke_usernames(
    message: Message, state: FSMContext, oauth: OAuthService, sheets: SheetsClient
) -> None:
    uid = message.from_user.id if message.from_user else 0
    user = await get_user(uid)
    if not user or not user_paid(user):
        await state.clear()
        await message.answer(
            "<b>⭐ Отзыв доступа</b> — только с подпиской.\n👉 <code>/tarifs</code>",
            parse_mode=ParseMode.HTML,
        )
        return
    data = await state.get_data()
    row_id = int(data.get("share_row_id") or 0)
    spreadsheet_id = str(data.get("share_spreadsheet_id") or "")
    row = await get_user_spreadsheet_row(uid, row_id)
    if row is None or not is_owned_sheet(row) or str(row["spreadsheet_id"]) != spreadsheet_id:
        await state.clear()
        return

    usernames = parse_telegram_usernames(message.text or "")
    if not usernames:
        await message.answer(
            "<b>⚠️ Не нашёл никнеймов</b>",
            parse_mode=ParseMode.HTML,
        )
        return

    revoked: list[str] = []
    no_access: list[str] = []
    unknown: list[str] = []

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
        for un in usernames:
            tid = await get_telegram_id_by_username_lower(un)
            if tid is None:
                unknown.append(un)
                continue
            sh = await find_shared_row_for_user(tid, uid, spreadsheet_id)
            if not sh:
                no_access.append(un)
                continue
            pid = sh.get("drive_permission_id")
            if pid:
                try:
                    await asyncio.to_thread(user_sheets.remove_permission, spreadsheet_id, str(pid))
                except Exception as exc:
                    if oauth.is_invalid_grant(exc):
                        await handle_invalid_grant(message, uid)
                        return
                    log.exception("remove_permission revoke")
            await delete_user_spreadsheet_by_id(int(sh["id"]))
            revoked.append(un)
    finally:
        user_sheets.close()

    parts: list[str] = []
    if revoked:
        rs = ", ".join(f"@{x}" for x in revoked)
        parts.append(f"<b>🔒 Доступ отозван:</b> {rs}")
    if no_access:
        na = ", ".join(f"@{x}" for x in no_access)
        parts.append(f"<b>ℹ️ Доступа к этой таблице не было:</b> {na}")
    if unknown:
        uk = ", ".join(f"@{x}" for x in unknown)
        parts.append(f"<b>😕 Не найдены в боте:</b> {uk}")
    if parts:
        await message.answer("\n\n".join(parts), parse_mode=ParseMode.HTML)
    else:
        await message.answer(
            "<b>ℹ️ Ничего не изменилось</b>\n\nПроверь ники — доступ можно отозвать только у тех, кому он был выдан.",
            parse_mode=ParseMode.HTML,
        )
    await state.clear()


@router.callback_query(F.data.startswith("shiny:"))
async def share_invite_accept(
    callback: CallbackQuery, oauth: OAuthService, sheets: SheetsClient
) -> None:
    uid = callback.from_user.id if callback.from_user else 0
    try:
        inv_id = int((callback.data or "").split(":", 1)[1])
    except (IndexError, ValueError):
        await callback.answer("⚠️ Ошибка")
        return
    inv = await get_sheet_invitation(inv_id)
    if inv is None or int(inv["invitee_telegram_id"]) != uid:
        await callback.answer("🚫 Недоступно")
        return

    owner_id = int(inv["owner_telegram_id"])
    if await find_shared_row_for_user(uid, owner_id, str(inv["spreadsheet_id"])):
        await delete_sheet_invitation(inv_id)
        await callback.answer("Доступ уже есть")
        if callback.message:
            await callback.message.edit_text(
                "<b>✅ Эта таблица уже в твоём списке</b>",
                parse_mode=ParseMode.HTML,
            )
        return

    invitee = await get_user(uid)
    if not invitee or not invitee.get("refresh_token_enc"):
        await callback.answer("Сначала подключи Google: /start", show_alert=True)
        return
    email = (invitee.get("email") or "").strip()
    if not email:
        await callback.answer("Нет email от Google — пройди /start заново", show_alert=True)
        return
    if not user_paid(invitee):
        await callback.answer("Нужна платная подписка: /tarifs", show_alert=True)
        return

    owner_row = await get_user_spreadsheet_row(owner_id, int(inv["owner_sheet_row_id"]))
    if owner_row is None or not is_owned_sheet(owner_row):
        await delete_sheet_invitation(inv_id)
        await callback.answer("Приглашение устарело")
        return

    auth = await authorized_sheets(owner_id, oauth, sheets)
    if auth is None:
        await callback.answer("Владелец не авторизован — попроси пригласить снова", show_alert=True)
        return
    _ou, owner_sheets = auth
    try:
        perm_id = await asyncio.to_thread(
            owner_sheets.share_with_email,
            str(inv["spreadsheet_id"]),
            email,
        )
        await add_user_spreadsheet(
            uid,
            str(owner_row["name"]),
            str(inv["spreadsheet_id"]),
            str(owner_row["spreadsheet_url"]),
            is_enabled=True,
            share_owner_id=owner_id,
            drive_permission_id=perm_id,
        )
        await delete_sheet_invitation(inv_id)
    except Exception as exc:
        if oauth.is_invalid_grant(exc):
            if callback.message:
                await handle_invalid_grant(callback.message, owner_id)
            await callback.answer("Ошибка доступа владельца")
            return
        log.exception("share_invite_accept")
        await callback.answer("Не удалось получить доступ, попробуйте еще раз!", show_alert=True)
        return
    finally:
        owner_sheets.close()

    await callback.answer("✅ Готово!")
    if callback.message:
        await callback.message.edit_text(
            "<b>✅ Доступ к таблице получен</b>\n\nОна появится в <code>/tables</code>.",
            parse_mode=ParseMode.HTML,
        )

    inv_un = (callback.from_user.username or "").strip().lower()
    inv_label = f"@{inv_un}" if inv_un else escape(str(invitee.get("person_name") or str(uid)))
    await callback.bot.send_message(
        owner_id,
        f"<b>✅ Приглашение принято</b>\n\n{inv_label} принял(а) доступ к «<b>{escape(str(inv['table_name']))}</b>».",
        parse_mode=ParseMode.HTML,
    )


@router.callback_query(F.data.startswith("shind:"))
async def share_invite_decline(callback: CallbackQuery) -> None:
    uid = callback.from_user.id if callback.from_user else 0
    try:
        inv_id = int((callback.data or "").split(":", 1)[1])
    except (IndexError, ValueError):
        await callback.answer("⚠️ Ошибка")
        return
    inv = await get_sheet_invitation(inv_id)
    if inv is None or int(inv["invitee_telegram_id"]) != uid:
        await callback.answer("🚫 Недоступно")
        return

    owner_id = int(inv["owner_telegram_id"])
    await delete_sheet_invitation(inv_id)

    await callback.answer("Ок")
    if callback.message:
        await callback.message.edit_text(
            "<b>Приглашение отклонено</b>",
            parse_mode=ParseMode.HTML,
        )

    invitee = await get_user(uid)
    inv_un = (callback.from_user.username or "").strip().lower()
    inv_label = f"@{inv_un}" if inv_un else escape(str(invitee.get("person_name") or str(uid)))
    await callback.bot.send_message(
        owner_id,
        f"<b>❌ Приглашение не принято</b>\n\n{inv_label} отклонил(а) доступ к «<b>{escape(str(inv['table_name']))}</b>». "
        "Можно пригласить снова позже.",
        parse_mode=ParseMode.HTML,
    )

