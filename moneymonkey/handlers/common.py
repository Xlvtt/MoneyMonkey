from __future__ import annotations

import asyncio
import logging
import re
from datetime import datetime, timedelta
from typing import Literal

from html import escape
from urllib.parse import quote

from aiogram import F, Router
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandObject, CommandStart, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder

from moneymonkey.auth import OAuthService
from moneymonkey.db import (
    clear_oauth_tokens,
    find_spreadsheet_by_name,
    get_user,
    list_user_spreadsheets,
    set_user_telegram_username,
)
from moneymonkey.sheets import SheetsClient

log = logging.getLogger(__name__)

router = Router(name="moneymonkey")
_PENDING_TYPE_CHOICE: dict[int, dict] = {}


class RedoStates(StatesGroup):
    waiting_tx = State()


class NameStates(StatesGroup):
    waiting_new_name = State()


class CreateTableStates(StatesGroup):
    waiting_name = State()


class ShareStates(StatesGroup):
    waiting_grant_usernames = State()
    waiting_revoke_usernames = State()


MONTHS_RU = (
    "январь",
    "февраль",
    "март",
    "апрель",
    "май",
    "июнь",
    "июль",
    "август",
    "сентябрь",
    "октябрь",
    "ноябрь",
    "декабрь",
)


def _subscription_level(user: dict | None) -> int:
    if not user:
        return 0
    return int(user.get("subscription_level") or 0)


def user_paid(user: dict | None) -> bool:
    return _subscription_level(user) >= 1


def user_premium(user: dict | None) -> bool:
    return _subscription_level(user) >= 2


def user_ai_mode_enabled(user: dict | None) -> bool:
    if not user_premium(user):
        return False
    return int(user.get("ai_mode_enabled", 1) or 0) != 0


def is_owned_sheet(row: dict) -> bool:
    return row.get("share_owner_id") is None


def parse_telegram_usernames(text: str) -> list[str]:
    found = re.findall(r"(?:^|[\s,;]+)@?([a-zA-Z][a-zA-Z0-9_]{4,31})\b", text)
    seen: set[str] = set()
    out: list[str] = []
    for u in found:
        ul = u.lower()
        if ul not in seen:
            seen.add(ul)
            out.append(ul)
    return out


async def touch_username_from_event(from_user) -> None:
    if from_user and from_user.username:
        await set_user_telegram_username(from_user.id, from_user.username)


@router.message.middleware()
async def touch_username_message_mw(handler, event: Message, data):
    await touch_username_from_event(event.from_user)
    return await handler(event, data)


@router.callback_query.middleware()
async def touch_username_cq_mw(handler, event: CallbackQuery, data):
    await touch_username_from_event(event.from_user)
    return await handler(event, data)


def html_link(title: str, url: str) -> str:
    safe_href = quote(url, safe="/:?#[]@!$&'()*+,;=%")
    return f'<a href="{safe_href}">{escape(title)}</a>'


def spreadsheet_file_title(display_name: str) -> str:
    return f"MoneyMonkey - {display_name.strip()}"


async def resolve_transaction_targets(
    uid: int,
    user: dict,
    explicit_table_name: str | None,
) -> list[dict]:
    sheets = await list_user_spreadsheets(uid)
    if not sheets:
        raise ValueError("Нет таблиц. Нажми /start.")
    if explicit_table_name:
        row = await find_spreadsheet_by_name(uid, explicit_table_name.strip())
        if row is None:
            raise ValueError(f"Таблица «{explicit_table_name.strip()}» не найдена.")
        return [row]
    if user_paid(user):
        enabled = [s for s in sheets if s["is_enabled"]]
        if not enabled:
            raise ValueError(
                "Нет включённых таблиц. Включи хотя бы одну в /tables или укажи корректное имя таблицы в конце сообщения."
            )
        return enabled
    en = [s for s in sheets if s["is_enabled"]]
    return [en[0]] if en else [sheets[0]]


async def targets_from_batch(uid: int, batch: list[dict]) -> list[dict]:
    if not batch:
        return []
    sheets = await list_user_spreadsheets(uid)
    by_sid = {str(s["spreadsheet_id"]): s for s in sheets}
    out: list[dict] = []
    seen: set[str] = set()
    for r in batch:
        sid = str(r["spreadsheet_id"])
        if sid in seen:
            continue
        row = by_sid.get(sid)
        if row is not None:
            out.append(row)
            seen.add(sid)
    return out


def ref_from_append(spreadsheet_id: str, sheet_title: str, row_1based: int | None) -> dict | None:
    if row_1based is None:
        return None
    return {
        "spreadsheet_id": spreadsheet_id,
        "sheet_title": sheet_title,
        "row_1based": row_1based,
    }


def parse_error_reply(reason: str) -> str:
    return (
        "<b>🤔 Не разобрал сообщение</b>\n"
        f"<i>Причина:</i> {escape(reason)}"
    )


from moneymonkey.reply_format import parse_query_reply_html


def who_user(m: Message) -> str:
    return str(m.from_user.id) if m.from_user else "0"


def buy_button_markup() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="💳 Купить подписку", callback_data="buy_open")]]
    )


def merge_buy_button(reply_markup=None):
    if reply_markup is None:
        return buy_button_markup()
    if isinstance(reply_markup, InlineKeyboardMarkup):
        rows = [list(r) for r in reply_markup.inline_keyboard]
        has_buy = any(btn.callback_data == "buy_open" for row in rows for btn in row if btn.callback_data)
        if not has_buy:
            rows.append([InlineKeyboardButton(text="💳 Купить подписку", callback_data="buy_open")])
        return InlineKeyboardMarkup(inline_keyboard=rows)
    return reply_markup


async def answer_with_buy(message: Message, text: str, *, include_buy: bool = True, **kwargs):
    if include_buy:
        kwargs["reply_markup"] = merge_buy_button(kwargs.get("reply_markup"))
    return await _RAW_MESSAGE_ANSWER(message, text, **kwargs)


def tarifs_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🆓 Базовый", callback_data="tarif_base")],
            [InlineKeyboardButton(text="⭐ Pro", callback_data="tarif_pro")],
            [InlineKeyboardButton(text="💎 Premium", callback_data="tarif_premium")],
        ]
    )


def tarifs_text() -> str:
    return (
        "<b>💎 Тарифы MoneyMonkey</b>\n\n"
        "<b>🆓 Базовый</b>\n"
        "• Базовые команды и учёт транзакций\n"
        "• Одна Google Таблица\n\n"
        "<b>⭐ Pro</b>\n"
        "• Сколько угодно таблиц\n"
        "• Совместный доступ\n"
        "• Теги к транзакциям\n\n"
        "<b>💎 Premium</b>\n"
        "• Весь функционал Pro версии\n"
        "• AI-ассистент в чате (если сообщение не распознано как транзакция)\n\n"
        "<b>Выбери тариф:</b>"
    )


TARIF_CALLBACKS = {"tarif_base", "tarif_pro", "tarif_premium"}


def is_tarif_prompt_markup(markup) -> bool:
    if not isinstance(markup, InlineKeyboardMarkup):
        return False
    callbacks = {btn.callback_data for row in markup.inline_keyboard for btn in row if btn.callback_data}
    return bool(callbacks & TARIF_CALLBACKS)


_RAW_MESSAGE_ANSWER = Message.answer


async def message_answer_with_buy(self: Message, text: str, **kwargs):
    markup = kwargs.get("reply_markup")
    include_buy = True
    chat_id = getattr(self.chat, "id", None)
    if isinstance(chat_id, int):
        user_row = await get_user(chat_id)
        if user_row and int(user_row.get("subscription_level") or 0) >= 1:
            include_buy = False
    if include_buy and not is_tarif_prompt_markup(markup):
        kwargs["reply_markup"] = merge_buy_button(markup)
    return await _RAW_MESSAGE_ANSWER(self, text, **kwargs)


Message.answer = message_answer_with_buy


async def authorized_sheets(
    user_id: int,
    oauth: OAuthService,
    sheets_factory: SheetsClient,
) -> tuple[dict[str, str | None], SheetsClient] | None:
    user = await get_user(user_id)
    if user is None:
        return None
    refresh_enc = user["refresh_token_enc"]
    if not isinstance(refresh_enc, str) or not refresh_enc:
        return None
    access_plain = None
    if isinstance(user["access_token_enc"], str):
        access_plain = oauth.decrypt(user["access_token_enc"])
    refresh_plain = oauth.decrypt(refresh_enc)
    creds = oauth.credentials_from_tokens(access_plain, refresh_plain, user["token_expiry"])
    return user, sheets_factory.from_credentials(creds)


async def handle_invalid_grant(message: Message, user_id: int) -> None:
    await clear_oauth_tokens(user_id)
    await message.answer(
        "<b>🔐 Доступ Google отозван</b>\n\n"
        "Выполни <code>/start</code> и авторизуйся заново.",
        parse_mode=ParseMode.HTML,
    )


def name_prompt_text() -> str:
    return (
        "<b>👤 Как тебя звать?</b>\n\n"
        "Имя попадёт в колонку «Человек» в таблице. "
        "<b>После сохранения изменить его нельзя.</b>"
    )


def change_name_prompt_text() -> str:
    return "<b>✏️ Новое имя</b>\n\nНапиши текстом или нажми кнопку ниже ⬇️"


def name_prompt_markup() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="📱 Взять из Telegram", callback_data="name_from_tg")]]
    )


async def ask_type_choice(
    message: Message,
    *,
    user_id: int,
    mode: str,
    spreadsheet_targets: list[dict],
    batch_refs: list[dict] | None,
    text: str,
    tx_date: str,
    parent_category: str,
    subcategory: str,
    amount: float,
    person_name: str,
    added_at: str,
    note: str,
    tag: str | None = None,
    next_expense_id: int = 0,
    next_income_id: int = 0,
) -> None:
    _PENDING_TYPE_CHOICE[user_id] = {
        "mode": mode,
        "spreadsheet_targets": spreadsheet_targets,
        "batch_refs": batch_refs or [],
        "text": text,
        "tx_date": tx_date,
        "parent_category": parent_category,
        "subcategory": subcategory,
        "amount": str(amount),
        "person_name": person_name,
        "added_at": added_at,
        "note": note,
        "tag": tag or "",
        "next_expense_id": next_expense_id,
        "next_income_id": next_income_id,
    }
    kb = InlineKeyboardBuilder()
    kb.button(text="📈 Доход", callback_data=f"tx_type:income:{user_id}")
    kb.button(text="📉 Расход", callback_data=f"tx_type:expense:{user_id}")
    kb.adjust(2)
    await answer_with_buy(
        message,
        "<b>⚖️ Двойная подкатегория</b>\n\n"
        "Для этой подкатегории разрешены и доход, и расход.\n"
        "<b>Куда записать эту транзакцию?</b>",
        include_buy=False,
        parse_mode=ParseMode.HTML,
        reply_markup=kb.as_markup(),
    )


BalancePeriod = Literal["month", "week", "day"]


def _balance_period_start_and_caption(now: datetime, period: BalancePeriod) -> tuple[datetime, str]:
    if period == "month":
        start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        caption = f"{MONTHS_RU[now.month - 1]} {now.year}"
        return start, caption
    if period == "week":
        end_d = now.date()
        start_d = end_d - timedelta(days=6)
        start = datetime.combine(start_d, datetime.min.time())
        caption = f"{start_d.strftime('%d.%m')}–{end_d.strftime('%d.%m.%Y')}"
        return start, caption
    start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    caption = now.strftime("%d.%m.%Y")
    return start, caption


async def format_balance_block(
    user_sheets: SheetsClient,
    spreadsheet_id: str,
    *,
    table_title: str | None,
    now: datetime,
    period: BalancePeriod = "month",
) -> str:
    start, caption = _balance_period_start_and_caption(now, period)
    income, expense = await asyncio.to_thread(user_sheets.monthly_totals, spreadsheet_id, start, now)
    diff = income - expense
    sign_diff = "+" if diff >= 0 else ""
    head = f"<b>📊 {escape(table_title)}</b>\n" if table_title else ""
    if period == "month":
        label = f"за {escape(caption)}"
    elif period == "week":
        label = f"за 7 дней ({escape(caption)})"
    else:
        label = f"за день {escape(caption)}"
    return (
        f"{head}"
        f"<b>Баланс</b> {label}:\n"
        f"📈 <b>Доходы:</b> +{income:g} ₽\n"
        f"📉 <b>Расходы:</b> −{expense:g} ₽\n"
        f"⚖️ <b>Разница:</b> {sign_diff}{diff:g} ₽"
    )
