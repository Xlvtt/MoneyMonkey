from __future__ import annotations

import asyncio
import logging

from moneymonkey.db import save_last_transaction_batch
from moneymonkey.parser import ParsedTransaction, format_added_at, format_tx_date
from moneymonkey.sheets import EXPENSES_TITLE, INCOMES_TITLE, SheetsClient, TxContext

log = logging.getLogger(__name__)


async def _tag_cell_for_append(
    user_sheets: SheetsClient,
    spreadsheet_id: str,
    raw_tag: str | None,
    paid: bool,
) -> str:
    if not paid or not (raw_tag or "").strip():
        return ""
    return await asyncio.to_thread(user_sheets.ensure_tag, spreadsheet_id, raw_tag.strip())


async def append_parsed_batch(
    uid: int,
    user_sheets: SheetsClient,
    targets: list[dict],
    person_name: str,
    paid: bool,
    command_text: str,
    parsed: ParsedTransaction,
    tx_ctx: TxContext,
) -> list[dict]:
    if parsed.requires_type_choice:
        raise ValueError("requires_type_choice")
    sheet_title = INCOMES_TITLE if parsed.is_income else EXPENSES_TITLE
    type_label = "Доходы" if parsed.is_income else "Расходы"
    next_id = tx_ctx.next_income_id if parsed.is_income else tx_ctx.next_expense_id
    txd = format_tx_date(parsed.tx_date)
    added = format_added_at()
    refs: list[dict] = []
    for t in targets:
        sid = str(t["spreadsheet_id"])
        tag_cell = await _tag_cell_for_append(user_sheets, sid, parsed.tag, paid)
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
            command=command_text,
            note=parsed.comment,
            tag=tag_cell,
            next_id=next_id,
        )
        if row_1based is not None:
            refs.append(
                {"spreadsheet_id": sid, "sheet_title": sheet_title, "row_1based": row_1based}
            )
        next_id += 1
    await save_last_transaction_batch(uid, refs)
    return refs
