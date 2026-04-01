from __future__ import annotations

from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any, Awaitable, Callable

_agent_ctx_var: ContextVar["AgentRunContext | None"] = ContextVar("moneymonkey_agent_ctx", default=None)


@dataclass
class AgentRunContext:
    uid: int
    user: dict
    message: Any
    user_sheets: Any
    oauth: Any
    targets: list[dict]
    person_name: str
    paid: bool
    command_text: str
    cat_sid: str
    tx_ctx: Any
    subs: list[Any]
    cats: list[str]
    note_pending_new_category: Callable[[dict], None]
    on_requires_type_choice: Callable[..., Awaitable[None]]
    ask_mode: str = "add"
    batch_refs: list | None = None
    category_prompt_sent: bool = False
    transaction_completed: bool = False


def get_agent_ctx() -> AgentRunContext | None:
    return _agent_ctx_var.get()


def agent_ctx_token(ctx: AgentRunContext | None):
    return _agent_ctx_var.set(ctx)


def agent_ctx_reset(token) -> None:
    _agent_ctx_var.reset(token)
