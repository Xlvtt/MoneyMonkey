from __future__ import annotations

import json
import logging
import os
from collections import deque
from datetime import datetime
from typing import Any, Literal

from langchain_core.messages import AIMessage, HumanMessage, SystemMessage, ToolMessage
from langchain_openai import ChatOpenAI

from moneymonkey.agent.context import AgentRunContext, agent_ctx_reset, agent_ctx_token
from moneymonkey.agent.tools import build_agent_tools

log = logging.getLogger(__name__)

_TOOL_ARGS_LOG_MAX = 8000
_MAX_AGENT_TOOL_ROUNDS = 15
_DIALOG_TURN_MAX = 5
_dialog_history: dict[int, deque[tuple[Literal["user", "assistant"], str]]] = {}


def _history_for_uid(uid: int) -> deque[tuple[Literal["user", "assistant"], str]]:
    d = _dialog_history.get(uid)
    if d is None:
        d = deque(maxlen=_DIALOG_TURN_MAX)
        _dialog_history[uid] = d
    return d


def _tool_call_args_repr(args: Any, *, max_len: int = _TOOL_ARGS_LOG_MAX) -> str:
    try:
        s = json.dumps(args, ensure_ascii=False, default=str, sort_keys=True)
    except (TypeError, ValueError):
        s = repr(args)
    if len(s) > max_len:
        return f"{s[: max_len - 24]} … [truncated]"
    return s


HF_OPENAI_BASE_URL = os.getenv("HF_OPENAI_BASE_URL", "https://router.huggingface.co/v1")
HF_CHAT_MODEL = os.getenv("HF_CHAT_MODEL", "openai/gpt-oss-120b:fastest")

SYSTEM_PROMPT_TOOLS = """Ты — финансовый ассистент MoneyMonkey (учёт расходов и доходов в Google Таблицах).

Текущие дата и время: {now}

## Роль и границы
Помогай с учётом денег, командами бота, форматом сообщений и таблицей категорий.
На темы вне личных финансов и работы этого бота не отвечай — вежливо откажись.
Перед текущим сообщением пользователя в запросе могут идти до пяти предыдущих реплик вашего диалога (чередование пользователь и ассистент) — учитывай их для связности ответа.

## Команды бота (в подсказках пользователю используй их дословно)
- `/start` — подключение Google, создание таблицы, если её ещё нет; повторная привязка аккаунта.
- `/help` — краткая справка в чате.
- `/month`, `/week`, `/day` — баланс за текущий месяц, последние 7 календарных дней (включая сегодня) или за сегодня; к команде можно добавить имя таблицы (`/month Имя` и т.д.).
- `/tarifs` или `/payment` — тарифы и оформление подписки.
- `/tables` — меню таблиц: ссылки, создание; на платном тарифе — несколько таблиц, включение/выключение, удаление своей через кнопки.
- `/on Имя` и `/off Имя` — включить или выключить таблицу с указанным именем (платные возможности).
- `/settings` — настройки: смена имени для записей в таблицу; для Premium — AI-режим (все сообщения через ассистента или классический парсер как в Pro).
- `/del` — отменить последнюю записанную операцию.
- `/red` — режим замены последней операции: после команды пользователь отправляет новую транзакцию в обычном текстовом формате.

## Как устроен парсинг обычных сообщений (классический режим)
Пользователь отправляет **одну строку текста** (не начинающуюся с `/`). Бот разбирает:
1. **Сумму** — число в рублях.
2. **Категорию** — по названию подкатегории и **синонимам** с листа «Категории» в Google Таблице пользователя; совпадение по словам и фразам из строки.
3. **Доход или расход** — задаётся **настройками категории в таблице** (отмечено ли «доход»/«расход» для этой подкатегории), а не только словами пользователя. Если категория в таблице допускает оба варианта — бот может запросить выбор кнопками.
4. **Дату** — если указана: форматы вроде ДД.ММ.ГГГГ, ДД.ММ (год — текущий); отдельное число может интерпретироваться как день текущего месяца. Без даты используется сегодняшняя дата.
5. **Комментарий** — остальной смысловой текст.
6. **Тег** — фрагменты вида `#название` в любом месте строки (на платных тарифах).
7. **Имя таблицы** — в конце строки можно указать имя таблицы из списка пользователя, чтобы записать только в неё (несколько таблиц — платная функция).

Примеры: `500 такси`, `30000 зарплата`, `500 кафе 15.04`, `500 кафе #отпуск`.

## Подсказки по командам
Если пользователь просит сделать то, что в боте выполняется **через команды** (отменить последнюю операцию, сменить имя, открыть тарифы, выключить таблицу, посмотреть баланс, заменить запись и т.д.) — **не придумывай новых действий и инструментов**: явно посоветуй нужную **команду**, кратко опиши **порядок шагов** (например: отправь `/del`; или сначала `/red`, затем одним сообщением новую сумму и категорию).

## Объяснение парсинга
Если спрашивают, **как работает разбор сообщений**, «как правильно написать», «почему не распознало» — опирайся на раздел выше: синонимы и названия с листа «Категории», сумма, дата, теги, имя таблицы в конце. Напомни, что список категорий и синонимов настраивается **в самой Google Таблице**.

## Инструменты
- **record_transaction** — записать транзакцию (сумма + смысл); имена подкатегорий — в описании инструмента.
- **fetch_transactions_data** — последние 50k строк одного листа (расходы или доходы), фильтры и агрегаты через pandasql; нужны `table` и `sheet_type`; список таблиц — в описании инструмента.
- **get_category_tree** — JSON: надкатегория → список подкатегорий, плюс ключ `"empty"` для подкатегорий без надкатегории / «Неизвестно». Опционально `spreadsheet_id`.
- **get_table_users** — JSON-массив `{{telegram_id, person_name}}` участников таблицы из БД бота (без запроса к Sheets). Опционально `spreadsheet_id`.

Справка по командам и парсингу без записи в таблицу — **без инструментов**, только текстом.
"""


def _now_str() -> str:
    return datetime.now().strftime("%d.%m.%Y %H:%M:%S")


def _make_llm() -> ChatOpenAI:
    api_key = (os.getenv("HF_TOKEN") or "").strip()
    if not api_key:
        raise RuntimeError("HF_TOKEN is not set")
    return ChatOpenAI(
        model=HF_CHAT_MODEL,
        api_key=api_key,
        base_url=HF_OPENAI_BASE_URL,
        temperature=float(os.getenv("HF_CHAT_TEMPERATURE", "0.3")),
    )


async def run_financial_assistant_async(ctx: AgentRunContext, user_text: str) -> str | None:
    text = (user_text or "").strip()
    if not text:
        return "Напиши вопрос про учёт или транзакцию."

    names = sorted({s.name.strip() for s in ctx.subs if s.name.strip()})
    categories_lines = "\n".join(f"• {n}" for n in names) or "• (список пуст)"
    table_lines = "\n".join(
        f"• {(t.get('name') or '').strip() or '(без имени)'} — spreadsheet_id: `{t.get('spreadsheet_id', '')}`"
        for t in ctx.targets
    ) or "• (нет подключённых таблиц)"
    tools = build_agent_tools(categories_lines, table_lines)
    tool_by_name = {t.name: t for t in tools}

    llm = _make_llm()
    llm_tools = llm.bind_tools(tools)

    tok = agent_ctx_token(ctx)
    hist = _history_for_uid(ctx.uid)
    try:
        sys_content = SYSTEM_PROMPT_TOOLS.format(now=_now_str())
        msgs: list[Any] = [SystemMessage(content=sys_content)]
        for role, body in hist:
            if role == "user":
                msgs.append(HumanMessage(content=body))
            else:
                msgs.append(AIMessage(content=body))
        msgs.append(HumanMessage(content=text))
        cur: Any = await llm_tools.ainvoke(msgs)
        rounds = 0
        while isinstance(cur, AIMessage) and cur.tool_calls:
            rounds += 1
            if rounds > _MAX_AGENT_TOOL_ROUNDS:
                log.warning(
                    "agent tool rounds exceeded max uid=%s rounds=%s",
                    ctx.uid,
                    rounds,
                )
                break
            msgs.append(cur)
            for tc in cur.tool_calls:
                name = tc.get("name")
                tid = tc.get("id") or ""
                args = tc.get("args") or {}
                if not args and tc.get("function"):
                    raw = (tc["function"] or {}).get("arguments") or "{}"
                    try:
                        args = json.loads(raw) if isinstance(raw, str) else raw
                    except json.JSONDecodeError:
                        args = {}
                chosen = tool_by_name.get(name or "")
                if chosen is None:
                    msgs.append(
                        ToolMessage(
                            content="Неизвестный инструмент.",
                            tool_call_id=tid,
                        )
                    )
                    continue
                log.info(
                    "tool_call uid=%s name=%s tool_call_id=%s args=%s",
                    ctx.uid,
                    name,
                    tid,
                    _tool_call_args_repr(args),
                )
                try:
                    out = await chosen.ainvoke(args)
                except Exception as e:
                    log.exception("tool %s", name)
                    out = f"Ошибка инструмента: {e!s}"
                msgs.append(ToolMessage(content=str(out), tool_call_id=tid))
            cur = await llm_tools.ainvoke(msgs)

        if isinstance(cur, AIMessage):
            final = (
                cur.content
                if isinstance(cur.content, str)
                else (str(cur.content) if cur.content else "")
            )
        else:
            final = str(cur) if cur else ""

        if ctx.category_prompt_sent or ctx.transaction_completed:
            reply_to_user = None
        else:
            reply_to_user = (final or "").strip() or None

        hist.append(("user", text))
        if reply_to_user:
            hist.append(("assistant", reply_to_user))
        return reply_to_user
    finally:
        agent_ctx_reset(tok)
