from __future__ import annotations

import re
from html import escape

# Разделитель для временных плейсхолдеров готовых HTML-кусков (U+2063 INVISIBLE SEPARATOR)
_PH = "\u2063"


def markdown_to_telegram_html(text: str) -> str:
    if not text:
        return ""

    placeholders: list[str] = []

    def stash(html: str) -> str:
        placeholders.append(html)
        return f"{_PH}PH{len(placeholders) - 1}{_PH}"

    s = text.replace("\r\n", "\n")

    s = re.sub(
        r"```(?:\w*\n)?([\s\S]*?)```",
        lambda m: stash("<pre>" + escape(m.group(1).rstrip("\n")) + "</pre>"),
        s,
    )

    s = re.sub(
        r"`([^`\n]+)`",
        lambda m: stash("<code>" + escape(m.group(1)) + "</code>"),
        s,
    )

    def link_repl(m: re.Match[str]) -> str:
        label = escape(m.group(1))
        url = m.group(2).strip()
        if url.startswith(("http://", "https://")):
            u = escape(url, quote=True)
            return stash(f'<a href="{u}">{label}</a>')
        return escape(m.group(0))

    s = re.sub(r"\[([^\]]*)\]\(([^)]+)\)", link_repl, s)

    while True:
        m = re.search(r"\*\*([^*]+)\*\*", s)
        if not m:
            break
        inner = escape(m.group(1))
        s = s[: m.start()] + stash(f"<b>{inner}</b>") + s[m.end() :]

    while True:
        m = re.search(r"(?<!\*)\*([^*\n]+?)\*(?!\*)", s)
        if not m:
            break
        inner = escape(m.group(1))
        s = s[: m.start()] + stash(f"<i>{inner}</i>") + s[m.end() :]

    lines_out: list[str] = []
    for line in s.split("\n"):
        hm = re.match(r"^(#{1,6})\s+(.*)$", line)
        if hm:
            lines_out.append("<b>" + escape(hm.group(2).strip()) + "</b>")
            continue
        if re.match(r"^[\-\*]\s+", line):
            rest = re.sub(r"^[\-\*]\s+", "", line, count=1)
            lines_out.append("• " + rest)
            continue
        lines_out.append(line)
    s = "\n".join(lines_out)

    parts = re.split(rf"({_PH}PH\d+{_PH})", s)
    out: list[str] = []
    for p in parts:
        mm = re.fullmatch(rf"{_PH}PH(\d+){_PH}", p)
        if mm:
            out.append(placeholders[int(mm.group(1))])
        else:
            out.append(escape(p))
    return "".join(out)


def parse_query_reply_html(
    type_label: str,
    parent_category: str,
    subcategory: str,
    amount: float,
    txd: str,
    note: str,
    *,
    success_title: str = "Записано",
    tag: str | None = None,
) -> str:
    text = f"<b>✅ {escape(success_title)}</b> в {escape(type_label.lower())}\n"
    text += f"📁 <b>Надкатегория:</b> {escape(parent_category)}\n"
    text += f"📂 <b>Категория:</b> {escape(subcategory)}\n"
    text += f"💰 <b>Сумма:</b> {amount:g} ₽\n"
    text += f"📅 <b>Дата:</b> {escape(txd)}\n"
    if tag:
        text += f"🏷 <b>Тег:</b> {escape(tag)}\n"
    if note:
        text += f"📝 <b>Примечание:</b> {escape(note)}\n"
    return text
