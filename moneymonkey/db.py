import json
from datetime import datetime, timezone

import aiosqlite

from moneymonkey.config import DATABASE_PATH

SUBSCRIPTION_FREE = 0
SUBSCRIPTION_PRO = 1
SUBSCRIPTION_PREMIUM = 2

SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    telegram_id INTEGER PRIMARY KEY,
    email TEXT,
    person_name TEXT,
    access_token_enc TEXT,
    refresh_token_enc TEXT,
    token_expiry TEXT,
    subscription_level INTEGER NOT NULL DEFAULT 0,
    telegram_username TEXT,
    ai_mode_enabled INTEGER NOT NULL DEFAULT 1
);
"""

USER_SPREADSHEETS_SCHEMA = """
CREATE TABLE IF NOT EXISTS user_spreadsheets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    telegram_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    spreadsheet_id TEXT NOT NULL,
    spreadsheet_url TEXT NOT NULL,
    is_enabled INTEGER NOT NULL DEFAULT 1,
    share_owner_id INTEGER,
    drive_permission_id TEXT
);
"""

LAST_BATCH_SCHEMA = """
CREATE TABLE IF NOT EXISTS last_transaction_batch (
    telegram_id INTEGER PRIMARY KEY,
    refs_json TEXT NOT NULL
);
"""

SHEET_INVITATIONS_SCHEMA = """
CREATE TABLE IF NOT EXISTS sheet_invitations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    owner_telegram_id INTEGER NOT NULL,
    owner_sheet_row_id INTEGER NOT NULL,
    invitee_telegram_id INTEGER NOT NULL,
    spreadsheet_id TEXT NOT NULL,
    table_name TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(owner_sheet_row_id, invitee_telegram_id)
);
"""


async def _add_missing_columns(
    db: aiosqlite.Connection, table: str, specs: tuple[tuple[str, str], ...]
) -> None:
    cur = await db.execute(f"PRAGMA table_info({table})")
    have = {str(r[1]) for r in await cur.fetchall()}
    for col, decl in specs:
        if col not in have:
            await db.execute(f"ALTER TABLE {table} ADD COLUMN {col} {decl}")


async def init_db() -> None:
    DATABASE_PATH.parent.mkdir(parents=True, exist_ok=True)
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute(SCHEMA)
        await db.execute(USER_SPREADSHEETS_SCHEMA)
        await db.execute(LAST_BATCH_SCHEMA)
        await db.execute(SHEET_INVITATIONS_SCHEMA)
        await _add_missing_columns(
            db,
            "users",
            (
                ("email", "TEXT"),
                ("person_name", "TEXT"),
                ("access_token_enc", "TEXT"),
                ("refresh_token_enc", "TEXT"),
                ("token_expiry", "TEXT"),
                ("subscription_level", "INTEGER NOT NULL DEFAULT 0"),
                ("telegram_username", "TEXT"),
                ("ai_mode_enabled", "INTEGER NOT NULL DEFAULT 1"),
            ),
        )
        await _add_missing_columns(
            db,
            "user_spreadsheets",
            (
                ("share_owner_id", "INTEGER"),
                ("drive_permission_id", "TEXT"),
            ),
        )
        await db.commit()


async def get_user(telegram_id: int) -> dict[str, str | None] | None:
    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            """
            SELECT email, person_name, access_token_enc, refresh_token_enc, token_expiry
                 , subscription_level, telegram_username, COALESCE(ai_mode_enabled, 1) AS ai_mode_enabled
            FROM users
            WHERE telegram_id = ?
            """,
            (telegram_id,),
        )
        row = await cur.fetchone()
        if row is None:
            return None
        return {
            "email": row["email"],
            "person_name": row["person_name"],
            "access_token_enc": row["access_token_enc"],
            "refresh_token_enc": row["refresh_token_enc"],
            "token_expiry": row["token_expiry"],
            "subscription_level": int(row["subscription_level"] or 0),
            "telegram_username": row["telegram_username"],
            "ai_mode_enabled": int(row["ai_mode_enabled"]),
        }


async def upsert_user(
    telegram_id: int,
    email: str | None = None,
    person_name: str | None = None,
) -> None:
    email_v = email or ""
    person_name_v = person_name or ""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute(
            """
            INSERT INTO users (telegram_id, email, person_name)
            VALUES (?, ?, ?)
            ON CONFLICT(telegram_id) DO UPDATE SET
                email = excluded.email,
                person_name = CASE
                    WHEN users.person_name IS NULL OR users.person_name = '' THEN excluded.person_name
                    ELSE users.person_name
                END
            """,
            (telegram_id, email_v, person_name_v),
        )
        await db.commit()


async def save_oauth_tokens(
    telegram_id: int,
    access_token_enc: str | None,
    refresh_token_enc: str | None,
    token_expiry: str | None,
) -> None:
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute(
            """
            INSERT INTO users (
                telegram_id,
                email,
                person_name,
                access_token_enc,
                refresh_token_enc,
                token_expiry
            )
            VALUES (?, '', '', ?, ?, ?)
            ON CONFLICT(telegram_id) DO UPDATE SET
                access_token_enc = excluded.access_token_enc,
                refresh_token_enc = excluded.refresh_token_enc,
                token_expiry = excluded.token_expiry
            """,
            (telegram_id, access_token_enc, refresh_token_enc, token_expiry),
        )
        await db.commit()


async def clear_oauth_tokens(telegram_id: int) -> None:
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute("DELETE FROM last_transaction_batch WHERE telegram_id = ?", (telegram_id,))
        await db.execute(
            """
            UPDATE users
            SET access_token_enc = NULL,
                refresh_token_enc = NULL,
                token_expiry = NULL
            WHERE telegram_id = ?
            """,
            (telegram_id,),
        )
        await db.commit()


async def set_subscription_level(telegram_id: int, level: int) -> None:
    lv = max(SUBSCRIPTION_FREE, min(SUBSCRIPTION_PREMIUM, int(level)))
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute(
            """
            INSERT INTO users (telegram_id, email, subscription_level)
            VALUES (?, '', ?)
            ON CONFLICT(telegram_id) DO UPDATE SET
                subscription_level = excluded.subscription_level
            """,
            (telegram_id, lv),
        )
        await db.commit()


async def set_person_name_if_empty(telegram_id: int, person_name: str) -> bool:
    clean = person_name.strip()
    if not clean:
        return False
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute(
            """
            INSERT INTO users (telegram_id, email, person_name)
            VALUES (?, '', ?)
            ON CONFLICT(telegram_id) DO NOTHING
            """,
            (telegram_id, clean),
        )
        cur = await db.execute(
            """
            UPDATE users
            SET person_name = ?
            WHERE telegram_id = ?
              AND (person_name IS NULL OR person_name = '')
            """,
            (clean, telegram_id),
        )
        await db.commit()
        return cur.rowcount > 0


async def set_person_name(telegram_id: int, person_name: str) -> None:
    clean = person_name.strip()
    if not clean:
        return
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute(
            """
            INSERT INTO users (telegram_id, email, person_name)
            VALUES (?, '', ?)
            ON CONFLICT(telegram_id) DO UPDATE SET
                person_name = excluded.person_name
            """,
            (telegram_id, clean),
        )
        await db.commit()


async def set_user_ai_mode(telegram_id: int, enabled: bool) -> None:
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute(
            "UPDATE users SET ai_mode_enabled = ? WHERE telegram_id = ?",
            (1 if enabled else 0, telegram_id),
        )
        await db.commit()


async def list_user_spreadsheets(telegram_id: int) -> list[dict[str, int | str | bool | None]]:
    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            """
            SELECT id, telegram_id, name, spreadsheet_id, spreadsheet_url, is_enabled, share_owner_id, drive_permission_id
            FROM user_spreadsheets
            WHERE telegram_id = ?
            ORDER BY id ASC
            """,
            (telegram_id,),
        )
        rows = await cur.fetchall()
    out: list[dict[str, int | str | bool | None]] = []
    for r in rows:
        so = r["share_owner_id"]
        dp = r["drive_permission_id"]
        out.append(
            {
                "id": int(r["id"]),
                "telegram_id": int(r["telegram_id"]),
                "name": str(r["name"]),
                "spreadsheet_id": str(r["spreadsheet_id"]),
                "spreadsheet_url": str(r["spreadsheet_url"]),
                "is_enabled": bool(r["is_enabled"]),
                "share_owner_id": int(so) if so is not None else None,
                "drive_permission_id": str(dp) if dp else None,
            }
        )
    return out


async def list_person_names_for_spreadsheet(spreadsheet_id: str) -> list[dict[str, int | str]]:
    sid = (spreadsheet_id or "").strip()
    if not sid:
        return []
    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            """
            SELECT DISTINCT us.telegram_id AS telegram_id,
                   COALESCE(NULLIF(TRIM(u.person_name), ''), '') AS person_name
            FROM user_spreadsheets us
            LEFT JOIN users u ON u.telegram_id = us.telegram_id
            WHERE us.spreadsheet_id = ?
            ORDER BY us.telegram_id
            """,
            (sid,),
        )
        rows = await cur.fetchall()
    return [
        {"telegram_id": int(r["telegram_id"]), "person_name": str(r["person_name"] or "").strip()}
        for r in rows
    ]


async def add_user_spreadsheet(
    telegram_id: int,
    name: str,
    spreadsheet_id: str,
    spreadsheet_url: str,
    *,
    is_enabled: bool = True,
    share_owner_id: int | None = None,
    drive_permission_id: str | None = None,
) -> int:
    async with aiosqlite.connect(DATABASE_PATH) as db:
        cur = await db.execute(
            """
            INSERT INTO user_spreadsheets (
                telegram_id, name, spreadsheet_id, spreadsheet_url, is_enabled, share_owner_id, drive_permission_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                telegram_id,
                name.strip(),
                spreadsheet_id,
                spreadsheet_url,
                1 if is_enabled else 0,
                share_owner_id,
                drive_permission_id,
            ),
        )
        await db.commit()
        return int(cur.lastrowid)


async def get_user_spreadsheet_row(telegram_id: int, row_id: int) -> dict[str, int | str | bool | None] | None:
    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            """
            SELECT id, telegram_id, name, spreadsheet_id, spreadsheet_url, is_enabled, share_owner_id, drive_permission_id
            FROM user_spreadsheets
            WHERE telegram_id = ? AND id = ?
            """,
            (telegram_id, row_id),
        )
        r = await cur.fetchone()
    if r is None:
        return None
    so = r["share_owner_id"]
    dp = r["drive_permission_id"]
    return {
        "id": int(r["id"]),
        "telegram_id": int(r["telegram_id"]),
        "name": str(r["name"]),
        "spreadsheet_id": str(r["spreadsheet_id"]),
        "spreadsheet_url": str(r["spreadsheet_url"]),
        "is_enabled": bool(r["is_enabled"]),
        "share_owner_id": int(so) if so is not None else None,
        "drive_permission_id": str(dp) if dp else None,
    }


async def find_spreadsheet_by_name(telegram_id: int, name: str) -> dict[str, int | str | bool] | None:
    target = name.strip().casefold()
    if not target:
        return None
    sheets = await list_user_spreadsheets(telegram_id)
    for s in sheets:
        if str(s["name"]).strip().casefold() == target:
            return s
    return None


async def set_spreadsheet_enabled(telegram_id: int, row_id: int, enabled: bool) -> bool:
    async with aiosqlite.connect(DATABASE_PATH) as db:
        cur = await db.execute(
            """
            UPDATE user_spreadsheets
            SET is_enabled = ?
            WHERE telegram_id = ? AND id = ?
            """,
            (1 if enabled else 0, telegram_id, row_id),
        )
        await db.commit()
        return cur.rowcount > 0


async def delete_user_spreadsheet_row(telegram_id: int, row_id: int) -> bool:
    async with aiosqlite.connect(DATABASE_PATH) as db:
        cur = await db.execute(
            "DELETE FROM user_spreadsheets WHERE telegram_id = ? AND id = ?",
            (telegram_id, row_id),
        )
        await db.commit()
        return cur.rowcount > 0


LastTxRef = dict[str, int | str]


async def save_last_transaction_batch(telegram_id: int, refs: list[LastTxRef]) -> None:
    payload = json.dumps(refs, ensure_ascii=False)
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute(
            """
            INSERT INTO last_transaction_batch (telegram_id, refs_json)
            VALUES (?, ?)
            ON CONFLICT(telegram_id) DO UPDATE SET refs_json = excluded.refs_json
            """,
            (telegram_id, payload),
        )
        await db.commit()


async def get_last_transaction_batch(telegram_id: int) -> list[LastTxRef]:
    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            "SELECT refs_json FROM last_transaction_batch WHERE telegram_id = ?",
            (telegram_id,),
        )
        row = await cur.fetchone()
    if row is None or not row["refs_json"]:
        return []
    try:
        data = json.loads(str(row["refs_json"]))
    except json.JSONDecodeError:
        return []
    if not isinstance(data, list):
        return []
    out: list[LastTxRef] = []
    for item in data:
        if not isinstance(item, dict):
            continue
        sid = item.get("spreadsheet_id")
        rw = item.get("row_1based")
        if not isinstance(sid, str) or not isinstance(rw, (int, float)):
            continue
        ref: LastTxRef = {"spreadsheet_id": sid, "row_1based": int(rw)}
        sh = item.get("sheet_id")
        st = item.get("sheet_title")
        if isinstance(sh, (int, float)):
            ref["sheet_id"] = int(sh)
        if isinstance(st, str):
            ref["sheet_title"] = st
        if "sheet_id" in ref or "sheet_title" in ref:
            out.append(ref)
    return out


async def clear_last_transaction_batch(telegram_id: int) -> None:
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute("DELETE FROM last_transaction_batch WHERE telegram_id = ?", (telegram_id,))
        await db.commit()


async def set_user_telegram_username(telegram_id: int, username: str | None) -> None:
    if not username:
        return
    clean = username.strip().lstrip("@").lower()
    if not clean:
        return
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute(
            """
            INSERT INTO users (telegram_id, email, telegram_username)
            VALUES (?, '', ?)
            ON CONFLICT(telegram_id) DO UPDATE SET
                telegram_username = excluded.telegram_username
            """,
            (telegram_id, clean),
        )
        await db.commit()


async def get_telegram_id_by_username_lower(username: str) -> int | None:
    clean = username.strip().lstrip("@").lower()
    if not clean:
        return None
    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            "SELECT telegram_id FROM users WHERE telegram_username = ? LIMIT 1",
            (clean,),
        )
        row = await cur.fetchone()
    return int(row["telegram_id"]) if row else None


async def is_paid_user(telegram_id: int) -> bool:
    u = await get_user(telegram_id)
    return bool(u and int(u.get("subscription_level") or 0) >= SUBSCRIPTION_PRO)


async def create_sheet_invitation(
    owner_telegram_id: int,
    owner_sheet_row_id: int,
    invitee_telegram_id: int,
    spreadsheet_id: str,
    table_name: str,
) -> int:
    created = datetime.now(timezone.utc).isoformat()
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute(
            "DELETE FROM sheet_invitations WHERE owner_sheet_row_id = ? AND invitee_telegram_id = ?",
            (owner_sheet_row_id, invitee_telegram_id),
        )
        cur = await db.execute(
            """
            INSERT INTO sheet_invitations (
                owner_telegram_id, owner_sheet_row_id, invitee_telegram_id, spreadsheet_id, table_name, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (owner_telegram_id, owner_sheet_row_id, invitee_telegram_id, spreadsheet_id, table_name, created),
        )
        await db.commit()
        return int(cur.lastrowid)


async def get_sheet_invitation(invitation_id: int) -> dict[str, int | str] | None:
    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            """
            SELECT id, owner_telegram_id, owner_sheet_row_id, invitee_telegram_id, spreadsheet_id, table_name, created_at
            FROM sheet_invitations
            WHERE id = ?
            """,
            (invitation_id,),
        )
        r = await cur.fetchone()
    if r is None:
        return None
    return {
        "id": int(r["id"]),
        "owner_telegram_id": int(r["owner_telegram_id"]),
        "owner_sheet_row_id": int(r["owner_sheet_row_id"]),
        "invitee_telegram_id": int(r["invitee_telegram_id"]),
        "spreadsheet_id": str(r["spreadsheet_id"]),
        "table_name": str(r["table_name"]),
        "created_at": str(r["created_at"]),
    }


async def delete_sheet_invitation(invitation_id: int) -> None:
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute("DELETE FROM sheet_invitations WHERE id = ?", (invitation_id,))
        await db.commit()


async def delete_invitations_for_spreadsheet(owner_telegram_id: int, spreadsheet_id: str) -> None:
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute(
            "DELETE FROM sheet_invitations WHERE owner_telegram_id = ? AND spreadsheet_id = ?",
            (owner_telegram_id, spreadsheet_id),
        )
        await db.commit()


async def list_spreadsheet_collaborators(
    owner_telegram_id: int,
    spreadsheet_id: str,
) -> list[dict[str, int | str | None]]:
    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            """
            SELECT us.telegram_id AS telegram_id, u.person_name, u.telegram_username
            FROM user_spreadsheets us
            LEFT JOIN users u ON u.telegram_id = us.telegram_id
            WHERE us.share_owner_id = ? AND us.spreadsheet_id = ?
            ORDER BY u.telegram_id ASC
            """,
            (owner_telegram_id, spreadsheet_id),
        )
        rows = await cur.fetchall()
    out: list[dict[str, int | str | None]] = []
    for r in rows:
        out.append(
            {
                "telegram_id": int(r["telegram_id"]),
                "person_name": str(r["person_name"]) if r["person_name"] else None,
                "telegram_username": str(r["telegram_username"]) if r["telegram_username"] else None,
            }
        )
    return out


async def find_shared_row_for_user(
    user_telegram_id: int,
    owner_telegram_id: int,
    spreadsheet_id: str,
) -> dict[str, int | str | bool | None] | None:
    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            """
            SELECT id, telegram_id, name, spreadsheet_id, spreadsheet_url, is_enabled, share_owner_id, drive_permission_id
            FROM user_spreadsheets
            WHERE telegram_id = ? AND share_owner_id = ? AND spreadsheet_id = ?
            LIMIT 1
            """,
            (user_telegram_id, owner_telegram_id, spreadsheet_id),
        )
        r = await cur.fetchone()
    if r is None:
        return None
    so = r["share_owner_id"]
    dp = r["drive_permission_id"]
    return {
        "id": int(r["id"]),
        "telegram_id": int(r["telegram_id"]),
        "name": str(r["name"]),
        "spreadsheet_id": str(r["spreadsheet_id"]),
        "spreadsheet_url": str(r["spreadsheet_url"]),
        "is_enabled": bool(r["is_enabled"]),
        "share_owner_id": int(so) if so is not None else None,
        "drive_permission_id": str(dp) if dp else None,
    }


async def delete_user_spreadsheet_by_id(row_id: int) -> bool:
    async with aiosqlite.connect(DATABASE_PATH) as db:
        cur = await db.execute("DELETE FROM user_spreadsheets WHERE id = ?", (row_id,))
        await db.commit()
        return cur.rowcount > 0


async def delete_all_spreadsheet_rows_for_file(spreadsheet_id: str, owner_telegram_id: int) -> None:
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute(
            """
            DELETE FROM user_spreadsheets
            WHERE spreadsheet_id = ? AND (telegram_id = ? OR share_owner_id = ?)
            """,
            (spreadsheet_id, owner_telegram_id, owner_telegram_id),
        )
        await db.execute(
            "DELETE FROM sheet_invitations WHERE owner_telegram_id = ? AND spreadsheet_id = ?",
            (owner_telegram_id, spreadsheet_id),
        )
        await db.commit()


