from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from html import escape
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from aiohttp import web
from aiogram.enums import ParseMode
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from cryptography.fernet import Fernet
from google.auth.exceptions import RefreshError
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google_auth_oauthlib.flow import Flow
from oauthlib.oauth2.rfc6749.errors import InvalidGrantError

from moneymonkey.config import WEBAPP_HOST, WEBAPP_PORT
from moneymonkey.db import (
    add_user_spreadsheet,
    clear_oauth_tokens,
    get_user,
    list_user_spreadsheets,
    save_oauth_tokens,
    upsert_user,
)
from moneymonkey.sheets import SheetsClient, SheetsSetupHttpError, _close_http_transport

log = logging.getLogger(__name__)

SCOPES = (
    "openid",
    "https://www.googleapis.com/auth/userinfo.email",
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.file",
)


@dataclass(frozen=True)
class OAuthTokens:
    access_token: str
    refresh_token: str | None
    expiry_iso: str | None
    email: str | None


class OAuthService:
    def __init__(self, client_secret_path: str | Path, redirect_uri: str, encryption_key: str) -> None:
        self._client_secret_path = str(client_secret_path)
        self._redirect_uri = redirect_uri
        self._cipher = Fernet(encryption_key.encode("utf-8"))
        self._code_verifiers: dict[str, str] = {}
        self._onboarding_message_ids: dict[int, int] = {}
        raw = json.loads(Path(self._client_secret_path).read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            raise RuntimeError("OAuth client secret JSON must be an object")
        if "web" in raw and isinstance(raw["web"], dict):
            cfg = raw["web"]
        elif "installed" in raw and isinstance(raw["installed"], dict):
            cfg = raw["installed"]
        else:
            cfg = raw
        self._token_uri = str(cfg["token_uri"])
        self._client_id = str(cfg["client_id"])
        self._client_secret = str(cfg["client_secret"])

    def _flow(self, state: str | None = None) -> Flow:
        flow = Flow.from_client_secrets_file(
            self._client_secret_path,
            scopes=SCOPES,
            state=state,
            redirect_uri=self._redirect_uri,
        )
        flow.redirect_uri = self._redirect_uri
        return flow

    def build_authorization_url(self, user_id: int) -> str:
        state = str(user_id)
        flow = self._flow(state)
        url, _ = flow.authorization_url(
            access_type="offline",
            include_granted_scopes="true",
            prompt="consent",
            state=state,
        )
        verifier = getattr(flow, "code_verifier", None)
        if isinstance(verifier, str) and verifier:
            self._code_verifiers[state] = verifier
        return url

    def exchange_code(self, code: str, state: str) -> OAuthTokens:
        flow = self._flow(state)
        creds: Credentials | None = None
        try:
            verifier = self._code_verifiers.pop(state, None)
            if not verifier:
                raise RuntimeError("Missing OAuth code_verifier. Start OAuth flow again.")
            flow.fetch_token(code=code, code_verifier=verifier)
            creds = flow.credentials
        finally:
            session = getattr(flow, "oauth2session", None)
            if session is not None:
                with contextlib.suppress(Exception):
                    session.close()

        if creds is None:
            raise RuntimeError("OAuth failed before credentials were issued.")

        oauth2 = build("oauth2", "v2", credentials=creds, cache_discovery=False)
        email: str | None = None
        try:
            info = oauth2.userinfo().get().execute()
            email_val = info.get("email")
            if isinstance(email_val, str):
                email = email_val
        except Exception:
            log.exception("oauth userinfo failed")
        finally:
            http = getattr(oauth2, "_http", None) or getattr(oauth2, "http", None)
            _close_http_transport(http)
            if hasattr(oauth2, "close"):
                with contextlib.suppress(Exception):
                    oauth2.close()

        expiry_iso = creds.expiry.isoformat() if creds.expiry else None
        return OAuthTokens(
            access_token=creds.token or "",
            refresh_token=creds.refresh_token,
            expiry_iso=expiry_iso,
            email=email,
        )

    def remember_onboarding_message_id(self, user_id: int, message_id: int) -> None:
        self._onboarding_message_ids[user_id] = message_id

    def get_onboarding_message_id(self, user_id: int) -> int | None:
        return self._onboarding_message_ids.get(user_id)

    def encrypt(self, text: str) -> str:
        return self._cipher.encrypt(text.encode("utf-8")).decode("utf-8")

    def decrypt(self, encrypted: str) -> str:
        return self._cipher.decrypt(encrypted.encode("utf-8")).decode("utf-8")

    def credentials_from_tokens(
        self,
        access_token: str | None,
        refresh_token: str,
        expiry_iso: str | None,
    ) -> Credentials:
        expiry = datetime.fromisoformat(expiry_iso) if expiry_iso else None
        return Credentials(
            token=access_token,
            refresh_token=refresh_token,
            token_uri=self._token_uri,
            client_id=self._client_id,
            client_secret=self._client_secret,
            scopes=SCOPES,
            expiry=expiry,
        )

    @staticmethod
    def is_invalid_grant(exc: Exception) -> bool:
        if isinstance(exc, InvalidGrantError):
            return True
        if isinstance(exc, RefreshError) and "invalid_grant" in str(exc).lower():
            return True
        if isinstance(exc, HttpError) and "invalid_grant" in (exc.reason or "").lower():
            return True
        return False


def build_oauth_app(bot, oauth: OAuthService, sheets: SheetsClient) -> web.Application:
    app = web.Application()
    routes = web.RouteTableDef()

    def _name_prompt_markup() -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="📱 Взять из Telegram", callback_data="name_from_tg")]]
        )

    def _table_ready_html(url: str, *, need_name: bool) -> str:
        link = f'<a href="{escape(url)}">Открыть таблицу</a>'
        body = f"<b>🎉 Google подключён!</b>\n\n🔗 {link}"
        if need_name:
            body += "\n\n<b>👤 Как тебя звать?</b> Имя попадёт в колонку «Человек»."
        return body

    async def _send_or_edit_onboarding(
        user_id: int,
        message_id: int | None,
        text: str,
        reply_markup: InlineKeyboardMarkup | None = None,
    ) -> None:
        if message_id is not None:
            try:
                await bot.edit_message_text(
                    chat_id=user_id,
                    message_id=message_id,
                    text=text,
                    parse_mode=ParseMode.HTML,
                    reply_markup=reply_markup,
                )
                return
            except Exception:
                log.exception("failed to edit onboarding message")
        sent = await bot.send_message(
            user_id, text, parse_mode=ParseMode.HTML, reply_markup=reply_markup
        )
        oauth.remember_onboarding_message_id(user_id, sent.message_id)

    @routes.get("/oauth2callback")
    async def oauth2callback(request: web.Request) -> web.Response:
        code = request.query.get("code")
        state = request.query.get("state")
        if not code or not state:
            return web.Response(text="Missing code/state", status=400)
        try:
            user_id = int(state)
        except ValueError:
            return web.Response(text="Invalid state", status=400)

        try:
            tokens = await asyncio.to_thread(oauth.exchange_code, code, state)
            access_enc = oauth.encrypt(tokens.access_token) if tokens.access_token else None
            refresh_enc = oauth.encrypt(tokens.refresh_token) if tokens.refresh_token else None
            await save_oauth_tokens(user_id, access_enc, refresh_enc, tokens.expiry_iso)
            user = await get_user(user_id)
            refresh_cipher = user["refresh_token_enc"] if user else None
            if refresh_cipher is None:
                raise RuntimeError("No refresh token returned by Google. Re-run consent.")
            refresh_plain = oauth.decrypt(refresh_cipher)
            access_plain = oauth.decrypt(user["access_token_enc"]) if user and user["access_token_enc"] else None
            creds = oauth.credentials_from_tokens(access_plain, refresh_plain, user["token_expiry"] if user else None)
            user_sheets = sheets.from_credentials(creds)
            try:
                person_name = user["person_name"] if user else None
                onboarding_message_id = oauth.get_onboarding_message_id(user_id)
                existing_sheets = await list_user_spreadsheets(user_id)
                if not existing_sheets:
                    sid, url = await asyncio.to_thread(
                        user_sheets.create_spreadsheet_for_user, "MoneyMonkey - Моя таблица"
                    )
                    await add_user_spreadsheet(user_id, "Моя таблица", sid, url, is_enabled=True)
                    await upsert_user(user_id, email=tokens.email)
                    spreadsheet_url = url
                else:
                    spreadsheet_url = str(existing_sheets[0]["spreadsheet_url"])
                    if tokens.email:
                        await upsert_user(user_id, email=tokens.email)
            finally:
                user_sheets.close()

            if person_name:
                await _send_or_edit_onboarding(
                    user_id,
                    onboarding_message_id if isinstance(onboarding_message_id, int) else None,
                    _table_ready_html(str(spreadsheet_url), need_name=False),
                )
            else:
                await _send_or_edit_onboarding(
                    user_id,
                    onboarding_message_id if isinstance(onboarding_message_id, int) else None,
                    _table_ready_html(str(spreadsheet_url), need_name=True),
                    reply_markup=_name_prompt_markup(),
                )
            return web.Response(text="Authorization successful. You can close this tab.")
        except SheetsSetupHttpError as exc:
            log.exception("oauth callback sheets error")
            await bot.send_message(
                user_id,
                "<b>⚠️ Авторизация ОК, таблица не создалась</b>\n\nПопробуй <code>/start</code> позже.",
                parse_mode=ParseMode.HTML,
            )
            return web.Response(text=f"Authorization completed, table creation failed: {exc}", status=500)
        except Exception as exc:
            if oauth.is_invalid_grant(exc):
                await clear_oauth_tokens(user_id)
                await bot.send_message(
                    user_id,
                    "<b>🔐 Сессия Google устарела</b>\n\nВыполни <code>/start</code> и войди снова.",
                    parse_mode=ParseMode.HTML,
                )
            elif "code_verifier" in str(exc):
                await bot.send_message(
                    user_id,
                    "<b>⏳ Ссылка устарела</b>\n\nНажми <code>/start</code> и открой новую.",
                    parse_mode=ParseMode.HTML,
                )
            log.exception("oauth callback failed")
            return web.Response(text="Authorization failed", status=500)

    app.add_routes(routes)
    return app


async def start_webapp(bot, oauth: OAuthService, sheets: SheetsClient) -> web.AppRunner:
    app = build_oauth_app(bot, oauth, sheets)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, WEBAPP_HOST, WEBAPP_PORT)
    await site.start()
    log.info("OAuth callback server started at %s:%s", WEBAPP_HOST, WEBAPP_PORT)
    return runner