import asyncio
import logging

from aiogram import Bot, Dispatcher
from aiogram.dispatcher.middlewares.base import BaseMiddleware
from aiogram.fsm.storage.memory import MemoryStorage

from moneymonkey.auth import OAuthService, start_webapp
from moneymonkey.config import (
    BOT_TOKEN,
    OAUTH_CLIENT_SECRET_PATH,
    OAUTH_REDIRECT_URI,
    TOKEN_ENCRYPTION_KEY,
    validate_config,
)
from moneymonkey.db import init_db
from moneymonkey.handlers import router
from moneymonkey.sheets import SheetsClient


class ServicesMiddleware(BaseMiddleware):
    def __init__(self, sheets: SheetsClient, oauth: OAuthService) -> None:
        self._sheets = sheets
        self._oauth = oauth

    async def __call__(self, handler, event, data):
        data["sheets"] = self._sheets
        data["oauth"] = self._oauth
        return await handler(event, data)


async def main() -> None:
    validate_config()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    await init_db()
    oauth = OAuthService(OAUTH_CLIENT_SECRET_PATH, OAUTH_REDIRECT_URI, TOKEN_ENCRYPTION_KEY)
    sheets = SheetsClient()
    bot = Bot(BOT_TOKEN)
    dp = Dispatcher(storage=MemoryStorage())
    dp.update.middleware(ServicesMiddleware(sheets, oauth))
    dp.include_router(router)
    runner = await start_webapp(bot, oauth, sheets)
    try:
        await dp.start_polling(bot)
    finally:
        await runner.cleanup()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
