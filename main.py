import logging
import asyncio
import os

from aiogram import Bot, Dispatcher, types
from aiogram.filters.command import Command

from db.config import Config as DBConfig
from db.db import Manager as DBManager

from handlers.event_volunteer import router as event_volunteer_router
from handlers.participant import router as participant_router
from handlers.common import router as common_router

from middlewares.db_middleware import DBMiddleware

from settings import Settings


async def main():
  settings = Settings()

  db_manager = DBManager(settings.db_config)
  await db_manager.setup()

  BOT_TOKEN = settings.bot_token.get_secret_value()
  bot = Bot(token=BOT_TOKEN)

  dp = Dispatcher()
  dp.update.outer_middleware(DBMiddleware(db_manager))
  dp.include_router(participant_router)

  await bot.delete_webhook(drop_pending_updates=True)
  try:
    await dp.start_polling(bot)
  finally:
    await db_manager.close()

if __name__ == '__main__':
  logging.basicConfig(level=logging.DEBUG)
  asyncio.run(main())
