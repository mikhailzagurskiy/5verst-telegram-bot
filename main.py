import logging
import asyncio
import os

from aiogram import Bot, Dispatcher, types
from aiogram.filters.command import Command

from config_reader import config

BOT_TOKEN = config.bot_token.get_secret_value()

logging.basicConfig(level=logging.INFO)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

@dp.message(Command("start"))
async def cmd_start(msg: types.Message):
    await msg.answer("Start message")


async def main():
  await dp.start_polling(bot)

if __name__ == '__main__':
  asyncio.run(main())