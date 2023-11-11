import logging
import asyncio
import os

from aiogram import Bot, Dispatcher, types
from aiogram.filters.command import Command

API_TOKEN = os.environ['API_TOKEN']

logging.basicConfig(level=logging.INFO)

bot = Bot(token=API_TOKEN)
dp = Dispatcher()

@dp.message(Command("start"))
async def cmd_start(msg: types.Message):
    await msg.answer("Start message")


async def main():
  await dp.start_polling(bot)

if __name__ == '__main__':
  asyncio.run(main())