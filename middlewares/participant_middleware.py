from logging import debug
from typing import Awaitable, Callable, Dict, Any
from aiogram import BaseMiddleware
from aiogram.fsm.context import FSMContext
from aiogram.types import Message
from aiogram.fsm.storage.base import StorageKey

from db.db import Manager as DbManager
from handlers.participant import cmd_register_participant


class ParticipantMiddleware(BaseMiddleware):
  def __init__(self, storage: DbManager):
    self.storage = storage

  async def __call__(
    self,
    handler: Callable[[Message, Dict[str, Any]], Awaitable[Any]],
    message: Message,
    data: Dict[str, Any]
  ) -> Any:
    db_manager = data["db_manager"]

    telegram_username = message.from_user.username
    telegram_id = message.from_user.id

    async with db_manager.use_connection() as conn:
      participant = await db_manager.find_participant(conn, telegram_username)

    if participant == None:
      debug('Unknown participant. Invoke register command')
      await message.answer(text="Вы не зарегистрированы")

      storage_key = StorageKey(
        bot_id=message.bot.id, chat_id=message.chat.id, user_id=message.from_user.id)
      new_context = FSMContext(self.storage, storage_key)
      res = await cmd_register_participant(message, new_context, db_manager)
    else:
      debug(
        f'Identified as {participant[3]} {participant[2]} ({participant[4]})')
      data["participant"] = participant
      res = await handler(message, data)
      debug(f'Handler {res}')
    return res
