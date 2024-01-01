from logging import debug
from typing import Awaitable, Callable, Dict, Any
from aiogram import BaseMiddleware
from aiogram.types import Message

from db.db import Manager as DbManager


class DBMiddleware(BaseMiddleware):
  def __init__(self, db_manager: DbManager):
    self.db_manager = db_manager

  async def __call__(
    self,
    handler: Callable[[Message, Dict[str, Any]], Awaitable[Any]],
    event: Message,
    data: Dict[str, Any]
  ) -> Any:
    data["db_manager"] = self.db_manager
    res = await handler(event, data)
    debug(f'Handler {res}')
    return res
