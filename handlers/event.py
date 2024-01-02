from datetime import datetime

from aiogram import Router, F, Bot
from aiogram.types import Message
from aiogram.filters import Command, StateFilter, CommandObject
from aiogram.fsm.context import FSMContext

from db.db import Manager as DBManager
from handlers.common import HandlerStatus

router = Router()


class EventStatus:
  name = 'Event'

  def failed():
    return HandlerStatus.failed(EventStatus.name)

  def in_progress():
    return HandlerStatus.in_progress(EventStatus.name)

  def done():
    return HandlerStatus.done(EventStatus.name)


@router.message(Command("create_event"))
async def cmd_create_event(message: Message, command: CommandObject, db_manager: DBManager):
  if command.args is None:
    await message.answer(text=f"Укажите дату")
    return EventStatus.failed()

  date = command.args

  parsed = True
  try:
    datetime.strptime(date, '%d.%m.%Y')
  except:
    parsed = False

  if not parsed:
    await message.answer(text=f"Неверный формат даты")
    return EventStatus.failed()

  async with db_manager.use_connection() as conn:
    await db_manager.register_event(conn, date)

  await message.answer(text=f"Забег на {date} создан")

  return EventStatus.done()


@router.message(Command("list_events"))
async def cmd_list_events(message: Message, db_manager: DBManager):
  async with db_manager.use_connection() as conn:
    events = await db_manager.list_events(conn)

  text_events = "\n".join([f"[{event[0]}]: {event[1]}" for event in events])

  await message.answer(text=f"Список ивентов:\n\n{text_events}")

  return EventStatus.done()


@router.message(Command("delete_events"))
async def cmd_list_events(message: Message, db_manager: DBManager):
  pass
