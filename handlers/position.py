from aiogram import Router, F, Bot
from aiogram.filters import Command, StateFilter
from aiogram.types import Message
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.context import FSMContext

from db.db import Manager as DBManager
from handlers.common import HandlerStatus

router = Router()


class PositionStatus:
  name = 'Position'

  def failed():
    return HandlerStatus.failed(PositionStatus.name)

  def in_progress():
    return HandlerStatus.in_progress(PositionStatus.name)

  def done():
    return HandlerStatus.done(PositionStatus.name)


class PositionRegistration(StatesGroup):
  entering_name = State()
  entering_emoji = State()


@router.message(StateFilter(None), Command("create_position"))
async def cmd_create(message: Message, state: FSMContext):
  await message.answer(text=f"Название позиции:")
  await state.set_state(PositionRegistration.entering_name)

  return PositionStatus.in_progress()


@router.message(PositionRegistration.entering_name, (F.text.len() > 2))
async def name_entered(message: Message, state: FSMContext):
  await state.update_data(name=message.text)
  await message.answer(text=f"Emoji позиции:")
  await state.set_state(PositionRegistration.entering_emoji)

  return PositionStatus.in_progress()


@router.message(PositionRegistration.entering_name)
async def wrong_name_entered(message: Message):
  await message.answer(text="Неверное название. Попробуйте ещё раз")

  return PositionStatus.in_progress()


@router.message(PositionRegistration.entering_emoji, (F.text.len() > 1 & F.text.len() < 5))
async def emoji_entered(message: Message, state: FSMContext, db_manager: DBManager):
  await state.update_data(emoji=message.text)
  position = await state.get_data()

  # (TODO): Add confirmation request

  async with db_manager.use_connection() as conn:
    await db_manager.create_volunteer_position(conn, position["name"], position["emoji"])

  await message.answer(text=f"Позиция {position['name']}, {position['emoji']} создана")

  await state.clear()

  return PositionStatus.done()


@router.message(PositionRegistration.entering_emoji)
async def wrong_emoji_entered(message: Message):
  await message.answer(text="Неверное emoji. Попробуйте ещё раз")

  return PositionStatus.in_progress()


@router.message(Command("list_positions"))
async def cmd_list(message: Message, db_manager: DBManager):
  async with db_manager.use_connection() as conn:
    positions = await db_manager.list_volunteer_positions(conn)

  text_positions = "\n".join([
    f"[{pos[0]}]: {pos[1]} | {pos[2]} {'default' if pos[3] != 0 else ''}" for pos in positions])

  await message.answer(text=f"Список позиций:\n\n{text_positions}")

  return PositionStatus.done()


@router.message(Command("delete"))
async def cmd_delete(message: Message):
  pass
