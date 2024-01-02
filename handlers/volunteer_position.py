from aiogram import Router, F, Bot
from aiogram.filters import Command, StateFilter
from aiogram.types import Message
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.context import FSMContext

from db.db import Manager as DBManager
from handlers.common import HandlerStatus

router = Router()


class VolunteerPositionStatus:
  name = 'VolunteerPosition'

  def failed():
    return HandlerStatus.failed(VolunteerPositionStatus.name)

  def in_progress():
    return HandlerStatus.in_progress(VolunteerPositionStatus.name)

  def done():
    return HandlerStatus.done(VolunteerPositionStatus.name)


class VolunteerPositionRegistration(StatesGroup):
  entering_name = State()
  entering_emoji = State()


@router.message(StateFilter(None), Command("create_position"))
async def cmd_create(message: Message, state: FSMContext):
  await message.answer(text=f"Название позиции:")
  await state.set_state(VolunteerPositionRegistration.entering_name)

  return VolunteerPositionStatus.in_progress()


@router.message(VolunteerPositionRegistration.entering_name, (F.text.len() > 2))
async def name_entered(message: Message, state: FSMContext):
  await state.update_data(name=message.text)
  await message.answer(text=f"Emoji позиции:")
  await state.set_state(VolunteerPositionRegistration.entering_emoji)

  return VolunteerPositionStatus.in_progress()


@router.message(VolunteerPositionRegistration.entering_name)
async def wrong_name_entered(message: Message):
  await message.answer(text="Неверное название. Попробуйте ещё раз")

  return VolunteerPositionStatus.in_progress()


@router.message(VolunteerPositionRegistration.entering_emoji, (F.text.len() > 1 & F.text.len() < 5))
async def emoji_entered(message: Message, state: FSMContext, db_manager: DBManager):
  await state.update_data(emoji=message.text)
  volunteer_position = await state.get_data()

  # (TODO): Add confirmation request

  async with db_manager.use_connection() as conn:
    await db_manager.create_volunteer_position(conn, volunteer_position["name"], volunteer_position["emoji"])

  await message.answer(text=f"Позиция {volunteer_position['name']}, {volunteer_position['emoji']} создана")

  await state.clear()

  return VolunteerPositionStatus.done()


@router.message(VolunteerPositionRegistration.entering_emoji)
async def wrong_emoji_entered(message: Message):
  await message.answer(text="Неверное emoji. Попробуйте ещё раз")

  return VolunteerPositionStatus.in_progress()


@router.message(Command("list_positions"))
async def cmd_list(message: Message, db_manager: DBManager):
  async with db_manager.use_connection() as conn:
    positions = await db_manager.list_volunteer_positions(conn)

  text_positions = "\n".join([
    f"[{pos[0]}]: {pos[1]} | {pos[2]} {'default' if pos[3] != 0 else ''}" for pos in positions])

  await message.answer(text=f"Список позиций:\n\n{text_positions}")

  return VolunteerPositionStatus.done()


@router.message(Command("delete"))
async def cmd_delete(message: Message):
  pass
