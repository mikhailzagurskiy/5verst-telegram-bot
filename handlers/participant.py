from aiogram import Router, F, Bot
from aiogram.types import Message
from aiogram.filters import Command, StateFilter
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.context import FSMContext

from db.pool import Connection
from db.db import Manager as DBManager
from handlers.common import HandlerStatus

router = Router()


class ParticipantStatus:
  name = 'Participant'

  def failed():
    return HandlerStatus.failed(ParticipantStatus.name)

  def in_progress():
    return HandlerStatus.in_progress(ParticipantStatus.name)

  def done():
    return HandlerStatus.done(ParticipantStatus.name)


class ParticipantRegistration(StatesGroup):
  use_profile_data = State()
  entering_name = State()
  entering_surname = State()
  entering_age = State()


@router.message(StateFilter(None), Command("register"))
async def cmd_register_participant(message: Message, state: FSMContext, db_manager: DBManager):
  telegram_username = message.from_user.username
  telegram_id = message.from_user.id
  if telegram_username == None:
    await message.answer(text=f"Невозможно получить username пользователя")
    return ParticipantStatus.failed()

  async with db_manager.use_connection() as conn:
    participant = await db_manager.find_participant(conn, telegram_username)

    if participant != None:
      await message.answer(text=f"Вы уже зарегистрированы как {participant[3]} {participant[2]} ({participant[4]})")
      return ParticipantStatus.failed()
    else:
      id = await db_manager.register_participant(conn, telegram_id, telegram_username)
      await state.update_data(id=id)

  if message.from_user.first_name:
    await message.answer(text=f"Вас зовут '{message.from_user.last_name} {message.from_user.first_name}'?")
    await state.set_state(ParticipantRegistration.use_profile_data)
  else:
    await message.answer(text=f"Назовите ваше имя")
    await state.set_state(ParticipantRegistration.entering_name)

  return ParticipantStatus.in_progress()


@router.message(ParticipantRegistration.use_profile_data, (F.text.lower() == 'да') | (F.text.lower() == 'yes') | (F.text.lower() == 'y') | (F.text.lower() == 'д') | (F.text.lower() == 'ага') | (F.text.lower() == 'угу') | (F.text.lower() == 'yep'))
async def confirm_profile_data(message: Message, state: FSMContext):
  await state.update_data(name=message.from_user.first_name, surname=message.from_user.last_name)
  await message.answer(text="Назовите ваш возраст")
  await state.set_state(ParticipantRegistration.entering_age)
  return ParticipantStatus.in_progress()


@router.message(ParticipantRegistration.use_profile_data)
async def decline_profile_data(message: Message, state: FSMContext):
  await message.answer(text=f"Назовите ваше имя")
  await state.set_state(ParticipantRegistration.entering_name)
  return ParticipantStatus.in_progress()


@router.message(ParticipantRegistration.entering_name, (F.text.len() > 2) & F.text.regexp(r"^([А-я ]+)$"))
async def name_entered(message: Message, state: FSMContext):
  await state.update_data(name=message.text)
  await message.answer(text=f"Назовите вашу фамилию")
  await state.set_state(ParticipantRegistration.entering_surname)
  return ParticipantStatus.in_progress()


@router.message(ParticipantRegistration.entering_name)
async def wrong_name_entered(message: Message):
  await message.answer(text="Неверно введено имя. Попробуйте ещё раз")

  return ParticipantStatus.in_progress()


@router.message(ParticipantRegistration.entering_surname, (F.text.len() > 2) & F.text.regexp(r"^([А-я ]+)$"))
async def surname_entered(message: Message, state: FSMContext):
  await state.update_data(surname=message.text)
  await message.answer(text="Назовите ваш возраст")
  await state.set_state(ParticipantRegistration.entering_age)

  return ParticipantStatus.in_progress()


@router.message(ParticipantRegistration.entering_surname)
async def wrong_surname_entered(message: Message):
  await message.answer(text="Неверно введена фамилия. Попробуйте ещё раз")

  return ParticipantStatus.in_progress()


@router.message(ParticipantRegistration.entering_age, (F.text.len() > 1) & F.text.regexp(r"^(\d+)$"))
async def age_entered(message: Message, state: FSMContext, db_manager: DBManager):
  try:
    age = int(message.text)
  except:
    # TODO
    pass

  await state.update_data(age=age)
  participant = await state.get_data()
  async with db_manager.use_connection() as conn:
    await db_manager.update_participant(conn, participant['id'], participant['name'], participant['surname'], participant['age'])

  await message.answer(text=f"Добро пожаловать {participant['surname']} {participant['name']} ({participant['age']} лет)")

  await state.clear()

  return ParticipantStatus.done()


@router.message(ParticipantRegistration.entering_age)
async def wrong_age_entered(message: Message):
  await message.answer(text="Неверно введён возраст. Попробуйте ещё раз")
