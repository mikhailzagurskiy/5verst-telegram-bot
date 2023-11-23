from aiogram import Router, F, Bot
from aiogram.types import Message
from aiogram.filters import Command, StateFilter
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.context import FSMContext

router = Router()


class ParticipantRegistration(StatesGroup):
  entering_name = State()
  entering_surname = State()
  entering_age = State()


@router.message(StateFilter(None), Command("register"))
async def cmd_register_participant(message: Message, state: FSMContext):
  await message.answer(text="Ваше имя:")
  await state.set_state(ParticipantRegistration.entering_name)


@router.message(ParticipantRegistration.entering_name, (F.text.len() > 2) & F.text.regexp(r"^([А-я ]+)$"))
async def name_entered(message: Message, state: FSMContext):
  await state.update_data(entered_name=message.text)
  await message.answer(text="Ваша фамилия:")
  await state.set_state(ParticipantRegistration.entering_surname)


@router.message(ParticipantRegistration.entering_name)
async def wrong_name_entered(message: Message):
  await message.answer(text="Неверно введено имя. Попробуйте ещё раз")


@router.message(ParticipantRegistration.entering_surname, (F.text.len() > 2) & F.text.regexp(r"^([А-я ]+)$"))
async def surname_entered(message: Message, state: FSMContext):
  await state.update_data(entered_surname=message.text)
  await message.answer(text="Ваш возраст:")
  await state.set_state(ParticipantRegistration.entering_age)


@router.message(ParticipantRegistration.entering_surname)
async def wrong_surname_entered(message: Message):
  await message.answer(text="Неверно введена фамилия. Попробуйте ещё раз")


@router.message(ParticipantRegistration.entering_age, (F.text.len() > 1) & F.text.regexp(r"^(\d+)$"))
async def age_entered(message: Message, state: FSMContext):
  try:
    age = int(message.text)
  except:
    # TODO
    pass
  await state.update_data(entered_age=age)
  participant = await state.get_data()
  await message.answer(text=f"Добро пожаловать {participant['entered_surname']} {participant['entered_name']} ({participant['entered_age']} лет)")
  await state.clear()


@router.message(ParticipantRegistration.entering_age)
async def wrong_age_entered(message: Message):
  await message.answer(text="Неверно введён возраст. Попробуйте ещё раз")
