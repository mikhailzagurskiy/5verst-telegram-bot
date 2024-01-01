from aiogram import F, Router
from aiogram.filters import Command
from aiogram.filters import StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import default_state
from aiogram.types import Message, ReplyKeyboardRemove

from enum import Enum


class Status(Enum):
  UNDEFINED = 0
  FAILED = 1
  IN_PROGRESS = 2
  DONE = 3


class HandlerStatus:
  def __init__(self, status: Status, handler_name: str):
    self.status = status
    self.name = handler_name

  def __repr__(self):
    return f'{self.name} is {self.status}'

  def failed(handler_name: str):
    return HandlerStatus(Status.FAILED, handler_name)

  def in_progress(handler_name: str):
    return HandlerStatus(Status.IN_PROGRESS, handler_name)

  def done(handler_name: str):
    return HandlerStatus(Status.DONE, handler_name)

  def is_failed(self):
    return self.status == Status.FAILED

  def is_done(self):
    return self.status == Status.DONE


router = Router()


@router.message(StateFilter(None), Command(commands=["cancel"]))
@router.message(default_state, F.text.lower() == "отмена")
async def cmd_cancel_no_state(message: Message, state: FSMContext):
  await state.set_data({})
  await message.answer(
      text="Нечего отменять",
      reply_markup=ReplyKeyboardRemove()
  )


@router.message(Command(commands=["cancel"]))
@router.message(F.text.lower() == "отмена")
async def cmd_cancel(message: Message, state: FSMContext):
  await state.clear()
  await message.answer(
      text="Действие отменено",
      reply_markup=ReplyKeyboardRemove()
  )
