from datetime import date
from entities.common import as_bool, as_int


class Participant:
  id: int
  telegram_nickname: str
  name: str
  surname: str
  age: int
  is_admin: bool
  verst_id: int

  def __init__(self, id: int, telegram_nickname: str, name: str, surname: str, age: int, is_admin: bool, verst_id: int):
    self.id = id
    self.telegram_nickname = telegram_nickname
    self.name = name
    self.surname = surname
    self.age = age
    self.is_admin = is_admin
    self.verst_id = verst_id

  def __str__(self):
    age = f'({self.age} лет)' if self.age else ''
    admin = f'ADMIN' if self.is_admin else ''
    verst = f'{self.verst_id}' if self.verst_id else ''
    return f'[{self.id}] {admin} {self.surname} {self.name} {age} @{self.telegram_nickname} {verst}'

  def from_db_row(item):
    if not item:
      return

    id = as_int(item[0])
    age = as_int(item[4])
    is_admin = as_bool(item[5])
    verst_id = as_int(item[6])

    return Participant(id, item[1], item[2], item[3], age, is_admin, verst_id)
