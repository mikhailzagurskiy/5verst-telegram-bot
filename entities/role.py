from datetime import date
from entities.common import as_bool, as_int


class Role:
  id: int
  name: str
  emoji: str
  is_default: bool

  def __init__(self, id: int,
               name: str,
               emoji: str,
               is_default: bool):
    self.id = id
    self.name = name
    self.emoji = emoji
    self.is_default = is_default

  def __str__(self):
    default = f'DEFAULT' if self.is_default else ''
    return f'[{self.id}] {self.name} {self.emoji} {default}'

  def from_db_row(item):
    if not item:
      return

    id = as_int(item[0])
    name = item[1]
    emoji = item[2]
    is_default = as_bool(item[3])

    return Role(id, name, emoji, is_default)
