from datetime import date
from entities.common import as_bool, as_int


class Volunteer:
  event_id: int
  role_id: int
  participant_id: int

  def __init__(self,
               event_id: int,
               role_id: int,
               participant_id: int):
    self.event_id = event_id
    self.role_id = role_id
    self.participant_id = participant_id

  def __str__(self):
    return f'[{self.event_id}] {self.role_id} {self.participant_id}'

  def from_db_row(item):
    if not item:
      return

    event_id = as_int(item[0])
    role_id = as_int(item[1])
    participant_id = as_int(item[2])

    return Volunteer(event_id, role_id, participant_id)
