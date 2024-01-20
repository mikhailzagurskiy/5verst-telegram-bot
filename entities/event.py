from datetime import date, time
from entities.common import as_date, as_int, as_time


class Event:
  id: int
  event_date: date
  event_time: time
  description: str

  def __init__(self, id: int,
               event_date: date,
               event_time: time,
               description: str
               ):
    self.id = id
    self.event_date = event_date
    self.event_time = event_time
    self.description = description

  def __str__(self):
    return f'[{self.id}] {self.event_date.isoformat()} at {self.event_time.strftime("%H:%M")}'

  def from_db_row(item):
    if not item:
      return

    id = as_int(item[0])
    event_date = as_date(item[1])
    event_time = as_time(item[2])
    description = item[3]

    return Event(id, event_date, event_time, description)
