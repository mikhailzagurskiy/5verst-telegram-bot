from db.pool import Connection

from entities.event import Event
from entities.common import as_int


class EventOrm:
  async def create(self, conn: Connection, event_date: str, event_time: str) -> int:
    async with conn.execute('''INSERT INTO Event(event_date, event_time) VALUES(:1, :2) RETURNING id''', [event_date, event_time]) as cursor:
      row = await cursor.fetchone()
      return as_int(row[0])

  async def list(self, conn: Connection) -> list[Event]:
    return [Event.from_db_row(row) for row in await conn.execute_fetchall('''SELECT * FROM Event''')]

  async def get(self, conn: Connection, event_id: int) -> Event:
    async with conn.execute('''SELECT * FROM Event WHERE id=:1''', [event_id]) as cursor:
      row = await cursor.fetchone()
      return Event.from_db_row(row)

  async def update(self, conn: Connection, event_id: int, **fields):
    if len(fields.keys()) == 0 or not event_id:
      return

    field_names, field_values = zip(*fields.items())
    field_names = [(f'{name} = :{idx + 1}')
                   for (idx, name) in enumerate(field_names)]

    field_values = list(field_values)
    id_pos = len(field_names) + 1
    field_values.append(event_id)

    field_names = ', '.join(field_names)
    sql = f'''UPDATE Event SET {field_names} WHERE id = :{id_pos}'''

    await conn.execute(sql, field_values)

  async def find(self, conn: Connection, **fields) -> Event:
    field_names, field_values = zip(*fields.items())
    field_names = [(f'{name}=:{idx + 1}')
                   for (idx, name) in enumerate(field_names)]

    field_values = list(field_values)
    field_names = ', '.join(field_names)
    sql = f'''SELECT * FROM Event WHERE {field_names}'''

    async with conn.execute(sql, field_values) as cursor:
      row = await cursor.fetchone()
      return Event.from_db_row(row)

  async def delete(self, conn: Connection, event_id: int):
    await conn.execute('''DELETE FROM Event WHERE id = :1''', [event_id])
