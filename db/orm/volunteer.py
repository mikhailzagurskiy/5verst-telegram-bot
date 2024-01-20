from db.pool import Connection

from entities.volunteer import Volunteer
from entities.common import as_int


class VolunteerOrm:
  async def create(self, conn: Connection, event_id: int, role_id: int, participant_id: int) -> tuple[int, int]:
    async with conn.execute('''INSERT INTO Volunteer(event_id, role_id, participant_id) VALUES(:1, :2, :3) RETURNING event_id, role_id''', [event_id, role_id, participant_id]) as cursor:
      row = await cursor.fetchone()
      return (as_int(row[0]), as_int(row[1]))

  async def list(self, conn: Connection) -> list[Volunteer]:
    return [Volunteer.from_db_row(row) for row in await conn.execute_fetchall('''SELECT * FROM Volunteer''')]

  async def get(self, conn: Connection, event_id: int, role_id: int) -> Volunteer:
    async with conn.execute('''SELECT * FROM Volunteer WHERE event_id=:1 AND role_id=:2''', [event_id, role_id]) as cursor:
      row = await cursor.fetchone()
      return Volunteer.from_db_row(row)

  async def update(self, conn: Connection, event_id: int, role_id: int, **fields):
    if len(fields.keys()) == 0 or not event_id or not role_id:
      return

    field_names, field_values = zip(*fields.items())
    field_names = [(f'{name} = :{idx + 1}')
                   for (idx, name) in enumerate(field_names)]

    field_values = list(field_values)
    event_id_pos = len(field_names) + 1
    role_id_pos = len(field_names) + 2
    field_values.append(event_id)
    field_values.append(role_id)

    field_names = ', '.join(field_names)
    sql = f'''UPDATE Volunteer SET {field_names} WHERE event_id = :{event_id_pos} AND role_id = :{role_id_pos}'''

    await conn.execute(sql, field_values)

  async def find(self, conn: Connection, **fields) -> Volunteer:
    field_names, field_values = zip(*fields.items())
    field_names = [(f'{name}=:{idx + 1}')
                   for (idx, name) in enumerate(field_names)]

    field_values = list(field_values)
    field_names = ', '.join(field_names)
    sql = f'''SELECT * FROM Volunteer WHERE {field_names}'''

    async with conn.execute(sql, field_values) as cursor:
      row = await cursor.fetchone()
      return Volunteer.from_db_row(row)

  async def delete(self, conn: Connection, event_id: int, role_id: int):
    await conn.execute('''DELETE FROM Volunteer WHERE event_id = :1 AND role_id = :2''', [event_id, role_id])
