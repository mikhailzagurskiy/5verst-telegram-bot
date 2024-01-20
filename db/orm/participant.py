from db.pool import Connection

from entities.participant import Participant
from entities.common import as_int


class ParticipantOrm:
  async def register(self, conn: Connection, telegram_id: int, telegram_nickname: str) -> int:
    async with conn.execute('''INSERT INTO Participant(id, telegram_nickname) VALUES(:1, :2) RETURNING id''', [telegram_id, telegram_nickname]) as cursor:
      row = await cursor.fetchone()
      return as_int(row[0])

  async def update(self, conn: Connection, participant_id: int, **fields):
    if len(fields.keys()) == 0 or not participant_id:
      return

    field_names, field_values = zip(*fields.items())
    field_names = [(f'{name} = :{idx + 1}')
                   for (idx, name) in enumerate(field_names)]

    field_values = list(field_values)
    id_pos = len(field_names) + 1
    field_values.append(participant_id)

    field_names = ', '.join(field_names)
    sql = f'''UPDATE Participant SET {field_names} WHERE id = :{id_pos}'''

    await conn.execute(sql, field_values)

  async def list(self, conn: Connection) -> list[Participant]:
    return [Participant.from_db_row(row) for row in await conn.execute_fetchall('''SELECT * FROM Participant''')]

  async def get(self, conn: Connection, participant_id: int) -> Participant:
    async with conn.execute('''SELECT * FROM Participant WHERE id=:1''', [participant_id]) as cursor:
      row = await cursor.fetchone()
      return Participant.from_db_row(row)

  async def find(self, conn: Connection, **fields) -> Participant:
    field_names, field_values = zip(*fields.items())
    field_names = [(f'{name}=:{idx + 1}')
                   for (idx, name) in enumerate(field_names)]

    field_values = list(field_values)
    field_names = ', '.join(field_names)
    sql = f'''SELECT * FROM Participant WHERE {field_names}'''

    async with conn.execute(sql, field_values) as cursor:
      row = await cursor.fetchone()
      return Participant.from_db_row(row)

  async def delete(self, conn: Connection, participant_id: int):
    pass
