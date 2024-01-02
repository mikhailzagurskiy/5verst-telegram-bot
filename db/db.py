import logging

from db.migration import Manager as MigrationManager
from db.pool import ConnectionPool, Connection
from db.common import execute_query, execute_transaction
from db.config import Config

from contextlib import asynccontextmanager


class Manager:
  def __init__(self, config: Config):
    self.config = config
    self.pool = ConnectionPool(self.config.path, self.config.max_connections)

  async def setup(self):
    async with self.pool.connection() as conn:
      try:
        manager = await MigrationManager.create(conn, self.config.migrations)
        await manager.execute_migrations(conn)
      except:
        raise RuntimeError("Unable to execute migrations on setup")
      finally:
        await self.close()

  @asynccontextmanager
  async def use_connection(self) -> Connection:
    async with self.pool.connection() as conn:
      async with execute_transaction(conn):
        yield conn

  # Participant ##########################################################

  async def register_participant(self, conn: Connection, telegram_id: int, telegram_nickname: str):
    async with conn.execute('''INSERT INTO Participant(id, telegram_nickname) VALUES(:1, :2) RETURNING id''', [telegram_id, telegram_nickname]) as cursor:
      row = await cursor.fetchone()
      return row[0]

  async def update_participant(self, conn: Connection, participant_id: int, name: str, surname: str, age: int):
    await conn.execute('''UPDATE Participant SET name=:1, surname=:2, age=:3 WHERE id = :4''', [name, surname, age, participant_id])

  async def list_participants(self, conn: Connection):
    return await conn.execute_fetchall('''SELECT * FROM Participant''')

  async def get_participant(self, conn: Connection, participant_id: int):
    async with conn.execute('''SELECT * FROM Participant WHERE id=:1''', [participant_id]) as cursor:
      row = await cursor.fetchone()
      # TODO: Implement anonymous object
      return row

  async def find_participant(self, conn: Connection, telegram_nickname: str):
    async with conn.execute('''SELECT * FROM Participant WHERE telegram_nickname=:1''', [telegram_nickname]) as cursor:
      row = await cursor.fetchone()
      # TODO: Implement anonymous object
      return row

  ########################################################################

  # VerstParticipant #####################################################

  async def register_verst_participant(self, conn: Connection, participant_id: int, verst_id: int, verst_link: str):
    await conn.execute('''INSERT INTO VerstParticipant VALUES(:1, :2)''', [verst_id, verst_link])
    await conn.execute('''UPDATE Participant SET verst_id=:1 WHERE id = :2''', [verst_id, participant_id])

  ########################################################################

  # VolunteerPosition ####################################################

  async def create_volunteer_position(self, conn: Connection, name: str, emoji: str, is_default: bool = False):
    async with conn.execute('''INSERT INTO VolunteerPosition(name, emoji) VALUES(:1, :2) RETURNING id''', [name, emoji]) as cursor:
      row = await cursor.fetchone()
      return row[0]

  async def list_volunteer_positions(self, conn: Connection):
    async with conn.execute('''SELECT * FROM VolunteerPosition''') as cursor:
      rows = await cursor.fetchall()
      # TODO: Implement anonymous object
      return rows

  async def get_volunteer_position(self, conn: Connection, position_id: int):
    async with conn.execute('''SELECT * FROM VolunteerPosition WHERE id=:1''', [position_id]) as cursor:
      row = await cursor.fetchone()
      # TODO: Implement anonymous object
      return row

  async def update_volunteer_position(self, conn: Connection, position_id: int, name: str, emoji: str):
    await conn.execute('''UPDATE VolunteerPosition SET name=:1, emoji=:2 WHERE id=:3''', [name, emoji, position_id])

  async def delete_volenteer_position(self, conn: Connection, position_id: int):
    await conn.execute('''DELETE FROM VolunteerPosition WHERE id = :1''', [position_id])

  ########################################################################

  # Event ################################################################

  async def register_event(self, conn: Connection, date: str):
    async with conn.execute('''INSERT INTO Event(date) VALUES(:1) RETURNING id''', [date]) as cursor:
      row = await cursor.fetchone()
      return row[0]

  async def list_events(self, conn: Connection):
    async with conn.execute('''SELECT * FROM Event''') as cursor:
      rows = await cursor.fetchall()
      # TODO: Implement anonymous object
      return rows

  async def get_event(self, conn: Connection, id: int):
    async with conn.execute('''SELECT * FROM Event WHERE id=:1''', [id]) as cursor:
      row = await cursor.fetchone()
      # TODO: Implement anonymous object
      return row

  ########################################################################

  # EventVolunteer #######################################################

  async def create_event_volunteer(self, conn: Connection, event_id: int, position_id: int, participant_id: int):
    async with conn.execute('''INSERT INTO EventVolunteer(event_id, position_id, participant_id) VALUES(:1, :2, :3) RETURNING event_id, position_id''', [event_id, position_id, participant_id]) as cursor:
      row = await cursor.fetchone()
      return row

  async def list_event_volunteers(self, conn: Connection, event_id: int):
    async with conn.execute('''SELECT * FROM EventVolunteer WHERE event_id=:1''', [event_id]) as cursor:
      rows = await cursor.fetchall()
      # TODO: Implement anonymous object
      return rows

  async def get_event_volunteer(self, conn: Connection, event_id: int, position_id: int):
    async with conn.execute('''SELECT * FROM EventVolunteer WHERE event_id=:1 AND position_id=:2''', [event_id, position_id]) as cursor:
      row = await cursor.fetchone()
      # TODO: Implement anonymous object
      return row

  async def update_event_volunteer(self, conn: Connection, event_id: int, position_id: int, participant_id: int):
    await conn.execute('''UPDATE EventVolunteer SET participant_id=:1 WHERE event_id=:2 AND position_id=:3''', [participant_id, event_id, position_id])

  async def delete_event_volenteer(self, conn: Connection, event_id: int, position_id: int):
    await conn.execute('''DELETE FROM EventVolunteer WHERE event_id=:1 AND position_id=:2''', [event_id, position_id])

  ########################################################################

  async def close(self):
    logging.debug("Close DBManager")
    await self.pool.close()
