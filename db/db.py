from db.migration import Manager as MigrationManager
from db.pool import ConnectionPool
from db.common import execute_query, execute_transaction


class Config:
  dbpath: str
  migrations_path: str
  max_connections: int


class Manager:
  def __init__(self, config: Config):
    self.config = config
    self.pool = ConnectionPool(self.config.dbpath, self.config.max_connections)

  async def setup(self):
    async with self.pool.connection() as conn:
      try:
        manager = await MigrationManager.create(conn, self.config.migrations_path)
        await manager.execute_migrations(conn)
      except:
        raise RuntimeError("Unable to execute migrations on setup")

  async def register_participant(self, telegram_nickname: str):
    async with self.pool.connection() as conn:
      async with execute_transaction(conn):
        async with conn.execute('''INSERT INTO Participant(telegram_nickname) VALUES(:1) RETURNING id''', [telegram_nickname]) as cursor:
          row = await cursor.fetchone()
          return row[0]

  async def register_verst_participant(self, participant_id: int, verst_id: int, verst_link: str):
    async with self.pool.connection() as conn:
      async with execute_transaction(conn):
        await conn.execute('''INSERT INTO VerstParticipant VALUES(:1, :2)''', [verst_id, verst_link])
        await conn.execute('''UPDATE Participant SET verst_id=:1 WHERE id = :2''', [verst_id, participant_id])

  async def update_participant(self, participant_id: int, name: str, surname: str):
    async with self.pool.connection() as conn:
      async with execute_transaction(conn):
        await conn.execute('''UPDATE Participant SET name=:1, surname=:2 WHERE id = :3''', [name, surname, participant_id])

  async def create_volunteer_position(self, name: str, emoji: str):
    async with self.pool.connection() as conn:
      async with execute_transaction(conn):
        async with conn.execute('''INSERT INTO VolunteerPosition(name, emoji) VALUES(:1, :2) RETURNING id''', [name, emoji]) as cursor:
          row = await cursor.fetchone()
          return row[0]

  async def get_volunteer_position(self, position_id: int):
    async with self.pool.connection() as conn:
      async with execute_transaction(conn):
        async with conn.execute('''SELECT * FROM VolunteerPosition WHERE id=:1''', [position_id]) as cursor:
          row = await cursor.fetchone()
          # TODO: Implement anonymous object
          return row

  async def update_volunteer_position(self, position_id: int, name: str, emoji: str):
    async with self.pool.connection() as conn:
      async with execute_transaction(conn):
        await conn.execute('''UPDATE VolunteerPosition SET name=:1, emoji=:2 WHERE id=:3''', [name, emoji, position_id])

  async def delete_volenteer_position(self, position_id: int):
    async with self.pool.connection() as conn:
      async with execute_transaction(conn):
        await conn.execute('''DELETE FROM VolunteerPosition WHERE id = :1''', [position_id])

  async def close(self):
    await self.pool.close()
