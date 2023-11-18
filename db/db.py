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
        async with await conn.execute('''INSERT INTO Participant(telegram_nickname) VALUES(:1) RETURNING id''', [telegram_nickname]) as cursor:
          row = await cursor.fetchone()
          return row[0]

  async def close(self):
    await self.pool.close()
