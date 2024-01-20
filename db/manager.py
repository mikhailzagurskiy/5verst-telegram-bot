import logging

from db.migration import Manager as MigrationManager
from db.pool import ConnectionPool, Connection
from db.common import execute_transaction
from db.config import Config

from db.orm.participant import ParticipantOrm
from db.orm.role import RoleOrm
from db.orm.event import EventOrm
from db.orm.volunteer import VolunteerOrm

from contextlib import asynccontextmanager


class Manager:
  def __init__(self, config: Config):
    self.config = config
    self.pool = ConnectionPool(self.config.path, self.config.max_connections)
    self.participants = ParticipantOrm()
    self.roles = RoleOrm()
    self.events = EventOrm()
    self.volunteers = VolunteerOrm()

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

  async def close(self):
    logging.debug("Close DBManager")
    await self.pool.close()
