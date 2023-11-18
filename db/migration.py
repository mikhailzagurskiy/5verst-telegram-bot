'''
Stolen from https://github.com/clutchski/caribou/blob/master/caribou.py
'''

import logging
import glob
import os
from aiosqlite import Connection

from typing import List

from db.common import execute_query, execute_transaction

MIGRATION_TABLE = "migration_version"
UTC_LENGTH = 14


class Migration:
  '''Represent one migration entry'''

  def __init__(self, path):
    self.version = None
    self.path = path
    self.__init_version_and_name()
    self.up = self.__load_source(os.path.join(self.path, "up.sql"))
    self.down = self.__load_source(os.path.join(self.path, "down.sql"))

  def __init_version_and_name(self):
    self.name = os.path.basename(self.path)
    self.version = os.path.basename(self.path).split('-', 1)[0]

  def __load_source(self, path):
    with open(path, "r") as f:
      return f.read()

  def get_version(self):
    return self.version

  async def upgrade(self, conn: Connection):
    logging.info('execute migration %s' % self.name)
    for stmt in self.up.split(';'):
      await conn.execute(stmt)

  async def downgrade(self, conn: Connection):
    logging.info('rollback migration %s' % self.name)
    for stmt in self.down.split(';'):
      await conn.execute(stmt)


class Manager:
  '''Controls migrations'''

  @classmethod
  async def create(cls, conn, migrations_path):
    self = cls()
    if not (os.path.exists(migrations_path) and os.path.isdir(migrations_path)):
      raise Exception('Specified incorrect migration path')

    await self.__init_migrations_version_control(conn)
    self.migrations = self.__init_migrations(migrations_path)

    return self

  def __init_migrations(self, path):
    migration_files = list(
      map(
        lambda item: os.path.dirname(item[0]),
        filter(
          lambda item:
            len(item) == 2
            and os.path.dirname(item[0]) == os.path.dirname(item[1]),
          zip(
              glob.glob(os.path.join(path, "**/up.sql")),
              glob.glob(os.path.join(path, "**/down.sql"))
            )
        )
      )
    )

    migrations = [Migration(file) for file in migration_files]
    migrations.sort(
      key=lambda x: x.get_version(), reverse=False)

    return migrations

  async def execute_migrations(self, conn: Connection) -> bool:
    current_version = await self.__get_current_version(conn)
    new_version = current_version
    if not self.migrations or len(self.migrations) == 0:
      return False

    for migration in self.migrations:
      if migration.get_version() <= current_version:
        continue

      await migration.upgrade(conn)
      new_version = migration.get_version()
      async with execute_transaction(conn):
        await conn.execute('''insert into %s values (:1)''' %
                           MIGRATION_TABLE, [new_version])

    return True

  async def __init_migrations_version_control(self, conn: Connection):
    if not await self.__check_table_exists(conn, MIGRATION_TABLE):
      async with execute_transaction(conn):
        await conn.execute(
          '''create table if not exists %s (version text)''' % MIGRATION_TABLE)

  async def __get_current_version(self, conn: Connection):
    async with execute_query(conn, '''select version from %s''' % MIGRATION_TABLE) as cursor:
      result = await cursor.fetchall()
      return result[0][0] if result else '0'

  async def __check_table_exists(self, conn: Connection, table_name):
    sql = '''select * from sqlite_master where type = 'table' and name = :1'''
    async with execute_query(conn, sql, [table_name]) as cursor:
      return bool(await cursor.fetchall())
