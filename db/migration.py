'''
Stolen from https://github.com/clutchski/caribou/blob/master/caribou.py
'''

import logging
import glob
from contextlib import contextmanager
import os
import sqlite3

from typing import List

MIGRATION_TABLE = "migration_version"
UTC_LENGTH = 14


@contextmanager
def execute_query(conn: sqlite3.Connection, sql, params=None) -> sqlite3.Cursor:
  params = [] if params is None else params
  cursor = conn.execute(sql, params)
  try:
    yield cursor
  finally:
    cursor.close()


@contextmanager
def execute_transaction(conn: sqlite3.Connection):
  try:
    yield
    conn.commit()
  except:
    conn.rollback()
    raise Exception('Unable execute transaction')


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

  def upgrade(self, conn: sqlite3.Connection):
    logging.info('execute migration %s' % self.name)
    for stmt in self.up.split(';'):
      conn.execute(stmt)

  def downgrade(self, conn: sqlite3.Connection):
    logging.info('rollback migration %s' % self.name)
    for stmt in self.down.split(';'):
      conn.execute(stmt)


class Manager:
  '''Controls migrations'''

  def __init__(self, conn, migrations_path):

    if not (os.path.exists(migrations_path) and os.path.isdir(migrations_path)):
      raise Exception('Specified incorrect migration path')

    self.__init_migrations_version_control(conn)
    self.migrations: List[Migration] = self.__init_migrations(migrations_path)

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

  def execute_migrations(self, conn: sqlite3.Connection) -> bool:
    current_version = self.__get_current_version(conn)
    new_version = current_version
    if not self.migrations or len(self.migrations) == 0:
      return False

    for migration in self.migrations:
      if migration.get_version() <= current_version:
        continue

      migration.upgrade(conn)
      new_version = migration.get_version()
      with execute_transaction(conn):
        conn.execute('''insert into %s values (:1)''' %
                     MIGRATION_TABLE, [new_version])

    return True

  def __init_migrations_version_control(self, conn: sqlite3.Connection):
    if not self.__check_table_exists(conn, MIGRATION_TABLE):
      with execute_transaction(conn):
        conn.execute(
          '''create table if not exists %s (version text)''' % MIGRATION_TABLE)

  def __get_current_version(self, conn: sqlite3.Connection):
    with execute_query(conn, '''select version from %s''' % MIGRATION_TABLE) as cursor:
      result = cursor.fetchall()
      return result[0][0] if result else '0'

  def __check_table_exists(self, conn: sqlite3.Connection, table_name):
    sql = '''select * from sqlite_master where type = 'table' and name = :1'''
    with execute_query(conn, sql, [table_name]) as cursor:
      return bool(cursor.fetchall())
