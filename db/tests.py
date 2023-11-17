from aiosqlite import connect
import shutil
import os
import tempfile
from typing import List

from unittest import TestCase, IsolatedAsyncioTestCase

from db.migration import Migration, Manager
from db.pool import ConnectionPool


class TestMigration(IsolatedAsyncioTestCase):
  __migrations = {
    "20231110_080000": {"name": "name1", "up": "create table a(field text)", "down": "drop table a"},
    "20231110_100000": {"name": "name2", "up": "create table b(field text)", "down": "drop table b"},
    "20231111_080000": {"name": "name3", "up": "create table c(field text)", "down": "drop table c"},
    "20231112_080000": {"name": "name4", "up": "drop table b;\ncreate table d(field text)", "down": "create table b(field text);\ndrop table d"},
    "20231113_080000": {"name": "name5", "up": "drop table a", "down": "create table a(field text)"}
  }

  @classmethod
  def setUpClass(cls):
    cls.__migrations_path = os.path.join(tempfile.gettempdir(), "migrations")

    if os.path.exists(cls.__migrations_path):
      shutil.rmtree(cls.__migrations_path)

    try:
      for (ver, migration) in cls.__migrations.items():
        cur_path = os.path.join(
          cls.__migrations_path,
          "%s-%s" % (ver, migration["name"])
        )

        os.makedirs(cur_path)

        with open(os.path.join(cur_path, "up.sql"), "w") as f:
          f.write(migration["up"])

        with open(os.path.join(cur_path, "down.sql"), "w") as f:
          f.write(migration["down"])
    except:
      raise Exception("Unable to setup test suite")

  @classmethod
  def tearDownClass(cls):
    try:
      shutil.rmtree(cls.__migrations_path)
    except:
      raise Exception("Unable to teardown test suite")

  async def test_construct_migration(self):
    with self.assertRaises(TypeError):
      Migration(None)

    with self.assertRaises(FileNotFoundError):
      Migration('')

    with self.assertRaises(FileNotFoundError):
      Migration('fake//path')

    for (ver, migration) in self.__migrations.items():
      try:
        Migration(
          os.path.join(
            self.__migrations_path,
            "%s-%s" % (ver, migration["name"])
          )
        )
      except:
        self.fail("Migration() contructor raised unexpectedly on %s" % ver)

  async def test_get_version(self):
    for (ver, migration) in self.__migrations.items():
      migration = Migration(
        os.path.join(
          self.__migrations_path,
          "%s-%s" % (ver, migration["name"])
        )
      )

      self.assertEqual(migration.get_version(), ver)

  async def test_up_migration(self):
    async with connect(":memory:") as conn:
      migrations: List[Migration] = []
      for (ver, migration) in self.__migrations.items():
        migration = Migration(
          os.path.join(
            self.__migrations_path,
            "%s-%s" % (ver, migration["name"])
          )
        )

        migrations.append(migration)

      try:
        for migration in migrations:
          await migration.upgrade(conn)
      except:
        self.fail("Migration.upgrade() raised unexpectedly")

      async with conn.execute(
        '''select name from sqlite_master where type = 'table' order by name''') as cursor:
        tables = [table for table in await cursor.fetchall()]

        self.assertEqual(tables[0][0], 'c')
        self.assertEqual(tables[1][0], 'd')

  async def test_down_migration(self):
    async with connect(":memory:") as conn:
      migrations: List[Migration] = []
      for (ver, migration) in self.__migrations.items():
        migration = Migration(
          os.path.join(
            self.__migrations_path,
            "%s-%s" % (ver, migration["name"])
          )
        )

        migrations.append(migration)

      try:
        for migration in migrations.copy():
          await migration.upgrade(conn)
      except:
        self.fail("Migration.upgrade() raised unexpectedly")

      migrations = migrations[3:]
      migrations.reverse()

      try:
        for migration in migrations:
          await migration.downgrade(conn)
      except:
        self.fail("Migration.downgrade() raised unexpectedly")

      async with conn.execute(
        '''select name from sqlite_master where type = 'table' order by name''') as cursor:
        tables = [table for table in await cursor.fetchall()]

        self.assertEqual(tables[0][0], 'a')
        self.assertEqual(tables[1][0], 'b')
        self.assertEqual(tables[2][0], 'c')


def list_dir(rootDir):
  for lists in os.listdir(rootDir):
    path = os.path.join(rootDir, lists)
    print(path)
    if os.path.isdir(path):
      list_dir(path)


class TestMigrationManager(IsolatedAsyncioTestCase):
  __migrations = {
    "20231110_080000": {"name": "name1", "up": "create table a(field text)", "down": "drop table a"},
    "20231110_100000": {"name": "name2", "up": "create table b(field text)", "down": "drop table b"},
    "20231111_080000": {"name": "name3", "up": "create table c(field text)", "down": "drop table c"},
    "20231112_080000": {"name": "name4", "up": "drop table b;\ncreate table d(field text)", "down": "create table b(field text);\ndrop table d"},
    "20231113_080000": {"name": "name5", "up": "drop table a", "down": "create table a(field text)"}
  }

  @classmethod
  def setUpClass(cls):
    cls.__migrations_path = os.path.join(
      tempfile.gettempdir(), "migrations_manager")
    try:
      for (ver, migration) in cls.__migrations.items():
        cur_path = os.path.join(
          cls.__migrations_path,
          "%s-%s" % (ver, migration["name"])
        )

        os.makedirs(cur_path)

        with open(os.path.join(cur_path, "up.sql"), "w") as f:
          f.write(migration["up"])

        with open(os.path.join(cur_path, "down.sql"), "w") as f:
          f.write(migration["down"])
    except:
      raise Exception("Unable to setup test suite")

  @classmethod
  def tearDownClass(cls):
    try:
      shutil.rmtree(cls.__migrations_path)
    except:
      raise Exception("Unable to teardown test suite")

  async def test(self):
    async with connect(":memory:") as conn:
      manager: Manager = await Manager.create(conn, self.__migrations_path)

      try:
        await manager.execute_migrations(conn)
      except:
        self.fail("Migration.upgrade() raised unexpectedly")

      async with conn.execute(
        '''select name from sqlite_master where type = 'table' order by name''') as cursor:
        tables = [table for table in await cursor.fetchall()]

        self.assertEqual(tables[0][0], 'c')
        self.assertEqual(tables[1][0], 'd')

      async with conn.execute('''select version from migration_version''') as cursor:
        self.assertListEqual(
          [version[0] for version in await cursor.fetchall()], list(self.__migrations.keys()))



    try:
    except:



