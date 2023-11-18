from aiosqlite import connect
import shutil
import os
import tempfile
from typing import List

from unittest import TestCase, IsolatedAsyncioTestCase

from db.migration import Migration, Manager
from db.pool import ConnectionPool
from db.db import Manager as DbManager, Config as DbConfig


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

  async def test_execute_migrations(self):
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


class TestConnectionPool(IsolatedAsyncioTestCase):
  __migrations = {
    "20231110_080000": {"name": "name1", "up": "create table a(field text)", "down": "drop table a"},
    "20231110_090000": {"name": "name1", "up": "insert into a values('text1'),('text2'),('text3'),('text 4')", "down": "drop table a;\ncreate table a(field text)"},
  }

  @classmethod
  def setUpClass(cls):
    cls.__migrations_path = os.path.join(
      tempfile.gettempdir(), "connection_pool")

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

  async def test_not_opened_conenction(self):
    pool = ConnectionPool(":memory:", 2)

    async with pool.connection(100) as conn1:
      async with pool.connection(100) as conn2:
        with self.assertRaises(RuntimeError):
          async with pool.connection(1) as conn3:
            pass

    await pool.close()

  async def test_using(self):
    pool = ConnectionPool(":memory:", 2)
    async with pool.connection() as conn:
      manager: Manager = await Manager.create(conn, self.__migrations_path)

      try:
        await manager.execute_migrations(conn)
      except:
        self.fail("Migration.upgrade() raised unexpectedly")

      async with conn.execute(
        '''select name from sqlite_master where type = 'table' order by name''') as cursor:
        tables = [table for table in await cursor.fetchall()]

        self.assertEqual(tables[0][0], 'a')

      async with conn.execute('''select version from migration_version''') as cursor:
        self.assertListEqual(
          [version[0] for version in await cursor.fetchall()], list(self.__migrations.keys()))

    await pool.close()


class TestDbManager(IsolatedAsyncioTestCase):
  __migrations = {
    "20231110_080000": {
      "name": "name1",
      "up": """
        CREATE TABLE
          Participant (
            id INTEGER PRIMARY KEY,
            telegram_nickname TEXT NOT NULL,
            name TEXT,
            surname TEXT,
            verst_id INTEGER,
            FOREIGN KEY (verst_id) REFERENCES VerstParticipant (id) ON DELETE CASCADE
          );

        CREATE TABLE
          VerstParticipant (id INTEGER PRIMARY KEY, link TEXT NOT NULL);
      """,
      "down": """
        DROP TABLE Participant;

        DROP TABLE VerstParticipant;
      """
    },
  }

  @classmethod
  def setUpClass(cls):
    cls.__migrations_path = os.path.join(
      tempfile.gettempdir(), "db_manager")

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

  async def test_setup(self):
    config = DbConfig()
    config.dbpath = ':memory:'
    config.max_connections = 2
    config.migrations_path = self.__migrations_path

    manager = DbManager(config)
    try:
      await manager.setup()
    except:
      self.fail("DbManager.setup() raised unexpectedly")

    await manager.close()

  async def test_register_participant(self):
    config = DbConfig()
    config.dbpath = ':memory:'
    config.max_connections = 2
    config.migrations_path = self.__migrations_path

    manager = DbManager(config)
    await manager.setup()

    try:
      id = await manager.register_participant("qwerty")
    except:
      self.fail("DbManager.register_participant() raised unexpectedly")

    self.assertEqual(id, 1)

    await manager.close()

  async def test_register_verst_participant(self):
    config = DbConfig()
    config.dbpath = ':memory:'
    config.max_connections = 2
    config.migrations_path = self.__migrations_path

    manager = DbManager(config)
    await manager.setup()

    id = await manager.register_participant("qwerty")

    try:
      await manager.register_verst_participant(id, 12345, "https://5verst.ru/userstats/12345")
    except:
      self.fail("DbManager.register_verst_participant() raised unexpectedly")

    self.assertEqual(id, 1)

    await manager.close()

  async def test_update_participant(self):
    config = DbConfig()
    config.dbpath = ':memory:'
    config.max_connections = 2
    config.migrations_path = self.__migrations_path

    manager = DbManager(config)
    await manager.setup()

    id = await manager.register_participant("qwerty")

    try:
      await manager.update_participant(id, "name", "surname")
    except:
      self.fail("DbManager.update_participant() raised unexpectedly")

    self.assertEqual(id, 1)

    await manager.close()
