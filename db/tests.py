from aiosqlite import connect, DatabaseError
import shutil
import os
import tempfile
from typing import List

from unittest import TestCase, IsolatedAsyncioTestCase

from db.migration import Migration, Manager
from db.pool import ConnectionPool
from db.db import Manager as DbManager, Config as DBConfig


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

        CREATE TABLE
          VolunteerPosition (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            emoji TEXT NOT NULL
          );

        CREATE TABLE
          Event (id INTEGER PRIMARY KEY, date TEXT NOT NULL);

        CREATE TABLE
          EventVolunteer (
            event_id INTEGER NOT NULL,
            position_id INTEGER NOT NULL,
            participant_id INTEGER NOT NULL,
            PRIMARY KEY (event_id, position_id),
            FOREIGN KEY (event_id) REFERENCES Event (id) ON DELETE CASCADE,
            FOREIGN KEY (position_id) REFERENCES VolunteerPosition (id) ON DELETE CASCADE,
            FOREIGN KEY (participant_id) REFERENCES Participant (id) ON DELETE CASCADE
          );
      """,
      "down": """
        DROP TABLE Participant;

        DROP TABLE VerstParticipant;

        DROP TABLE VolunteerPosition;
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

  async def asyncSetUp(self):
    config = DBConfig()
    config.dbpath = ':memory:'
    config.max_connections = 2
    config.migrations_path = self.__migrations_path

    self.manager = DbManager(config)
    await self.manager.setup()

  async def asyncTearDown(self):
    await self.manager.close()

  async def test_setup(self):
    try:
      await self.manager.setup()
    except:
      self.fail("DbManager.setup() raised unexpectedly")

  async def test_register_participant(self):
    try:
      async with self.manager.use_connection() as conn:
        id = await self.manager.register_participant(conn, "qwerty")
    except:
      self.fail("DbManager.register_participant() raised unexpectedly")

    self.assertEqual(id, 1)

  async def test_register_verst_participant(self):
    async with self.manager.use_connection() as conn:
      id = await self.manager.register_participant(conn, "qwerty")

      try:
        await self.manager.register_verst_participant(conn, id, 12345, "https://5verst.ru/userstats/12345")
      except:
        self.fail("DbManager.register_verst_participant() raised unexpectedly")

      self.assertEqual(id, 1)

  async def test_update_participant(self):
    async with self.manager.use_connection() as conn:
      id = await self.manager.register_participant(conn, "qwerty")

      try:
        await self.manager.update_participant(conn, id, "name", "surname", 10)
      except:
        self.fail("DbManager.update_participant() raised unexpectedly")

      self.assertEqual(id, 1)

  async def test_CRUD_volunteer_position(self):
    async with self.manager.use_connection() as conn:
      try:
        id = await self.manager.create_volunteer_position(conn, "Position1", "🦺")
      except:
        self.fail("DbManager.create_volunteer_position() raised unexpectedly")

      self.assertEqual(id, 1)

      try:
        pos = await self.manager.get_volunteer_position(conn, id)
      except:
        self.fail("DbManager.get_volunteer_position() raised unexpectedly")

      self.assertEqual(pos[0], 1)
      self.assertEqual(pos[1], "Position1")
      self.assertEqual(pos[2], "🦺")

      try:
        pos = await self.manager.update_volunteer_position(conn, id, "Position2", "⏱️")
      except:
        self.fail("DbManager.update_volunteer_position() raised unexpectedly")

      try:
        pos = await self.manager.get_volunteer_position(conn, id)
      except:
        self.fail("DbManager.get_volunteer_position() raised unexpectedly")

      self.assertEqual(pos[0], 1)
      self.assertEqual(pos[1], "Position2")
      self.assertEqual(pos[2], "⏱️")

      try:
        await self.manager.delete_volenteer_position(conn, id)
      except:
        self.fail("DbManager.delete_volunteer_position() raised unexpectedly")

      result = await self.manager.get_volunteer_position(conn, id)
      self.assertEqual(result, None)

  async def test_register_event(self):
    async with self.manager.use_connection() as conn:
      try:
        id = await self.manager.register_event(conn, "2023-11-18 09:00:00.000")
      except:
        self.fail("DbManager.register_event() raised unexpectedly")

      self.assertEqual(id, 1)

  async def test_get_event(self):
    async with self.manager.use_connection() as conn:
      id = await self.manager.register_event(conn, "2023-11-18 09:00:00.000")

      try:
        row = await self.manager.get_event(conn, id)
      except:
        self.fail("DbManager.get_event() raised unexpectedly")

      self.assertEqual(row[0], 1)
      self.assertEqual(row[1], "2023-11-18 09:00:00.000")

  async def test_CRUD_event_volunteer(self):
    async with self.manager.use_connection() as conn:
      event_id = await self.manager.register_event(conn, "2023-11-18 09:00:00.000")

      pos_id1 = await self.manager.create_volunteer_position(conn, "Position1", "🦺")
      pos_id2 = await self.manager.create_volunteer_position(conn, "Position2", "⏱️")
      pos_id3 = await self.manager.create_volunteer_position(conn, "Position3", "🤸")

      participant_id1 = await self.manager.register_participant(conn, "Participant1")
      participant_id2 = await self.manager.register_participant(conn, "Participant2")
      participant_id3 = await self.manager.register_participant(conn, "Participant3")
      participant_id4 = await self.manager.register_participant(conn, "Participant4")

      try:
        row1 = await self.manager.create_event_volunteer(conn, event_id, pos_id1, participant_id1)
        row2 = await self.manager.create_event_volunteer(conn, event_id, pos_id2, participant_id2)
        row3 = await self.manager.create_event_volunteer(conn, event_id, pos_id3, participant_id3)
      except:
        self.fail("DbManager.create_event_volunteer() raised unexpectedly")

      self.assertEqual(row1[0], event_id)
      self.assertEqual(row1[1], pos_id1)

      self.assertEqual(row2[0], event_id)
      self.assertEqual(row2[1], pos_id2)

      self.assertEqual(row3[0], event_id)
      self.assertEqual(row3[1], pos_id3)

      try:
        row = await self.manager.get_event_volunteer(conn, event_id, pos_id2)
      except:
        self.fail("DbManager.get_event_volunteer() raised unexpectedly")

      self.assertEqual(row[0], event_id)
      self.assertEqual(row[1], pos_id2)
      self.assertEqual(row[2], participant_id2)

      try:
        await self.manager.update_event_volunteer(conn, event_id, pos_id2, participant_id4)
      except:
        self.fail("DbManager.update_event_volunteer() raised unexpectedly")

      try:
        row = await self.manager.get_event_volunteer(conn, event_id, pos_id2)
      except:
        self.fail("DbManager.get_volunteer_position() raised unexpectedly")

      self.assertEqual(row[0], event_id)
      self.assertEqual(row[1], pos_id2)
      self.assertEqual(row[2], participant_id4)

      try:
        await self.manager.delete_event_volenteer(conn, event_id, pos_id3)
      except:
        self.fail("DbManager.delete_event_volenteer() raised unexpectedly")

      result = await self.manager.get_event_volunteer(conn, event_id, pos_id3)
      self.assertEqual(result, None)
