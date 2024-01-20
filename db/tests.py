from aiosqlite import connect, DatabaseError
import shutil
import os
import tempfile
from typing import List

from unittest import TestCase, IsolatedAsyncioTestCase

from db.migration import Migration, Manager
from db.pool import ConnectionPool
from db.manager import Manager as DbManager, Config as DBConfig

from db.orm.participant import ParticipantOrm
from db.orm.role import RoleOrm
from db.orm.event import EventOrm
from db.orm.volunteer import VolunteerOrm


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


class TestParticipantOrm(IsolatedAsyncioTestCase):
  __migrations = {
    "20231110_080000": {
      "name": "name1",
      "up": """
        CREATE TABLE
          Participant (
            id BIGINT PRIMARY KEY,
            telegram_nickname TEXT NOT NULL,
            name TEXT,
            surname TEXT,
            age INTEGER,
            is_admin BOOLEAN DEFAULT FALSE,
            verst_id INTEGER
          );
      """,
      "down": """
        DROP TABLE Participant;
      """
    }
  }

  @classmethod
  def setUpClass(cls):
    cls.__migrations_path = os.path.join(
      tempfile.gettempdir(), "participant_orm")

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
    config = DBConfig(path=':memory:', max_connections=2,
                      migrations=self.__migrations_path)

    self.manager = DbManager(config)
    await self.manager.setup()

  async def asyncTearDown(self):
    await self.manager.close()

  async def test_CRUD(self):
    orm = ParticipantOrm()
    try:
      async with self.manager.use_connection() as conn:
        id = await orm.register(conn, 12345, "qwerty")
    except:
      self.fail("ParticipantOrm.register() raised unexpectedly")

    self.assertEqual(id, 12345)

    try:
      await orm.update(conn, id, **{"name": "new_name", "surname": "new_surname", "age": 10})
    except:
      self.fail("ParticipantOrm.update() raised unexpectedly")

    try:
      async with self.manager.use_connection() as conn:
        participant = await orm.get(conn, id)
    except:
      self.fail("ParticipantOrm.get() raised unexpectedly")

    self.assertEqual(participant.age, 10)
    self.assertEqual(participant.name, "new_name")
    self.assertEqual(participant.surname, "new_surname")
    self.assertEqual(participant.id, 12345)
    self.assertEqual(participant.telegram_nickname, "qwerty")

    try:
      async with self.manager.use_connection() as conn:
        id = await orm.register(conn, 12346, "qwerty1")
    except:
      self.fail("ParticipantOrm.register() raised unexpectedly")

    try:
      async with self.manager.use_connection() as conn:
        participants = await orm.list(conn)
    except:
      self.fail("ParticipantOrm.list() raised unexpectedly")

    self.assertEqual(len(participants), 2)
    self.assertEqual(participants[0].id, 12345)
    self.assertEqual(participants[0].telegram_nickname, "qwerty")
    self.assertEqual(participants[1].id, 12346)
    self.assertEqual(participants[1].telegram_nickname, "qwerty1")

    try:
      async with self.manager.use_connection() as conn:
        await orm.delete(conn, id)
    except:
      self.fail("ParticipantOrm.delete() raised unexpectedly")


class TestRoleOrm(IsolatedAsyncioTestCase):
  __migrations = {
    "20231110_080000": {
      "name": "name1",
      "up": """
        CREATE TABLE
          Role (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            emoji TEXT NOT NULL,
            is_default BOOLEAN NOT NULL DEFAULT FALSE
          );
      """,
      "down": """
        DROP TABLE Role;
      """
    }
  }

  @classmethod
  def setUpClass(cls):
    cls.__migrations_path = os.path.join(
      tempfile.gettempdir(), "role_orm")

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
    config = DBConfig(path=':memory:', max_connections=2,
                      migrations=self.__migrations_path)

    self.manager = DbManager(config)
    await self.manager.setup()

  async def asyncTearDown(self):
    await self.manager.close()

  async def test_CRUD(self):
    orm = RoleOrm()
    try:
      async with self.manager.use_connection() as conn:
        id = await orm.create(conn, "Role1", "🦺")
    except:
      self.fail("RoleOrm.create() raised unexpectedly")

    self.assertEqual(id, 1)

    try:
      await orm.update(conn, id, **{"is_default": True, "emoji": "⏱️"})
    except:
      self.fail("RoleOrm.update() raised unexpectedly")

    self.assertEqual(id, 1)

    try:
      async with self.manager.use_connection() as conn:
        role = await orm.get(conn, id)
    except:
      self.fail("RoleOrm.get() raised unexpectedly")

    self.assertEqual(role.name, "Role1")
    self.assertEqual(role.id, 1)
    self.assertEqual(role.emoji, "⏱️")
    self.assertEqual(role.is_default, True)

    try:
      async with self.manager.use_connection() as conn:
        id = await orm.create(conn, "Role2", "🤸")
    except:
      self.fail("RoleOrm.create() raised unexpectedly")

    try:
      async with self.manager.use_connection() as conn:
        roles = await orm.list(conn)
    except:
      self.fail("RoleOrm.list() raised unexpectedly")

    self.assertEqual(len(roles), 2)
    self.assertEqual(roles[0].id, 1)
    self.assertEqual(roles[0].name, "Role1")
    self.assertEqual(roles[0].emoji, "⏱️")
    self.assertEqual(roles[0].is_default, True)
    self.assertEqual(roles[1].id, 2)
    self.assertEqual(roles[1].name, "Role2")
    self.assertEqual(roles[1].emoji, "🤸")
    self.assertEqual(roles[1].is_default, False)

    try:
      async with self.manager.use_connection() as conn:
        await orm.delete(conn, id)
    except:
      self.fail("RoleOrm.delete() raised unexpectedly")

    try:
      async with self.manager.use_connection() as conn:
        role = await orm.get(conn, id)
    except:
      self.fail("RoleOrm.get() raised unexpectedly")

    self.assertEqual(role, None)


class TestEventOrm(IsolatedAsyncioTestCase):
  __migrations = {
    "20231110_080000": {
      "name": "name1",
      "up": """
        CREATE TABLE
          Event (
            id INTEGER PRIMARY KEY,
            event_date TEXT NOT NULL,
            event_time TEXT NOT NULL,
            description TEXT
          );
      """,
      "down": """
        DROP TABLE Event;
      """
    }
  }

  @classmethod
  def setUpClass(cls):
    cls.__migrations_path = os.path.join(
      tempfile.gettempdir(), "event_orm")

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
    config = DBConfig(path=':memory:', max_connections=2,
                      migrations=self.__migrations_path)

    self.manager = DbManager(config)
    await self.manager.setup()

  async def asyncTearDown(self):
    await self.manager.close()

  async def test_CRUD(self):
    orm = EventOrm()
    try:
      async with self.manager.use_connection() as conn:
        id = await orm.create(conn, "2024-01-21", "09:00")
    except:
      self.fail("EventOrm.create() raised unexpectedly")

    self.assertEqual(id, 1)

    try:
      await orm.update(conn, id, **{"event_date": "2024-01-20", "event_time": "10:00"})
    except:
      self.fail("EventOrm.update() raised unexpectedly")

    self.assertEqual(id, 1)

    try:
      async with self.manager.use_connection() as conn:
        event = await orm.get(conn, id)
    except:
      self.fail("EventOrm.get() raised unexpectedly")

    self.assertEqual(event.id, 1)
    self.assertEqual(event.event_date.isoformat(), "2024-01-20")
    self.assertEqual(event.event_time.isoformat(), "10:00:00")

    try:
      async with self.manager.use_connection() as conn:
        id = await orm.create(conn, "2024-02-02", "09:00")
    except:
      self.fail("EventOrm.create() raised unexpectedly")

    try:
      async with self.manager.use_connection() as conn:
        events = await orm.list(conn)
    except:
      self.fail("EventOrm.list() raised unexpectedly")

    self.assertEqual(len(events), 2)
    self.assertEqual(events[0].id, 1)
    self.assertEqual(events[0].event_date.isoformat(), "2024-01-20")
    self.assertEqual(events[0].event_time.isoformat(), "10:00:00")
    self.assertEqual(events[1].id, 2)
    self.assertEqual(events[1].event_date.isoformat(), "2024-02-02")
    self.assertEqual(events[1].event_time.isoformat(), "09:00:00")

    try:
      async with self.manager.use_connection() as conn:
        await orm.delete(conn, id)
    except:
      self.fail("EventOrm.delete() raised unexpectedly")

    try:
      async with self.manager.use_connection() as conn:
        event = await orm.get(conn, id)
    except:
      self.fail("EventOrm.get() raised unexpectedly")

    self.assertEqual(event, None)


class TestDbManager(IsolatedAsyncioTestCase):
  __migrations = {
    "20231110_080000": {
      "name": "name1",
      "up": """
        CREATE TABLE
          Participant (
            id BIGINT PRIMARY KEY,
            telegram_nickname TEXT NOT NULL,
            name TEXT,
            surname TEXT,
            age INTEGER,
            is_admin BOOLEAN DEFAULT FALSE,
            verst_id INTEGER
          );

        CREATE TABLE
          Role (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            emoji TEXT NOT NULL,
            is_default BOOLEAN NOT NULL DEFAULT FALSE
          );

        CREATE TABLE
          Event (
            id INTEGER PRIMARY KEY,
            event_date TEXT NOT NULL,
            event_time TEXT NOT NULL,
            description TEXT
          );

        CREATE TABLE
          Volunteer (
            event_id INTEGER NOT NULL,
            role_id INTEGER NOT NULL,
            participant_id BIGINT,
            PRIMARY KEY (event_id, role_id),
            FOREIGN KEY (event_id) REFERENCES Event (id),
            FOREIGN KEY (role_id) REFERENCES Position(id),
            FOREIGN KEY (participant_id) REFERENCES Participant (id)
          );
      """,
      "down": """
        DROP TABLE Participant;

        DROP TABLE Role;

        DROP TABLE Event;

        DROP TABLE Volunteer;
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
    config = DBConfig(path=':memory:', max_connections=2,
                      migrations=self.__migrations_path)

    self.manager = DbManager(config)
    await self.manager.setup()

  async def asyncTearDown(self):
    await self.manager.close()

  async def test_setup(self):
    try:
      await self.manager.setup()
    except:
      self.fail("DbManager.setup() raised unexpectedly")

  async def test_CRUD_volunteer(self):
    try:
      async with self.manager.use_connection() as conn:
        event_id = await self.manager.events.create(conn, "2024-01-21", "09:00")
        role_id_1 = await self.manager.roles.create(conn, "Role1", "🤸")
        participant_id_1 = await self.manager.participants.register(conn, 12345, "Participant1")
        role_id_2 = await self.manager.roles.create(conn, "Role2", "🤸")
        participant_id_2 = await self.manager.participants.register(conn, 12346, "Participant2")
        participant_id_3 = await self.manager.participants.register(conn, 12347, "Participant3")
    except:
      self.fail("VolunteerOrm initial steps raised unexpectedly")

    orm = VolunteerOrm()
    try:
      async with self.manager.use_connection() as conn:
        id = await orm.create(conn, event_id, role_id_1, participant_id_1)
    except:
      self.fail("VolunteerOrm.create() raised unexpectedly")

    self.assertEqual(id, (1, 1))

    try:
      await orm.update(conn, *id, **{"participant_id": participant_id_3})
    except:
      self.fail("VolunteerOrm.update() raised unexpectedly")

    try:
      async with self.manager.use_connection() as conn:
        volunteer = await orm.get(conn, *id)
    except:
      self.fail("VolunteerOrm.get() raised unexpectedly")

    self.assertEqual(volunteer.event_id, event_id)
    self.assertEqual(volunteer.role_id, role_id_1)
    self.assertEqual(volunteer.participant_id, participant_id_3)

    try:
      async with self.manager.use_connection() as conn:
        id = await orm.create(conn, event_id, role_id_2, participant_id_2)
    except:
      self.fail("VolunteerOrm.create() raised unexpectedly")

    try:
      async with self.manager.use_connection() as conn:
        volunteers = await orm.list(conn)
    except:
      self.fail("VolunteerOrm.list() raised unexpectedly")

    self.assertEqual(len(volunteers), 2)
    self.assertEqual(volunteers[0].event_id, 1)
    self.assertEqual(volunteers[0].role_id, role_id_1)
    self.assertEqual(volunteers[0].participant_id, participant_id_3)
    self.assertEqual(volunteers[1].event_id, 1)
    self.assertEqual(volunteers[1].role_id, role_id_2)
    self.assertEqual(volunteers[1].participant_id, participant_id_2)

    try:
      async with self.manager.use_connection() as conn:
        await orm.delete(conn, *id)
    except:
      self.fail("VolunteerOrm.delete() raised unexpectedly")

    try:
      async with self.manager.use_connection() as conn:
        volunteer = await orm.get(conn, *id)
    except:
      self.fail("VolunteerOrm.get() raised unexpectedly")

    self.assertEqual(volunteer, None)
