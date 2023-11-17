'''
Stolen from https://codereview.stackexchange.com/questions/285730/simple-connection-pool-for-sqlite-in-python/285776
'''

from asyncio import Lock, Condition, get_running_loop, wait_for, TimeoutError
from aiosqlite import Connection, connect
from collections import deque
from contextlib import asynccontextmanager


class ConnectionPool:
  def __init__(self, db_path, max_connections):
    if db_path == '':
      raise ValueError('Empty db path not allowed')
    self._db_path = db_path

    if max_connections < 1:
      raise ValueError(
        'Invalid `max_connections` number. Should be greater that 0')
    self._max_connections = max_connections
    self._created_connections = 0

    self._req_lock = Lock()
    self._returned = Condition()
    self._pool = deque()

  async def close(self):
    ''' Close all connections '''
    async with self._req_lock:
      while self._pool:
        conn = self._pool.popleft()
        await self.__close_connection(conn)

  async def __close_connection(self, conn: Connection):
    await conn.close()

  async def __create_connection(self) -> Connection:
    return await connect(self._db_path)

  async def __acquire_connection(self, timeout) -> Connection:
    while True:
      async with self._req_lock:
        if self._pool:
          conn = self._pool.popleft()
          return conn

        if self._created_connections < self._max_connections:
          self._created_connections += 1
          conn = await self.__create_connection()
          return conn

        # We must wait for a resource to be returned
        async def wait_for_conn():
          async with self._returned:
            await self._returned.wait()

        try:
          await wait_for(wait_for_conn(), timeout=timeout)
        except TimeoutError:
          raise RuntimeError('Timeout: No connections available in the pool')

  async def __release_connection(self, conn: Connection):
    await conn.rollback()
    self._pool.append(conn)
    async with self._returned:
      self._returned.notify()

  @asynccontextmanager
  async def connection(self, timeout=10) -> Connection:
    conn = await self.__acquire_connection(timeout)
    try:
      yield conn
    finally:
      await self.__release_connection(conn)
