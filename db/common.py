from aiosqlite import Connection, Cursor
from contextlib import asynccontextmanager


@asynccontextmanager
async def execute_query(conn: Connection, sql, params=None) -> Cursor:
  params = [] if params is None else params
  cursor = await conn.execute(sql, params)
  try:
    yield cursor
  finally:
    await cursor.close()


@asynccontextmanager
async def execute_transaction(conn: Connection):
  try:
    yield
    await conn.commit()
  except:
    await conn.rollback()
    raise Exception('Unable execute transaction')
