from db.pool import Connection

from entities.role import Role
from entities.common import as_int


class RoleOrm:
  async def create(self, conn: Connection, name: str, emoji: str) -> int:
    async with conn.execute('''INSERT INTO Role(name, emoji) VALUES(:1, :2) RETURNING id''', [name, emoji]) as cursor:
      row = await cursor.fetchone()
      return as_int(row[0])

  async def list(self, conn: Connection) -> list[Role]:
    return [Role.from_db_row(row) for row in await conn.execute_fetchall('''SELECT * FROM Role''')]

  async def get(self, conn: Connection, role_id: int) -> Role:
    async with conn.execute('''SELECT * FROM Role WHERE id=:1''', [role_id]) as cursor:
      row = await cursor.fetchone()
      return Role.from_db_row(row)

  async def update(self, conn: Connection, role_id: int, **fields):
    if len(fields.keys()) == 0 or not role_id:
      return

    field_names, field_values = zip(*fields.items())
    field_names = [(f'{name} = :{idx + 1}')
                   for (idx, name) in enumerate(field_names)]

    field_values = list(field_values)
    id_pos = len(field_names) + 1
    field_values.append(role_id)

    field_names = ', '.join(field_names)
    sql = f'''UPDATE Role SET {field_names} WHERE id = :{id_pos}'''

    await conn.execute(sql, field_values)

  async def find(self, conn: Connection, **fields) -> Role:
    field_names, field_values = zip(*fields.items())
    field_names = [(f'{name}=:{idx + 1}')
                   for (idx, name) in enumerate(field_names)]

    field_values = list(field_values)
    field_names = ', '.join(field_names)
    sql = f'''SELECT * FROM Role WHERE {field_names}'''

    async with conn.execute(sql, field_values) as cursor:
      row = await cursor.fetchone()
      return Role.from_db_row(row)

  async def delete(self, conn: Connection, role_id: int):
    await conn.execute('''DELETE FROM Role WHERE id = :1''', [role_id])
