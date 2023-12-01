from pydantic import BaseModel, FilePath, DirectoryPath, PositiveInt


class Config(BaseModel):
  path: FilePath
  migrations: DirectoryPath
  max_connections: PositiveInt
