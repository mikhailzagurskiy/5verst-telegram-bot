from typing import Union
from pydantic import BaseModel, NewPath, FilePath, DirectoryPath, PositiveInt


class Config(BaseModel):
  path: Union[FilePath, NewPath]
  migrations: DirectoryPath
  max_connections: PositiveInt
