from typing import Literal, Union
from pydantic import BaseModel, NewPath, FilePath, DirectoryPath, PositiveInt


class Config(BaseModel):
  path: Union[FilePath, NewPath, Literal[":memory:"]]
  migrations: DirectoryPath
  max_connections: PositiveInt
