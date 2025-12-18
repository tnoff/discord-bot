# Schema commons
from typing import Literal
from pydantic import BaseModel

# Pydantic models
class StorageConfig(BaseModel):
    '''Storage backend configuration'''
    backend: Literal['s3']
