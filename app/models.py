from pydantic import BaseModel
from typing import Optional

class Item(BaseModel):
    name: str
    quantity: int

class UpdateItem(BaseModel):
    name: Optional[str] = None
    quantity: Optional[int] = None

