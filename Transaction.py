from datetime import datetime
from sqlmodel import Field, SQLModel
from uuid import UUID, uuid4

class Transaction(SQLModel, table=True):
    id: UUID = Field(default_factory=uuid4, primary_key=True)
    amount: float
    currency: str
    user_id: str
    date: datetime
    deleted: bool = False
