from sqlmodel import SQLModel, Field
from uuid import UUID, uuid4

class Currency(SQLModel, table=True):
    id: UUID = Field(default_factory=uuid4, primary_key=True)
    currency: str 
    country: str