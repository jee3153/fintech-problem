from pydantic import BaseModel
from datetime import datetime
from uuid import UUID

class TransactionRequest(BaseModel):
    id: UUID | None = None
    amount: float
    currency: str
    user_id: str
    date: datetime = datetime.now()

class CurrencyRequest(BaseModel):
    id: UUID | None = None
    currency: str
    country: str
