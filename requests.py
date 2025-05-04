from pydantic import BaseModel
from datetime import datetime
from uuid import uuid4, UUID

class TransactionRequest(BaseModel):
    amount: float
    currency: str
    user_id: str
    date: datetime = datetime.now()

class CurrencyRequest(BaseModel):
    currency: str
    country: str
