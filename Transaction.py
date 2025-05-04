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

    def __eq__(self, other):
        if not isinstance(other, Transaction):
            return False
        return (
            self.id == other.id and
            self.amount == other.amount and
            self.currency == other.currency and
            self.user_id == other.user_id and
            self.date == other.date and
            self.deleted == other.deleted
        )
