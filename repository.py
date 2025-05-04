from db_config import start_db_engine
from sqlmodel import Session, select, func, update, insert as create
from Transaction import Transaction
from enum import Enum
from datetime import date
from uuid import UUID
from currency_config import Currency

class Status(Enum):
    SUCCESS=0
    FAILURE=1

class GroupBy(Enum):
    USER=0
    DAY=1
    CURRENCY=2    


class Repository:
    def __init__(self, engine=start_db_engine()) -> Status:
        self.engine = engine

    def create_transactions(self, transactions:list[any]):
        with Session(self.engine) as session:  
            try:      
                currencies = {tx.currency for tx in transactions}
                stmt = select(Currency).where(Currency.currency.in_(currencies))
                valid_currencies = {c.currency for c in session.exec(stmt)}
                invalid_currencies = currencies - valid_currencies
                
                if invalid_currencies:
                    return {
                        "status": Status.FAILURE,
                        "message": f"transaction failed because there are/is invalid currencies {invalid_currencies}."
                    }
                    
                session.add_all([
                    Transaction(
                        id=tx.id,
                        amount=tx.amount,
                        currency=tx.currency,
                        user_id=tx.user_id,
                        date=tx.date
                    ) for tx in transactions
                ]) 
                session.commit()    

                return {
                    "status": Status.SUCCESS,
                    "message": f"transactions {transactions} submitted successfully."
                }     
            except Exception as e:
                session.rollback()
                return {
                    "status": Status.FAILURE,
                    "message": f"transaction failed due to {e}"
                }   

    def register_currency(self, currency: Currency):
        with Session(self.engine) as session:
            try:
                stmt = create(Currency).values(
                    currency=currency.currency,
                    country=currency.country
                )
                session.exec(stmt)
                session.commit()
                return {
                    "status": Status.SUCCESS, 
                    "message": f"currency {currency} successfully created"
                }         
            except Exception as e:
                session.rollback()
                return {
                    "status": Status.FAILURE, 
                    "message": f"registering currency {currency} failed due to {e}"
                }
            
    def fetch_transactions(self):
        with Session(self.engine) as session:
            try:
                stmt = select(Transaction).where(Transaction.deleted == False)
                return {"status": Status.SUCCESS, "transactions": session.exec(stmt).fetchall()}
            except Exception as e:
                return {"status": Status.FAILURE, "message": e}
            
    def fetch_total_by_currency(self, currency):
        with Session(self.engine) as session:
            try:
                stmt = select(func.sum(Transaction.amount)).where(Transaction.currency == currency)
                total = session.exec(stmt).one()
                
                return {"status": Status.SUCCESS, "amount": total}
            except Exception as e:
                return {"status": Status.FAILURE, "message": e}
            
    def fetch_transactions_by_date(self, date_str: str):
        tx_date = date.fromisoformat(date_str)
        with Session(self.engine) as session:
            try:
                stmt = select(Transaction).where(func.date(Transaction.date) == tx_date).where(Transaction.deleted == False)
                return {"status": Status.SUCCESS, "transactions": session.exec(stmt).fetchall()}
            except Exception as e:
                return {"status": Status.FAILURE, "message": f"failed to fetch transactions by date cause:{e}"}

    def fetch_transactions_within_amount_range(self, start: float, end: float):
        with Session(self.engine) as session:
            try:
                stmt = select(Transaction).where(start <= Transaction.amount).where(Transaction.amount <= end).where(Transaction.deleted == False)
                return {"status": Status.SUCCESS, "transactions": session.exec(stmt).fetchall()}
            except Exception as e:
                return {"status": Status.FAILURE, "message": f"failed to fetch transaction with given range {start} - {end} due to: {e}"}
            
    def paginated_transactions(self, offset: int, limit: int):
        with Session(self.engine) as session:
            try:
                stmt = select(Transaction).offset(offset).limit(limit).where(Transaction.deleted == False)
                return {"status": Status.SUCCESS, "transactions": session.exec(stmt).fetchall()}
            except Exception as e:
                return {"status": Status.FAILURE, "message": f"fetching paginated transactions failed due to {e}"}
            
    def fetch_transactions_by_user_id(self, user_id: str):
        with Session(self.engine) as session:
            try:
                stmt = select(Transaction).where(Transaction.user_id == user_id).where(Transaction.deleted == False)
                return {"status": Status.SUCCESS, "transactions": session.exec(stmt).fetchall()}
            except Exception as e:
                return {"status": Status.FAILURE, "message": f"fetching transactions of user {user_id} failed due to {e}"}
            
    def delete_transaction(self, transaction_id: UUID):
        with Session(self.engine) as session:
            try:
                stmt = update(Transaction).where(Transaction.id == transaction_id)
                transaction = session.exec(stmt).one()  
                transaction.deleted = True      
                session.commit()
                session.refresh(transaction)
                return {"status": Status.SUCCESS, "result": f"transaction {transaction_id} is successfully deleted."}
            except Exception as e:
                return {"status": Status.FAILURE, "message": f"Failed to delete transaction {transaction_id}."}
            
    def get_report(self, from_date, to_date, groups: list[str]):
        print(groups)
        group_by_map = {
            GroupBy.CURRENCY: Transaction.currency, 
            GroupBy.DAY: func.date(Transaction.date).label("date"), 
            GroupBy.USER: Transaction.user_id
        }
        groups = [GroupBy[group_by] for group_by in groups ]

        with Session(self.engine) as session:
            try:
                select_column = [func.sum(Transaction.amount).label("total_amount")]
                group_by_column = []
                for group in groups:
                    select_column.append(group_by_map[group])
                    group_by_column.append(group_by_map[group])

                stmt = select(*select_column).where(Transaction.deleted == False)

                if from_date and to_date:
                    start = date.fromisoformat(from_date)
                    end = date.fromisoformat(to_date)
                    stmt = stmt\
                            .where(func.date(Transaction.date) >= start)\
                            .where(func.date(Transaction.date) <= end)               

                stmt = stmt.group_by(*group_by_column)

                results = session.exec(stmt).all()
                formatted_results = []

                for result in results:
                    result_dict = {"total_amount": float(result.total_amount or 0)}

                    if GroupBy.USER in groups:
                        result_dict["user_id"] = result.user_id if result.user_id else ""
                    if GroupBy.CURRENCY in groups:
                        result_dict["currency"] = result.currency if result.currency else ""
                    if GroupBy.DAY in groups:
                        result_dict["date"] = str(result.date) if result.date else ""     
                   
                    formatted_results.append(result_dict)

                final_result = {}
                for r in formatted_results:
                    current_level = final_result

                    for i, group in enumerate(groups):
                        if GroupBy.USER ==  group:
                            current_key = r["user_id"]
                        elif GroupBy.CURRENCY == group:
                            current_key = r["currency"]
                        elif GroupBy.DAY == group:
                            current_key = r["date"]  
                        else: continue    

                        if i == len(groups)-1:
                            current_level[current_key] = r["total_amount"]
                        else:
                            current_level = current_level.setdefault(current_key, {})    
                return {"status": Status.SUCCESS, "results": final_result}
            except Exception as e:
                return {"status": Status.FAILURE, "message": f"Fetching report failed due to {e}"}