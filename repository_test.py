from datetime import datetime, date
from unittest import TestCase
from repository import Repository, Status
from requests import TransactionRequest
from currency_config import Currency
from pydantic_core._pydantic_core import ValidationError
from uuid import uuid4
from Transaction import Transaction
from sqlmodel import Session, delete

class TestRepository(TestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.repository = Repository()
        cls.repository.register_currency(Currency(currency="TEST1", country="United Kingdom")) 
        cls.repository.register_currency(Currency(currency="TEST2", country="United States"))

    def setUp(self):
        self.repository = self.__class__.repository
        with Session(self.repository.engine) as session:
            session.exec(delete(Transaction))
            session.commit()        

    @classmethod
    def tearDownClass(cls):
        with Session(cls.repository.engine) as session:
            session.exec(delete(Transaction))
            session.exec(delete(Currency))
            session.commit()
        super().tearDownClass()      

    def test_successful_transaction_creation(self):
        tx_req = TransactionRequest(amount=20.0, currency="TEST1", user_id="123")
        transactions = [tx_req]
        res = self.repository.create_transactions(transactions)
        self.assertEqual(res["status"], Status.SUCCESS)
        self.assertEqual(res["message"], f"transactions {transactions} submitted successfully.")

    def test_transaction_with_missing_required_fields(self):

        with self.assertRaises(ValidationError) as context:
            tx_req = TransactionRequest(
                amount=20.0,
                currency="TEST1"
            )
            self.repository.create_transactions([tx_req])
        print(context.exception)
        self.assertIn("user_id", str(context.exception))

    def test_unresigered_currency(self):
        INVALID = "INVALID"
        invalid_currencies = {INVALID}
        tx_req = TransactionRequest(amount=20.0, currency=INVALID, user_id="123")
        transactions = [tx_req]
        res = self.repository.create_transactions(transactions)
        self.assertEqual(res["status"], Status.FAILURE)
        self.assertEqual(res["message"], f"transaction failed because there are/is invalid currencies {invalid_currencies}.")  

    def test_fetch_transactions(self): 
        timestamp = datetime.now()   
        id_one = uuid4()
        id_two = uuid4()

        transactions = [
            TransactionRequest(id=id_one, amount=2.0, currency="TEST1", user_id="123", date=timestamp),
            TransactionRequest(id=id_two, amount=3.0, currency="TEST1", user_id="253", date=timestamp)
        ]
        self.repository.create_transactions(transactions)
        res = self.repository.fetch_transactions()

        expected_transactions = [
            Transaction(id=id_one, amount=2.0, currency="TEST1", user_id="123", date=timestamp),
            Transaction(id=id_two, amount=3.0, currency="TEST1", user_id="253", date=timestamp)
        ]
        
        self.assertEqual(res["status"], Status.SUCCESS)
        self.assertIn(expected_transactions[0], res["transactions"])
        self.assertIn(expected_transactions[1], res["transactions"])

    def test_fetch_total_by_currency(self):
        timestamp = datetime.now()   
        id = uuid4()

        transactions = [TransactionRequest(id=id, amount=99.0, currency="TEST2", user_id="445", date=timestamp)]
        # expected = [Transaction(id=id, amount=99.0, currency="TEST2", user_id="445", date=timestamp)]
        self.repository.create_transactions(transactions)
        res = self.repository.fetch_total_by_currency("TEST2")
        self.assertEqual(res["status"], Status.SUCCESS)
        self.assertEqual(99.0, float(res["amount"]))

             
