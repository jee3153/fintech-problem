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
    # @classmethod
    # def setUpClass(cls):
    #     super().setUpClass()
    #     cls.repository = Repository()
    #     cls.repository.register_currency(Currency(currency="TEST1", country="United Kingdom")) 
    #     cls.repository.register_currency(Currency(currency="TEST2", country="United States"))

    def setUp(self):
        self.repository = Repository()
        self.repository.register_currency(Currency(currency="TEST1", country="United Kingdom")) 
        self.repository.register_currency(Currency(currency="TEST2", country="United States"))
        with Session(self.repository.engine) as session:
            session.exec(delete(Transaction))
            session.commit()        

    # @classmethod
    # def tearDownClass(cls):
    #     with Session(cls.repository.engine) as session:
    #         session.exec(delete(Transaction))
    #         session.exec(delete(Currency))
    #         session.commit()
    #     super().tearDownClass()  

    def tearDown(self):
        with Session(self.repository.engine) as session:
            session.exec(delete(Transaction))
            session.exec(delete(Currency))
            session.commit()

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
        res = self.repository.create_transactions([TransactionRequest(amount=20.0, currency=INVALID, user_id="123")])
        self.assertEqual(res["status"], Status.FAILURE)
        self.assertEqual(res["message"], f"transaction failed because there are/is invalid currencies {invalid_currencies}.")

    def test_register_currency(self):
        currency = Currency(currency="TEST3", country="Some_country")
        res = self.repository.register_currency(currency)
        self.assertEqual(res["status"], Status.SUCCESS)
        self.assertEqual(res["message"], f"currency {currency} successfully created")  

    def test_registering_existing_currency(self):
        currency = Currency(currency="TEST1", country="Some_country")  
        res = self.repository.register_currency(currency)

        self.assertEqual(res["status"], Status.FAILURE)
        self.assertEqual(f"The currency {currency} already exists.", res["message"])      

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
        id_one = uuid4()
        id_two = uuid4()

        transactions = [
            TransactionRequest(id=id_one, amount=99.0, currency="TEST2", user_id="445", date=timestamp),
            TransactionRequest(id=id_two, amount=100.0, currency="TEST2", user_id="435", date=timestamp )
        ]
        self.repository.create_transactions(transactions)
        res = self.repository.fetch_total_by_currency("TEST2")
        self.assertEqual(res["status"], Status.SUCCESS)
        self.assertEqual(199.0, float(res["amount"]))

    def test_fetch_total_by_date(self):
        timestamp_one = datetime(2025, 5, 5, 9, 0)
        timestamp_two = datetime(2024, 5, 4, 9, 0)
        id_one = uuid4()
        id_two = uuid4()

        transactions = [
            TransactionRequest(id=id_one, amount=23.0, currency="TEST2", user_id="445", date=timestamp_one),
            TransactionRequest(id=id_two, amount=99999.0, currency="TEST2", user_id="435", date=timestamp_two)
        ]    

        self.repository.create_transactions(transactions)
        res = self.repository.fetch_transactions_by_date("2024-05-04")
        expected = [Transaction(id=id_two, amount=99999.0, currency="TEST2", user_id="435", date=timestamp_two)]
        
        self.assertEqual(res["status"], Status.SUCCESS)
        self.assertCountEqual(res["transactions"], expected)
        self.assertListEqual(res["transactions"], expected)

    def test_fetch_transactions_within_amount_range(self):
        register_res = self.repository.register_currency(Currency(currency="TEST4", country="United Kingdom"))

        self.assertEqual(register_res["status"], Status.SUCCESS)

        timestamp = datetime.now()     
        id_one = uuid4()
        id_two = uuid4()
        id_three = uuid4()
        id_four = uuid4()

        tx1 = Transaction(id=id_one, amount=100000.0, currency="TEST4", user_id="445", date=timestamp)
        tx2 = Transaction(id=id_two, amount=9999999.0, currency="TEST4", user_id="435", date=timestamp)
        tx3 = Transaction(id=id_three, amount=20000000.0, currency="TEST4", user_id="235", date=timestamp)
        tx4 = Transaction(id=id_four, amount=40000000.0, currency="TEST4", user_id="333", date=timestamp)
        
        transactions = [
            TransactionRequest(id=id_one, amount=100000.0, currency="TEST4", user_id="445", date=timestamp),
            TransactionRequest(id=id_two, amount=9999999.0, currency="TEST4", user_id="435", date=timestamp),
            TransactionRequest(id=id_three, amount=20000000.0, currency="TEST4", user_id="235", date=timestamp),
            TransactionRequest(id=id_four, amount=40000000.0, currency="TEST4", user_id="333", date=timestamp)
        ]  
    
        create_res = self.repository.create_transactions(transactions)
        self.assertEqual(create_res["status"], Status.SUCCESS)

        res = self.repository.fetch_transactions_within_amount_range(start=9999999.0, end=50000000.0)

        self.assertEqual(res["status"], Status.SUCCESS)
        self.assertEqual(len(res["transactions"]), 3)

        self.assertNotIn(tx1, res["transactions"])
        self.assertIn(tx2, res["transactions"])
        self.assertIn(tx3, res["transactions"])
        self.assertIn(tx4, res["transactions"])

    def test_paginated_transactions(self):
        timestamp = datetime.now()     
        id_one = uuid4()
        id_two = uuid4()
        id_three = uuid4()
        id_four = uuid4()    

        transactions = [
            Transaction(id=id_one, amount=1.0, currency="TEST1", user_id="445", date=timestamp),
            Transaction(id=id_two, amount=2.0, currency="TEST1", user_id="435", date=timestamp),
            Transaction(id=id_three, amount=3.0, currency="TEST1", user_id="235", date=timestamp),
            Transaction(id=id_four, amount=4.0, currency="TEST1", user_id="333", date=timestamp)    
        ]

        create_res = self.repository.create_transactions(transactions)
        self.assertEqual(create_res["status"], Status.SUCCESS)

        res = self.repository.paginated_transactions(offset=2, limit=4)
        self.assertEqual(res["status"], Status.SUCCESS)
        self.assertEqual(len(res["transactions"]), 2)

    def test_fetch_transactions_by_user_id(self):
        timestamp = datetime.now()     
        id_one = uuid4()
        id_two = uuid4()
        id_three = uuid4()
        id_four = uuid4()

        tx1 = Transaction(id=id_one, amount=1.0, currency="TEST1", user_id="445", date=timestamp)
        tx2 = Transaction(id=id_two, amount=2.0, currency="TEST1", user_id="435", date=timestamp)
        tx3 = Transaction(id=id_three, amount=3.0, currency="TEST1", user_id="333", date=timestamp)
        tx4 = Transaction(id=id_four, amount=4.0, currency="TEST1", user_id="333", date=timestamp)        

        transactions = [tx1,tx2,tx3,tx4]

        create_res = self.repository.create_transactions(transactions)
        self.assertEqual(create_res["status"], Status.SUCCESS)

        res = self.repository.fetch_transactions_by_user_id(user_id="333")

        self.assertEqual(res["status"], Status.SUCCESS)
        self.assertEqual(len(res["transactions"]), 2)
        self.assertIn(tx3, res["transactions"])
        self.assertIn(tx4, res["transactions"])

    def test_delete_transaction(self):
        timestamp = datetime.now()     
        id_one = uuid4()
        id_two = uuid4()
        id_three = uuid4()
        id_four = uuid4()    

        tx1 = Transaction(id=id_one, amount=1.0, currency="TEST1", user_id="445", date=timestamp)
        tx2 = Transaction(id=id_two, amount=2.0, currency="TEST1", user_id="435", date=timestamp)
        tx3 = Transaction(id=id_three, amount=3.0, currency="TEST1", user_id="333", date=timestamp)
        tx4 = Transaction(id=id_four, amount=4.0, currency="TEST1", user_id="333", date=timestamp)

        transactions = [tx1,tx2,tx3,tx4]

        create_res = self.repository.create_transactions(transactions)
        self.assertEqual(create_res["status"], Status.SUCCESS)

        res = self.repository.delete_transaction(tx1.id)
        self.assertEqual(res["status"], Status.SUCCESS)
        self.assertEqual(res["result"], f"transaction {tx1.id} is successfully deleted.")

        fetch_res = self.repository.fetch_transactions_by_user_id(tx1.user_id)

        self.assertEqual(fetch_res["status"], Status.SUCCESS)
        self.assertNotIn(tx1, fetch_res["transactions"])

    def test_get_report_grouped_by_currency(self):  
        ts_one = datetime(2025, 5, 5, 9, 0) 
        ts_two = datetime(2025, 1, 5, 9, 0) 
        ts_three = datetime(2023, 1, 5, 9, 0)
        ts_four = datetime(2024, 3, 3, 9, 0)   

        id_one = uuid4()
        id_two = uuid4()
        id_three = uuid4()
        id_four = uuid4()    

        tx1 = Transaction(id=id_one, amount=1.0, currency="TEST1", user_id="445", date=ts_one)
        tx2 = Transaction(id=id_two, amount=2.0, currency="TEST1", user_id="435", date=ts_two)
        tx3 = Transaction(id=id_three, amount=3.0, currency="TEST2", user_id="333", date=ts_three)
        tx4 = Transaction(id=id_four, amount=4.0, currency="TEST2", user_id="333", date=ts_four)
        transactions = [tx1,tx2,tx3,tx4]

        expected = {"TEST1": 3.0,"TEST2": 4.0}

        create_res = self.repository.create_transactions(transactions)
        self.assertEqual(create_res["status"], Status.SUCCESS)

        report_res = self.repository.get_report(from_date="2024-01-01", to_date="2026-01-01", groups=["CURRENCY"])
        self.assertEqual(report_res["status"], Status.SUCCESS)
        self.assertDictEqual(report_res["results"], expected)

    def test_get_report_grouped_by_user(self):
        ts_one = datetime(2025, 5, 5, 9, 0) 
        ts_two = datetime(2025, 1, 5, 9, 0) 
        ts_three = datetime(2023, 1, 5, 9, 0)
        ts_four = datetime(2024, 3, 3, 9, 0)   

        id_one = uuid4()
        id_two = uuid4()
        id_three = uuid4()
        id_four = uuid4()    

        tx1 = Transaction(id=id_one, amount=1.0, currency="TEST1", user_id="445", date=ts_one)
        tx2 = Transaction(id=id_two, amount=2.0, currency="TEST1", user_id="435", date=ts_two)
        tx3 = Transaction(id=id_three, amount=3.0, currency="TEST2", user_id="333", date=ts_three)
        tx4 = Transaction(id=id_four, amount=4.0, currency="TEST2", user_id="333", date=ts_four)
        transactions = [tx1,tx2,tx3,tx4]

        expected = {"445": 1.0, "435": 2.0, "333": 7.0}

        create_res = self.repository.create_transactions(transactions)
        self.assertEqual(create_res["status"], Status.SUCCESS)

        report_res = self.repository.get_report(groups=["USER"])
        self.assertEqual(report_res["status"], Status.SUCCESS)
        self.assertDictEqual(report_res["results"], expected)

    def test_get_report_grouped_by_day(self):
        ts_one = datetime(2025, 5, 5, 9, 0) 
        ts_two = datetime(2025, 1, 5, 9, 0) 
        ts_three = datetime(2023, 1, 5, 9, 0)
        ts_four = datetime(2024, 3, 3, 9, 0)   

        id_one = uuid4()
        id_two = uuid4()
        id_three = uuid4()
        id_four = uuid4()    

        tx1 = Transaction(id=id_one, amount=1.0, currency="TEST1", user_id="445", date=ts_one)
        tx2 = Transaction(id=id_two, amount=2.0, currency="TEST1", user_id="435", date=ts_two)
        tx3 = Transaction(id=id_three, amount=3.0, currency="TEST2", user_id="333", date=ts_three)
        tx4 = Transaction(id=id_four, amount=4.0, currency="TEST2", user_id="333", date=ts_four)
        transactions = [tx1,tx2,tx3,tx4]

        expected = {"2025-05-05": 1.0, "2025-01-05": 2.0, "2023-01-05": 3.0, "2024-03-03": 4.0}

        create_res = self.repository.create_transactions(transactions)
        self.assertEqual(create_res["status"], Status.SUCCESS)

        report_res = self.repository.get_report(groups=["DAY"])
        self.assertEqual(report_res["status"], Status.SUCCESS)
        self.assertDictEqual(report_res["results"], expected)

    def test_get_report_grouped_by_day_currency(self):
        ts_one = datetime(2025, 5, 5, 9, 0) 
        ts_two = datetime(2025, 1, 5, 9, 0) 
        ts_three = datetime(2023, 1, 5, 9, 0)
        ts_four = datetime(2024, 3, 3, 9, 0)   

        id_one = uuid4()
        id_two = uuid4()
        id_three = uuid4()
        id_four = uuid4()    

        tx1 = Transaction(id=id_one, amount=1.0, currency="TEST1", user_id="445", date=ts_one)
        tx2 = Transaction(id=id_two, amount=2.0, currency="TEST1", user_id="435", date=ts_two)
        tx3 = Transaction(id=id_three, amount=3.0, currency="TEST2", user_id="333", date=ts_three)
        tx4 = Transaction(id=id_four, amount=4.0, currency="TEST2", user_id="333", date=ts_four)
        transactions = [tx1,tx2,tx3,tx4]

        expected = {
            "2025-05-05": {"TEST1": 1.0}, 
            "2025-01-05": {"TEST1": 2.0}, 
            "2023-01-05": {"TEST2": 3.0}, 
            "2024-03-03": {"TEST2": 4.0}
        }

        create_res = self.repository.create_transactions(transactions)
        self.assertEqual(create_res["status"], Status.SUCCESS)

        report_res = self.repository.get_report(groups=["DAY", "CURRENCY"])
        self.assertEqual(report_res["status"], Status.SUCCESS)
        self.assertDictEqual(report_res["results"], expected)

    def test_get_report_grouped_by_user_day(self):
        ts_one = datetime(2025, 5, 5, 9, 0) 
        ts_two = datetime(2025, 1, 5, 9, 0) 
        ts_three = datetime(2023, 1, 5, 9, 0)
        ts_four = datetime(2024, 3, 3, 9, 0)   

        id_one = uuid4()
        id_two = uuid4()
        id_three = uuid4()
        id_four = uuid4()    

        tx1 = Transaction(id=id_one, amount=1.0, currency="TEST1", user_id="445", date=ts_one)
        tx2 = Transaction(id=id_two, amount=2.0, currency="TEST1", user_id="435", date=ts_two)
        tx3 = Transaction(id=id_three, amount=3.0, currency="TEST2", user_id="333", date=ts_three)
        tx4 = Transaction(id=id_four, amount=4.0, currency="TEST2", user_id="333", date=ts_four)
        transactions = [tx1,tx2,tx3,tx4]

        expected = {
            "445": {"2025-05-05": 1.0}, 
            "435": {"2025-01-05": 2.0}, 
            "333": {"2023-01-05": 3.0, "2024-03-03": 4.0}, 
        }

        create_res = self.repository.create_transactions(transactions)
        self.assertEqual(create_res["status"], Status.SUCCESS)

        report_res = self.repository.get_report(groups=["USER", "DAY"])
        self.assertEqual(report_res["status"], Status.SUCCESS)
        self.assertDictEqual(report_res["results"], expected)

    def test_get_report_grouped_by_currency_user(self):
        ts_one = datetime(2025, 5, 5, 9, 0) 
        ts_two = datetime(2025, 1, 5, 9, 0) 
        ts_three = datetime(2023, 1, 5, 9, 0)
        ts_four = datetime(2024, 3, 3, 9, 0)   

        id_one = uuid4()
        id_two = uuid4()
        id_three = uuid4()
        id_four = uuid4()    

        tx1 = Transaction(id=id_one, amount=1.0, currency="TEST1", user_id="445", date=ts_one)
        tx2 = Transaction(id=id_two, amount=2.0, currency="TEST1", user_id="435", date=ts_two)
        tx3 = Transaction(id=id_three, amount=3.0, currency="TEST2", user_id="333", date=ts_three)
        tx4 = Transaction(id=id_four, amount=4.0, currency="TEST2", user_id="333", date=ts_four)
        transactions = [tx1,tx2,tx3,tx4]

        expected = {
            "TEST1": {"445": 1.0, "435": 2.0}, 
            "TEST2": {"333": 7.0}, 
        }

        create_res = self.repository.create_transactions(transactions)
        self.assertEqual(create_res["status"], Status.SUCCESS)

        report_res = self.repository.get_report(groups=["CURRENCY", "USER"])
        self.assertEqual(report_res["status"], Status.SUCCESS)
        self.assertDictEqual(report_res["results"], expected)


