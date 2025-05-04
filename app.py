from typing import Union
from fastapi import FastAPI, HTTPException
from requests import TransactionRequest, CurrencyRequest
from collections import defaultdict
from datetime import date
from operator import itemgetter, attrgetter
from repository import Repository, Status, GroupBy
from db_config import start_db_engine
from uuid import UUID
from enum import Enum

app = FastAPI()
transactions: list[TransactionRequest] = []
transactions_of_currency = defaultdict(float)

repository = Repository(start_db_engine())

@app.get("/health")
def check_health():
    return {"health": "OK"}

@app.get("/v1/transactions")
def read_transactions():
    response = repository.fetch_transactions()
    if response["status"] == Status.FAILURE:
        return {"message": response["message"]}
    return {"transactions": response["transactions"]}

@app.post("/v1/transaction")
async def create_transaction(transaction: TransactionRequest):
    response = repository.create_transactions([transaction])
    if response["status"] == Status.FAILURE:
        raise HTTPException(status_code=409, detail=response["message"])
    return f"transaction {transaction} submitted succesfully"

@app.post("/v1/currency")
async def create_currency(currency: CurrencyRequest):
    response = repository.register_currency(currency)
    if response["status"] == Status.FAILURE:
        raise HTTPException(status_code=409, detail=response["message"])
    return response["message"]

@app.get("/v1/total/{currency}")
async def get_total_amount_of(currency: str):
    response = repository.fetch_total_by_currency(currency)
    if response["status"] == Status.FAILURE:
        raise HTTPException(status_code=400, detail=f"{response.message}")
    return {"amount": response["amount"]}

@app.get("/v1/transactions/{date_str}")
async def get_transactions_by_date(date_str: str):
    response = repository.fetch_transactions_by_date(date_str)
    if response["status"] == Status.FAILURE:
        raise HTTPException(status_code=400, detail=f'{response["message"]}')
    return {"transactions": response["transactions"]}

@app.get("/v1/amount")
async def get_transactions_within_range(start: float, end: float):
    response = repository.fetch_transactions_within_amount_range(start, end)
    if response["status"] == Status.FAILURE:
        raise HTTPException(status_code=400, detail=response["message"])          
    return {"transactions": response["transactions"]}

@app.get("/v1/paginated")
async def get_paginated_transactions(offset: int = 0, limit: int = 10):
    response = repository.paginated_transactions(offset, limit)
    if response["status"] == Status.FAILURE:
        raise HTTPException(status_code=400, detail=response["message"])
    return {"transactions": response["transactions"]}

@app.get("/v1/user/{user_id}")
async def get_transactions_by_user(user_id: str):
    response = repository.fetch_transactions_by_user_id(user_id)
    if response["status"] == Status.FAILURE:
        raise HTTPException(status_code=400, detail=response["message"])
    return {"transactions": response["transactions"]} 

@app.put("/v1/delete/{transaction_id}")
async def delete_transaction(transaction_id: UUID):
    response = repository.delete_transaction(transaction_id)
    if response["status"] == Status.FAILURE:
        raise HTTPException(status_code=400, detail=response["message"])
    return response["result"]

@app.get("/v1/report")
async def read_report(from_date: str|None, to_date: str|None, group_by: str):
    group_by = group_by.upper()
    groups = group_by.split(sep=",")

    for group in groups:
        if group not in [GroupBy.CURRENCY.name, GroupBy.DAY.name, GroupBy.USER.name]:
            raise HTTPException(status_code=400, detail=f"the query group_by {group_by} is not valid. it should be either user, currency or day")
    
    response = repository.get_report(from_date, to_date, groups)
    if response["status"] == Status.FAILURE:
        raise HTTPException(status_code=400, detail=response["message"])
    return  response["results"]
    

