from fastapi import FastAPI
from pydantic import BaseModel
from transaction_service import perform_transaction
from settings import OUR_BANK_ACCOUNT, DEBIT, CREDIT
from billing_dal import create_credit_transaction, update_credit_transaction


class Transaction(BaseModel):
    dst_bank_account: str
    amount: float


app = FastAPI()


@app.post("/perform_advance/")
async def perform_advance(transaction: Transaction):
    create_credit_transaction(status='In Process')

    try:
        perform_transaction(OUR_BANK_ACCOUNT, transaction.dst_bank_account, transaction.amount, CREDIT)
    except Exception:
        return {"status": 'Fail'}


    return {"status": 'Success'}


