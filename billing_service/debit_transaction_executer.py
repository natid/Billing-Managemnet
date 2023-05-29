from transaction_service import perform_transaction
import debit_transaction_mq
from settings import DEBIT, OUR_BANK_ACCOUNT


def debit_transaction_executer():
    while True:
        debit_transaction = debit_transaction_mq.read_debit_transaction()
        try:
            res = perform_transaction(debit_transaction.dst_bank_account, OUR_BANK_ACCOUNT, debit_transaction.amount, DEBIT)
        except Exception:
            continue
