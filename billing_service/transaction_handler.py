from apscheduler.schedulers.blocking import BlockingScheduler
from transaction_service import download_report
from billing_dal import update_debit_transaction, update_transaction_status, get_open_transactions, \
    update_credit_transaction, create_pending_debit_transaction, get_last_debit_transaction_date
from settings import CREDIT, DEBIT, FAIL, SUCCESS
from datetime import datetime, timedelta

scheduler = BlockingScheduler()


@scheduler.scheduled_job('interval', minutes=15)
def transaction_handler():

    # Download the transaction report
    # download_report returns list of values, each row represents a transaction
    report = download_report()

    # Get only transactions in status "In process" or "Pending"
    open_transactions = get_open_transactions(report)

    for transaction_id, status, credit_amount, direction in open_transactions:
        if direction == CREDIT:
            if status == FAIL:
                update_credit_transaction(transaction_id=transaction_id, status=FAIL)

            elif status == SUCCESS:
                update_credit_transaction(transaction_id=transaction_id, status=SUCCESS)

                scheduled_date = datetime.now()

                for _ in range(12):
                    # Calculate the scheduled time for the next debit transaction
                    scheduled_date = scheduled_date + timedelta(weeks=1)

                    create_pending_debit_transaction(credit_transaction_id=transaction_id, amount=credit_amount/12, date=scheduled_date)

        if direction == DEBIT:
            if status == FAIL:
                update_debit_transaction(transaction_id=transaction_id, status=FAIL)

                # In case of failed debit transaction, create new debit transaction a week after the last one
                last_transaction_date = get_last_debit_transaction_date(transaction_id)
                create_pending_debit_transaction(transaction_id=transaction_id, date=last_transaction_date+timedelta(weeks=1))

            elif status == SUCCESS:
                update_debit_transaction(transaction_id=transaction_id, status=SUCCESS)


if __name__ == '__main__':
    scheduler.start()