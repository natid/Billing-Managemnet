from apscheduler.schedulers.blocking import BlockingScheduler
from billing_dal import get_all_pending_debit_transactions, update_debit_transaction
import debit_transaction_mq
from settings import IN_PROCESS

scheduler = BlockingScheduler()

@scheduler.scheduled_job('interval', days=1)
def daily_debit_handler():
    debit_transactions = get_all_pending_debit_transactions()

    for debit_transaction in debit_transactions:
        update_debit_transaction(transaction_id=debit_transaction.id, status=IN_PROCESS)
        debit_transaction_mq.write(debit_transaction)


if __name__ == '__main__':
    scheduler.start()
