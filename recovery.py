import transaction_log_utils
from constants import *
from threading import Thread
from communication_utils import sendMessageToCohort
import time

class RecoveryThread(Thread):

    def __init__(self, protocol_DB, prepare_timeout_info, timeout_transaction_info):
        Thread.__init__(self)
        self.protocol_DB = protocol_DB
        self.prepare_timeout_info = prepare_timeout_info
        self.timeout_transaction_info = timeout_transaction_info
   
    def set_channel(self, channel):
        self.channel = channel

    def run(self):

        while True:

            prepare_timed_out_transactions = self.check_prepare_timeout()
            ack_completed_transactions = self.check_ack_timeout()

            # Delete the transactions which timed out waiting for reply for PREPARE from the timeout map
            for transaction_id in prepare_timed_out_transactions:
                del self.prepare_timeout_info[transaction_id]
                self.timeout_transaction_info[transaction_id] = COMMIT_ACK_TIMEOUT

            # Delete the transactions which have received all the ACKs from the timeout map
            for transaction_id in ack_completed_transactions:
                del self.timeout_transaction_info[transaction_id]

            time.sleep(1)

    def check_prepare_timeout(self):

        prepare_timed_out_transactions = []
        # Timeout check after PREPARE was sent
        for transaction_id in self.prepare_timeout_info.keys():
            timeout = self.prepare_timeout_info[transaction_id]
            cohorts = self.protocol_DB.transactions[transaction_id].cohorts

            if timeout == 0:
                print("Timed out waiting for reply to PREPARE from the cohorts for transaction id: " + transaction_id)
                self.protocol_DB.force_abort_transaction(transaction_id)
                prepare_timed_out_transactions.append(transaction_id)

                transaction = self.protocol_DB.transactions[transaction_id]

                transaction_log_utils.insert_log(transaction)
                for cohort in cohorts:
                    print("Sending ABORT to cohort " + str(cohort))
                    sendMessageToCohort(self.channel, cohort, transaction.state,
                                        transaction_id)
            else:
                self.prepare_timeout_info[transaction_id] = timeout - 1

        return prepare_timed_out_transactions

    def check_ack_timeout(self):

        ack_completed_transactions = []
        # Timeout check after COMMIT/ABORT was sent
        for transaction_id in self.timeout_transaction_info.keys():
            timeout = self.timeout_transaction_info[transaction_id]
            cohorts = self.protocol_DB.transactions[transaction_id].cohorts

            if self.protocol_DB.check_all_cohorts_acked(transaction_id):
                transaction_log_utils.delete_log(transaction_id)
                if transaction_id in self.timeout_transaction_info:
                    ack_completed_transactions.append(transaction_id)
                continue

            # Timeout has happened
            if timeout == 0:
                print("Timed out waiting for ACK for transaction id: " + transaction_id)
                for cohort in cohorts:
                    print("Resending COMMIT/ABORT to cohort " + str(cohort))
                    sendMessageToCohort(self.channel, cohort, self.protocol_DB.transactions[transaction_id].state,
                                        transaction_id)
                self.timeout_transaction_info[transaction_id] = COMMIT_ACK_TIMEOUT
            # Timeout not yet elapsed
            else:
                self.timeout_transaction_info[transaction_id] = timeout - 1

        return ack_completed_transactions