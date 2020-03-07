import transaction_log_utils
from constants import *
from threading import Thread
from communication_utils import sendMessageToCohort
import time

class RecoveryThread(Thread):

    def __init__(self, protocol_DB, timeout_transaction_info):
        Thread.__init__(self)
        self.protocol_DB = protocol_DB
        self.timeout_transaction_info = timeout_transaction_info
    
    def run(self):
        while True:
            for key in self.timeout_transaction_info.keys():
                transaction_id = key
                timeout = self.timeout_transaction_info[transaction_id]
                if self.protocol_DB.check_all_cohorts_acked(transaction_id):
                    transaction_log_utils.delete_log(transaction_id)
                    del self.timeout_transaction_info[transaction_id]
                    continue
                cohorts = self.protocol_DB[transaction_id].cohorts
                if len(cohorts) > 0 and timeout == 0:
                    for cohort in self.cohorts:
                        sendMessageToCohort(self.channel, cohort, self.protocol_DB[transaction_id].state)
                    self.timeout_transaction_info[transaction_id] = COMMIT_ACK_TIMEOUT
                else:
                    self.timeout_transaction_info[transaction_id] = timeout - 1
                time.sleep(1)

