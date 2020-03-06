from recovery import RecoveryThread
from enum import Enum

COMMIT_ACK_TIMEOUT = 500

class TransactionState(Enum):
    NOT_STARTED = 1
    INITIATED = 2
    COMMITTABLE = 3
    ABORTABLE = 4
    COMPLETED = 5


class Transaction:

    def __init__(self, id):
        self.id = id
        self.ack_table = dict()
        self.prepared_table = dict()
        self.state = TransactionState.NOT_STARTED
        self.cohorts = []

    def set_ack_received(self, cohort):
        if cohort in self.cohorts:
            self.ack_table[cohort] = True

    def set_cohort_prepared_status(self, cohort, cohort_status):
        if cohort in self.cohorts:
            self.prepared_table[cohort] = cohort_status

    def set_transaction_state(self, state):
        self.state = state

    def set_cohort_list(self, cohorts):
        self.cohorts = cohorts

    def check_all_cohorts_prepared(self):
        return len(self.ack_table) == len(self.cohorts)

    def check_all_cohorts_acked(self):
        return len(self.ack_table) == len(self.cohorts)


class ProtocolDB:

    def __init__(self):
        self.transactions = dict()

    def add_transaction(self, transaction, cohorts):
        self.transactions[transaction.id] = transaction
        self.transactions[transaction.id].set_cohort_list(cohorts)

    def remove_transaction(self, transaction_id):
        del self.transactions[transaction_id]

    def set_ack_received(self, transaction_id, cohort):
        self.transactions[transaction_id].set_ack_received(cohort)

    def set_cohort_prepared_status(self, transaction_id, cohort, cohort_status):
        self.transactions[transaction_id].set_cohort_prepared_status(cohort, cohort_status)

    def set_transaction_state(self, transaction_id, state):
        self.transactions[transaction_id].set_transaction_state(state)

    def check_all_cohorts_prepared(self, transaction_id):
        return self.transactions[transaction_id].check_all_cohorts_prepared()

    def check_all_cohorts_acked(self, transaction_id):
        return self.transactions[transaction_id].check_all_cohorts_acked()

    def empty(self):
        return len(self.transactions) == 0


class Coordinator:
    """
    Coordinator for a 2 Phase Commit
    """

    def __init__(self):
        """Constructor"""
        self.cohorts = []
        #self.log_file = log_file
        self.protocol_DB = ProtocolDB()
        self.recovery_thread = RecoveryThread()
        self.timeout_transaction_list = dict()

    def start(self):
        pass
        #self.reconstruct_protocol_DB()

    def run(self):
        if not self.protocol_DB.empty():
            for transaction in self.protocol_DB.transactions:
                self.timeout_transaction_list[transaction.id, COMMIT_ACK_TIMEOUT]
            # send commit/abort to all
            
        #while (read from file/pipe)
            #self.construct_transaction()

if __name__ == "__main__":
    COORDINATOR = Coordinator()
    COORDINATOR.start()
    COORDINATOR.run()
