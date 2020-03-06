from enum import Enum


class TransactionState(Enum):
    NOT_STARTED = 1
    INITIATED = 2
    PREPARED = 3
    COMMITTED = 4
    ABORTED = 5
    COMPLETED = 6


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


class Coordinator:
    """
    Coordinator for a 2 Phase Commit
    """

    def __init__(self, unique_id, log_file):
        """Constructor"""
        self.unique_id = unique_id
        self.cohorts = []
        self.log_file = log_file
        self.protocol_DB = ProtocolDB()

    def start(self):
        self.recover_from_logs()

    def run(self):
        self.construct_transaction()
