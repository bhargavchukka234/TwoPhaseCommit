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
        self.state = NOT_STARTED
        self.cohorts = []

    def set_ack_received(cohort):
        if cohort in cohorts:
            ack_table[cohort] = True
        
    def set_cohort_prepared_status(cohort, cohort_status):
        if cohort in cohorts:
            prepared_table[cohort] = cohort_status

    def set_transaction_state(state):
        self.state = state

    def set_cohort_list(cohorts):
        self.cohorts = cohorts

    def check_all_cohorts_prepared():
        return len(ack_table) == len(cohorts)

    def check_cohorts_acked():
        return len(ack_table) == len(cohorts)

    
class ProtocolDB:

    def __init__(self):
        self.transactions = dict()

    def add_transaction(transaction, cohorts):
        self.transactions[transaction.id] = transaction
        self.transactions[transaction.id].set_cohort_list

    def remove_transaction(transaction_id):
        del self.transactions[transaction.id]

    def set_ack_received(transaction_id, cohort):
        self.transactions[transaction.id].set_ack_received(cohort)

    def set_cohort_prepared_status(transaction_id, cohort, cohort_status):
        self.transactions[transaction.id].set_cohort_prepared_status(cohort, cohort_status)

    def set_transaction_state(transaction_id, state):
        self.transactions[transaction.id].set_transaction_state(state)

    def check_all_cohorts_prepared(transaction_id):
        return self.transactions[transaction.id].check_all_cohorts_prepared()

    def check_all_cohorts_acked(transaction_id):
        return self.transactions[transaction.id].check_all_cohorts_acked()
        
    
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

    def start():
        self.recover_from_logs()

    def run():
        self.construct_transaction()


