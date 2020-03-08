from constants import State, ACTIVE_TRANSACTIONS_LIMIT


class ProtocolDB:

    def __init__(self):
        self.transactions = dict()
        self.transactions_limit = ACTIVE_TRANSACTIONS_LIMIT

    def add_transaction(self, transaction, cohorts):
        self.transactions[transaction.id] = transaction
        self.transactions[transaction.id].set_cohort_list(cohorts)

    def remove_transaction(self, transaction_id):
        del self.transactions[transaction_id]

    def set_ack_received(self, transaction_id, cohort):
        self.transactions[transaction_id].set_ack_received(cohort)

    def set_cohort_decision(self, transaction_id, cohort, cohort_status):
        self.transactions[transaction_id].set_cohort_decision(cohort, cohort_status)

    def set_transaction_state(self, transaction_id, state):
        self.transactions[transaction_id].set_transaction_state(state)

    def check_all_cohorts_responded_to_prepare(self, transaction_id):
        return self.transactions[transaction_id].check_all_cohorts_responded_to_prepare()

    def check_all_cohorts_acked(self, transaction_id):
        return self.transactions[transaction_id].check_all_cohorts_acked()

    def compute_decision(self, transaction_id):
        return self.transactions[transaction_id].compute_decision()

    def check_if_transactions_limit_reached(self):
        return len(self.transactions) >= self.transactions_limit

    def empty(self):
        return self.transactions is None or len(self.transactions) == 0

    def force_abort_transaction(self, transaction_id):
        self.transactions[transaction_id].set_transaction_state(State.ABORT)
        self.transactions[transaction_id].needs_force_abort = True

    def needs_force_abort(self, transaction_id):
        return self.transactions[transaction_id].needs_force_abort