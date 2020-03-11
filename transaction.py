from constants import State

class Transaction:

    def __init__(self, id):
        self.id = id
        self.ack_table = dict()
        self.prepared_table = dict()
        self.state = State.INITIATED
        self.cohorts = []

    def set_ack_received(self, cohort):
        if cohort in self.cohorts:
            self.ack_table[cohort] = True

    def set_cohort_decision(self, cohort, cohort_status):
        if cohort in self.cohorts:
            self.prepared_table[cohort] = cohort_status

    def set_transaction_state(self, state):
        self.state = state

    def get_transaction_state(self):
        return self.state

    def set_cohort_list(self, cohorts):
        self.cohorts = cohorts

    def check_all_cohorts_responded_to_prepare(self):
        return len(self.prepared_table) == len(self.cohorts)

    def check_all_cohorts_acked(self):
        return len(self.ack_table) == len(self.cohorts)

    def compute_decision(self):
        for decision in self.prepared_table.values():
            if decision == State.ABORT:
                self.state = State.ABORT
                return
        self.state = State.COMMIT
