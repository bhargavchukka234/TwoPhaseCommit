from communication_utils import sendMessageToCohort
from recovery import RecoveryThread
from constants import *
import pika
import json

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

    def set_cohort_list(self, cohorts):
        self.cohorts = cohorts

    def check_all_cohorts_responded_to_prepare(self):
        return len(self.prepared_table) == len(self.cohorts)

    def check_all_cohorts_acked(self):
        return len(self.ack_table) == len(self.cohorts)

    def get_prepared_decision(self):
        for cohort, decision in self.prepared_table:
            if decision == State.ABORT:
                return State.ABORT
        return State.COMMIT


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

    def set_cohort_decision(self, transaction_id, cohort, cohort_status):
        self.transactions[transaction_id].set_cohort_decision(cohort, cohort_status)

    def set_transaction_state(self, transaction_id, state):
        self.transactions[transaction_id].set_transaction_state(state)

    def check_all_cohorts_responded_to_prepare(self, transaction_id):
        return self.transactions[transaction_id].check_all_cohorts_responded_to_prepare()

    def check_all_cohorts_acked(self, transaction_id):
        return self.transactions[transaction_id].check_all_cohorts_acked()

    def get_prepared_decision(self, transaction_id):
        return self.transactions[transaction_id].get_prepared_decision()

    def empty(self):
        return len(self.transactions) == 0


class Coordinator:
    """
    Coordinator for a 2 Phase Commit
    """

    def __init__(self):
        """Constructor"""
        self.cohorts = []
        self.protocol_DB = ProtocolDB()
        self.timeout_transaction_info = dict()
        self.recovery_thread = RecoveryThread(self.protocol_DB, self.timeout_transaction_info)

    def start(self):
        rabbitMQConnection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        # create one channel which can create multiple queues
        self.channel = rabbitMQConnection.channel()
        # start the recovery thread
        self.recovery_thread.start()

    def cohortResponse(self, channel, method, properties, body):
        print(" [x] Received response from cohort %r" % body)

        #the coordinator proceeds with sending the next message after receiving a message from receiver
        dict_obj = json.loads(body)
        new_obj = dict_obj.get('message')
        state = new_obj.get('state')
        transaction_id = new_obj.get('id')
        cohort = new_obj.get('cohort')

        if(state == State.PREPARED or state == State.ABORT):
            # mark the receipt of this PREPARED message in the protocol DB for the particular cohort
            self.protocol_DB.set_cohort_decision(transaction_id, cohort, state)
            # check if all cohorts have responded to prepare
            if self.protocol_DB.check_all_cohorts_responded_to_prepare(transaction_id):
                # send COMMIT/ABORT depending on the final decision of the cohorts
                sendMessageToCohort(channel, cohort, self.protocol_DB.get_prepared_decision(transaction_id))
                # add this transaction to the timer monitor list for recovery 
                self.timeout_transaction_info[transaction_id] = COMMIT_ACK_TIMEOUT

        elif(state == State.ACK):
            # mark the receipt of this ACK message in the protocol DB for the particular cohort
            self.protocol_DB.set_ack_received(transaction_id, cohort, state)
            # check if we received acks from all the cohorts
            if self.protocolDB.check_all_acks_received(transaction_id):
                # Remove the transaction from the protocol DB
                self.protocolDB.remove_transaction(transaction_id)
                # Remove the transaction from the timer monitor list
                if transaction_id in self.timeout_transaction_info.keys():
                    del self.timeout_transaction_info[transaction_id]


    def run(self):
        # required if coordinator comes up after crash/failure
        if not self.protocol_DB.empty():
            for transaction in self.protocol_DB.transactions:
                self.timeout_transaction_info[transaction.id] = COMMIT_ACK_TIMEOUT
                for cohort in transaction.cohorts:
                    sendMessageToCohort(self.channel, cohort, State.COMMIT)

        cohortQueues = ["queue1", "queue2", "queue3"]
        # initialize a queue per cohort
        self.channel.queue_declare(queue=cohortQueues[0], durable=True)
        # Queue on which the coordinator recieves a response from the cohorts
        self.channel.queue_declare(queue='coordinatorQueue', durable=True)
        # send out the first message to each coordinator
        i = 1
        sendMessageToCohort(self.channel, i, State.PREPARE)
        # send out the first set of messages */
        # while True:
        # loop through all the queues to send transactions to each site
        # time.sleep(2)
        self.channel.basic_consume(queue='coordinatorQueue',
                                   auto_ack=True,
                                   on_message_callback=self.cohortResponse)
        self.channel.start_consuming()


if __name__ == "__main__":
    COORDINATOR = Coordinator()
    COORDINATOR.start()
    COORDINATOR.run()
