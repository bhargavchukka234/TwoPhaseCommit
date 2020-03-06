from recovery import RecoveryThread
from enum import Enum
import pika
from enum import IntEnum
import json

COMMIT_ACK_TIMEOUT = 500


class State(IntEnum):
    INITIATED = 0
    PREPARE = 1
    PREPARED = 2
    COMMIT = 3
    ABORT = 4
    ACK = 5


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
        # self.log_file = log_file
        self.protocol_DB = ProtocolDB()
        self.recovery_thread = RecoveryThread()
        self.timeout_transaction_list = dict()

    def start(self):
        rabbitMQConnection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        # create one channel which can create multiple queues
        self.channel = rabbitMQConnection.channel()

    def run(self):
        if not self.protocol_DB.empty():
            for transaction in self.protocol_DB.transactions:
                self.timeout_transaction_list[transaction.id, COMMIT_ACK_TIMEOUT]
                # send commit/abort to all

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
                                           on_message_callback=cohortResponse)
                self.channel.start_consuming()


def sendMessageToCohort(channel, processNumber, state):
    # TODO: remove the hardcoded value for the queue1 and pass the actual queue name for the function
    queueName = "queue" + str(processNumber)
    messageBody = ""
    if (state == State.PREPARE):
        count = 0
        for line in fd_observation:
            if (count <= 10):
                if (line.__contains__("INSERT")):
                    messageBody = messageBody + line + ":"
                    count += 1
        message = {"sender": processNumber, "state": int(state), "messageBody": messageBody}
        jsonMessage = json.dumps(message)
        channel.basic_publish(exchange='',
                              routing_key=queueName,
                              body=jsonMessage)
    elif (state == State.COMMIT or state == State.ABORT):
        message = {"sender": processNumber, "state": state, "messageBody": ""}
        jsonMessage = json.dumps(message)
        channel.basic_publish(exchange='',
                              routing_key=queueName,
                              body=jsonMessage)

def cohortResponse(channel, method, properties, body):
    print(" [x] Received response from cohort %r" % body)
    #the coordinator proceeds with sending the next message after receiving a message from receiver
    dict_obj = json.loads(body)
    new_obj = dict_obj.get('message')
    state = new_obj.get('state')
    if(state == State.PREPARED):
        # receive prepared from all the cohorts
        sendMessageToCohort(channel, 1, State.COMMIT)
    elif(state == State.ACK):
        '''TODO: delete from protocol DB'''
    elif(state == State.ABORT):
        # send messages to all the
        sendMessageToCohort(channel, 1, State.ABORT)


if __name__ == "__main__":
    COORDINATOR = Coordinator()
    COORDINATOR.start()
    COORDINATOR.run()
