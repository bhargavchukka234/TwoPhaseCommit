from communication_utils import sendMessageToCohort
from recovery import RecoveryThread
from protocol_db import ProtocolDB
from constants import *
import transaction_log_utils
import pika
import json
import uuid
import time
import threading

from transaction import Transaction

class Coordinator:
    """
    Coordinator for a 2 Phase Commit
    """

    def __init__(self, number_of_cohorts):
        """Constructor"""
        self.cohorts = range(number_of_cohorts)
        self.protocol_DB = ProtocolDB()
        self.prepare_timeout_info = dict()
        self.timeout_transaction_info = dict()
        self.recovery_thread = RecoveryThread(self.protocol_DB, self.prepare_timeout_info, self.timeout_transaction_info)

    def start(self):
        rabbitMQConnection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        # create one channel which can create multiple queues
        self.channel = rabbitMQConnection.channel()
        # self.send_channel = rabbitMQConnection.channel()
        self.recovery_thread.set_channel(self.channel)
        # start the recovery thread
        self.recovery_thread.start()

    def run(self):
        # required if coordinator comes up after crash/failure
        self.protocol_DB.transactions = transaction_log_utils.get_pending_transactions()

        if not self.protocol_DB.empty():
            for transaction in self.protocol_DB.transactions.values():
                self.timeout_transaction_info[transaction.id] = COMMIT_ACK_TIMEOUT
                for cohort in transaction.cohorts:
                    sendMessageToCohort(self.channel, cohort, transaction.state, transaction.id)

        # initialize a queue per cohort
        for cohortQueue in self.cohorts:
            self.channel.queue_declare(queue="queue" + str(cohortQueue), durable=True)
        # Queue on which the coordinator receives a response from the cohorts
        self.channel.queue_declare(queue='coordinatorQueue', durable=True)

        mq_recieve_thread = threading.Thread(target=self.initialize_listener)
        mq_recieve_thread.start()
        self.generate_transactions_from_file()

    def initialize_listener(self):
        rabbitMQConnection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        # create one channel which can create multiple queues
        self.consumer_channel = rabbitMQConnection.channel()
        self.consumer_channel.basic_consume(queue='coordinatorQueue',
                                            auto_ack=True,
                                            on_message_callback=self.cohortResponse)
        self.consumer_channel.start_consuming()

    # reads each line from pipe and calls begin_transaction_if_eligible with sql statements of a transaction
    def generate_transactions_from_file(self):

        insert_records_batch = []
        current_transaction_size = 0
        insert_statements_pipe = open("pipe", "r")
        counter = 0
        for line in insert_statements_pipe:
            if current_transaction_size < TRANSACTION_SIZE:
                insert_records_batch.append(line.strip())
                current_transaction_size += 1
            else:
                self.begin_transaction_if_eligible(insert_records_batch)
                current_transaction_size = 1
                insert_records_batch = []
                insert_records_batch.append(line.strip())

        self.begin_transaction_if_eligible(insert_records_batch)

    # starts the transaction if there is space in protocol_DB for the new transaction
    def begin_transaction_if_eligible(self, insert_records_batch):

        if len(insert_records_batch) == 0:
            return

        while (self.protocol_DB.check_if_transactions_limit_reached()):
            time.sleep(1)

        transaction, cohort_insert_statements_in_group = self.generate_transaction(insert_records_batch)

        cohorts_set = set()
        for entry in cohort_insert_statements_in_group:
            cohorts_set.add(entry[0])
        cohorts = list(cohorts_set)

        self.protocol_DB.add_transaction(transaction, cohorts)
        # send out the insert statements in batch to cohorts
        for cohort,current_insert_statements in cohort_insert_statements_in_group:
            sendMessageToCohort(self.channel, cohort, State.INITIATED, transaction.id,
                                current_insert_statements)
        # send out prepare message to each cohort
        for cohort in cohorts:
            sendMessageToCohort(self.channel, cohort, State.PREPARE, transaction.id)

        self.prepare_timeout_info[transaction.id] = PREPARED_TIMEOUT

        #print(self.protocol_DB.transactions)

    # create transaction object and cohort to its insert statements mapping
    def generate_transaction(self, insert_records_batch):

        transaction = Transaction(str(uuid.uuid1()))
        cohort_insert_statements_in_group = []
        prev_hash = -1
        curr_insert_list = []
        for insert_record in insert_records_batch:
            temp = insert_record.split(" ", 2)
            timestamp = temp[0]
            sensor_id = temp[1]
            insert_statement = temp[2]
            hash_value = hash((timestamp, sensor_id)) % len(self.cohorts)
            if prev_hash == -1:
                prev_hash = hash_value
                curr_insert_list.append(insert_statement)
            elif prev_hash != hash_value:
                cohort_insert_statements_in_group.append([prev_hash, curr_insert_list])
                prev_hash = hash_value
                curr_insert_list = [insert_statement]
            else:
                curr_insert_list.append(insert_statement)

        if len(curr_insert_list) > 0:
            cohort_insert_statements_in_group.append([prev_hash, curr_insert_list])

        return transaction, cohort_insert_statements_in_group

    def cohortResponse(self, channel, method, properties, body):

        # the coordinator proceeds with sending the next message after receiving a message from receiver
        dict_obj = json.loads(body)
        state = dict_obj.get('state')
        transaction_id = dict_obj.get('id')
        cohort = dict_obj.get('sender')
        # print("in response : " + str(self.protocol_DB.transactions))
        if (state == State.PREPARED or state == State.ABORT):
            # mark the receipt of this PREPARED message in the protocol DB for the particular cohort
            self.protocol_DB.set_cohort_decision(transaction_id, cohort, state)
            # check if all cohorts have responded to prepare
            if self.protocol_DB.check_all_cohorts_responded_to_prepare(transaction_id) and not self.protocol_DB.needs_force_abort(transaction_id):
                self.protocol_DB.compute_decision(transaction_id)
                transaction = self.protocol_DB.transactions[transaction_id]
                # Log to persistent storage
                transaction_log_utils.insert_log(transaction)
                # send COMMIT/ABORT depending on the final decision of the cohorts
                for cohort_name in transaction.cohorts:
                    sendMessageToCohort(channel, cohort_name, transaction.state,
                                        transaction_id)
                # add this transaction to the timer monitor list for recovery
                self.timeout_transaction_info[transaction_id] = COMMIT_ACK_TIMEOUT
                print("=================================")
                if transaction_id in self.prepare_timeout_info.keys():
                    del self.prepare_timeout_info[transaction_id]

        elif (state == State.ACK):
            print("received an acknowledgement from the cohort")
            # mark the receipt of this ACK message in the protocol DB for the particular cohort
            self.protocol_DB.set_ack_received(transaction_id, cohort)
            # check if we received acks from all the cohorts
            if self.protocol_DB.check_all_cohorts_acked(transaction_id):
                # Update in transaction logs
                transaction_log_utils.delete_log(transaction_id)
                # Remove the transaction from the protocol DB
                self.protocol_DB.remove_transaction(transaction_id)
                # Remove the transaction from the timer monitor list
                if transaction_id in self.timeout_transaction_info.keys():
                    del self.timeout_transaction_info[transaction_id]


if __name__ == "__main__":
    COORDINATOR = Coordinator(NUMBER_OF_COHORTS)
    COORDINATOR.start()
    COORDINATOR.run()
