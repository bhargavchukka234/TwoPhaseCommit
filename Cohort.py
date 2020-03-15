import getopt

import pika
import psycopg2
from psycopg2 import Error
from psycopg2 import pool
import json
import sys

from cohort_recovery import CohortRecoveryThread
from constants import *

# create a connection the database
# dbConnection.autocommit = False
# cursor = dbConnection.cursor()
class Cohort:

    METADATA = "/Users/bhargav/Documents/winter2020/cs223_PM_DDM/project/project1/data/low_concurrency/metadata.sql"
    CREATE = "/Users/bhargav/Documents/winter2020/cs223_PM_DDM/project/project1/schema/create.sql"
    DROP = "/Users/bhargav/Documents/winter2020/cs223_PM_DDM/project/project1/schema/drop.sql"

    # This is to indicate that a transaction did not succeed before PREPARE was run
    def startTransaction(self, transaction_id):
        ps_connection = self.transaction_connection.get(transaction_id)
        cursor = ps_connection.cursor()
        cursor.execute('''BEGIN''')
        cursor.execute('''START TRANSACTION''')


    # This function is called when the transaction in itself aborts
    def immediateAbort(self, transaction_id):
        ps_connection = self.transaction_connection.get(transaction_id)
        cursor = ps_connection.cursor()
        cursor.execute("rollback")
        self.postgreSQL_pool.putconn(ps_connection)


    def executeStatements(self, transaction_id, transactionMessage):
        ps_connection = self.transaction_connection.get(transaction_id)
        if ps_connection is None or ps_connection == "":
            print("no connection found ")
            return -1
        cursor = ps_connection.cursor()
        Lines = transactionMessage.split(';')
        try:
            for line in Lines:
                if line != "":
                    cursor.execute(line)
        except Error:
            # transaction errors that might occur at initiated state
            self.immediateAbort(transaction_id)
            self.transaction_connection[transaction_id] = None


    def prepareTransaction(self, transaction_id):
        ps_connection = self.transaction_connection.get(transaction_id)
        cursor = ps_connection.cursor()
        prepare_stmt = "PREPARE TRANSACTION '" + str(transaction_id) + "'"
        try:
            cursor.execute(prepare_stmt)
            self.decision_timeout_transaction_info[transaction_id] = DECISION_TIMEOUT
        except Error:
            self.abortTransaction(transaction_id)
            return State.ABORT
        return State.PREPARED


    def commitTransaction(self, transaction_id):
        ps_connection = self.transaction_connection.get(transaction_id)
        cursor = ps_connection.cursor()
        prepare_stmt = "COMMIT PREPARED '" + str(transaction_id) + "';"
        cursor.execute(prepare_stmt)
        del self.transaction_connection[transaction_id]
        del self.decision_timeout_transaction_info[transaction_id]
        self.postgreSQL_pool.putconn(ps_connection)


    # This function is called when the coordinator sends an abort statement
    def abortTransaction(self, transaction_id):
        ps_connection = self.transaction_connection.get(transaction_id)
        cursor = ps_connection.cursor()
        prepare_stmt = "ROLLBACK PREPARED '" + str(transaction_id) + "';"
        cursor.execute(prepare_stmt)
        # close the connection to the database once aborted to the database
        del self.transaction_connection[transaction_id]
        del self.decision_timeout_transaction_info[transaction_id]
        self.postgreSQL_pool.putconn(ps_connection)


    def callback(self, channel, method, properties, body):
        dict_obj = json.loads(body)
        sender = dict_obj.get('sender')
        state = dict_obj.get('state')
        transaction_id = dict_obj.get('id')
        if state == State.INITIATED:
            transactionMessage = dict_obj.get('messageBody')
            '''
            check if a connection exists for the transaction ID else create a connection and start the transaction
            '''
            if transaction_id not in self.transaction_connection:
                self.createDatabaseConnection(transaction_id)
                self.startTransaction(transaction_id)
                self.executeStatements(transaction_id, transactionMessage)
            elif transaction_id in self.transaction_connection:
                self.executeStatements(transaction_id, transactionMessage)
        elif state == State.PREPARE:
            print("received a prepare message from the coordinator for transaction " + str(transaction_id))
            if self.transaction_connection.get(transaction_id) is None:
                ''' The transaction has already been rolled back 
                    because of insert statement failures inform 
                    the coordinator to abort and remove connection
                    mapping
                '''
                ret_state = State.ABORT
                del self.transaction_connection[transaction_id]
            else:
                ''' Prepare the transaction for commit '''
                ret_state = self.prepareTransaction(transaction_id)
            self.sendMessageToCoordinator(channel, sender, transaction_id, ret_state)

        elif state == State.COMMIT:
            print("received a commit message from the coordinator for transaction " + str(transaction_id))
            if transaction_id in self.transaction_connection:
                self.commitTransaction(str(transaction_id))
            self.sendMessageToCoordinator(channel, sender, transaction_id, State.ACK)
        elif state == State.ABORT:
            print("received an abort message from the coordinator " + str(transaction_id))
            if transaction_id in self.transaction_connection:
                self.abortTransaction(str(transaction_id))
            # coordinator expects ack from cohort for abort
            self.sendMessageToCoordinator(channel, sender, transaction_id, State.ACK)


    def sendMessageToCoordinator(self, channel, sender, transaction_id, state):
        newMessage = {"sender": sender, "id": transaction_id, "state": int(state), "messageBody": ""}
        jsonMessage = json.dumps(newMessage)
        channel.basic_publish(exchange='',
                              routing_key='coordinatorQueue',
                              body=jsonMessage)


    def createDatabaseConnection(self, transaction_id):
        db_connection = self.postgreSQL_pool.getconn()
        ''' enabling autocommit to run direct psql commands '''
        db_connection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        self.transaction_connection[transaction_id] = db_connection


    def intializeDB(self, fileName, ps_connection):
        fd = open(fileName, "r+")
        sqlFile = fd.read()
        fd.close()
        cursor = ps_connection.cursor()

        commands = sqlFile.split(';')
        for command in commands:
            command = command.strip()
            if command != "":
                cursor.execute(command)


    def addMetaData(self, ps_connection):
        fd = open(self.METADATA, "r+")
        cursor = ps_connection.cursor()
        Lines = fd.readlines()
        for line in Lines:
            if line.__contains__('INSERT'):
                line = line[:-1]
                cursor.execute(line)


    def cleanupPrepared(self, ps_connection):
        curs = ps_connection.cursor()
        gids = self.getPreparedTransactions(ps_connection)
        for gid in gids:
            curs.execute("ROLLBACK PREPARED %s", (gid,))


    def getPreparedTransactions(self, ps_connection):
        curs = ps_connection.cursor()
        curs.execute("SELECT gid FROM pg_prepared_xacts WHERE database = current_database()")
        gids = []
        for (gid,) in curs.fetchall():
            gids.append(gid)
        return gids


    def dbCleanup(self):
        ps_connection = self.postgreSQL_pool.getconn()
        # clean up previous prepared Statements
        ps_connection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        self.cleanupPrepared(ps_connection)
        self.intializeDB(self.DROP, ps_connection)
        self.intializeDB(self.CREATE, ps_connection)
        self.addMetaData(ps_connection)
        print("cleanup done")
        self.postgreSQL_pool.putconn(ps_connection)


    def loadPendingTransactions(self):
        ps_connection = self.postgreSQL_pool.getconn()
        # get uncommitted prepared transactions and initialize connection dictionary
        ps_connection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        for transaction_id in self.getPreparedTransactions(ps_connection):
            self.createDatabaseConnection(transaction_id)
        print("loaded existing prepared transactions")
        self.postgreSQL_pool.putconn(ps_connection)


    def initializeRecoveryThread(self, sender):

        parameters = pika.ConnectionParameters(heartbeat=0)
        rabbitMQConnection = pika.BlockingConnection(parameters)
        channel = rabbitMQConnection.channel()

        # request transaction decision for prepared transactions and add timer
        for transaction_id in self.transaction_connection.keys():
            self.sendMessageToCoordinator(channel, sender, transaction_id,  State.STATE_REQUEST)
            self.decision_timeout_transaction_info[transaction_id] = DECISION_TIMEOUT

        # start the recovery thread
        self.cohort_recovery = CohortRecoveryThread(channel, sender, self.decision_timeout_transaction_info)
        self.cohort_recovery.start()

    def __init__(self, cohortId, port, isCleanUp = False):
        self.cohortId = cohortId
        self.isCleanUp = isCleanUp
        self.postgreSQL_pool = psycopg2.pool.SimpleConnectionPool(1, 20, user="newuser",
                                                         password="password",
                                                         host="127.0.0.1",
                                                         port=str(port),
                                                         database="test")
        self.transaction_connection = {}
        self.decision_timeout_transaction_info = {}

        if isCleanUp:
            self.dbCleanup()
        else:
            self.loadPendingTransactions()

        self.initializeRecoveryThread(cohortId)

    def run(self):
        parameters = pika.ConnectionParameters(heartbeat=0)
        rabbitMQConnection = pika.BlockingConnection(parameters)
        channel = rabbitMQConnection.channel()
        # listen on the queue created by the coordinator
        channel.basic_consume(queue='queue' + str(self.cohortId),
                              auto_ack=True,
                              on_message_callback=self.callback)
        channel.start_consuming()


# there should be aleast one argument to give sender(cohort_id)
if __name__ == "__main__":

    myopts, args = getopt.getopt(sys.argv[1:], "cq:p:")
    port = None
    cohortId = None
    isCleanUp = False

    for opts, arg in myopts:
        if opts == '-c':
            isCleanUp = True
        elif opts == '-p':
            port = arg
        elif opts == '-q':
            cohortId = int(arg)

    if cohortId is None or port is None:
        print("Port(-p) or queue Id(-q) not provided. Exiting ...")
        sys.exit(5)

    cohort = Cohort(cohortId, port, isCleanUp)
    cohort.run()