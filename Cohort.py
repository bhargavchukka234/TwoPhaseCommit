import pika
import time
from enum import IntEnum
import psycopg2
from psycopg2 import Error
from psycopg2 import pool
import json
from constants import *

# create a connection the database
# dbConnection.autocommit = False
# cursor = dbConnection.cursor()
postgreSQL_pool = psycopg2.pool.SimpleConnectionPool(1, 20, user="newuser",
                                                     password="password",
                                                     host="127.0.0.1",
                                                     port="5433",
                                                     database="test")
transaction_connection = {}
METADATA = "/Users/tanvigupta/Documents/Winter_2020/CS223/project2/project2/data/low_concurrency/metadata.sql"
CREATE = "/Users/tanvigupta/Documents/Winter_2020/CS223/project2/project2/schema/create.sql"
DROP = "/Users/tanvigupta/Documents/Winter_2020/CS223/project2/project2/schema/drop.sql"


def startTransaction(transaction_id):
    ps_connection = transaction_connection.get(transaction_id)
    cursor = ps_connection.cursor()
    cursor.execute('''BEGIN''')
    cursor.execute('''START TRANSACTION''')


# This function is called when the transaction in itself aborts
def immediateAbort(transaction_id):
    ps_connection = transaction_connection.get(transaction_id)
    cursor = ps_connection.cursor()
    cursor.execute("rollback")


def executeStatements(transaction_id, transactionMessage):
    ps_connection = transaction_connection.get(transaction_id)
    if ps_connection == "":
        print("no connection found ")
        return -1
    cursor = ps_connection.cursor()
    Lines = transactionMessage.split(';')
    for line in Lines:
        if line != "":
            try:
                cursor.execute(line)
            except Error:
                # transaction errors that might occur at initiated state
                immediateAbort(transaction_id)
                # This is to indicate that a transaction did not succeed before PREPARE was run
                transaction_connection[transaction_id] = None


def prepareTransaction(transaction_id):
    ps_connection = transaction_connection.get(transaction_id)
    cursor = ps_connection.cursor()
    prepare_stmt = "PREPARE TRANSACTION '" + str(transaction_id.split('-')[0]) + "'"
    try:
        cursor.execute(prepare_stmt)
    except Error:
        abortTransaction(transaction_id)
        return State.ABORT
    return State.PREPARED


def commitTransaction(transaction_id):
    ps_connection = transaction_connection.get(transaction_id)
    cursor = ps_connection.cursor()
    prepare_stmt = "COMMIT PREPARED '" + str(transaction_id.split('-')[0]) + "';"
    cursor.execute(prepare_stmt)
    del transaction_connection[transaction_id]


# This function is called when the coordinator sends an abort statement
def abortTransaction(transaction_id):
    ps_connection = transaction_connection.get(transaction_id)
    cursor = ps_connection.cursor()
    prepare_stmt = "ROLLBACK PREPARED '" + str(transaction_id.split('-')[0]) + "';"
    cursor.execute(prepare_stmt)
    # close the connection to the database once aborted to the database
    del transaction_connection[transaction_id]


def callback(ch, method, properties, body):
    dict_obj = json.loads(body)
    sender = dict_obj.get('sender')
    state = dict_obj.get('state')
    transaction_id = dict_obj.get('id')
    if state == State.INITIATED:
        transactionMessage = dict_obj.get('messageBody')
        '''
        check if a connection exists for the transaction ID else create a connection and start the transaction
        '''
        if transaction_id not in transaction_connection:
            db_connection = postgreSQL_pool.getconn()
            ''' enabling autocommit to run direct psql commands '''
            ps_connection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
            transaction_connection[transaction_id] = db_connection
            startTransaction(transaction_id)
            executeStatements(transaction_id, transactionMessage)
        elif transaction_id in transaction_connection:
            executeStatements(transaction_id, transactionMessage)
    elif state == State.PREPARE:
        if transaction_connection.get(transaction_id) is None:
            ''' The transaction has already been rolled back 
                because of insert statement failures inform 
                the coordinator to abort and remove connection
                mapping
            '''
            ret_state = State.ABORT
            del transaction_connection[transaction_id]
        else:
            ''' Prepare the transaction for commit '''
            ret_state = prepareTransaction(transaction_id)

        newMessage = {"sender": sender, "id": transaction_id, "state": ret_state, "messageBody": ""}
        jsonMessage = json.dumps(newMessage)
        print("sending PREPARED message to coordinator"+ str(transaction_id))
        channel.basic_publish(exchange='',
                              routing_key='coordinatorQueue',
                              body=jsonMessage)
    elif state == State.COMMIT:
        print("received a commit message from the coordinator for transaction" + str(transaction_id))
        commitTransaction(str(transaction_id))
        newMessage = {"sender": sender, "id": transaction_id, "state": int(State.ACK), "messageBody": ""}
        jsonMessage = json.dumps(newMessage)
        channel.basic_publish(exchange='',
                              routing_key='coordinatorQueue',
                              body=jsonMessage)
    elif state == State.ABORT:
        print("received an abort message from the coordinator"+ str(transaction_id))
        if transaction_id in transaction_connection:
            abortTransaction(str(transaction_id))


def intializeDB(fileName, ps_connection):
    fd = open(fileName, "r+")
    sqlFile = fd.read()
    fd.close()
    cursor = ps_connection.cursor()

    commands = sqlFile.split(';')
    for command in commands:
        command = command.strip()
        if command != "":
            cursor.execute(command)


def addMetaData(ps_connection):
    fd = open(METADATA, "r+")
    cursor = ps_connection.cursor()
    Lines = fd.readlines()
    for line in Lines:
        if line.__contains__('INSERT'):
            line = line[:-1]
            cursor.execute(line)

def cleanupPrepared(ps_connection):
    curs = ps_connection.cursor()
    curs.execute("SELECT gid FROM pg_prepared_xacts WHERE database = current_database()")
    for (gid,) in curs.fetchall():
        curs.execute("ROLLBACK PREPARED %s", (gid,))


if __name__ == "__main__":
    ps_connection = postgreSQL_pool.getconn()
    # clean up previous prepared Statements
    ps_connection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cleanupPrepared(ps_connection)
    print("hello1")
    intializeDB(DROP, ps_connection)
    print("hello2")
    intializeDB(CREATE, ps_connection);
    print("hello3")
    addMetaData(ps_connection)
    print("done")

    rabbitMQConnection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = rabbitMQConnection.channel()
    # TODO: handle the restart and re-reading the transaction DB
    # listen on the queue created by the coordinator
    channel.basic_consume(queue='queue1',
                          auto_ack=True,
                          on_message_callback=callback)
    channel.start_consuming()
