import pika
import time
from enum import IntEnum
import psycopg2
from psycopg2 import pool
import json
from constants import *

# create a connection the database
# dbConnection = psycopg2.connect(database="test", user="newuser", password="password", host="127.0.0.1", port="5433")
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


def prepareTransaction(message, transaction_id,):
    if postgreSQL_pool:
        ps_connection = postgreSQL_pool.getconn()
        if ps_connection == "":
            return State.ABORT
        ps_connection.autocommit = False
        print("creating a connection for " + str(transaction_id))
        transaction_connection[transaction_id] = ps_connection
    else:
        return State.ABORT

    lines = message.split(';')
    cursor = ps_connection.cursor()
    for line in lines:
        if line != "":
            cursor.execute(line)


def commitTransaction(transaction_id):
    # get the connection for which the transaction has to be committed
    ps_connection = transaction_connection.get(transaction_id)
    print("committing for the transaction " + str(transaction_id))
    if ps_connection:
        ps_connection.commit()
    else:
        print("Oops! connection does not exist")
    ps_connection.close()


def abortTransaction(transaction_id):
    # get the connection for which the transaction has to be aborted
    ps_connection = transaction_connection.get(transaction_id)
    print("aborting for the transaction " + str(transaction_id))
    if ps_connection:
        ps_connection.commit()
    else:
        print("Oops! connection does not exist")
    ps_connection.close()


def callback(ch, method, properties, body):
    # on receiving a message from the coordinator, send a response
    dict_obj = json.loads(body)
    sender = dict_obj.get('sender')
    state = dict_obj.get('state')
    transaction_id = dict_obj.get('id')
    if state == State.PREPARE:
        transactionMessage = dict_obj.get('messageBody')
        # Function to begin a transaction and create a connection for that transaction
        returnState = prepareTransaction(transactionMessage, str(transaction_id))

        newMessage = {"sender": sender, "id": transaction_id, "state": returnState, "messageBody": ""}
        jsonMessage = json.dumps(newMessage)
        print("sending prepared message to coordinator")
        channel.basic_publish(exchange='',
                              routing_key='coordinatorQueue',
                              body=jsonMessage)
    elif state == State.COMMIT:
        print("received a commit message from the coordinator")
        commitTransaction(str(transaction_id))
        newMessage = {"sender": sender, "id": transaction_id, "state": int(State.ACK), "messageBody": ""}
        jsonMessage = json.dumps(newMessage)
        channel.basic_publish(exchange='',
                              routing_key='coordinatorQueue',
                              body=jsonMessage)
    elif state == State.ABORT:
        print("received an abort message from the coordinator")
        abortTransaction(str(transaction_id))


def intializeDB(fileName):
    fd = open(fileName, "r+")
    sqlFile = fd.read()
    fd.close()

    ps_connection = postgreSQL_pool.getconn()
    cursor = ps_connection.cursor()

    commands = sqlFile.split(';')
    for command in commands:
        command = command.strip()
        if command != "":
            # print("Executing " + command)
            cursor.execute(command)
    ps_connection.commit()


def addMetaData():
    fd = open(METADATA, "r+")
    ps_connection = postgreSQL_pool.getconn()
    cursor = ps_connection.cursor()
    Lines = fd.readlines()
    for line in Lines:
        if line.__contains__('INSERT'):
            line = line[:-1]
            cursor.execute(line)
    ps_connection.commit()


if __name__ == "__main__":
    print("hello1")
    intializeDB(DROP)
    print("hello2")
    intializeDB(CREATE);
    print("hello3")
    addMetaData();
    print("done")
    # TODO: handle the restart and re-reading the transaction DB
    rabbitMQConnection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = rabbitMQConnection.channel()
    # channel.queue_declare(queue='hello', durable=True)

    # listen on the queue created by the coordinator
    channel.basic_consume(queue='queue1',
                          auto_ack=True,
                          on_message_callback=callback)
    channel.start_consuming()
