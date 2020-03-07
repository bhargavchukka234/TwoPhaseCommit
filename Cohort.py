import pika
import time
from enum import IntEnum 
import psycopg2
import json

class State(IntEnum):
    PREPARE  = 1
    PREPARED = 2
    COMMIT   = 3
    ABORT    = 4
    ACK      = 5

#create a connection the database 
dbConnection = psycopg2.connect(database="test", user="newuser", password="password", host="127.0.0.1", port="5433")
dbConnection.autocommit = False
cursor = dbConnection.cursor()
METADATA = "/Users/bhargav/Documents/winter2020/cs223_PM_DDM/project/project1/data/low_concurrency/metadata.sql"
CREATE = "/Users/bhargav/Documents/winter2020/cs223_PM_DDM/project/project1/schema/create.sql"
DROP = "/Users/bhargav/Documents/winter2020/cs223_PM_DDM/project/project1/schema/drop.sql"

def prepareTransaction(message):
    lines = message.split(';')
    for line in lines:
        if line != "":
            cursor.execute(line)

def commitTransaction():
    dbConnection.commit()

def abortTransaction():
    dbConnection.rollback()

def callback(ch, method, properties, body):
    #on recieveing a message from the coordinator, send a response
    dict_obj = json.loads(body)
    sender = dict_obj.get('sender')
    state = dict_obj.get('state')
    transaction_id = dict_obj.get('id')
    newState = None
    #print ("state %d" + state)
    if(state == State.PREPARE):
        transactionMessage = dict_obj.get('messageBody')
        #get a return to check if commit can take place
        prepareTransaction(transactionMessage)
        newMessage = {"sender" : sender, "id": transaction_id,"state" : int(State.PREPARED), "messageBody" : ""}
        jsonMessage = json.dumps(newMessage)
        print("sending prepared message to coordinator")
        channel.basic_publish(exchange='',
                        routing_key='coordinatorQueue',
                        body=jsonMessage)
    elif(state == State.COMMIT):
        print("received a commit message from the coordinator")
        commitTransaction()
        newMessage = {"sender" : sender, "id": transaction_id, "state" : int(State.ACK), "messageBody":""}
        jsonMessage = json.dumps(newMessage)
        channel.basic_publish(exchange='',
                        routing_key='coordinatorQueue',
                        body=jsonMessage)
    elif(state == State.ABORT):
        print("received an abort message from the coordinator")
        abortTransaction()


def intializeDB(fileName):
    fd = open(fileName, "r+")
    sqlFile = fd.read()
    fd.close()

    commands = sqlFile.split(';')
    for command in commands:
        command = command.strip()
        if command != "":
            #print("Executing " + command)
            cursor.execute(command)


def addMetaData():
    fd = open(METADATA, "r+")
    Lines = fd.readlines();
    for line in Lines:
        if line.__contains__('INSERT'):
            line = line[:-1]
            cursor.execute(line)


if __name__== "__main__":
    print("hello1")
    intializeDB(DROP)
    print("hello2")
    intializeDB(CREATE);
    print("hello3")
    addMetaData();
    print ("done")
    rabbitMQConnection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = rabbitMQConnection.channel()
    #channel.queue_declare(queue='hello', durable=True)

    #listen on the queue created by the coordinator
    channel.basic_consume(queue='queue1',
                      auto_ack=True,
                      on_message_callback=callback)
    channel.start_consuming()
