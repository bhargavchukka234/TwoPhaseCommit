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
METADATA = "/Users/tanvigupta/Documents/Winter_2020/CS223/project2/project2/data/low_concurrency/metadata_small.sql"
CREATE = "/Users/tanvigupta/Documents/Winter_2020/CS223/project2/project2/schema/create.sql"
DROP = "/Users/tanvigupta/Documents/Winter_2020/CS223/project2/project2/schema/drop.sql"


def prepareTransaction(message):
    lines = message.split(':')
    for line in lines:
        cursor.execute(line)

def commitTransaction():
    dbConnection.commit()

def abortTransaction():
    dbConnection.rollback()

def callback(ch, method, properties, body):
    #on recieveing a message from the coordinator, send a response
    print(" [x] Received information from the coordinator")
    dict_obj = json.loads(body)
    new_obj = dict_obj.get('message')
    sender = new_obj.get('sender')
    state = new_obj.get('state')
    newState = None
    print ("state %d" + state)
    if(state == State.PREPARE):
        message = new_obj.get('messageBody')
        #get a return to check if commit can take place
        prepareTransaction(message)
        newMessage = {"sender" : sender, "state" : int(State.PREPARED), "messageBody" : ""}
        jsonMessage = json.dumps(newMessage)
        channel.basic_publish(exchange='',
                        routing_key='coordinatorQueue',
                        body=jsonMessage)
    elif(state == State.COMMIT):
        commitTransaction()
        newMessage = {"sender" : sender, "state" : int(State.ACK), "messageBody":""}
        jsonMessage = json.dumps(newMessage)
        channel.basic_publish(exchange='',
                        routing_key='coordinatorQueue',
                        body=jsonMessage)
    elif(state == State.ABORT):
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
    intializeDB(DROP)
    intializeDB(CREATE);
    addMetaData();
    print ("hello")
    rabbitMQConnection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = rabbitMQConnection.channel()
    #channel.queue_declare(queue='hello', durable=True)

    #listen on the queue created by the coordinator
    channel.basic_consume(queue='queue1',
                      auto_ack=True,
                      on_message_callback=callback)
    channel.start_consuming()
