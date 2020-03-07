import json
from constants import State

def sendMessageToCohort(channel, processNumber, state, transaction_id, insert_statements_list = None,):
    # TODO: remove the hardcoded value for the queue1 and pass the actual queue name for the function
    queueName = "queue" + str(processNumber)
    messageBody = ""
    if (state == State.INITIATED):
        count = 0
        if (insert_statements_list == None):
            return

        for line in insert_statements_list:
            if (count <= 10):
                if (line.__contains__("INSERT")):
                    messageBody = messageBody + line.rstrip()
                    count += 1
        message = {"sender": processNumber, "id": transaction_id, "state": int(state), "messageBody": messageBody}
        jsonMessage = json.dumps(message)
        print("Sending transactions to the cohort "+str(processNumber))
        channel.basic_publish(exchange='',
                              routing_key=queueName,
                              body=jsonMessage)
    elif (state == State.PREPARE or state == State.COMMIT or state == State.ABORT):
        message = {"sender": processNumber, "id" : transaction_id,"state": state, "messageBody": ""}
        jsonMessage = json.dumps(message)
        channel.basic_publish(exchange='',
                              routing_key=queueName,
                              body=jsonMessage)

