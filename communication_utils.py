import json
from constants import State

def sendMessageToCohort(channel, processNumber, state, insert_statements_list = None):
    # TODO: remove the hardcoded value for the queue1 and pass the actual queue name for the function
    queueName = "queue" + str(processNumber)
    messageBody = ""
    if (state == State.PREPARE):
        count = 0
        if (insert_statements_list == None):
            return

        for line in insert_statements_list:
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

