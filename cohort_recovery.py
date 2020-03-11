import transaction_log_utils
from constants import *
from threading import Thread
import time
import json

class CohortRecoveryThread(Thread):

    def __init__(self, channel, sender, decision_timeout_transaction_info):
        Thread.__init__(self)
        self.channel = channel
        self.sender = sender
        # self.prepare_timeout_info = prepare_timeout_info
        self.decision_timeout_transaction_info = decision_timeout_transaction_info

    def run(self):

        while True:

            # resend state request if decision not received
            self.check_decision_timeout()
            time.sleep(1)

    def check_decision_timeout(self):

        # Timeout check after state request is sent
        for transaction_id in list(self.decision_timeout_transaction_info.keys()):
            try:
                timeout = self.decision_timeout_transaction_info[transaction_id]

                # Timeout has happened
                if timeout == 0:
                    print("Timed out waiting for decision for transaction id: " + transaction_id)
                    print("Resending state request to coordinator")
                    # need to test
                    self.send_message_to_coordinator(self.sender, transaction_id, State.STATE_REQUEST)
                    self.decision_timeout_transaction_info[transaction_id] = DECISION_TIMEOUT
                # Timeout not yet elapsed
                else:
                    self.decision_timeout_transaction_info[transaction_id] = timeout - 1
            except RuntimeError:
                pass

    def send_message_to_coordinator(self, sender, transaction_id, state):
        newMessage = {"sender": sender, "id": transaction_id, "state": int(state), "messageBody": ""}
        jsonMessage = json.dumps(newMessage)
        self.channel.basic_publish(exchange='',
                              routing_key='coordinatorQueue',
                              body=jsonMessage)