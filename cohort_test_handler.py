import time
import os
from constants import PREPARED_TIMEOUT, COMMIT_ACK_TIMEOUT, DECISION_TIMEOUT


class CohortTestHandler:

    test_name = None
    delay_induced = False

    def __init__(self, test_name):
        self.test_name = test_name

    def set_test_name(self, test_name):
        self.test_name = test_name

    def handle_case6(self):
        if self.test_name == "test6":
            os._exit(9)

    def handle_case7_8(self):
        if self.test_name == "test7" or self.test_name == "test8":
            time.sleep(COMMIT_ACK_TIMEOUT + 2)

    def handle_case9(self):
        if self.test_name == "test9" or self.test_name == "test10":
            time.sleep(COMMIT_ACK_TIMEOUT + 2)
            os._exit(9)

    def handle_case10(self):
        if self.test_name == "test11":
            time.sleep(DECISION_TIMEOUT + 2)
            os._exit(9)