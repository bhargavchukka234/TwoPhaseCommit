import time
import os
from constants import PREPARED_TIMEOUT,COMMIT_ACK_TIMEOUT

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

    def handle_case7(self):
        if not self.delay_induced and self.test_name == "test7":
            time.sleep()
            os._exit(9)

    def handle_case3and4(self):
        if not self.delay_induced and (self.test_name == "test3" or self.test_name == "test4"):
            time.sleep(COMMIT_ACK_TIMEOUT + 2)
            self.delay_induced = True