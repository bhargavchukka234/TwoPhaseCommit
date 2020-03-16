import time
import os
from constants import PREPARED_TIMEOUT,COMMIT_ACK_TIMEOUT

class CoordinatorTestHandler:

    delay_induced = False

    def __init__(self, test_name):
        self.test_name = test_name

    def handle_case1(self):
        if not self.delay_induced and self.test_name == "test1":
            time.sleep(PREPARED_TIMEOUT + 2)
            self.delay_induced = True

    def handle_case2(self):
        if self.test_name == "test2":
            os._exit(9)

    def handle_case3and4(self):
        if not self.delay_induced and (self.test_name == "test3" or self.test_name == "test4"):
            time.sleep(COMMIT_ACK_TIMEOUT + 2)
            self.delay_induced = True

    def handle_case5(self):
        if self.test_name == "test5":
            return True
        return False
