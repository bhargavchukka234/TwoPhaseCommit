#!/usr/bin/python3
from multiprocessing import Process
import time
import unittest

from pika.exceptions import StreamLostError

import transaction_log_utils
from coordinator import Coordinator
from Cohort import Cohort
from threading import Thread

from database_state_verifier import DatabaseStateVerifier

COHORT1_ID = 0
COHORT2_ID = 1
COHORT3_ID = 2
COORDINATOR_PORT = 5431
COHORT1_PORT = 5433
COHORT2_PORT = 5434
COHORT3_PORT = 5435


class TestTwoPhaseCommit(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(TestTwoPhaseCommit, self).__init__(*args, **kwargs)

    def setUp(self):
        print("test setup begins")
        self.coordinator = Coordinator(3)
        self.cohort1 = Cohort(COHORT1_ID, COHORT1_PORT)
        self.cohort2 = Cohort(COHORT2_ID, COHORT2_PORT)
        self.cohort3 = Cohort(COHORT3_ID, COHORT3_PORT)
        self.coordinator.start()
        self.coordinator.channel.queue_purge(queue = 'coordinatorQueue')
        self.coordinator.channel.queue_purge(queue='queue0')
        self.coordinator.channel.queue_purge(queue='queue1')
        self.coordinator.channel.queue_purge(queue='queue2')
        print("test setup successful")

    def db_clean_up(self):
        transaction_log_utils.clear_db()
        self.cohort1.dbCleanup()
        self.cohort2.dbCleanup()
        self.cohort3.dbCleanup()

    def test_1(self):
        self.db_clean_up()
        self.coordinator.set_test_name("test1")
        coordinator_process = Process(target=self.coordinator.run)
        cohort1_process = Process(target=self.cohort1.run)
        cohort2_process = Process(target=self.cohort2.run)
        cohort3_process = Process(target=self.cohort3.run)
        print("coordinator starting")
        coordinator_process.start()
        print("coordinator started")
        cohort1_process.start()
        print("1st cohort started")
        cohort2_process.start()
        print("2nd cohort started")
        cohort3_process.start()
        print("3rd cohort started")

        cohort_port_list = [COHORT1_PORT, COHORT2_PORT, COHORT3_PORT]
        verifier = DatabaseStateVerifier(COORDINATOR_PORT, cohort_port_list)
        time.sleep(10)
        while verifier.is_transaction_active_at_coordinator():
            time.sleep(1)
        self.assertTrue(verifier.is_aborted())

        print("Assets passed")
        coordinator_process.terminate()
        cohort1_process.terminate()
        cohort2_process.terminate()
        cohort3_process.terminate()

        # try:
        #     self.coordinator.stop()
        #     self.cohort1.stop()
        #     coordinator_main_thread._stop()
        #     cohort1_main_thread._stop()
        #     # cohort2_main_thread._stop()
        #     # cohort3_main_thread._stop()
        # except StreamLostError:
        #     pass

    # def test_2(self):
    #   cohort_port_list = [COHORT1_PORT, COHORT2_PORT, COHORT3_PORT]
    #   verifier = DBStateVerifier(COORDINATOR_PORT, cohort_port_list)

    #    self.assertTrue(verifier.is_aborted())
    #   self.assertTrue(verifier.is_transaction_active_at_coordinator())

    # def test_3(self):


if __name__ == "__main__":
    unittest.main()
