#!/usr/bin/python3
import threading
import time
import unittest
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
        self.coordinator = Coordinator(1)
        self.cohort1 = Cohort(COHORT1_ID, COHORT1_PORT)
        self.cohort2 = Cohort(COHORT2_ID, COHORT2_PORT)
        self.cohort3 = Cohort(COHORT3_ID, COHORT3_PORT)

    def setUp(self):
        print("test setup begins")
        self.coordinator.start()
        self.coordinator.channel.queue_purge(queue = 'coordinatorQueue')
        self.coordinator.channel.queue_purge(queue='queue0')
        self.coordinator.channel.queue_purge(queue='queue1')
        self.coordinator.channel.queue_purge(queue='queue2')
        print("test setup successful")

    # def tearDown(self):
    #     self.cohort1.dbCleanup()
    #     self.cohort2.dbCleanup()
    #     self.cohort3.dbCleanup()
    #     self.coordinator.stop()

    def test_1(self):
        self.coordinator.set_test_name("test1")
        stop_event = threading.Event()
        coordinator_main_thread = threading.Thread(target=self.coordinator.run)
        cohort_stop_event = threading.Event()
        cohort1_main_thread = Thread(target=self.cohort1.run)
        # cohort2_main_thread = Thread(target=self.cohort2.run)
        # cohort3_main_thread = Thread(target=self.cohort3.run)
        print("coordinator starting")
        coordinator_main_thread.start()
        print("coordinator started")
        time.sleep(1)
        cohort1_main_thread.start()
        print("1st cohort started")
        # cohort2_main_thread.start()
        # print("2nd cohort started")
        # cohort3_main_thread.start()
        # print("3rd cohort started")

        cohort_port_list = [COHORT1_PORT, COHORT2_PORT, COHORT3_PORT]
        verifier = DatabaseStateVerifier(COORDINATOR_PORT, cohort_port_list)

        while verifier.is_transaction_active_at_coordinator():
            time.sleep(1)
        self.assertTrue(verifier.is_aborted())
        # self.assertTrue(verifier.is_transaction_active_at_coordinator())

        #stop_event.set()
        #cohort_stop_event.set()
        # cohort2_main_thread._stop()
        # cohort3_main_thread._stop()

    # def test_2(self):
    #   cohort_port_list = [COHORT1_PORT, COHORT2_PORT, COHORT3_PORT]
    #   verifier = DBStateVerifier(COORDINATOR_PORT, cohort_port_list)

    #    self.assertTrue(verifier.is_aborted())
    #   self.assertTrue(verifier.is_transaction_active_at_coordinator())

    # def test_3(self):


if __name__ == "__main__":
    unittest.main()