#!/usr/bin/python3

import unittest
from coordinator import Coordinator
from Cohort import Cohort
from threading import Thread

COHORT1_ID = 1
COHORT1_ID = 2
COHORT1_ID = 3
COHORT1_PORT = 5433
COHORT2_PORT = 5434
COHORT3_PORT = 5435

class TestTwoPhaseCommit(unittest.TestCase):
    
    def __init__(self):
        self.coordinator = Coordinator(3)
        self.cohort1 = Cohort(COHORT1_ID, COHORT1_PORT)
        self.cohort2 = Cohort(COHORT2_ID, COHORT2_PORT)
        self.cohort3 = Cohort(COHORT3_ID, COHORT3_PORT)

    def setUp(self):
        self.coordinator.start()

    def tearDown(self):
        self.cohort1.dbCleanup()
        self.cohort2.dbCleanup()
        self.cohort3.dbCleanup()
        self.coordinator.stop()

    def test_1(self):
        coordinator_main_thread = Thread(target = self.coordinator.run(), args = [])
        cohort1_main_thread = Thread(target = self.cohort1.run(), args = [])
        cohort2_main_thread = Thread(target = self.cohort2.run(), args = [])
        cohort3_main_thread = Thread(target = self.cohort3.run(), args = [])

        coordinator_main_thread.start()
        cohort1_main_thread.start()
        cohort2_main_thread.start()
        cohort3_main_thread.start()

        cohort_port_list = [COHORT1_PORT, COHORT2_PORT, COHORT3_PORT]
        verifier = DatabaseStateVerifier(COORDINATOR_PORT, cohort_port_list)

        self.assertTrue(verifier.is_aborted())
        self.assertTrue(verifier.is_transaction_active_at_coordinator())

        coordinator_main_thread.stop()
        cohort1_main_thread.stop()
        cohort2_main_thread.stop()
        cohort3_main_thread.stop()

    #def test_2(self):
    #   cohort_port_list = [COHORT1_PORT, COHORT2_PORT, COHORT3_PORT]
    #   verifier = DBStateVerifier(COORDINATOR_PORT, cohort_port_list)

    #    self.assertTrue(verifier.is_aborted())
    #   self.assertTrue(verifier.is_transaction_active_at_coordinator())

    #def test_3(self):
        
        
