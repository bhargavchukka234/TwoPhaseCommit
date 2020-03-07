from enum import IntEnum

COMMIT_ACK_TIMEOUT = 10
TRANSACTION_SIZE = 10

# Database constants
INSERT_SQL = "INSERT INTO TRANSACTION_LOG (ID, STATE, COHORTS) VALUES (%s, %s, %s)"
DELETE_SQL = "DELETE FROM TRANSACTION_LOG where ID = %s"
SELECT_SQL = "SELECT * from TRANSACTION_LOG"

class State(IntEnum):
    INITIATED = 0
    PREPARE = 1
    PREPARED = 2
    COMMIT = 3
    ABORT = 4
    ACK = 5
