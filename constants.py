from enum import IntEnum
# coordinate configurations
PREPARED_TIMEOUT = 10
COMMIT_ACK_TIMEOUT = 10
TRANSACTION_SIZE = 2
ACTIVE_TRANSACTIONS_LIMIT = 1

# cohort configurations
DECISION_TIMEOUT = 5

# insert data file name
INSERT_DATA_FILE_NAME = "testData.txt"

# Coordinator database constants
INSERT_SQL = "INSERT INTO TRANSACTION_LOG (ID, STATE, COHORTS) VALUES (%s, %s, %s)"
DELETE_SQL = "DELETE FROM TRANSACTION_LOG where ID = "
SELECT_SQL = "SELECT * from TRANSACTION_LOG"

SELECT_READ_LINE_NUMBER_SQL = "SELECT number FROM LINE_NUMBER where file_name = "
INSERT_READ_LINE_NUMBER_SQL = "INSERT INTO LINE_NUMBER (file_name, number) VALUES (%s, %s)"
UPDATE_READ_LINE_NUMBER_SQL = "UPDATE LINE_NUMBER SET number = %s where file_name = %s"

class State(IntEnum):
    INITIATED = 0
    PREPARE = 1
    PREPARED = 2
    COMMIT = 3
    ABORT = 4
    ACK = 5
    STATE_REQUEST = 6
