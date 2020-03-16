from enum import IntEnum
# insert data file name
INSERT_DATA_FILE_NAME = "testData.txt"

# database details for all cohorts and coordinator
DATABASE = "test"
USERNAME = "newuser"
PASSWORD = "password"

# port on which coordinator database is running(transaction logs)
COORDINATOR_DB_PORT = 5431

# cohort database ports list
COHORT_DB_PORTS_LIST = [5433,5434,5435]

# coordinate configurations
PREPARED_TIMEOUT = 6
COMMIT_ACK_TIMEOUT = 6
TRANSACTION_SIZE = 100
ACTIVE_TRANSACTIONS_LIMIT = 1

# cohort configurations
DECISION_TIMEOUT = 6

# Please refrain from modifying these!!
class State(IntEnum):
    INITIATED = 0
    PREPARE = 1
    PREPARED = 2
    COMMIT = 3
    ABORT = 4
    ACK = 5
    STATE_REQUEST = 6

# Coordinator database constants
INSERT_SQL = "INSERT INTO TRANSACTION_LOG (ID, STATE, COHORTS) VALUES (%s, %s, %s)"
DELETE_SQL = "DELETE FROM TRANSACTION_LOG where ID = "
SELECT_SQL = "SELECT * from TRANSACTION_LOG"

SELECT_READ_LINE_NUMBER_SQL = "SELECT number FROM LINE_NUMBER where file_name = "
INSERT_READ_LINE_NUMBER_SQL = "INSERT INTO LINE_NUMBER (file_name, number) VALUES (%s, %s)"
UPDATE_READ_LINE_NUMBER_SQL = "UPDATE LINE_NUMBER SET number = %s where file_name = %s"
