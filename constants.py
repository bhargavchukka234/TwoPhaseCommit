from enum import IntEnum

COMMIT_ACK_TIMEOUT = 500

class State(IntEnum):
    INITIATED = 0
    PREPARE = 1
    PREPARED = 2
    COMMIT = 3
    ABORT = 4
    ACK = 5
