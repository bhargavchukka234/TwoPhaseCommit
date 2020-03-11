import psycopg2
from constants import *
import json

from transaction import Transaction

# create a connection the database
dbConnection = psycopg2.connect(database="test", user="newuser", password="password", host="127.0.0.1",
                                port="5431")
dbConnection.autocommit = True
cursor = dbConnection.cursor()


# get the line number of insert file until which coordinator has read and created transactions
def get_read_line_number():
    cursor.execute(SELECT_READ_LINE_NUMBER_SQL + "'" + INSERT_DATA_FILE_NAME + "'")
    record = cursor.fetchone()
    if record is None:
        cursor.execute(INSERT_READ_LINE_NUMBER_SQL, (INSERT_DATA_FILE_NAME, 0))
        return 0
    else:
        return record[0]


# set the line number of insert file until which coordinator has read and created transactions
def set_read_line_number(number):
    cursor.execute(UPDATE_READ_LINE_NUMBER_SQL, (number, INSERT_DATA_FILE_NAME))


def insert_log(transaction):
    cursor.execute(INSERT_SQL, (transaction.id, str(int(transaction.state)), json.dumps(transaction.cohorts)))
    pass


def delete_log(transaction_id):
    cursor.execute(DELETE_SQL + "'" + transaction_id + "'")
    pass


def get_pending_transactions():
    transactions = dict()
    cursor.execute(SELECT_SQL)
    records = cursor.fetchall()
    for record in records:
        transaction = Transaction(record[0])
        transaction.state = State(int(record[1]))
        transaction.cohorts = json.loads(record[2])
        transactions[transaction.id] = transaction
    return transactions
