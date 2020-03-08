import psycopg2
from constants import *
import json

from transaction import Transaction

# create a connection the database
dbConnection = psycopg2.connect(database="test", user="newuser", password="password", host="127.0.0.1",
                   port="5431")
dbConnection.autocommit = True
cursor = dbConnection.cursor()


def insert_log(transaction):
    cursor.execute(INSERT_SQL, (transaction.id, str(int(transaction.state)), json.dumps(transaction.cohorts)))


def delete_log(transaction_id):
    cursor.execute(DELETE_SQL + "'" + transaction_id + "'")


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
