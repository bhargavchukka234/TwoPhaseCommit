import psycopg2
from constants import *
import json

from transaction import Transaction

# create a connection the database
# dbConnection = psycopg2.connect(database="test", user="newuser", password="password", host="127.0.0.1",
#                    port="5431")
# cursor = dbConnection.cursor()


def insert_log(self, transaction):
    # cself.cursor.execute(INSERT_SQL, (transaction.id, int(transaction.state)), json.dumps(transaction.cohorts))
    pass


def delete_log(self, transaction_id):
    # self.cursor.execute(DELETE_SQL, transaction_id)
    pass


def get_pending_transactions(self):
    transactions = []
    records = self.cursor.execute(SELECT_SQL).fetchall()
    for record in records:
        transaction = Transaction(record[0])
        transaction.state = record[1]
        transaction.cohorts = json.loads(record[2])
        transactions.append(record)
