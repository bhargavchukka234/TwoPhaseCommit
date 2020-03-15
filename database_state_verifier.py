import pika
import time
from enum import IntEnum
import psycopg2
import json

# def checkAbort(connection )
class DatabaseStateVerifier:

    def __init__(self,coordinator_port, cohort_ports):
        self.coordinator_db_connection = psycopg2.connect(database="test", user="newuser", password="password", host="127.0.0.1", port=coordinator_port)
        self.cohort_db_connections = []
        for cohort_port in cohort_ports:
            cohort_connection = psycopg2.connect(database="test", user="newuser", password="password", host="127.0.0.1", port=cohort_port)
            self.cohort_db_connections.append(cohort_connection)
            self.connection_map[cohort_connection] = cohort_port

    def is_transaction_active_at_coordinator(self):
        cursor = self.coordinator_db_connection.cursor()
        cursor.execute("Select * from transaction_log;")
        record = cursor.fetchone()
        return record is not None

    def is_transaction_active_at_cohort(self, connection):
        cursor = connection.cursor()
        cursor.execute("Select * from pg_prepared_xacts;")
        record = cursor.fetchone()
        return record is not None

    def is_transaction_complete(self):

        if self.is_transaction_active_at_coordinator():
            print("Transaction is still active at coordinator")
            return False
        for cohort_db_connection in self.cohort_db_connections:
            if self.is_transaction_active_at_cohort(cohort_db_connection):
                print("Transaction is still active at cohort with PostgreSQL DB on port %s" % (str(self.connection_map[cohort_db_connection])))
                return False
        print("Transaction is complete")
        return True

    def get_insert_count(self, connection):

        cursor = connection.cursor()
        cursor.execute("Select count(*) from wemoobservation;")
        record = cursor.fetchone()
        print("Inserted data count in PostgreSQL DB on port %s is %s" % (str(self.connection_map[connection]), str(record[0])))
        return record[0]

    def check_insert_count_in_all_dbs(self, count):
        if not self.is_transaction_complete():
            return False
        record_count = 0
        for cohort_db_connection in self.cohort_db_connections:
            record_count = record_count + self.get_insert_count(cohort_db_connection)

        return record_count == count

    def close_connections(self):
        self.coordinator_db_connection.close()
        for cohort_db_connection in self.cohort_db_connections:
            cohort_db_connection.close()

    def is_aborted(self):
        result = self.check_insert_count_in_all_dbs(0)
        self.close_connections()
        return result

    def is_committed(self,count):
        result = self.check_insert_count_in_all_dbs(100)
        self.close_connections()
        return result


if __name__ == "__main__":
    databaseStateVerifier = DatabaseStateVerifier(5431, [5433,5434,5435])
    if databaseStateVerifier.is_aborted():
        print("Verified that the transaction was aborted")
    elif databaseStateVerifier.is_committed(100):


