import psycopg2

# def checkAbort(connection )
from constants import COORDINATOR_DB_PORT, COHORT_DB_PORTS_LIST, DATABASE, USERNAME, PASSWORD


class DatabaseStateVerifier:

    def __init__(self,coordinator_port, cohort_ports):
        self.coordinator_db_connection = psycopg2.connect(database=DATABASE, user=USERNAME, password=PASSWORD, host="127.0.0.1", port=coordinator_port)
        self.coordinator_db_connection.autocommit = True
        self.cohort_db_connections = []
        self.connection_map = {}
        for cohort_port in cohort_ports:
            cohort_connection = psycopg2.connect(database=DATABASE, user=USERNAME, password=PASSWORD, host="127.0.0.1", port=cohort_port)
            self.coordinator_db_connection.autocommit = True
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
        return result

    def is_committed(self,count):
        result = self.check_insert_count_in_all_dbs(100)
        return result

    def delete_log_info(self):
        cursor = self.coordinator_db_connection.cursor()
        cursor.execute("DELETE FROM TRANSACTION_LOG;")
        cursor.execute("DELETE FROM LINE_NUMBER;")

if __name__ == "__main__":
    databaseStateVerifier = DatabaseStateVerifier(COORDINATOR_DB_PORT, COHORT_DB_PORTS_LIST)
    if databaseStateVerifier.is_aborted():
        print("Verified that the transaction was aborted")
    elif databaseStateVerifier.is_committed(100):
        print("Verified that the transaction was committed")
    databaseStateVerifier.delete_log_info()
    databaseStateVerifier.close_connections()


