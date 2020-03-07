import time
from aifc import Error

import psycopg2

METADATA = "/home/akshayvb/Downloads/project1/data/low_concurrency/metadata_mysql.sql"
CREATE = "/home/akshayvb/Downloads/project1/schema/create.sql"
DROP = "/home/akshayvb/Downloads/project1/schema/drop.sql"

def execute_sql_file(file_name, cursor):
    fd = open(file_name, "r+")
    sqlFile = fd.read()
    fd.close()

    commands = sqlFile.split(';')
    for command in commands:
        command = command.strip()
        if command != "":
            #print("Executing " + command)
            start_time = time.time()
            cursor.execute(command)
            end_time = time.time()
            print("Executed " + command + " in " + str(end_time - start_time))


def main():
    try:
        connection = psycopg2.connect(database="project1", user="abc", password="abc", host="127.0.0.1", port="5432")
        #db_Info = connection.get_server_info()
        #print("Connected to PostgreSQL Server version ", db_Info)
        cursor = connection.cursor()
        execute_sql_file(CREATE, cursor)
        execute_sql_file(METADATA, cursor)
        connection.commit()
        print("Committed :)")

    except Error as e:
        execute_sql_file(DROP, cursor)
        print("Error while connecting to PostgreSQL", e)

    finally:
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")


if __name__ == "__main__":
    main()
