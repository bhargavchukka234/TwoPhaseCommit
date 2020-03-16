Steps to install rabbitmq and connect from python

1. Rabbitmq install:
    brew install rabbitmq (for mac)
    yum install rabbitmq-server (for centos/RHEL)	
2. Add rabbitmq path to environment variables:
    export PATH=$PATH:/usr/local/opt/rabbitmq/sbin (for mac)	
3. Start server from terminal:
    rabbitmq-server (for mac)
    service rabbitmq-server start (for centos/RHEL)	
4. UI:
    localhost:15672 
5. Install Python libraries for rabbitmq:
    pip3 install pika --upgrade
Steps to set up the PostgreSQL DBs

1. pip3 install psycopg2
2. Set up n PostgreSQL servers on different ports. Each server can be used for one cohort site.
    - Initialize a database in a particular directory 
        initdb -D /var/tmp/db1
    - Start the DB instance 
        Eg: pg_ctl -D /var/tmp/db1 -o “-p 5433” start
    - Login to the psql commandline through a particular port and user 
        Eg: psql -p 5433 postgres -U newuser
    - Create a new user and password for the same. Mention the username and password in constants.py
      This is the user and password used to connect to the DB at the coordinator and the cohort. 
        Eg : create user ‘username’ with password ‘password’;
             grant all privileges on database "test" to newuser;
    - Create a new database for the new user. This is the database used to store information at the cohort and the coordinator(protocolDB)
        Eg: create database “dbname”



# table for coordinator transaction logs(server running on 5431 port)
CREATE TABLE transaction_log(id varchar(100) primary key, state varchar(100) not null, cohorts varchar);

# alter the max_prepared_connections variable: 
1) Log into superuser and run : ALTER SYSTEM SET max_prepared_connections TO 100;
2) Restart the database server 

# view currently prepared transactions holding locks 
SELECT gid FROM pg_prepared_xacts WHERE database = current_database()


TEST CASES AND HOW TO EXECUTE THEM :)

Case 0: Scenario where the 2 Phase Commit is successful without timeouts or restarts
Simulation steps:
1. Start all cohorts with the command “python3 cohort.py -p <postgres-port> -q <cohort-queue-id> -c”
2. Start the coordinator with the command “python3 coordinator.py -n <num-of-cohorts>”
3. The following observations are expected:
   a. All the cohorts successfully COMMIT
   b. Run the verifier using the command “python3 database_state_verifier.py” and verify that the transaction has committed. 4. Also see the count of the inserts in each DB is shown.

Case 1: Scenario where coordinator times out waiting for votes from cohorts
Simulation steps:
1. Start all cohorts with the command “python3 cohort.py -p <postgres-port> -q <cohort-queue-id> -c”
2. Start the coordinator with the command “python3 coordinator.py -n <num-of-cohorts> -t test1”
3. The following observations are expected:
   a. Coordinator times out waiting for vote from cohorts
   b. Coordinator sends ABORT to each cohort 
   c. All the cohorts successfully ABORT
4. Run the verifier using the command “python3 database_state_verifier.py” and verify that the transaction has aborted.

Case 2: Scenario where coordinator goes down after it writes COMMIT decision to logs
Simulation steps:
1. Start all cohorts with the command “python3 cohort.py -p <postgres-port> -q <cohort-queue-id> -c”
2. Start the coordinator with the command “python3 coordinator.py -n <num-of-cohorts> -t test2”
3. After the coordinator goes down, start the coordinator with the command “python3 coordinator.py -n <num-of-cohorts>”
4. The following observations are expected:
   a. Coordinator comes back up, reads the decision written in the logs
   b. Coordinator sends COMMIT to each cohort 
   c. All the cohorts successfully COMMIT
5. Run the verifier using the command “python3 database_state_verifier.py” and verify that the transaction has committed.      Also see the count of the inserts in each DB is shown.

Case 3: Scenario where coordinator does not receive ACK for the COMMIT decision
Simulation steps:
1. tart all cohorts with the command “python3 cohort.py -p <postgres-port> -q <cohort-queue-id> -c”
2. Start the coordinator with the command “python3 coordinator.py -n <num-of-cohorts> -t test3”
3. The following observations are expected:
   a. Coordinator times out waiting for ACK from cohorts.
   b. Coordinator resends the COMMIT decision to all the cohorts 
   c. All the cohorts successfully COMMIT
4. Run the verifier using the command “python3 database_state_verifier.py” and verify that the transaction has committed.      Also see the count of the inserts in each DB is shown.

Case 4: Scenario where coordinator does not receive ACK for the ABORT decision
Simulation steps:
1. Start all but one cohorts with the command “python3 cohort.py -p <postgres-port> -q <cohort-queue-id> -c”
2. Start the coordinator with the command “python3 coordinator.py -n <num-of-cohorts> -t test4”
3. When the coordinator sends the ABORT message, start the left out cohort with the same command as mentioned above.
4. The following observations are expected:
   a. Coordinator times out waiting for ACK from cohorts.
   b. Coordinator resends the ABORT decision to all the cohorts 
   c. All the cohorts successfully ABORT
5. Run the verifier using the command “python3 database_state_verifier.py” and verify that the transaction has aborted.

Case 5: Scenario where INSERT statement execution fails at the cohorts
Simulation steps:
1. Start each cohort with the command “python3 cohort.py -p <postgres-port> -q <cohort-queue-id> -c”
2. Start the coordinator with the command “python3 coordinator.py -n <num-of-cohorts> -t test5”
3. The following observations are expected:
   a. Each cohort will send an ABORT vote to the coordinator
   b. Coordinator sends the ABORT decision to all the cohorts 
   c. All the cohorts successfully ABORT
4. Run the verifier using the command “python3 database_state_verifier.py” and verify that the transaction has aborted.

Case 6: Scenario where cohort goes down before receiving PREPARE
Simulation steps:
1. Start one cohort (let’s say cohort1) with the command “python3 cohort.py -p <postgres-port> -q <cohort-queue-id> -t test6 -c”
2. Start the other cohorts with the command “python3 cohort.py -p <postgres-port> -q <cohort-queue-id> -c”
3. Start the coordinator with the command “python3 coordinator.py -n <num-of-cohorts>”
4. When cohort shuts down before receiving PREPARE, restart it with the command “python3 cohort.py -p <postgres-port> -q      <cohort-queue-id>”
5. The following observations are expected:
   a. Coordinator times out waiting for votes from cohorts
   b. Coordinator responds to all cohorts with the ABORT decision.
   c. All the cohorts successfully ABORT
6. Run the verifier using the command “python3 database_state_verifier.py” and verify that the transaction has aborted.

Case 7: Scenario where one of the cohorts times out waiting for COMMIT decision from the coordinator.
Simulation steps:
1. Start one cohort (let’s say cohort1) with the command “python3 cohort.py -p <postgres-port> -q <cohort-queue-id> -t test7    -c”
2. Start the other cohorts with the command “python3 cohort.py -p <postgres-port> -q <cohort-queue-id> -c”
3. Start the coordinator with the command “python3 coordinator.py -n <num-of-cohorts>”
4. The following observations are expected:
   a. Cohort1 will time out and request for the decision from the coordinator.
   b. Coordinator responds to cohort1 with the COMMIT decision.
   c. All the cohorts successfully COMMIT
5. Run the verifier using the command “python3 database_state_verifier.py” and verify that the transaction has committed.   Also see the count of the inserts in each DB is shown.

Case 8: Scenario where one of the cohorts times out waiting for ABORT decision from the coordinator.
Simulation steps:
1. Start one cohort (let’s say cohort1) with the command “python3 cohort.py -p <postgres-port> -q <cohort-queue-id> -t test8 -c”
2. Start the other cohorts with the command “python3 cohort.py -p <postgres-port> -q <cohort-queue-id> -c”
3. Start the coordinator with the command “python3 coordinator.py -n <num-of-cohorts>”
4. The following observations are expected:
   a. Cohort1 will time out and request for the decision from the coordinator.
   b. Coordinator responds to cohort1 with the ABORT decision.
   c. All the cohorts successfully ABORT
5. Run the verifier using the command “python3 database_state_verifier.py” and verify that the transaction has aborted.

Case 9: Scenario where one of the cohorts goes down before sending an ACK to the coordinator.
Simulation steps:
1. Start one cohort (let’s say cohort1) with the command “python3 cohort.py -p <postgres-port> -q <cohort-queue-id> -t test9 -c”
2. Start the other cohorts with the command “python3 cohort.py -p <postgres-port> -q <cohort-queue-id> -c”
3. Start the coordinator with the command “python3 coordinator.py -n <num-of-cohorts>”
4. When cohort shuts down before sending ACK, restart it with the command “python3 cohort.py -p <postgres-port> -q <cohort-queue-id>”
5. The following observations are expected:
   a. Coordinator will timeout waiting for ACK from cohort1 and resends COMMIT to cohorts
   b. Cohort1 sends ACK in response to coordinator’s COMMIT message
   c. All the cohorts successfully COMMIT
6. Run the verifier using the command “python3 database_state_verifier.py” and verify that the transaction has committed. Also see the count of the inserts in each DB is shown.

Case 10: Scenario where one of the cohorts goes down before after executing PREPARE transaction
Simulation steps:
1. Start one cohort (let’s say cohort1) with the command “python3 cohort.py -p <postgres-port> -q <cohort-queue-id> -t test10 -c”
2. Start the other cohorts with the command “python3 cohort.py -p <postgres-port> -q <cohort-queue-id> -c”
3. Start the coordinator with the command “python3 coordinator.py -n <num-of-cohorts>”
4. When cohort shuts down before sending PREPARE vote, restart it with the command “python3 cohort.py -p <postgres-port> -q <cohort-queue-id>”
5. The following observations are expected:
   a. Coordinator will timeout waiting for PREPARE from cohort1 and sends ABORT to all cohorts
   b. When cohort1 is restarted, it responds to ABORT message with an ACK
   c. All the cohorts successfully ABORT
6. Run the verifier using the command “python3 database_state_verifier.py” and verify that the transaction has aborted.
