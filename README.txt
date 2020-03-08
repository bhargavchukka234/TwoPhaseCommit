#To install rabbitmq and connect from python

Rabbitmq install:
	brew install rabbitmq
	
Add rabbitmq path to environment variables:
	export PATH=$PATH:/usr/local/opt/rabbitmq/sbin
	
Start server from terminal:
	rabbitmq-server
UI:
	localhost:15672
Python libraries for rabbitmq:
	pip3 install pika --upgrade
	
# To pipe file for coordinator

#create pipe:
	mkfifo pipe
#move data to pipe:
	cat testData.txt > pipe

# table for coordinator transaction logs(server running on 5431 port)
CREATE TABLE transaction_log(id varchar(100) primary key, state varchar(100) not null, cohorts varchar);

# alter the max_prepared_connections variable: 
1) Log into superuser and run : ALTER SYSTEM SET max_prepared_connections TO 100;
2) Restart the database server 

# view currently prepared transactions holding locks 
SELECT gid FROM pg_prepared_xacts WHERE database = current_database()
