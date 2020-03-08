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
