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
