import pika
import time

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.queue_declare(queue='hello', durable=True)
while True:
    channel.basic_publish(exchange='',
                      routing_key='hello',
                      body='Hello World!')
    time.sleep(2)
print(" [x] Sent 'Hello World!'")
connection.close()
