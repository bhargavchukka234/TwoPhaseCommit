import pika


def callback(ch, method, properties, body):
    print(" [x] Received %r" % body)

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.queue_declare(queue='hello', durable=True)

channel.basic_consume(queue='hello',
                      auto_ack=True,
                      on_message_callback=callback)
channel.start_consuming()