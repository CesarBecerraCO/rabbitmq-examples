import pika
import os
from dotenv import load_dotenv
load_dotenv()

#credentials = pika.PlainCredentials(username=os.getenv('MQ_USR'), password=os.getenv('MQ_USR'))
#parameters = pika.ConnectionParameters(host='localhost', port=5672, virtual_host='/', credentials=credentials)
parameters = pika.URLParameters(os.getenv('MQ_URL'))
connection = pika.BlockingConnection(parameters)
assert connection.is_open

exchangeName, routingKey, queueName = "", "hello", "hello"
msgBody = "Hi Cesar!"
try:
    channel = connection.channel()
    assert channel.is_open

    channel.queue_declare(queue=queueName)
    channel.basic_publish(exchange=exchangeName, routing_key=routingKey, body=msgBody)

    print(f" [x] Sent '{msgBody}'")
finally:
    connection.close()