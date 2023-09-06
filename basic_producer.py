import pika
import os
from dotenv import load_dotenv
load_dotenv()

eName, rKey, qName, msg_body = "", "hello", "hello", "Hi Cesar!"
connection = pika.BlockingConnection(pika.URLParameters(os.getenv('MQ_URL')))

channel = connection.channel()
channel.queue_declare(queue=qName)
channel.basic_publish(exchange=eName, routing_key=rKey, body=msg_body)

print(f" [x] Sent '{msg_body}'")

connection.close()