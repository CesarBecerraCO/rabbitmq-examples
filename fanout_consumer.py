import pika, os, sys
from dotenv import load_dotenv
load_dotenv()

def main():
    eName, eType, qName = "eventos", "fanout", "qf"
    connection = pika.BlockingConnection(pika.URLParameters(os.getenv('MQ_URL')))
    channel = connection.channel()
    channel.exchange_declare(exchange=eName, exchange_type=eType)
    #channel.queue_declare(queue=qName)
    result = channel.queue_declare('', exclusive=True)
    qName = result.method.queue
    channel.queue_bind(queue=qName, exchange=eName, routing_key='')
    
    def msg_consumer(ch, method, properties, body):
        print(f" [x] Received {body}")
        channel.basic_ack(delivery_tag=method.delivery_tag)
    
    #channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=qName, on_message_callback=msg_consumer)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)