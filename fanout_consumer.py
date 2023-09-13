import pika, os, sys
from dotenv import load_dotenv
load_dotenv()

def main():
    parameters = pika.URLParameters(os.getenv('MQ_URL'))
    connection = pika.BlockingConnection(parameters)
    assert connection.is_open

    exchangeName, exchangeType, routingKey, queueName = "eventos", "fanout", "", "eventos"
    try:
        channel = connection.channel()
        assert channel.is_open

        channel.exchange_declare(exchange=exchangeName, exchange_type=exchangeType)
        channel.queue_declare(queue=queueName)
        #result = channel.queue_declare('', exclusive=True)
        #queueName = result.method.queue

        channel.queue_bind(queue=queueName, exchange=exchangeName, routing_key=routingKey)
        
        def msg_consumer(ch, method, properties, body):
            print(f" [x] Received {body}")
            channel.basic_ack(delivery_tag=method.delivery_tag)
        
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue=queueName, on_message_callback=msg_consumer)

        print(' [*] Waiting for messages. To exit press CTRL+C')
        channel.start_consuming()
    finally:
        connection.close()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)