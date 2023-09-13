import pika, os, sys
from dotenv import load_dotenv
load_dotenv()

def main(bindingKey):
    parameters = pika.URLParameters(os.getenv('MQ_URL'))
    connection = pika.BlockingConnection(parameters)
    assert connection.is_open

    exchangeName, eType, routingKey, queueName = "activated_alarms", "topic", bindingKey, "activated_alarms"
    try:
        channel = connection.channel()
        assert channel.is_open

        channel.exchange_declare(exchange=exchangeName, exchange_type=eType)
        #channel.queue_declare(queue=queueName)
        result = channel.queue_declare('', exclusive=True)
        queueName = result.method.queue
        channel.queue_bind(queue=queueName, exchange=exchangeName, routing_key=routingKey)
        
        def msg_consumer(ch, method, properties, body):
            print(f" [x] {method.routing_key}: {body}")
            channel.basic_ack(delivery_tag=method.delivery_tag)
        
        #channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue=queueName, on_message_callback=msg_consumer)

        print(' [*] Waiting for messages. To exit press CTRL+C')
        channel.start_consuming()
    finally:
        connection.close()

def myexit():
    try:
        sys.exit(0)
    except SystemExit:
        os._exit(0)


if __name__ == '__main__':
    try:
        bindingKey = sys.argv[1:][0]
        if not bindingKey:
            print(f"Usage: {sys.argv[0]} bindingKey...\n")
            myexit()
        main(bindingKey)
    except KeyboardInterrupt:
        print('Interrupted')
        myexit()