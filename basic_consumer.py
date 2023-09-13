import pika, sys, os
from dotenv import load_dotenv
load_dotenv()

def main():
    parameters = pika.URLParameters(os.getenv('MQ_URL'))
    connection = pika.BlockingConnection(parameters)
    assert connection.is_open

    queueName= "hello"
    try:
        channel = connection.channel()
        assert channel.is_open

        channel.queue_declare(queue=queueName)

        def callback(ch, method, properties, body):
            print(f" [x] Received {body}")
        
        channel.basic_consume(queue=queueName, on_message_callback=callback, auto_ack=True)

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