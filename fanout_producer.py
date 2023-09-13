import pika, os, sys, time
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

        i = 0
        while(True):
            msgBody=f"Evento {i}"
            print(f" [x] Sent '{msgBody}'")
            channel.basic_publish(exchange=exchangeName, routing_key=routingKey, body=msgBody)
            time.sleep(2)
            i += 1
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