import pika, os, sys
from dotenv import load_dotenv
load_dotenv()

def main(queueName):
    parameters = pika.URLParameters(os.getenv('MQ_URL'))
    connection = pika.BlockingConnection(parameters)
    assert connection.is_open

    try:
        channel = connection.channel()
        assert channel.is_open
        
        def callback(ch, method, properties, body):
            print(f" [x] {method.routing_key}: {body}")
            channel.basic_ack(delivery_tag=method.delivery_tag)
        
        #channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue=queueName, on_message_callback=callback)

        print(' [*] Waiting for messages. To exit press CTRL+C')
        channel.start_consuming()
    finally:
        connection.close()

def myexit():
    try:
        sys.exit(0)
    except SystemExit:
        os._exit(0)

"""
python topic_consumer.py "alarms_elec_yes"
python topic_consumer.py "alarms_calima"
python topic_consumer.py "alarms_all"
"""
if __name__ == '__main__':
    try:
        if len(sys.argv) != 2:
            print(f"Usage: {sys.argv[0]} queueName...\n")
            myexit()
        queueName = sys.argv[1:][0]            
        main(queueName)
    except KeyboardInterrupt:
        print('Interrupted')
        myexit()