import pika, sys, os
from dotenv import load_dotenv
load_dotenv()

def main():
    qName= "hello"
    #credentials = pika.PlainCredentials(username=os.getenv('MQ_USR'), password=os.getenv('MQ_USR'))
    #parameters = pika.ConnectionParameters(host='localhost', port=5672, virtual_host='/', credentials=credentials)
    parameters = pika.URLParameters(os.getenv('MQ_URL'))
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.queue_declare(queue=qName)

    def callback(ch, method, properties, body):
        print(f" [x] Received {body}")
    
    channel.basic_consume(queue=qName, on_message_callback=callback, auto_ack=True)

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