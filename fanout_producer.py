import pika, os, sys, time
from dotenv import load_dotenv
load_dotenv()

def main():
    eName, eType, qName = "eventos", "fanout", "qf"
    connection = pika.BlockingConnection(pika.URLParameters(os.getenv('MQ_URL')))
    channel = connection.channel()
    channel.exchange_declare(exchange=eName, exchange_type=eType)
    #channel.queue_declare(queue=qName)

    i = 0
    while(True):
        msg_body=f"Evento {i}"
        print(f" [x] Sent '{msg_body}'")
        channel.basic_publish(exchange=eName, routing_key='', body=msg_body)
        time.sleep(2)
        i += 1

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