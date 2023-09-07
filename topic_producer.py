import pika, os, sys, time, random
from dotenv import load_dotenv
load_dotenv()

def main():
    eName, eType, qName = "activated_alarms", "topic", "qalarms"
    connection = pika.BlockingConnection(pika.URLParameters(os.getenv('MQ_URL')))
    channel = connection.channel()
    channel.exchange_declare(exchange=eName, exchange_type=eType)
    #channel.queue_declare(queue=qName)

    i = 0
    substations = ["BUG", "CAL", "TUL", "ZAR"]
    protections = ["elec", "mec"]
    status = ["yes", "no"]
    while(True):
        rKey = f"{random.choice(substations)}.{random.choice(protections)}.{random.choice(status)}"
        msg_body=f"Msg {i} > Any activated '{rKey.split('.')[1]}' alarm in {rKey.split('.')[0]}: {rKey.split('.')[2]}"
        print(f" [x] Sent '{rKey}'")
        channel.basic_publish(exchange=eName, routing_key=rKey, body=msg_body)
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