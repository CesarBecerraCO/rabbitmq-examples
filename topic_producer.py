import pika, os, sys, time, random
from dotenv import load_dotenv
load_dotenv()

def main():
    parameters = pika.URLParameters(os.getenv('MQ_URL'))
    connection = pika.BlockingConnection(parameters)
    assert connection.is_open

    exchangeName, exchangeType, routingKey, queueName = "activated_alarms", "topic", "", "activated_alarms"
    try:
        channel = connection.channel()
        assert channel.is_open

        channel.exchange_declare(exchange=exchangeName, exchange_type=exchangeType)
        #channel.queue_declare(queue=queueName, passive=False, durable=True) #Keeps messages in this queue, but... 

        i = 0
        substations = ["BUG", "CAL", "TUL", "ZAR"]
        protections = ["elec", "mec"]
        status = ["yes", "no"]
        while(True):
            routingKey = f"{random.choice(substations)}.{random.choice(protections)}.{random.choice(status)}"
            msg_body=f"Msg {i} > Any activated '{routingKey.split('.')[1]}' alarm in {routingKey.split('.')[0]}: {routingKey.split('.')[2]}"
            print(f" [x] Sent '{routingKey}'")
            channel.basic_publish(exchange=exchangeName,
                                  routing_key=routingKey,
                                  body=msg_body,
                                  properties=pika.BasicProperties(delivery_mode=2) # make message persistent
                                  )
            time.sleep(2)
            i += 1
    finally:
        connection.close()

# python topic_consumer.py *.elec.*
# python topic_consumer.py BUG.elec.*

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)