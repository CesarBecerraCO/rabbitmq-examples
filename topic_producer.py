import pika, os, sys, time, random
from dotenv import load_dotenv
load_dotenv()

def binding(exchangeName, exchangeType):
    parameters = pika.URLParameters(os.getenv('MQ_URL'))
    connection = pika.BlockingConnection(parameters)
    assert connection.is_open

    try:
        channel = connection.channel()
        assert channel.is_open

        channel.exchange_declare(exchange=exchangeName, exchange_type=exchangeType)

        #Binding 1
        queueName = "alarms_all"
        channel.queue_declare(queue=queueName)
        channel.queue_bind(exchange=exchangeName,queue=queueName, routing_key="#")

        #Binding 2
        queueName = "alarms_calima"
        channel.queue_declare(queue=queueName)
        channel.queue_bind(exchange=exchangeName,queue=queueName, routing_key="CAL.#")

        #Binding 3
        queueName = "alarms_elec_yes"
        channel.queue_declare(queue=queueName)
        channel.queue_bind(exchange=exchangeName,queue=queueName, routing_key="*.elec.yes")
    finally:
        connection.close()

def main():
    parameters = pika.URLParameters(os.getenv('MQ_URL'))
    connection = pika.BlockingConnection(parameters)
    assert connection.is_open

    exchangeName, exchangeType = "xch_alarms_topic", "topic"

    binding(exchangeName, exchangeType)

    try:
        channel = connection.channel()
        assert channel.is_open

        i = 1
        substations = ["BUG", "CAL", "TUL", "ZAR"]
        protections = ["elec", "mec"]
        status = ["yes", "no"]
        while(True):
            routingKey = f"{random.choice(substations)}.{random.choice(protections)}.{random.choice(status)}"
            msg_body=f"Msg {i} > Any activated '{routingKey.split('.')[1]}' alarm in {routingKey.split('.')[0]}: {routingKey.split('.')[2]}"
            print(f" [x] Sent {i}: '{routingKey}'")
            channel.basic_publish(exchange=exchangeName,
                                  routing_key=routingKey,
                                  body=msg_body,
                                  properties=pika.BasicProperties(delivery_mode=2) # make message persistent
                                  )
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