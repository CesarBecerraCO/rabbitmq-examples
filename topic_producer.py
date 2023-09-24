import pika, os, sys, time, random
from dotenv import load_dotenv
load_dotenv()

def binding(exchangeName, exchangeType, queue_routing):
    parameters = pika.URLParameters(os.getenv('MQ_URL'))
    connection = pika.BlockingConnection(parameters)
    assert connection.is_open

    try:
        channel = connection.channel()
        assert channel.is_open

        for d in queue_routing:
            channel.exchange_declare(exchange=exchangeName, exchange_type=exchangeType)
            channel.queue_declare(queue=d["queueName"])
            channel.queue_bind(exchange=exchangeName, queue=d["queueName"], routing_key=d["routing_key"])
    finally:
        connection.close()

def main():
    exchangeName, exchangeType = "xch_alarms_topic", "topic"
    queue_routing = [{"queueName":"alarms_all", "routing_key":"#"},
                     {"queueName":"alarms_calima", "routing_key":"CAL.#"},
                     {"queueName":"alarms_elec_yes", "routing_key":"*.elec.yes"}]

    binding(exchangeName, exchangeType, queue_routing)

    parameters = pika.URLParameters(os.getenv('MQ_URL'))
    connection = pika.BlockingConnection(parameters)
    assert connection.is_open
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