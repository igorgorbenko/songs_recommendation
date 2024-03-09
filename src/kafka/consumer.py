import os
from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv

load_dotenv()
BOOTSTRAP_SERVERS = os.environ['KAFKA_BOOTSTRAP_SERVERS']
EVENTS_TOPIC_NAME = os.environ['KAFKA_EVENTS_TOPIC_NAME']
KAFKA_CONSUMER_GROUP_NAME = os.environ['KAFKA_CONSUMER_GROUP_NAME']


class KafkaEventConsumer:
    def __init__(self, bootstrap_servers, group_id, topic):
        self.config = {
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': 'earliest'
        }
        self.topic = topic
        self.consumer = Consumer(self.config)
        self.consumer.subscribe([self.topic])

    def consume_events(self):
        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)

                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        print(msg.error())
                        break

                print(f'Received message: {msg.timestamp()} - {msg.value().decode("utf-8")}')

        except KeyboardInterrupt:
            pass
        finally:
            self.consumer.close()


if __name__ == '__main__':
    consumer = KafkaEventConsumer(BOOTSTRAP_SERVERS, 'my-consumer-group', EVENTS_TOPIC_NAME)
    consumer.consume_events()
