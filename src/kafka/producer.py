import json
from confluent_kafka import Producer


class KafkaEventProducer:
    def __init__(self, bootstrap_servers, topic):
        self.producer = Producer({'bootstrap.servers': bootstrap_servers})
        self.topic = topic

    @staticmethod
    def acked(err, msg):
        if err is not None:
            print(f"Failed to deliver message: {err}")
        else:
            print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

    def produce_event(self, event):
        event_data = json.dumps(event).encode('utf-8')
        self.producer.produce(self.topic, event_data, callback=KafkaEventProducer.acked)
        self.producer.poll(0)

    def flush(self):
        self.producer.flush()

# if __name__ == '__main__':
#     producer = KafkaEventProducer(bootstrap_servers=BOOTSTRAP_SERVERS, topic=EVENTS_TOPIC_NAME)
#     impression_event = {
#         'event_type': 'impression',
#         'user_id': 1,
#         'song_id': 101,
#         'reaction': 'like'
#     }
#     producer.produce_event(impression_event)
#
#     recommend_event = {
#         'event_type': 'recommend',
#         'user_id': 1,
#         'recommended_songs': [201, 202, 203]
#     }
#     producer.produce_event(recommend_event)
