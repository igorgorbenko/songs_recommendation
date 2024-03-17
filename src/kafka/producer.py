import logging
import json
from confluent_kafka import Producer


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class KafkaEventProducer:
    def __init__(self, bootstrap_servers, topic):
        self.producer = Producer({'bootstrap.servers': bootstrap_servers})
        self.topic = topic

    @staticmethod
    def acked(err, msg):
        if err is not None:
            logger.error(f"Failed to deliver message: {err}")
        else:
            logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}]")

    def produce_event(self, event):
        event_data = json.dumps(event).encode('utf-8')
        self.producer.produce(self.topic, event_data, callback=KafkaEventProducer.acked)
        self.producer.poll(0)

    def flush(self):
        self.producer.flush()
