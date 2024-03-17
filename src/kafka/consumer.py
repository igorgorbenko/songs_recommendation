import os
import logging
import json
import numpy as np

from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv
from pymilvus import Collection, connections

from src.utils.utils import Utils


load_dotenv()
BOOTSTRAP_SERVERS = os.environ['KAFKA_BOOTSTRAP_SERVERS']
EVENTS_TOPIC_NAME = os.environ['KAFKA_EVENTS_TOPIC_NAME']
KAFKA_CONSUMER_GROUP_NAME = os.environ['KAFKA_CONSUMER_GROUP_NAME']

MILVUS_HOST = os.environ['MILVUS_HOST']
MILVUS_TOKEN = os.environ['MILVUS_TOKEN']
USERS_COLLECTION_NAME = os.environ['MILVUS_USERS_COLLECTION']
SONGS_COLLECTION_NAME = os.environ['MILVUS_SONGS_COLLECTION']
ARTISTS_COLLECTION_NAME = os.environ['MILVUS_ARTISTS_COLLECTION']
USERS_ARTISTS_COLLECTION_NAME = os.environ['MILVUS_USERS_ARTISTS_COLLECTION']

connections.connect("default", uri=MILVUS_HOST, token=MILVUS_TOKEN)
USERS_COLLECTION = Collection(USERS_COLLECTION_NAME)
SONGS_COLLECTION = Collection(SONGS_COLLECTION_NAME)
ARTISTS_COLLECTION = Collection(ARTISTS_COLLECTION_NAME)
USERS_ARTISTS_COLLECTION = Collection(USERS_ARTISTS_COLLECTION_NAME)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


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

    def process_impression_event(self, event_data):
        user_id = event_data['user_id']
        song_id = event_data['song_id']
        artist = event_data['artist']
        is_like = event_data['is_like']

        if is_like:
            user_vector = Utils.get_vector(USERS_COLLECTION, f"user_id == {user_id}")
            song_vector = Utils.get_vector(SONGS_COLLECTION, f"id == {song_id}")
            artist_vector = Utils.get_vector(ARTISTS_COLLECTION, f"artist == '{artist}'")

            self.update_vector(USERS_COLLECTION, user_id, user_vector, song_vector, "user")

            user_artist_vector = Utils.get_vector(USERS_ARTISTS_COLLECTION, f"user_id == {user_id}")
            self.update_vector(USERS_ARTISTS_COLLECTION, user_id, user_artist_vector, artist_vector, "user-artist")

    def update_vector(self, collection, user_id, existing_vector, new_vector, vector_type):
        if new_vector is not None:
            if existing_vector is not None:
                updated_vector = np.mean([np.array(existing_vector), np.array(new_vector)], axis=0).astype(np.float32)
                collection.upsert([[int(user_id)], [updated_vector]])
                logger.info(f'Updated {vector_type} vector for user {user_id}')
            else:
                new_vector = np.array(new_vector).astype(np.float32)
                collection.insert([[int(user_id)], [new_vector]])
                logger.info(f'Inserted new {vector_type} vector for user {user_id}')
        else:
            logger.warning(f'Vector for {vector_type} not found')

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
                        logger.error(msg.error())
                        break

                event = json.loads(msg.value().decode('utf-8'))
                if event['event_name'] == 'impression':
                    self.process_impression_event(event)

        except KeyboardInterrupt:
            pass
        finally:
            self.consumer.close()


if __name__ == '__main__':
    consumer = KafkaEventConsumer(BOOTSTRAP_SERVERS, KAFKA_CONSUMER_GROUP_NAME, EVENTS_TOPIC_NAME)
    consumer.consume_events()
