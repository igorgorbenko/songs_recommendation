import logging
import os
from random import sample
from datetime import datetime, timezone

from pymilvus import Collection, connections, utility, MilvusClient
from redis import Redis
from dotenv import load_dotenv

from src.kafka.producer import KafkaEventProducer
from recommendations.cold_start import COLD_START_SONGS

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()
BOOTSTRAP_SERVERS = os.environ['KAFKA_BOOTSTRAP_SERVERS']
EVENTS_TOPIC_NAME = os.environ['KAFKA_EVENTS_TOPIC_NAME']

MILVUS_USERS_COLLECTION = os.environ['MILVUS_USERS_COLLECTION']
MILVUS_SONGS_COLLECTION = os.environ['MILVUS_SONGS_COLLECTION']
MILVUS_HOST = os.environ['MILVUS_HOST']
MILVUS_TOKEN = os.environ['MILVUS_TOKEN']

REDIS_CLIENT = Redis(host='localhost', port=6379, db=0)

connections.connect("default", uri=MILVUS_HOST, token=MILVUS_TOKEN)
USERS_COLLECTION = Collection(MILVUS_USERS_COLLECTION)
SONGS_COLLECTION = Collection(MILVUS_SONGS_COLLECTION)

TOP_N = 5


class Recommender:
    def __init__(self):
        self.kafka_producer = KafkaEventProducer(bootstrap_servers=BOOTSTRAP_SERVERS, topic=EVENTS_TOPIC_NAME)
        logger.info("Recommender initialized")

    def put_impression(self, user_id, song_id, is_like):
        # Record the impression in Redis
        try:
            REDIS_CLIENT.sadd(f"user:{user_id}:seen_songs", song_id)
            logger.info(f"The song {song_id} for User {user_id} has been saved to Redis")

            # Send the impression event to Kafka
            if is_like:
                event = {
                    'event_name': 'impression',
                    'is_like': True,
                    'ts': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f%z'),
                    'user_id': int(user_id),
                    'song_id': int(song_id)
                }
                message = self.generate_impression_schema(event)
                self.kafka_producer.produce_event(message)
                logger.info(f"Published impression event for user {user_id} and song {song_id} (like == {is_like})")
        except Exception as e:
            logger.error(e)
            return {'code': 500}

        return {'code': 200}

    def generate_impression_schema(self, event):
        schema = {
            "type": "struct",
            "fields": [
                {"type": "string", "optional": False, "field": "event_name"},
                {"type": "boolean", "optional": False, "field": "is_like"},
                {"type": "string", "optional": False, "field": "ts"},
                {"type": "int32", "optional": False, "field": "user_id"},
                {"type": "int32", "optional": False, "field": "song_id"}
            ]
        }
        return {"schema": schema, "payload": event}

    def get_recommendations(self, user_id):
        user_vector = Recommender.get_user_vector(user_id)

        if user_vector is None:
            recommended_songs = sample(COLD_START_SONGS, TOP_N)
            model_used = "coldstart"
            logger.info(f"User {user_id} has no vector, using cold start")
        else:
            recommended_songs = self.find_songs(user_vector)
            model_used = "online"
            logger.info(f"User {user_id} has a vector, using online model")

        recommended_songs = self.filter_seen_songs(user_id, recommended_songs)
        recommended_songs_desc = Recommender.get_songs_description(recommended_songs)
        self.publish_recommendation_info(user_id, recommended_songs, recommended_songs_desc, model_used)

        logger.info(f"Recommendations for user {user_id}: {recommended_songs}")
        return recommended_songs, recommended_songs_desc, user_vector

    @staticmethod
    def get_user_vector(user_id):
        entities = USERS_COLLECTION.query(
            expr=f'user_id == {user_id}',
            output_fields=["embedding"])
        if entities:
            return entities[0]['embedding']

    @staticmethod
    def get_songs_description(songs_list):
        entities = SONGS_COLLECTION.query(
            expr=f'id in {songs_list}',
            output_fields=["artist", "song"])

        return [f"{entity['artist']}: {entity['song']}" for entity in entities]

    def find_songs(self, user_vector):
        return []

    def filter_seen_songs(self, user_id, songs):
        return songs

    def generate_schema(self, event):
        schema = {
            "type": "struct",
            "fields": [
                {"type": "string", "optional": False, "field": "event_name"},
                {"type": "string", "optional": False, "field": "ts"},
                {"type": "int32",  "optional": False, "field": "user_id"},
                {"type": "array", "items": {"type": "int32"}, "optional": True, "field": "recommendations"},
                {"type": "array", "items": {"type": "string"}, "optional": True,"field": "recommendations_str"},
                {"type": "string", "optional": True, "field": "model"}
            ]
        }
        return {"schema": schema, "payload": event}

    def publish_recommendation_info(self, user_id, recommended_songs, recommended_songs_desc, model_used):
        event = {
            'event_name': 'recommend',
            'ts': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f%z'),
            'user_id': int(user_id),
            'recommendations': recommended_songs,
            'recommendations_str': recommended_songs_desc,
            'model': model_used
        }
        message = self.generate_schema(event)
        self.kafka_producer.produce_event(message)
        logger.info(f"Published recommendation event for user {user_id}")


#
r = Recommender()
# r.get_recommendations(1234556)
r.put_impression(1, 1, True)
r.kafka_producer.flush()
