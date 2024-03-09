import os
from random import sample
from pymilvus import Collection, connections, utility, MilvusClient
from redis import Redis
from dotenv import load_dotenv

from src.kafka.producer import KafkaEventProducer
from recommendations.cold_start import COLD_START_SONGS

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

    def get_recommendations(self, user_id):
        user_vector = Recommender.get_user_vector(user_id)

        if user_vector is None:
            recommended_songs = sample(COLD_START_SONGS, TOP_N)
            model_used = "coldstart"
        else:
            recommended_songs = self.find_songs(user_vector)
            model_used = "online"

        recommended_songs = self.filter_seen_songs(user_id, recommended_songs)
        recommended_songs_desc = Recommender.get_songs_description(recommended_songs)
        self.publish_recommendation_info(user_id, recommended_songs, recommended_songs_desc, model_used)

        return recommended_songs, recommended_songs_desc

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

    def publish_recommendation_info(self, user_id, recommended_songs, recommended_songs_desc, model_used):
        event = {
            'event_name': 'recommend',
            'user_id': user_id,
            'recommendations': recommended_songs,
            'recommendations_str': recommended_songs_desc,
            'model_used': model_used
        }
        self.kafka_producer.produce_event(event)

#
# r = Recommender()
# r.get_recommendations(1)
# r.kafka_producer.flush()
