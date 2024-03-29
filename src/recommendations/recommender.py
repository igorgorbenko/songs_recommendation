import json
import logging
import os
from random import sample, shuffle
from datetime import datetime, timezone
import uuid

from pymilvus import Collection, connections
from dotenv import load_dotenv
from redis import Redis

from src.kafka.producer import KafkaEventProducer
from src.utils.utils import Utils
from recommendations.cold_start import COLD_START_SONGS

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()
BOOTSTRAP_SERVERS = os.environ['KAFKA_BOOTSTRAP_SERVERS']
EVENTS_TOPIC_NAME = os.environ['KAFKA_EVENTS_TOPIC_NAME']

MILVUS_USERS_COLLECTION = os.environ['MILVUS_USERS_COLLECTION']
MILVUS_SONGS_COLLECTION = os.environ['MILVUS_SONGS_COLLECTION']
MILVUS_ARTISTS_COLLECTION = os.environ['MILVUS_ARTISTS_COLLECTION']
MILVUS_USERS_ARTISTS_COLLECTION = os.environ['MILVUS_USERS_ARTISTS_COLLECTION']

MILVUS_HOST = os.environ['MILVUS_HOST']
MILVUS_TOKEN = os.environ['MILVUS_TOKEN']

connections.connect("default", uri=MILVUS_HOST, token=MILVUS_TOKEN)
USERS_COLLECTION = Collection(MILVUS_USERS_COLLECTION)
SONGS_COLLECTION = Collection(MILVUS_SONGS_COLLECTION)
ARTISTS_COLLECTION = Collection(MILVUS_ARTISTS_COLLECTION)
USERS_ARTISTS_COLLECTION = Collection(MILVUS_USERS_ARTISTS_COLLECTION)

REDIS_CLIENT = Redis(host='localhost', port=6379, db=0)


class Recommender:
    def __init__(self):
        self.kafka_producer = KafkaEventProducer(bootstrap_servers=BOOTSTRAP_SERVERS, topic=EVENTS_TOPIC_NAME)
        logger.info("Recommender initialized")

    def put_impression(self, request_id, user_id, song_id, artist, is_like):
        try:
            # Send the impression event to Kafka
            event = {
                'event_name': 'impression',
                'request_id': request_id,
                'is_like': is_like,
                'ts': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f%z'),
                'user_id': int(user_id),
                'song_id': int(song_id),
                'artist': artist
            }
            # message = self.generate_impression_schema(event)
            self.kafka_producer.produce_event(event)
            logger.info(f"Published impression event for user {user_id} and "
                        f"song {song_id} by artist {artist} (like == {is_like})")
        except Exception as e:
            logger.error(e)
            return {'code': 500, 'message': str(e)}
        return {'code': 200}

    def get_recommendations(self, user_id):
        request_id = str(uuid.uuid4())
        logger.info("The process has started")
        user_vector = Utils.get_vector(USERS_COLLECTION, f"user_id == {user_id}")
        user_artist_vector = Utils.get_vector(USERS_ARTISTS_COLLECTION, f"user_id == {user_id}")
        recommendations = {}

        seen_songs = {int(song_id.decode('utf-8')) for song_id in REDIS_CLIENT.smembers(f"user:{user_id}:seen_songs")}

        if user_vector:
            logger.info(f"User {user_id} has a user_vector")
            song_ids, artists, songs = self.find_songs(user_vector, limit=100)
            filtered_songs = [(song_id, artist, song) for song_id, artist, song in zip(song_ids, artists, songs) if
                              song_id not in seen_songs]
            shuffle(filtered_songs)
            selected_songs = filtered_songs[:min(2, len(filtered_songs))]

            recommendations['online_user'] = {
                'song_ids': [song[0] for song in selected_songs],
                'artists': [song[1] for song in selected_songs],
                'songs': [song[2] for song in selected_songs]
            }

            if user_artist_vector:
                artist_names = self.find_artists(user_artist_vector, limit=20)
                song_ids, artists, songs = self.get_songs_by_artists(artist_names, seen_songs=seen_songs)

                all_songs = list(zip(song_ids, artists, songs))
                shuffle(all_songs)
                selected_songs = all_songs[:min(3, len(all_songs))]

                recommendations['online_user_artists'] = {
                    'song_ids': [song[0] for song in selected_songs],
                    'artists': [song[1] for song in selected_songs],
                    'songs': [song[2] for song in selected_songs]
                }

            cold_start_song = sample(COLD_START_SONGS, 1)
            cold_start_song_ids, cold_start_artists, cold_start_songs = Recommender.get_songs_description(cold_start_song)
            logger.info(f'Cold Start songs retrieved for User: {user_id}')
            recommendations['coldstart'] = {'song_ids': cold_start_song_ids,
                                            'artists': cold_start_artists,
                                            'songs': cold_start_songs}

        else:
            recommended_songs = sample(COLD_START_SONGS, 5)
            song_ids, artists, songs = self.get_songs_description(recommended_songs)
            recommendations['coldstart'] = {'song_ids': song_ids,
                                            'artists': artists,
                                            'songs': songs}

        formatted_vector = self.format_vector(user_vector) if user_vector is not None else 'None'
        self.publish_recommendation_info(request_id, user_id, recommendations)
        return request_id, recommendations, formatted_vector

    @staticmethod
    def format_vector(vector, precision=2):
        if vector is not None:
            return [round(float(x), precision) for x in vector]
        else:
            return None

    @staticmethod
    def get_songs_description(song_ids):
        entities = SONGS_COLLECTION.query(
            expr=f'id in {song_ids}',
            output_fields=["id", "artist", "song"]
        )

        song_ids = []
        artists = []
        songs = []
        for entity in entities:
            song_ids.append(entity['id'])
            artists.append(entity['artist'])
            songs.append(entity['song'])

        return song_ids, artists, songs

    @staticmethod
    def get_songs_by_artists(artists, seen_songs=set(), limit_per_artist=120):
        artists_str = ', '.join(f'"{artist}"' for artist in artists)
        seen_songs_str = ', '.join(str(song_id) for song_id in seen_songs)
        expr = f"id not in [{seen_songs_str}] and artist in [{artists_str}]"

        entities = SONGS_COLLECTION.query(expr=expr, output_fields=["id", "artist", "song"],
                                          limit=len(artists) * limit_per_artist)

        song_ids, artists_list, songs = [], [], []
        for entity in entities:
            song_ids.append(entity['id'])
            artists_list.append(entity['artist'])
            songs.append(entity['song'])

        return song_ids, artists_list, songs

    @staticmethod
    def find_artists(user_artist_vector, limit=10):
        search_results = ARTISTS_COLLECTION.search(
            data=[user_artist_vector],
            anns_field="embedding",
            param={"metric_type": "L2", "params": {"nprobe": 20}},
            limit=limit,
            output_fields=["artist"]
        )
        return [hit.entity.get('artist') for hit in search_results[0]]

    @staticmethod
    def find_songs(user_vector, limit=10):
        search_results = SONGS_COLLECTION.search(
            data=[user_vector],
            anns_field="embedding",
            param={"metric_type": "L2", "params": {"nprobe": 20}},
            limit=limit,
            output_fields=["id", "artist", "song"]
        )

        song_ids = []
        artists = []
        songs = []
        for hit in search_results[0]:
            song_ids.append(hit.entity.get('id'))
            artists.append(hit.entity.get('artist'))
            songs.append(hit.entity.get('song'))

        return song_ids, artists, songs

    def publish_recommendation_info(self, request_id, user_id, recommendations):
        event = {
            'event_name': 'recommend',
            'request_id': request_id,
            'ts': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f'),
            'user_id': int(user_id),
            'recommendations': recommendations
        }
        # message = self.generate_recommendation_schema(event)
        self.kafka_producer.produce_event(event)
        logger.info(f"Published recommendation event for user {user_id}")


if __name__ == '__main__':
    r = Recommender()
    request_id, recommendations, formatted_vector = r.get_recommendations(3)
    print(request_id)
    print(json.dumps(recommendations))
    # r.put_impression(1, 1, True)
    r.kafka_producer.flush()

