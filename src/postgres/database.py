import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


def create_database():
    conn = psycopg2.connect(
        host='localhost',
        user='postgres',
        password='example',
        dbname='postgres'
    )
    conn.autocommit = True

    cursor = conn.cursor()

    cursor.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = 'songs_recommendation'")
    exists = cursor.fetchone()
    if not exists:
        cursor.execute('CREATE DATABASE songs_recommendation')

    cursor.close()
    conn.close()

def init_db():
    engine = create_engine('postgresql://postgres:example@localhost:5432/songs_recommendation')
    Session = sessionmaker(bind=engine)
    from postgres.models import Base
    Base.metadata.create_all(engine)
    return Session


create_database()
Session = init_db()