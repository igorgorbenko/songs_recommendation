from postgres.database import Session
from postgres.models import User


def add_user(username, email):
    session = Session()
    new_user = User(username=username, email=email)
    session.add(new_user)
    session.commit()
    session.close()


add_user('Igor', 'dsfs')