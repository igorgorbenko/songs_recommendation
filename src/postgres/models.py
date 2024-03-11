from sqlalchemy import Column, Integer, String, ForeignKey, ARRAY, TIMESTAMP
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class User(Base):
    __tablename__ = 'users'
    id = Column(Integer, primary_key=True)
    username = Column(String)
    email = Column(String)


class Events(Base):
    __tablename__ = 'events'
    id = Column(Integer, primary_key=True)
    event_name = Column(String)
    # ts = Column(Integer)
    # user_id = Column(Integer)
    # recommendations = Column(ARRAY(Integer))
    # recommendations_str = Column(ARRAY(String))
    # model = Column(String)
