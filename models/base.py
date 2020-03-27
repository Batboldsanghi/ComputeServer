import urllib.parse
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from config import db

dbKey = db['default']

engine = create_engine('{driver}://{user}:{password}@{host}/{database}?charset=utf8mb4'.format(
                                                                    driver=db[dbKey]['driver'],
                                                                    host=db[dbKey]['host'],
                                                                    user=db[dbKey]['user'],
                                                                    password=urllib.parse.quote(db[dbKey]['password']),
                                                                    database=db[dbKey]['database']
                                                                ))
Session = sessionmaker(bind=engine,autocommit=True)
session = Session()
Base = declarative_base()
Base.metadata.bind = engine