# kafka_producer_peliculas.py
# Diana Marcela Castiblanco Sanchez
# Grupo 202016911_17

import time
import json
import random
from kafka import KafkaProducer

def generate_movie_data():
    movies = [
        {"title": "Inception", "popularity": 95, "vote_average": 8.7, "release_date": "2010-07-16"},
        {"title": "Avatar", "popularity": 99, "vote_average": 8.3, "release_date": "2009-12-18"},
        {"title": "Interstellar", "popularity": 97, "vote_average": 8.9, "release_date": "2014-11-07"},
        {"title": "The Matrix", "popularity": 93, "vote_average": 8.6, "release_date": "1999-03-31"},
        {"title": "The Dark Knight", "popularity": 98, "vote_average": 9.0, "release_date": "2008-07-18"},
    ]
    return random.choice(movies)

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)
# Creacion del topic peliculas_stream
while True:
    movie_data = generate_movie_data()
    producer.send('peliculas_stream', value=movie_data)
    print(f"Enviado: {movie_data}")
    time.sleep(1)
