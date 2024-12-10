import json
import time
import requests
from kafka import KafkaProducer

# Replace with your OpenWeatherMap API key
API_KEY = ''
CITY = 'Kuala Lumpur'
KAFKA_TOPIC = 'weather_data'

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

while True:
    response = requests.get(f"http://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={API_KEY}")
    weather_data = response.json()
    producer.send(KAFKA_TOPIC, value=weather_data)
    print(f"Sent data: {weather_data}")
    time.sleep(60)  # Fetch data every minute
