import argparse
import requests
import json
import sys
from kafka import KafkaProducer

# Setup
BOOTSTRAP_SERVERS = ['localhost:9092']
TOPIC_NAME = 'weather_stream'
API_URL = "https://api.open-meteo.com/v1/forecast"

def get_weather(lat, lon):
    params = {
        "latitude": lat,
        "longitude": lon,
        "current_weather": "true"
    }
    try:
        response = requests.get(API_URL, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching weather data: {e}")
        return None

def run_producer(data):
    try:
        producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        future = producer.send(TOPIC_NAME, data)
        result = future.get(timeout=60)
        
        print(f"Weather data sent to {result.topic} - Partition: {result.partition} - Offset: {result.offset}")
        print(f"Payload: {json.dumps(data, indent=2)}")
        
        producer.flush()
        producer.close()
    except Exception as e:
        print(f"Error sending to Kafka: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Fetch weather data and send to Kafka.')
    parser.add_argument('latitude', type=float, help='Latitude')
    parser.add_argument('longitude', type=float, help='Longitude')
    
    args = parser.parse_args()
    
    weather_data = get_weather(args.latitude, args.longitude)
    
    if weather_data:
        run_producer(weather_data)
    else:
        sys.exit(1)
