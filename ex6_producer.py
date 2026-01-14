#!/usr/bin/env python3
"""
Exercise 6: Dynamic Kafka Weather Producer
Fetches weather data for a specific city/country using Open-Meteo APIs
and produces enriched messages to Kafka.
"""

import argparse
import json
import logging
import time
from datetime import datetime, timezone
from typing import Optional, Tuple, Dict

import requests
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import KafkaError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
TOPIC_NAME = 'weather_stream'
GEOCODING_API_URL = "https://geocoding-api.open-meteo.com/v1/search"
WEATHER_API_URL = "https://api.open-meteo.com/v1/forecast"
DEFAULT_INTERVAL = 60  # seconds between messages


def create_topic_if_not_exists():
    """Create the Kafka topic if it doesn't exist."""
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            client_id='ex6_admin'
        )
        
        existing_topics = admin_client.list_topics()
        if TOPIC_NAME not in existing_topics:
            topic = NewTopic(name=TOPIC_NAME, num_partitions=3, replication_factor=1)
            admin_client.create_topics(new_topics=[topic], validate_only=False)
            logger.info(f"Topic '{TOPIC_NAME}' created successfully.")
        else:
            logger.info(f"Topic '{TOPIC_NAME}' already exists.")
        
        admin_client.close()
    except Exception as e:
        logger.error(f"Error managing topic: {e}")


def get_coordinates(city: str, country: str) -> Optional[Tuple[float, float, str]]:
    """
    Fetch geographic coordinates for a city using Open-Meteo Geocoding API.
    
    Args:
        city: City name
        country: Country name (used for filtering results)
    
    Returns:
        Tuple of (latitude, longitude, country_name) or None if not found
    """
    try:
        params = {
            'name': city,
            'count': 10,  # Get multiple results to filter by country
            'language': 'en',
            'format': 'json'
        }
        
        logger.info(f"Fetching coordinates for {city}, {country}...")
        response = requests.get(GEOCODING_API_URL, params=params, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        
        if 'results' not in data or not data['results']:
            logger.error(f"City '{city}' not found in geocoding API")
            return None
        
        # Try to find a result matching the country
        for result in data['results']:
            result_country = result.get('country', '')
            if country.lower() in result_country.lower():
                lat = result['latitude']
                lon = result['longitude']
                country_name = result.get('country', country)
                logger.info(f"Found coordinates: lat={lat}, lon={lon}, country={country_name}")
                return lat, lon, country_name
        
        # If no exact match, use the first result
        result = data['results'][0]
        lat = result['latitude']
        lon = result['longitude']
        country_name = result.get('country', country)
        logger.warning(f"No exact country match, using first result: {country_name}")
        return lat, lon, country_name
        
    except requests.exceptions.Timeout:
        logger.error("Geocoding API request timed out")
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching coordinates: {e}")
        return None
    except (KeyError, ValueError) as e:
        logger.error(f"Error parsing geocoding response: {e}")
        return None


def get_weather(latitude: float, longitude: float) -> Optional[Dict]:
    """
    Fetch current weather data from Open-Meteo API.
    
    Args:
        latitude: Latitude coordinate
        longitude: Longitude coordinate
    
    Returns:
        Dictionary with weather data or None if error
    """
    try:
        params = {
            'latitude': latitude,
            'longitude': longitude,
            'current_weather': 'true'
        }
        
        logger.info(f"Fetching weather for coordinates ({latitude}, {longitude})...")
        response = requests.get(WEATHER_API_URL, params=params, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        
        if 'current_weather' not in data:
            logger.error("No current weather data in API response")
            return None
        
        current = data['current_weather']
        weather_data = {
            'temperature': current.get('temperature'),
            'windspeed': current.get('windspeed'),
            'weather_code': current.get('weathercode')
        }
        
        logger.info(f"Weather data: temp={weather_data['temperature']}°C, "
                   f"wind={weather_data['windspeed']}m/s")
        return weather_data
        
    except requests.exceptions.Timeout:
        logger.error("Weather API request timed out")
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching weather: {e}")
        return None
    except (KeyError, ValueError) as e:
        logger.error(f"Error parsing weather response: {e}")
        return None


def create_message(city: str, country: str, latitude: float, longitude: float, 
                   weather_data: Dict) -> Dict:
    """
    Create a Kafka message with all required fields.
    
    Args:
        city: City name
        country: Country name
        latitude: Latitude coordinate
        longitude: Longitude coordinate
        weather_data: Weather data dictionary
    
    Returns:
        Complete message dictionary
    """
    message = {
        'event_time': datetime.now(timezone.utc).isoformat(),
        'city': city,
        'country': country,
        'latitude': latitude,
        'longitude': longitude,
        'temperature': weather_data['temperature'],
        'windspeed': weather_data['windspeed'],
        'weather_code': weather_data['weather_code']
    }
    return message


def run_producer(city: str, country: str, interval: int = DEFAULT_INTERVAL):
    """
    Main producer loop: fetch weather and send to Kafka continuously.
    
    Args:
        city: City name
        country: Country name
        interval: Seconds between messages
    """
    # Get coordinates once at startup
    coords = get_coordinates(city, country)
    if coords is None:
        logger.error("Failed to get coordinates. Exiting.")
        return
    
    latitude, longitude, resolved_country = coords
    
    # Use resolved country name from geocoding API
    country = resolved_country
    
    # Create Kafka producer
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',
            retries=3
        )
        logger.info("Kafka producer initialized successfully")
    except KafkaError as e:
        logger.error(f"Failed to create Kafka producer: {e}")
        return
    
    logger.info(f"Starting weather producer for {city}, {country}")
    logger.info(f"Coordinates: ({latitude}, {longitude})")
    logger.info(f"Publishing interval: {interval}s")
    
    message_count = 0
    
    try:
        while True:
            # Fetch current weather
            weather_data = get_weather(latitude, longitude)
            
            if weather_data is None:
                logger.warning("Failed to fetch weather, will retry...")
                time.sleep(interval)
                continue
            
            # Create message
            message = create_message(city, country, latitude, longitude, weather_data)
            
            # Send to Kafka
            try:
                future = producer.send(TOPIC_NAME, value=message)
                record_metadata = future.get(timeout=10)
                message_count += 1
                
                logger.info(f"Message #{message_count} sent successfully")
                logger.info(f"  Topic: {record_metadata.topic}")
                logger.info(f"  Partition: {record_metadata.partition}")
                logger.info(f"  Offset: {record_metadata.offset}")
                logger.debug(f"  Payload: {json.dumps(message, indent=2)}")
                
            except KafkaError as e:
                logger.error(f"Failed to send message to Kafka: {e}")
            
            # Wait before next iteration
            time.sleep(interval)
            
    except KeyboardInterrupt:
        logger.info("\nShutting down producer...")
    finally:
        producer.flush()
        producer.close()
        logger.info(f"Producer stopped. Total messages sent: {message_count}")


def main():
    """Parse arguments and start the producer."""
    parser = argparse.ArgumentParser(
        description='Kafka Weather Producer with dynamic city/country selection'
    )
    parser.add_argument(
        '--city',
        type=str,
        required=True,
        help='City name (e.g., "Paris", "London")'
    )
    parser.add_argument(
        '--country',
        type=str,
        required=True,
        help='Country name (e.g., "France", "United Kingdom")'
    )
    parser.add_argument(
        '--interval',
        type=int,
        default=DEFAULT_INTERVAL,
        help=f'Interval between messages in seconds (default: {DEFAULT_INTERVAL})'
    )
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Enable debug logging'
    )
    
    args = parser.parse_args()
    
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Validate inputs
    if not args.city.strip():
        logger.error("City name cannot be empty")
        return
    
    if not args.country.strip():
        logger.error("Country name cannot be empty")
        return
    
    if args.interval < 1:
        logger.error("Interval must be at least 1 second")
        return
    
    # Create topic if needed
    create_topic_if_not_exists()
    
    # Run producer
    run_producer(args.city, args.country, args.interval)


if __name__ == "__main__":
    main()
