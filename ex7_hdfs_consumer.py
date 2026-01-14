#!/usr/bin/env python3
"""
Exercise 7: Kafka to HDFS Consumer for Weather Alerts
Consumes weather alerts from Kafka and persists them to HDFS in a hierarchical structure.
"""

import json
import logging
import signal
import sys
from pathlib import Path
from typing import Dict, Optional

from kafka import KafkaConsumer
from kafka.errors import KafkaError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
TOPIC_NAME = 'weather_transformed'
CONSUMER_GROUP = 'hdfs-alert-consumer'
HDFS_BASE_PATH = './hdfs-data'  # Local simulation of HDFS
# For real HDFS, use: HDFS_BASE_PATH = '/hdfs-data'

# Global flag for graceful shutdown
shutdown_flag = False


def signal_handler(signum, frame):
    """Handle shutdown signals gracefully."""
    global shutdown_flag
    logger.info(f"Received signal {signum}, initiating graceful shutdown...")
    shutdown_flag = True


def is_alert_message(message: Dict) -> bool:
    """
    Check if a message contains an alert.
    
    Args:
        message: Parsed JSON message
    
    Returns:
        True if message contains wind or heat alert (level >= 1)
    """
    wind_alert = message.get('wind_alert_level', 0)
    heat_alert = message.get('heat_alert_level', 0)
    
    # Convert string levels to int if needed
    try:
        if isinstance(wind_alert, str):
            # Handle "level_1", "level_2" format
            if wind_alert.startswith('level_'):
                wind_alert = int(wind_alert.split('_')[1])
            else:
                wind_alert = int(wind_alert)
        
        if isinstance(heat_alert, str):
            if heat_alert.startswith('level_'):
                heat_alert = int(heat_alert.split('_')[1])
            else:
                heat_alert = int(heat_alert)
    except (ValueError, IndexError):
        logger.warning(f"Could not parse alert levels: wind={wind_alert}, heat={heat_alert}")
        return False
    
    return wind_alert >= 1 or heat_alert >= 1


def get_hdfs_path(country: str, city: str) -> Path:
    """
    Construct HDFS path based on country and city.
    
    Args:
        country: Country name
        city: City name
    
    Returns:
        Path object for the alerts file
    """
    # Sanitize country and city names (remove special characters, spaces)
    country_clean = country.replace(' ', '_').replace('/', '_')
    city_clean = city.replace(' ', '_').replace('/', '_')
    
    # Construct path: /hdfs-data/{country}/{city}/alerts.json
    path = Path(HDFS_BASE_PATH) / country_clean / city_clean / 'alerts.json'
    return path


def write_to_hdfs(message: Dict, hdfs_path: Path) -> bool:
    """
    Write message to HDFS (or local filesystem as simulation).
    
    Args:
        message: Message to write
        hdfs_path: Target path
    
    Returns:
        True if write successful, False otherwise
    """
    try:
        # Create parent directories if they don't exist
        hdfs_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Append message as JSON line
        with open(hdfs_path, 'a', encoding='utf-8') as f:
            json.dump(message, f, ensure_ascii=False)
            f.write('\n')
        
        logger.debug(f"Successfully wrote message to {hdfs_path}")
        return True
        
    except IOError as e:
        logger.error(f"Failed to write to {hdfs_path}: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error writing to HDFS: {e}")
        return False


def process_message(message_value: bytes) -> Optional[Dict]:
    """
    Parse and validate a Kafka message.
    
    Args:
        message_value: Raw message bytes
    
    Returns:
        Parsed message dict or None if invalid
    """
    try:
        message = json.loads(message_value.decode('utf-8'))
        
        # Validate required fields
        required_fields = ['city', 'country']
        for field in required_fields:
            if field not in message:
                logger.warning(f"Message missing required field '{field}': {message}")
                return None
        
        return message
        
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse JSON: {e}")
        return None
    except UnicodeDecodeError as e:
        logger.error(f"Failed to decode message: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error processing message: {e}")
        return None


def run_consumer():
    """
    Main consumer loop: read from Kafka, filter alerts, write to HDFS.
    """
    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Create Kafka consumer
    try:
        consumer = KafkaConsumer(
            TOPIC_NAME,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=CONSUMER_GROUP,
            enable_auto_commit=False,  # Manual commit after HDFS write
            auto_offset_reset='earliest',  # Start from beginning if no offset
            value_deserializer=lambda m: m  # Keep as bytes, we'll parse manually
        )
        logger.info(f"Kafka consumer initialized successfully")
        logger.info(f"Subscribed to topic: {TOPIC_NAME}")
        logger.info(f"Consumer group: {CONSUMER_GROUP}")
        logger.info(f"HDFS base path: {HDFS_BASE_PATH}")
        
    except KafkaError as e:
        logger.error(f"Failed to create Kafka consumer: {e}")
        return
    
    # Statistics
    messages_processed = 0
    alerts_saved = 0
    errors = 0
    
    try:
        logger.info("Starting consumer loop...")
        
        for message in consumer:
            # Check shutdown flag
            if shutdown_flag:
                logger.info("Shutdown flag detected, breaking consumer loop")
                break
            
            messages_processed += 1
            
            # Parse message
            parsed_message = process_message(message.value)
            if parsed_message is None:
                errors += 1
                continue
            
            # Check if it's an alert
            if not is_alert_message(parsed_message):
                logger.debug(f"Message is not an alert, skipping")
                # Commit offset even for non-alerts
                consumer.commit()
                continue
            
            # Extract location info
            country = parsed_message.get('country', 'Unknown')
            city = parsed_message.get('city', 'Unknown')
            
            logger.info(f"Alert detected for {city}, {country}")
            
            # Construct HDFS path
            hdfs_path = get_hdfs_path(country, city)
            
            # Write to HDFS
            if write_to_hdfs(parsed_message, hdfs_path):
                alerts_saved += 1
                logger.info(f"Alert saved to {hdfs_path}")
                
                # Commit offset only after successful write
                consumer.commit()
                logger.debug(f"Offset committed for partition {message.partition}, offset {message.offset}")
            else:
                errors += 1
                logger.error(f"Failed to save alert, offset NOT committed")
                # Don't commit offset - message will be reprocessed on restart
            
            # Log progress every 100 messages
            if messages_processed % 100 == 0:
                logger.info(f"Progress: {messages_processed} processed, {alerts_saved} saved, {errors} errors")
    
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")
    except Exception as e:
        logger.error(f"Unexpected error in consumer loop: {e}", exc_info=True)
    finally:
        # Close consumer gracefully
        logger.info("Closing consumer...")
        consumer.close()
        
        # Final statistics
        logger.info("=" * 60)
        logger.info("Consumer Statistics:")
        logger.info(f"  Messages processed: {messages_processed}")
        logger.info(f"  Alerts saved: {alerts_saved}")
        logger.info(f"  Errors: {errors}")
        logger.info("=" * 60)


def main():
    """Entry point."""
    logger.info("Starting HDFS Alert Consumer (Exercise 7)")
    logger.info("Press Ctrl+C to stop")
    
    # Create base HDFS directory if it doesn't exist
    Path(HDFS_BASE_PATH).mkdir(parents=True, exist_ok=True)
    
    # Run consumer
    run_consumer()
    
    logger.info("Consumer stopped")


if __name__ == "__main__":
    main()
