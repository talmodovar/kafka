from kafka import KafkaConsumer
import json
import sys

import argparse

# Setup
BOOTSTRAP_SERVERS = ['localhost:9092']
DEFAULT_TOPIC = 'weather_stream'

def run_consumer(topic_name):
    try:
        consumer = KafkaConsumer(
            topic_name,
            bootstrap_servers=BOOTSTRAP_SERVERS,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='weather_group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        
        print(f"Listening for messages on topic '{topic_name}'...")
        
        for message in consumer:
            print(f"Received message: {message.value}")
            print(f"Partition: {message.partition}, Offset: {message.offset}")
            
    except KeyboardInterrupt:
        print("\nStopping consumer...")
    except Exception as e:
        print(f"Error in consumer: {e}")
    finally:
         if 'consumer' in locals():
            consumer.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('topic', nargs='?', default=DEFAULT_TOPIC, help='Kafka topic to consume from')
    args = parser.parse_args()
    
    run_consumer(args.topic)
