from kafka import KafkaConsumer
import json
import sys

# Setup
BOOTSTRAP_SERVERS = ['localhost:9092']
TOPIC_NAME = 'weather_stream'

def run_consumer():
    try:
        consumer = KafkaConsumer(
            TOPIC_NAME,
            bootstrap_servers=BOOTSTRAP_SERVERS,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='weather_group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        
        print(f"Listening for messages on topic '{TOPIC_NAME}'...")
        
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
    run_consumer()
