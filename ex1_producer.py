from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
import json

# Setup
BOOTSTRAP_SERVERS = ['localhost:9092']
TOPIC_NAME = 'weather_stream'

def create_topic():
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            client_id='admin_client'
        )
        
        topic_list = []
        topic_list.append(NewTopic(name=TOPIC_NAME, num_partitions=1, replication_factor=1))
        
        existing_topics = admin_client.list_topics()
        if TOPIC_NAME not in existing_topics:
            admin_client.create_topics(new_topics=topic_list, validate_only=False)
            print(f"Topic '{TOPIC_NAME}' created successfully.")
        else:
            print(f"Topic '{TOPIC_NAME}' already exists.")
            
    except Exception as e:
        print(f"Error creating topic: {e}")

def run_producer():
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    message = {"msg": "Hello Kafka"}
    future = producer.send(TOPIC_NAME, message)
    result = future.get(timeout=60)
    
    print(f"Message sent to {result.topic} - Partition: {result.partition} - Offset: {result.offset}")
    print(f"Payload: {message}")
    
    producer.flush()
    producer.close()

if __name__ == "__main__":
    create_topic()
    run_producer()
