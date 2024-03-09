from confluent_kafka import Consumer, KafkaError

# Kafka consumer configuration
kafka_config = {
    'bootstrap.servers': 'localhost:9092',  # Replace with your Kafka broker address
    'group.id': 'card-transaction-consumer',
    'auto.offset.reset': 'earliest',
}

# Kafka topic for card transactions
kafka_topic = 'card_transactions_topic'

# Function to initialize Kafka consumer
def create_kafka_consumer():
    return Consumer(kafka_config)

# Function to consume card transactions from Kafka topic
def consume_card_transactions_from_kafka(consumer):
    consumer.subscribe([kafka_topic])

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    continue
                else:
                    print(f"Error: {msg.error()}")
                    break

            # Process the received message
            key = msg.key().decode('utf-8')
            value = msg.value().decode('utf-8')
            print(f"Received transaction - Key: {key}, Value: {value}")

    except KeyboardInterrupt:
        pass

    finally:
        # Close the consumer to release resources
        consumer.close()

if __name__ == "__main__":
    # Example: Consume card transactions from Kafka
    consumer = create_kafka_consumer()
    consume_card_transactions_from_kafka(consumer)
