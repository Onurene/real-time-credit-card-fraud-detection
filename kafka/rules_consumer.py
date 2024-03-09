from confluent_kafka import Consumer, KafkaException
import json
import statistics
from datetime import datetime, timedelta

# Kafka consumer configuration
kafka_config = {
    'bootstrap.servers': 'localhost:9092',  # Replace with your Kafka broker address
    'group.id': 'fraud-detection-consumer',
    'auto.offset.reset': 'earliest',
}

# Kafka topic for card transactions
kafka_topic = 'card_transactions_topic'

# Moving average and standard deviation window size
window_size = 10

# Dictionary to store moving averages and standard deviations
moving_stats = {}

# Function to initialize Kafka consumer
def create_kafka_consumer():
    return Consumer(kafka_config)

# Function to calculate moving average and standard deviation for a given card_id
def calculate_moving_stats(card_id, amount):
    if card_id not in moving_stats:
        moving_stats[card_id] = {'amounts': [], 'mean': 0, 'std_dev': 0}

    amounts = moving_stats[card_id]['amounts']
    amounts.append(amount)

    if len(amounts) > window_size:
        amounts.pop(0)

    mean = statistics.mean(amounts)
    std_dev = statistics.stdev(amounts)

    moving_stats[card_id]['amounts'] = amounts
    moving_stats[card_id]['mean'] = mean
    moving_stats[card_id]['std_dev'] = std_dev

# Function to perform fraud detection logic
def check_fraud(transaction_data):
    card_id = transaction_data.get('card_id')
    amount = transaction_data.get('amount')
    score = transaction_data.get('score', 0)
    transaction_dt_str = transaction_data.get('transaction_dt')
    transaction_dt = datetime.strptime(transaction_dt_str, '%Y-%m-%d %H:%M:%S')

    # Rule 1: Upper Control Limit (UCL)
    if card_id in moving_stats:
        ucl = moving_stats[card_id]['mean'] + 3 * moving_stats[card_id]['std_dev']
        if amount > ucl:
            return True

    # Rule 2: Credit Score
    if score < 200:
        return True

    # Rule 3: Zip Code Distance
    # For illustration purposes, checking if the transaction is from a different city within 1 hour
    if 'city' in transaction_data and 'transaction_city' in moving_stats[card_id]:
        last_transaction_city = moving_stats[card_id]['transaction_city']
        last_transaction_dt = moving_stats[card_id]['transaction_dt']
        time_difference = transaction_dt - last_transaction_dt

        if time_difference < timedelta(hours=1) and transaction_data['city'] != last_transaction_city:
            return True

    # Update moving stats for the next iteration
    calculate_moving_stats(card_id, amount)
    moving_stats[card_id]['transaction_city'] = transaction_data.get('city')
    moving_stats[card_id]['transaction_dt'] = transaction_dt

    return False

# Function to consume card transactions from Kafka topic and perform fraud detection
def consume_and_detect_fraud(consumer):
    consumer.subscribe([kafka_topic])

    try:
        for i in range(10):  # Assuming 10 transactions for simplicity
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaException._PARTITION_EOF:
                    # End of partition event
                    continue
                else:
                    print(f"Error: {msg.error()}")
                    break

            # Process the received message
            key = msg.key().decode('utf-8')
            value = msg.value().decode('utf-8')
            transaction_data = json.loads(value)

            # Perform fraud detection logic
            is_fraudulent = check_fraud(transaction_data)

            # Print the results
            print(f"Transaction - Key: {key}, Value: {value}, Fraudulent: {is_fraudulent}")

    except KeyboardInterrupt:
        pass

    finally:
        # Close the consumer to release resources
        consumer.close()

if __name__ == "__main__":
    consumer = create_kafka_consumer()
    consume_and_detect_fraud(consumer)
