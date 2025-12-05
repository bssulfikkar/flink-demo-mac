#!/usr/bin/env python3

import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

# Kafka configuration
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Sample data for realistic transactions
users = [f"user_{i:04d}" for i in range(1, 101)]
merchants = [
    "Amazon", "Walmart", "Target", "Best Buy", "Apple Store",
    "Starbucks", "McDonald's", "Subway", "Chipotle", "Panera",
    "Uber", "Lyft", "Netflix", "Spotify", "Hulu",
    "Shell Gas", "Chevron", "Whole Foods", "Trader Joe's", "Costco",
    "Home Depot", "Lowe's", "CVS", "Walgreens", "Rite Aid"
]
categories = [
    "Shopping", "Food & Dining", "Entertainment", "Transportation",
    "Groceries", "Healthcare", "Gas & Fuel", "Subscriptions"
]

def generate_transaction():
    """Generate a random transaction"""
    transaction_id = f"txn_{int(time.time() * 1000)}_{random.randint(1000, 9999)}"
    user_id = random.choice(users)
    merchant = random.choice(merchants)
    category = random.choice(categories)

    # Generate amount with occasional high-value transactions (for fraud detection)
    if random.random() < 0.05:  # 5% chance of high-value transaction
        amount = round(random.uniform(1000, 5000), 2)
    else:
        amount = round(random.uniform(5, 500), 2)

    timestamp = int(time.time() * 1000)

    return {
        "transaction_id": transaction_id,
        "user_id": user_id,
        "amount": amount,
        "merchant": merchant,
        "timestamp": timestamp,
        "category": category
    }

def main():
    print("=" * 60)
    print("Transaction Generator Started")
    print("=" * 60)
    print("Sending transactions to Kafka topic: transactions")
    print("Press Ctrl+C to stop")
    print("=" * 60)
    print()

    transaction_count = 0

    try:
        while True:
            # Generate 1-5 transactions per second
            num_transactions = random.randint(1, 5)

            for _ in range(num_transactions):
                transaction = generate_transaction()
                producer.send('transactions', value=transaction)
                transaction_count += 1

                # Print transaction info
                print(f"[{transaction_count:6d}] "
                      f"User: {transaction['user_id']:10s} | "
                      f"Amount: ${transaction['amount']:7.2f} | "
                      f"Category: {transaction['category']:20s} | "
                      f"Merchant: {transaction['merchant']}")

            # Flush to ensure messages are sent
            producer.flush()

            # Wait before sending next batch
            time.sleep(1)

    except KeyboardInterrupt:
        print()
        print("=" * 60)
        print(f"Generator stopped. Total transactions sent: {transaction_count}")
        print("=" * 60)
        producer.close()

if __name__ == "__main__":
    main()

