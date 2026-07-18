import json
import random
import time

from confluent_kafka import Producer
from faker import Faker


CHECKOUT_TOPIC = "ecommerce_checkout_events"
PAYMENT_TOPIC = "ecommerce_payment_events"
NUMBER_OF_ORDERS = 50
WAIT_BETWEEN_EVENTS_SECONDS = 1
KAFKA_CONFIG_FILE = "client.properties"


# A small product catalogue makes prices and categories commercially sensible.
PRODUCTS = [
    {"id": "prd_101", "name": "Classic Cotton Shirt", "category": "Apparel", "price": 899.0},
    {"id": "prd_102", "name": "Slim Fit Jeans", "category": "Apparel", "price": 1599.0},
    {"id": "prd_201", "name": "5G Smartphone", "category": "Electronics", "price": 24999.0},
    {"id": "prd_202", "name": "Wireless Headphones", "category": "Electronics", "price": 1999.0},
    {"id": "prd_203", "name": "Fitness Smartwatch", "category": "Electronics", "price": 6999.0},
    {"id": "prd_301", "name": "Running Shoes", "category": "Footwear", "price": 3499.0},
    {"id": "prd_401", "name": "Insulated Water Bottle", "category": "Home", "price": 499.0},
    {"id": "prd_402", "name": "Drip Coffee Maker", "category": "Home", "price": 4499.0},
    {"id": "prd_501", "name": "Premium Yoga Mat", "category": "Fitness", "price": 1499.0},
    {"id": "prd_601", "name": "SPF 50 Sunscreen", "category": "Beauty", "price": 599.0},
]

INDIAN_CITIES = [
    "Bengaluru",
    "Mumbai",
    "Delhi",
    "Hyderabad",
    "Chennai",
    "Pune",
    "Kolkata",
    "Jaipur",
    "Ahmedabad",
    "Kochi",
]

fake = Faker("en_IN")


def read_kafka_config():
    """Read Confluent Cloud connection properties from client.properties."""
    config = {}

    with open(KAFKA_CONFIG_FILE, "r", encoding="utf-8") as file:
        for line in file:
            line = line.strip()

            if line and not line.startswith("#"):
                key, value = line.split("=", 1)
                config[key.strip()] = value.strip()

    return config


def delivery_report(error, message):
    if error:
        print(f"Kafka delivery failed: {error}")


def publish(producer, topic, event):
    """Publish a plain JSON value without supplying a Kafka key."""
    producer.produce(
        topic,
        value=json.dumps(event).encode("utf-8"),
        callback=delivery_report,
    )
    producer.poll(0)

    print(
        f"Published to {topic:<28} "
        f"order_id={event['order_id']}"
    )


def create_checkout_event(order_number, event_timestamp):
    product = random.choice(PRODUCTS)
    quantity = random.randint(1, 3)

    # Customer tiers drive genuine business discount rules.
    customer_tier = random.choices(
        ["STANDARD", "SILVER", "GOLD"],
        weights=[65, 25, 10],
        k=1,
    )[0]

    discount_rate = {
        "STANDARD": 0.00,
        "SILVER": 0.05,
        "GOLD": 0.10,
    }[customer_tier]

    subtotal = product["price"] * quantity
    discount_amount = round(subtotal * discount_rate, 2)

    checkout_event = {
        "event_id": f"chk_evt_{fake.uuid4()}",
        "event_ts": event_timestamp,
        "order_id": f"ord_{int(time.time())}_{order_number:04d}",
        "customer_id": f"cust_{fake.random_number(digits=8, fix_len=True)}",
        "customer_name": fake.name(),
        "customer_email": fake.email(),
        "customer_tier": customer_tier,
        "product_id": product["id"],
        "product_name": product["name"],
        "category": product["category"],
        "quantity": quantity,
        "unit_price": product["price"],
        "discount_amount": discount_amount,
        "currency": "INR",
        "shipping_city": random.choice(INDIAN_CITIES),
        "sales_channel": random.choices(
            ["MOBILE_APP", "WEB"], weights=[65, 35], k=1
        )[0],
    }

    expected_amount = round(subtotal - discount_amount, 2)
    return checkout_event, expected_amount


def create_payment_event(checkout_event, expected_amount):
    payment_status = random.choices(
        ["AUTHORIZED", "FAILED"],
        weights=[90, 10],
        k=1,
    )[0]

    fraud_score = round(random.uniform(0.02, 0.40), 3)
    failure_reason = None

    if payment_status == "FAILED":
        failure_reason = random.choice(
            ["BANK_DECLINED", "INSUFFICIENT_FUNDS", "OTP_TIMEOUT"]
        )

    # Payments complete within 30 seconds to 3 event-time minutes.
    payment_timestamp = checkout_event["event_ts"] + random.randint(30, 180) * 1000

    return {
        "event_id": f"pay_evt_{fake.uuid4()}",
        "event_ts": payment_timestamp,
        "order_id": checkout_event["order_id"],
        "payment_id": f"pay_{fake.random_number(digits=10, fix_len=True)}",
        "payment_method": random.choices(
            ["UPI", "CARD", "WALLET", "NET_BANKING"],
            weights=[50, 30, 12, 8],
            k=1,
        )[0],
        "payment_status": payment_status,
        "amount": expected_amount,
        "currency": "INR",
        "gateway": random.choice(["Razorpay", "Stripe", "Paytm"]),
        "fraud_score": fraud_score,
        "failure_reason": failure_reason,
    }


def main():
    producer = Producer(read_kafka_config())
    event_time = int(time.time() * 1000) #epoch or milliseconds

    print("Starting checkout and payment producer...")
    print("Press Ctrl+C to stop.\n")

    try:
        for order_number in range(1, NUMBER_OF_ORDERS + 1):
            checkout_event, expected_amount = create_checkout_event(
                order_number, event_time
            )
            publish(producer, CHECKOUT_TOPIC, checkout_event)

            time.sleep(WAIT_BETWEEN_EVENTS_SECONDS)

            payment_event = create_payment_event(
                checkout_event, expected_amount
            )
            publish(producer, PAYMENT_TOPIC, payment_event)

            # Move the simulated business time forward for the next order.
            event_time = payment_event["event_ts"] + 30_000

            print()
            time.sleep(WAIT_BETWEEN_EVENTS_SECONDS)

    except KeyboardInterrupt:
        print("\nProducer stopped by user.")

    finally:
        producer.flush()
        print("Producer finished.")


if __name__ == "__main__":
    main()
