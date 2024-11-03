import json
from kafka import KafkaConsumer

ORDER_CONFIRMED_KAFKA_TPIC = "order_confirmed"


consumer = KafkaConsumer(
    ORDER_CONFIRMED_KAFKA_TPIC,
    bootstrap_servers = "localhost:29092",
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id = "confirmed_order_consumer_group_email",
)

print("Started listening for confirmed orders...")
email_sent_so_far = set()

while True:
    for message in consumer:
        consumed_message = json.loads(message.value.decode())
        user_id = consumed_message["customer_id"]
        user_email = consumed_message["customer_email"]
        total_cost = consumed_message["total_cost"]

        print(f"Sending email to {user_email}...")
        email_sent_so_far.add(user_email)

        print(f"So far email sent to {len(email_sent_so_far)} unique emails.")