import json
from kafka import KafkaConsumer, KafkaProducer


ORDER_KAFKA_TOPIC = "order_details"
ORDER_CONFIRMED_KAFKA_TPIC = "order_confirmed"


consumer = KafkaConsumer(
    ORDER_KAFKA_TOPIC,
    bootstrap_servers = "localhost:29092",
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id = "order_consumer_group_v1",
)

order_confirmed_producer = KafkaProducer(
    bootstrap_servers = "localhost:29092"
)


print("Transaction listening to order details...")

while True:
    for message in consumer:
        print("Ongoing transcation...")

        consumed_message = json.loads(message.value.decode())

        user_id = consumed_message["user_id"]
        total_cost = consumed_message["total_cost"]

        order_confirmed_data = {
            "customer_id" : user_id, 
            "customer_email": f"{user_id}@gmail.com",
            "total_cost": total_cost
        }

        print(f"Succesful transaction for {user_id}...")

        order_confirmed_producer.send(
            ORDER_CONFIRMED_KAFKA_TPIC,
            json.dumps(order_confirmed_data).encode("utf-8")
        )
