
import json
from kafka import KafkaConsumer

ORDER_CONFIRMED_KAFKA_TPIC = "order_confirmed"


consumer = KafkaConsumer(
    ORDER_CONFIRMED_KAFKA_TPIC,
    bootstrap_servers = "localhost:29092",
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id = "confirmed_order_consumer_group_analytics",
)

print("Analytics listening for confirmed orders...")

total_orders_count = 0 
total_revenue = 0 


while True:
    for message in consumer:
        print("Updating Analytics...")
        consumed_message = json.loads(message.value.decode())

        total_cost = float(consumed_message["total_cost"])

        total_orders_count+=1
        total_revenue+=total_cost

        print(f"Total Revenue {total_revenue}...")
        print(f"Total Orders {total_orders_count}...")