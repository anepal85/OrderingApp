import json
from kafka import KafkaConsumer, KafkaProducer


ORDER_KAFKA_TOPIC = "order_details"
ORDER_CONFIRMED_KAFKA_TPIC = "order_confirmed"


consumer = KafkaConsumer(
    ORDER_KAFKA_TOPIC,
    bootstrap_servers = "localhost:29092"
)

order_confirmed_producer = KafkaProducer(
    bootstrap_servers = "localhost:29092"
)


print("Started listening...")

while True:
    for message in consumer:
        print("Ongoing transcation...")

        consumed_message = json.loads(message.value.decode())

        print(consumed_message)