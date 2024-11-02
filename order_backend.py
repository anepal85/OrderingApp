import json 
import time
import random 
from kafka import KafkaProducer 


ORDER_KAFKA_TOPIC = "order_details"

ORDER_LIMIT = 20 

prdoucer = KafkaProducer(
    bootstrap_servers = "localhost:29092"
)

print("Generates Unique Order after 10 seconds !!")

FOOD_LIST = ["burger", "sandwich", "korean_bbq", "hot_dog", "chicken_korma", "ramen", "doener-kebab"]

for i in range(1, ORDER_LIMIT):

    data = {
        "order_id": i,
        "user_id" : f"arjun_{i}", 
        "total_cost": i*2,
        "items": FOOD_LIST[random.randint(0, len(FOOD_LIST)-1)]
    }

    prdoucer.send(
        ORDER_KAFKA_TOPIC, json.dumps(data).encode("utf-8")
        )
    
    print(f"done sending...{i}")

    time.sleep(2)