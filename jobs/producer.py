import random
import logging
import time as T
import json

from faker import Faker
from datetime import *
from typing import * 
from confluent_kafka import Producer
from admin import KafkaAdmin

from jobs.data.product_mapping import product_category_map,category_brand_map

class ProducerClass:

    def __init__(self,bootstrap_server:str,topic:str) -> None:

        self.logger = logging.getLogger(__class__.__name__)

        self.bootstrap_server = bootstrap_server
        self.topic = topic
        self.producer = Producer({'bootstrap.servers' : self.bootstrap_server})

        self.fake = Faker(locale='en_IN')

    def generate_product(self):

        category = random.choice(list(product_category_map.keys())) 
        product_name = random.choice(product_category_map[category])  
        brand = random.choice(category_brand_map[category])  
        product_id = random.choice([22334, 45454, 65653, 56546, 21233, 54665, 79898, 12545, 87985, 45878, 32325])
        unit_price = round(random.uniform(100, 5000), 2) 
        discount = round(random.uniform(0, 50), 2)

        return {
            "ProductID": product_id,
            "ProductName": product_name,
            "Category": category,
            "Brand": brand,
            "UnitPrice": unit_price,
            "Discount": discount
        }

    def generate_orders(self) -> dict:
        user = self.fake.simple_profile()

        return {
                    "OrderId": self.fake.uuid4(),
                    "OrderDate": f"{datetime.today().strftime('%Y-%m-%d')}",
                    "CustomerID": random.choice([
                        "user_001", "user_002", "user_003", "user_004", "user_005", 
                        "user_006", "user_007", "user_008", "user_009", "user_010"
                    ]),
                    "CustomerFirstName" : self.fake.first_name(),
                    "CustomerLastName" : self.fake.last_name(),
                    "CustomerPhone" : self.fake.phone_number(),
                    "CustomerEmail" : self.fake.email(),
                    "Location": {
                        "City": random.choice([
                            "Bengaluru", "Mumbai", "Delhi", "Chennai", "Kolkata", "Hyderabad",
                            "Ahmedabad", "Jaipur", "Lucknow", "Pune", "Surat", "Indore","Kochi","Mumbai"
                        ]),
                        "Pincode": random.choice([
                            560001, 400001, 110001, 600001, 700001, 
                            500001, 380001, 302001, 226001, 411001
                        ]),
                        "Region": random.choice(["North", "South", "East", "West", "Central"])
                    },
                    "Product": self.generate_product(),
                    "TotalUnits": round(random.uniform(1, 50), 0),
                    "POS": random.choice(['DMART', 'RELIANCE MART', 'INSTA MART', 'BIG BASKET', 'AMAZON', 'FLIPKART', 'MORE']),
                    "OrderStatus": random.choice(['PENDING', 'SHIPPED', 'IN-PROGRESS', 'CANCELLED', 'DELIVERED', 'TRANSACTION FAILED']),
                    "TotalAmount": round(random.uniform(10, 50000), 2),
                    "Payment": {
                        "PaymentMethod": random.choice(['UPI', 'CREDIT CARD', 'DEBIT CARD', 'NET BANKING', 'CASH', 'WALLET']),
                        "PaymentStatus": random.choice(["SUCCESS", "FAILED", "PENDING"]),
                        "TransactionID": self.fake.uuid4()
                    },
                    "ShippingDetails": {
                        "ShippingAddress": {
                            "Shipping_Street": self.fake.address(),
                            "Shipping_City": self.fake.city_name(),
                            "Shipping_State": self.fake.state(),
                            "Shipping_ZipCode": self.fake.pincode_in_state(),
                            "Shipping_Country": "India"
                        },
                        "ShippingMethod": random.choice(["Standard", "Express", "Same Day", "Pickup"]),
                        "EstimatedDelivery": f"{(datetime.today() + timedelta(days=random.randint(1, 10))).strftime('%Y-%m-%d')}"
                    },
                    "Timestamp": f"{datetime.now()}"
                }

    def send_message(self,message):
        try:
            self.producer.produce(self.topic,message)

            self.logger.info("Message sent..")
        except Exception as err:
            print(err)

    def commit(self):
        self.producer.flush()

if __name__  == '__main__':
    
    bootstrap_server = 'localhost:29092'
    
    topic = "orders_topic"

    a = KafkaAdmin(bootstrap_server)

    a.create_topic(topic,3,3)

    p = ProducerClass(bootstrap_server,topic)

    try:
        while True:
                
                msg = json.dumps(p.generate_orders())

                print(msg)

                p.send_message(msg)

                T.sleep(5)

    except KeyboardInterrupt:
        ...

    p.commit()
