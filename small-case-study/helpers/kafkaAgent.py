import threading
import requests
from kafka import KafkaConsumer, KafkaProducer
from json import dumps
class KafkaAgent:

    def __init__(self):
        self.consumer = None
        self.producer = None
        self.consumer_consume = False
        self.producer_produce = False

    def _create_kafka_producer(self,bootstrap_servers='localhost:9092', serializer=lambda x: dumps(x).encode('utf-8')):
        try:
            self.producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                                     value_serializer=serializer)

        except Exception as err:
            print("KafkaAgent: Failed to create Kafka producer:", str(err))
            self.producer = None

    def _create_kafka_consumer(self, bootstrap_servers='localhost:9092',topic=None, group_id=None, offset_reset="earliest"):
        # To consume latest messages and auto-commit offsets
        try:
            self.consumer = KafkaConsumer(topic,
                                     group_id=group_id,
                                     auto_offset_reset=offset_reset,
                                     bootstrap_servers=[bootstrap_servers])

        except Exception as err:
            print("KafkaAgent: Failed to create Kafka consumer - ", str(err))
            self.consumer = None

    def perceive_environment(self):
        # Implement perception logic here
        print("environment")
    def update_state(self):
        # Update agent's state based on perception
        print("state")

    def perform_action(self):
        # Implement action logic here
        print("action")

    def _consumer_loop_start(self):
        while self.consumer_consume:
            msg = next(self.consumer)
            response = requests.get('https://192.168.0.137:8000/process', json=msg)
            print(response)
            #todo caso aconteça alguma coisa com o cluster ou na rede é melhor parar o loop.

    def start(self,topic):
        self._create_kafka_consumer(topic=topic)
        self.consumer_consume= True
        threading.Thread(target=self._consumer_loop_start,daemon=True).start()

