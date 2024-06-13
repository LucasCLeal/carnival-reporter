from confluent_kafka import Producer
import json

class KafkaLogger:
    def __init__(self, bootstrap_servers='localhost:9092'):
        self.producer_config = {
            'bootstrap.servers': bootstrap_servers
        }
        self.producer = Producer(self.producer_config)

    def _send_message(self, topic, message):
        try:
            self.producer.produce(topic, message)
            self.producer.flush()
        except Exception as e:
            print(f"Failed to send message to {topic}: {e}")

    def log_producer(self, topic:str, producer:str, action:str):
        if not topic or not producer or not action:
            raise ValueError("All parameters (topic, producer, action) must be provided")
        message = {"topic":topic,"producer":producer,"action":action}

        self._send_message(topic = 'ProducerLog', message  =json.dumps(message))

    def log_consumer(self, topic, consumer, action):

        if not topic or not consumer or not action:
            raise ValueError("All parameters (topic, producer, action) must be provided")
        message = {"topic":topic,"consumer":consumer,"action":action}
        self._send_message(topic = 'ConsumerLog', message = json.dumps(message))