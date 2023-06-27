from kafka import KafkaConsumer

class KafkaHelper:
    def __init__(self):
        self._secret = "This is a secret."
        self.consumer = None
        self.producer = None

    def create_producer(self):
        print("This is a public function.")
        self._private_function()
    def _private_function(self):
        print("This is a private function.")
        print("Accessing the secret:", self._secret)

    def create_consumer(self, bootstrap_servers='localhost:9092',topic=None, group_id=None, offset_reset="earliest"):
        # To consume latest messages and auto-commit offsets
        try:
            self.consumer = KafkaConsumer(topic,
                                     group_id=group_id,
                                     auto_offset_reset=offset_reset,
                                     bootstrap_servers=[bootstrap_servers],
                                     consumer_timeout_ms=1000)
            print("beginning_offset: ", self.consumer.beginning_offsets(self.consumer.assignment()))
        except Exception as err:
            print(err)
            self.consumer = None
            raise Exception("Error creating consumer")
    def get_consumer_next_message(self):
        return next(self.consumer)