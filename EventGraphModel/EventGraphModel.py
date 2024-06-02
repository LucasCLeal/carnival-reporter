'''
Copyright (c) 2024 Unicamp

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE
OR OTHER DEALINGS IN THE SOFTWARE.

'''
from GraphWalkerModel import Model

class EventGraphModel(Model):

    '''
        This class converts the data from the kafka cluster
        and stores the information required to generate graphwalker modes.
        It also holds important information not represented in the GW model that
        can be use to improve the test case generation or system validatation
    '''

    def __init__(self, generator: str, name: str, kafka_config: Dict[str, Any]):
        super().__init__(generator, name)
        self.kafka_config = kafka_config
        self.topic_info = {}

    def fetch_kafka_data(self):
        consumer = Consumer(self.kafka_config)
        try:
            topics = consumer.list_topics().topics
            for topic in topics:
                if topic not in self.topic_info:
                    self.topic_info[topic] = self.get_topic_event_count(consumer, topic)
        finally:
            consumer.close()

    def get_topic_event_count(self, consumer: Consumer, topic: str) -> int:
        consumer.subscribe([topic])
        count = 0
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                break
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    break
            count += 1
        return count

    def create_edge_from_topic(self, topic: str, source_vertex: Vertex, target_vertex: Optional[Vertex]):
        event_count = self.topic_info.get(topic, 0)
        edge_name = f"{topic} (events: {event_count})"
        self.create_edge(name=edge_name, source_vertex=source_vertex, target_vertex=target_vertex)

    def to_dict(self):
        base_dict = super().to_dict()
        base_dict["topicInfo"] = self.topic_info
        return base_dict

    def to_json(self):
        return json.dumps(self.to_dict(), indent=4)