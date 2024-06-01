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
import json

from confluent_kafka import Consumer, KafkaException, KafkaError
from fastapi import BackgroundTasks, FastAPI
from dotenv import load_dotenv
import os

'''
The goal of this service is to monitor periodicaly the information in tha kafka topics in the cluster
use this information to Generate an EventGraphModel.
It must store the models in a topic to keep a track of if evolution.
It must notify the the service responsible for the test case generation
'''

app = FastAPI()
load_dotenv()  # This method will load environment variables from .env file
topic_sub_list = []

# Setup kafka consumer loop
def get_kafka_consumer():
    # Consumer configuration
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    conf = {
        'bootstrap.servers': os.getenv('BOOTSTRAP_SERVER'),  # Change this to your Kafka server configuration
        'group.id': 'full_cluster_monitor',
        'auto.offset.reset': 'earliest'
    }
    return Consumer(conf)


def fetch_and_subscribe_to_topics(consumer: Consumer, subscriptions: list | None) -> list:
    """ Fetches the current list of topics from the Kafka cluster. """
    cluster_metadata = consumer.list_topics(timeout=10)  # timeout in seconds
    cluster_topics = list(cluster_metadata.topics.keys())
    sub_list = []
    if subscriptions:
        sub_list = [topic for topic in cluster_topics if topic not in subscriptions]
        consumer.subscribe(sub_list)
    else:
        consumer.subscribe(cluster_topics)
    return sub_list if subscriptions else cluster_topics


def consume_messages(consumer: Consumer, topic_sub: list):
    """ Continuously consumes messages from all subscribed topics. """
    try:
        while True:
            msg = consumer.poll(timeout=1.0)  # Adjust the timeout as needed
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    print(f'{msg.topic()} [{msg.partition()}] reached end at offset {msg.offset()}')
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                print(f'Received message: {msg.value().decode("utf-8")} from topic {msg.topic()}')

            # update consumer topics
            fetch_and_subscribe_to_topics(consumer,topic_sub)

    except KeyboardInterrupt:
        print("Stopping consumer...")
    finally:
        consumer.close()


def start_monitor_loop():
    consumer = get_kafka_consumer()
    try:
        subscriptions = fetch_and_subscribe_to_topics(consumer,None)
        consume_messages(consumer,subscriptions)
    finally:
        consumer.close()


@app.on_event("startup")
async def startup_event():
    BackgroundTasks.add_task(start_monitor_loop())
    print('cluster_monitor started')


@app.get("/")
def read_root():
    return {"Hello": "World"}

@app.get("/getTopicsSubscription")
def read_root():
    consumer = get_kafka_consumer()
    cluster_metadata = consumer.list_topics(timeout=10)  # timeout in seconds
    return json.dumps({'topics':list(cluster_metadata.topics.keys())})

@app.get("/StartReporter")
def read_root():
    return {"Hello": "World"}


'''EventGraphModel management'''

# todo
# async function to read the kafka topics available
# todo
# function tha recieves the content of kafka topics and generate a model from it
# todo
# model - holds information about the behaviour of an application with the goal to generate test cases
# the class must
