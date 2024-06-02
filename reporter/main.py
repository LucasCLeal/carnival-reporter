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
from fastapi import FastAPI
from dotenv import load_dotenv
from threading import Thread
import os
import signal
import sys

shutdown_flag = False

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


def consume_messages():
    """ Continuously consumes messages from all subscribed topics. """
    try:
        # create kafka consumer
        consumer = get_kafka_consumer()
    except Exception as err:
        raise err

    try:
        while not shutdown_flag:
            # update list of topics on cluster
            cluster_metadata = consumer.list_topics(timeout=5.0)  # timeout in seconds
            cluster_topics = list(cluster_metadata.topics.keys())
            # subscribe consumer to topics
            consumer.subscribe(cluster_topics)

            msg = consumer.poll(timeout=5.0)  # Adjust the timeout as needed
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    print(f'{msg.topic()} [{msg.partition()}] reached end at offset {msg.offset()}')
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                # todo process the message
                print(f'Received message: {msg.value().decode("utf-8")} from topic {msg.topic()}')

    except Exception as exc:
        print(exc)
        print("Stopping consumer...")
    finally:
        consumer.close()
        print("Consumer closed")



def start_monitor_loop():
    try:
        consume_messages()
    except Exception as exc:
        print("Error in start_monitor_loop:", exc)
    finally:
        print("Monitor loop terminated")


def signal_handler(signal, frame):
    global shutdown_flag
    print('Signal received, shutting down...')
    shutdown_flag = True


# set up signal handlers
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

@app.on_event("startup")
async def startup_event():
    thread = Thread(target=start_monitor_loop)
    thread.start()
    print('Cluster monitor started')


@app.on_event("shutdown")
async def shutdown_event():
    global shutdown_flag
    shutdown_flag = True
    print('Application is shutting down...')


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/getTopicsSubscription")
def read_root():
    consumer = get_kafka_consumer()
    cluster_metadata = consumer.list_topics(timeout=10)  # timeout in seconds
    consumer.close()
    topic_list = list(cluster_metadata.topics.keys())
    return json.dumps({"topics": topic_list})


@app.get("/StartReporter")
def read_root():
    return {"Hello": "World"}
