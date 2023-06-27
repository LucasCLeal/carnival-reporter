#!/usr/bin/env python3
"Create by lucas leal for his PhD experiment"

import sys
sys.path.append('../helpers')
import os
import threading

from flask import Flask, make_response, jsonify
from kafka import KafkaConsumer

from kafkaHelper import KafkaHelper
#from ..helpers.kafkaHelper import KafkaHelper


def start_kafka_consumer(bootstrap_servers='localhost:9092'):
    # To consume latest messages and auto-commit offsets

    try:
        consumer = KafkaConsumer('beta_input_topic',
                            group_id='test-group',
                            auto_offset_reset = 'earliest',
                            bootstrap_servers=[bootstrap_servers],
                            consumer_timeout_ms=1000)
    except:
        consumer = None

    try:
        print("beginning_offsets",consumer.beginning_offsets(consumer.assignment()))
    except Exception as err:
        print(err)
    else:
        print("all set")

    return consumer

def create_app(app_name=None):
    app = Flask(__name__)

    consume_loop = True

    #startin kafka helper
    kfk_helper = KafkaHelper()
    app_topic = app_name.split(".")[0]+"_input_topic"
    app_group = app_name.split(".")[0]+"_group"
    try:
        kfk_helper.create_consumer(topic=app_topic, group_id=app_group)
    except Exception as e:
        print("Failed to start kafka consumer:", str(e))

    def consumer_loop(kfk_helper):

        while consume_loop:
            try:
                message = kfk_helper.get_consumer_next_message()
                print("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                                     message.offset, message.key,
                                                     message.value))
            except:
                print("no message in topic")

    def start_consumer_tread():
        #criando a thread para ficar lendo o tópico
        consumer_thread = threading.Thread(target=consumer_loop(kfk_helper))
        consumer_thread.daemon = True
        consumer_thread.start()

    @app.route("/")
    def hello_world():
        return "Hello World, I am the service BETA!!!!"

    @app.route("/foo/<someId>")
    def foo_url_arg(someId):
        return jsonify({"echo": someId})

    @app.route("/start_consumer_loop")
    def start_consumer_loop():
        start_consumer_tread()
        response = make_response('thread started', 202)
        return response

    @app.route("/test_kafka_consumer")
    def test_kafka_consumer():
        message = kfk_helper.get_consumer_next_message()
        print("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                             message.offset, message.key,
                                             message.value))

    return app

if __name__ == "__main__":
    #port = int(os.environ.get("PORT", 8000))
    app = create_app("beta")
    #app.run(host="0.0.0.0", port=port)
    app.run(port=5001)