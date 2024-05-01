#!/usr/bin/env python3
"""
Created by lucas leal
"""
import os
import threading
import sys
import time
from flask import Flask, make_response, jsonify
from kafka import KafkaConsumer
from helpers.kafkaHelper import KafkaHelper

def create_app(base_name:str):
    app = Flask(__name__)

    def kafka_consumer_loop(consumer: KafkaConsumer, consume: bool):
        back_off = 2
        while consume:
            try:
                message = next(consumer)
                print("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                                     message.offset, message.key,
                                                     message.value))
            except Exception as err:
                time.sleep(back_off)
                back_off = back_off+back_off if back_off < 512 else 512
                print("next_back_off:",str(back_off))
            else:
                back_off = 2
                print("processing message!!!!")

    @app.route("/")
    def hello_world():
        return "Hello World"

    @app.route("/kafka/toggle_consumer_loop")
    def hello_world():
        return "Hello World"
    @app.route("/kafka/start_kafka_consumer")
    def start_kafka_consumer():

        """starting start_kafka_consumer"""
        try:
            # startin kafka consumer
            app_topic = base_name.split(".")[0] + "_input_topic"
            app_group_id = base_name.split(".")[0] + "_group"
            print("app_topic:", app_topic)
            print("app_group_id:", app_group_id)
            helper = KafkaHelper()
            consumer = helper.create_consumer(topic=app_topic, group_id=app_group_id)
        except Exception as err:
            print(err)
            return make_response("No kafka consumer, consumer not started", 400)
        else:
            consumer_thread = threading.Thread(target=kafka_consumer_loop, args=(consumer, True))
            consumer_thread.start()
            return make_response("kafka consumer started", 202)

    return app


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    app = create_app(str(os.path.basename(__file__)))
    app.run(host="0.0.0.0", port=port)