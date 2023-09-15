#!/usr/bin/env python3
"Create by lucas leal for his PhD experiment"

import sys
import time
sys.path.append('../helpers')
import os
import threading

from flask import Flask, make_response, jsonify
from kafka import KafkaConsumer


def create_kafka_consumer(bootstrap_servers='localhost:9092',topic=None,group_id=None,offset_reset="earliest",time_out=None):
    # To consume latest messages and auto-commit offsets
    try:

        if time_out:
            consumer = KafkaConsumer(topic,
                                      group_id=group_id,
                                      auto_offset_reset=offset_reset,
                                      bootstrap_servers=[bootstrap_servers],
                                      consumer_timeout_ms=time_out)
        else:
            consumer = KafkaConsumer(topic,
                                     group_id=group_id,
                                     auto_offset_reset=offset_reset,
                                     bootstrap_servers=[bootstrap_servers])
    except:
        consumer = None
    else:
        try:
            print("beginning_offsets",consumer.beginning_offsets(consumer.assignment()))
        except Exception as err:
            print(err)
        else:
            print("kafaka consumer started")

    return consumer

def create_app(app_name=None):

    def consumer_loop(kfk_helper):
        while consume_loop:
            try:
                message = kfk_helper.get_consumer_next_message()
                print("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                                     message.offset, message.key,
                                                     message.value))
            except:
                time.sleep(10)
                print("no message in topic")

    def consumer_loop_test(consumer):
        for message in consumer:
            print("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                                     message.offset, message.key,
                                                     message.value))


    app = Flask(__name__)
    consume_loop = True

    #startin kafka consumer
    app_topic = app_name.split(".")[0]+"_input_topic"
    app_group = app_name.split(".")[0]+"_group"
    try:
        consumer = create_kafka_consumer(topic=app_topic, group_id=app_group,time_out=1000)
    except Exception as e:
        print("Failed to start kafka consumer:", str(e))
    else:
        consumer_loop_test(consumer)

    #starting consumer thread
    #consumer_thread = threading.Thread(target=consumer_loop_test(consumer))
    #consumer_thread.daemon = True
    #consumer_thread.start()



    @app.route("/")
    def hello_world():
        return "Hello World, I am the service BETA!!!!"

    @app.route("/foo/<someId>")
    def foo_url_arg(someId):
        return jsonify({"echo": someId})

    @app.route("/test_kafka_consumer")
    def test_kafka_consumer():
        message = consumer.next()
        print("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                             message.offset, message.key,
                                             message.value))

    return app

if __name__ == "__main__":
    #port = int(os.environ.get("PORT", 8000))
    app = create_app("beta")
    #app.run(host="0.0.0.0", port=port)
    app.run(port=5001)