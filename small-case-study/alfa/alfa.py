#!/usr/bin/env python3
"Create by lucas leal for his PhD experiment"

import os

from flask import Flask, jsonify
from kafka import KafkaProducer
from faker import Faker
from json import dumps 

""">>>>>>>>>>Beginnin KAFKA SECTION<<<<<<<<<<"""
'''kafka functions - required to start objects and deal with the comunication callbacks'''
def start_kafka_producer(host='localhost:9092'): 
    producer = KafkaProducer(  
        bootstrap_servers = [host],  
        value_serializer = lambda x:dumps(x).encode('utf-8')  
        )
    return producer

def on_send_success(record_metadata):
    print("topic: ",record_metadata.topic, "; partition: ", record_metadata.partition, "; offset: ", record_metadata.offset)

def on_send_error(excp):
    print("error sending msg",excp)
""">>>>>>>>>>>>>>END KAFKA SECTION<<<<<<<<<<<<"""

def create_app(config=None, kfk_prod=None):
    app = Flask(__name__)

    # See http://flask.pocoo.org/docs/latest/config/
    app.config.update(dict(DEBUG=True))
    app.config.update(config or {})

    @app.route("/")
    def hello_world():
        return "Hello World, I am the service Alfa"

    @app.route("/send_data/<someId>")
    def send_data(someId):
        fk = Faker()
        name = fk.name()
        date = fk.year()

        return jsonify({"echo": someId,"name":name,"date":date})

    return app


if __name__ == "__main__":

    #producer = start_kafka_producer()
    port = int(os.environ.get("PORT", 8000))
    app = create_app()
    app.run(host="0.0.0.0", port=port)