#!/usr/bin/env python3
"Create by lucas leal for small-case-study for PhD experiment"

import os
import warnings 
from flask import Flask, jsonify
from kafka import KafkaProducer
from faker import Faker
from json import dumps


"""
>>>>>>>>>>KAFKA SECTION<<<<<<<<<<
kafka functions - required to start objects and deal with the comunication callbacks'''
"""
def start_kafka_producer(bootstrap_servers='localhost:9092'): 
    
    try:
        producer = KafkaProducer(bootstrap_servers= bootstrap_servers,
                                 value_serializer = lambda x:dumps(x).encode('utf-8') )
        print("Kafka producer created successfully!")   
        return producer

    except Exception as e:
        print("Failed to create Kafka producer:", str(e))
    
    return None

def consume_kafka_producer(producer, topic, payload):
    
    key_parameters = [producer,topic,payload]
    
    if any(param is None for param in key_parameters):
        warnings.warn("consume_kafka_producer: One of key parameters was not provided", category=Warning)
        warnings.warn("message: "+ str(payload)+" \n Not delivered to topic: "+str(topic),category=UserWarning)
    
    # Enviando mensagem
    producer.send(topic,value = payload).add_callback(on_send_success).add_errback(on_send_error)

    # Flush and close the producer
    producer.flush()
    producer.close()

def on_send_success(record_metadata):
    print("topic: ",record_metadata.topic, "; partition: ", record_metadata.partition, "; offset: ", record_metadata.offset)

def on_send_error(excp):
    print("error sending msg",excp)
"""
>>>>>>>>>>>>>>APP Section<<<<<<<<<<<<
esse app tem 4 endpoints basicos
 / - hellor workd and app status
 /send_data/<someId> - send some data to a topic
 /sort_string - sort string and retur the result.
 /gen_string - generates a string of the requested size

"""

def create_app(config=None):
    app = Flask(__name__)

    # See http://flask.pocoo.org/docs/latest/config/
    app.config.update(dict(DEBUG=True))
    app.config.update(config or {})

    @app.route("/kafka_status")
    def poducer_available(producer):
        return False if producer is None else True

    @app.route("/")
    def hello_world():
        return "Hello World, I am the service Alfa"

    @app.route("/send_data/<someId>")
    def send_data(someId):
        
        #iniciando 
        producer = start_kafka_producer()
        if producer:

            #gerando conteudo aleatório para o payload
            fk = Faker()
            name = fk.name()
            consume_kafka_producer(producer=producer,payload=jsonify({"id":someId,"name":name}),topic="beta_input_topic")
        else:
            print("Not possible to deliver message to defined topic on bootstrap server")


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    app = create_app()
    app.run(host="0.0.0.0", port=port)