#!/usr/bin/env python3
"""
Module Docstring
"""

__author__ = "Your Name"
__version__ = "0.1.0"
__license__ = "MIT"

from kafka import KafkaConsumer

def create_consumer(bootstrap_servers='localhost:9092' ,topic=None, group_id=None, offset_reset='earliest'):
    # To consume latest messages and auto-commit offsets
    consumer = None
    try:
        consumer = KafkaConsumer(topic,
                                      group_id=group_id,
                                      auto_offset_reset=offset_reset,
                                      bootstrap_servers=[bootstrap_servers])
        print("beginning_offset: ", consumer.beginning_offsets(consumer.assignment()))
    except Exception as err:
        print(err)
        raise Exception("Error creating consumer")

    return consumer

def main():
    #startin kafka consumer

    app_topic = "beta_input_topic"

    try:
        consumer = create_consumer(topic=app_topic)
    except Exception as e:
        print("Failed to start kafka consumer:", str(e))

    for message in consumer:
        print("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                             message.offset, message.key,
                                             message.value))

if __name__ == "__main__":
    """ This is executed when run from the command line """
    main()






