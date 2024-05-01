#!/usr/bin/env python3
"""

Alfa Service
Purpose:

        Alfa Service is a Flask-based Python service designed to interact with a Kafka cluster.
        It creates and monitors a Kafka topic, processes incoming messages, and manages information about individuals,
        such as name, age, account balance, and country of origin.

Input and Output:

        Requires a .env file for Kafka cluster connection configurations.
        Outputs include the listing of available Kafka topics and
        the creation and dissemination of person information objects through Kafka topics.

Methodology:

        Loads the .env file for configuration.
        Creates the Flask application.
        Connects to the Kafka server.
        Sets a new topic using the service's name as the key.
        Retrieves a list of available topics in the cluster.
        Initiates a thread to monitor for new messages on the created topic.
        Processes each new message to trigger the creation of a new user object, which is then sent to one of the available topics in the cluster.

Dependencies:

        Utilizes libraries such as Flask for the web framework,
        Faker for generating fake data, JSON for data handling, and Python's threading for concurrent operations.

Error Handling and Limitations:

        Many could happen, all of them can be figured out.
        :)

Create by Lucas Leal for his PhD experiment, December 2023.


Created by lucas leal
"""

from flask import Flask, make_response
#from kafka import KafkaConsumer
#from helpers.kafkaHelper import KafkaHelper
from dotenv import load_dotenv
import os
import logging
import threading
import time

def create_app(base_name:str):
    app = Flask(__name__)
    app.config.update(dict(DEBUG=True))

    # Configure logging
    logging.basicConfig(filename='flaskapp.log', level=logging.DEBUG,
                        format='%(asctime)s %(levelname)s: %(message)s [in %(pathname)s:%(lineno)d]')

    @app.route("/")
    def hello_world():
        app.logger.info('Home page accessed')
        return "Hello World"

    return app


if __name__ == "__main__":

    # Load environment variables from .env file
    load_dotenv()

    # Access variables using os.environ
    kafka_host = os.getenv('KAFKA_HOST')
    kafka_port = int(os.getenv('KAFKA_PORT'))
    service_name = os.getenv('SERVICE_NAME')

    port = int(os.environ.get("PORT", 8000))
    app = create_app(service_name)
    app.run(host="0.0.0.0", port=port)