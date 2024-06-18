from confluent_kafka import Consumer, KafkaException, KafkaError
from fastapi import FastAPI
from dotenv import load_dotenv
from threading import Thread
from graphwalkerhelper import GraphWalkerHelper
from EventGraphModel.eventGraphModel import EventGraphModelManager
import os
import uuid
import signal
import json
import time

'''
The goal of this service is to monitor the system_report topic.
This service interacts with GraphWalker to generate the test cases, and select it accordingly to its configurations
'''

# start globa objects
app = FastAPI()
shutdown_flag = False
load_dotenv()  # This method will load environment variables from .env file


'''
[Cluster monitor functionality]
This section stores the code responsible to manage the topics on the target kafka cluster
listing the topics, defining which topics to be ignored, manage the runtime EventGraphModel
'''


# Setup kafka consumer loop
def get_consumer_sub_to_topic_set(topics: set, retry_interval=60, connected: bool = False):
    """ Attempt to subscribe to a topic with retries if the topic is not available. """

    try:
        # create kafka consumer
        group_id = f"my_consumer_group_{uuid.uuid4()}"
        conf = {
            'bootstrap.servers': os.getenv('BOOTSTRAP_SERVER'),  # Change this to your Kafka server configuration
            'group.id': group_id,
            'auto.offset.reset': 'latest'
        }
        consumer = Consumer(conf)
    except KafkaException as e:
        raise Exception("subscribe_to_topic:", e)

    while not connected and Consumer:
        try:
            topic_list = consumer.list_topics(timeout=5.0)  # timeout in seconds
            if topics < set(topic_list.topics.keys()):
                consumer.subscribe(list(topic_list))
            connected = True
            print(f"Successfully subscribed to {topics}")
        except KafkaException as e:
            print(f"Failed to subscribe to {topic_list}: {e}")
            print(f"Retrying in {retry_interval} seconds...")
            time.sleep(retry_interval)

    return consumer

def consume_messages():
    """
    Continuously consumes messages from two control topics.
    1st premise - All WS in the cluster will infor these two topics about the topics it is consuming and producing
    """
    consumer = None
    try:
        consumer = get_consumer_sub_to_topic_set(topics={'ReporterLog'})
        gw_helper = GraphWalkerHelper(ip="localhost", port=8887)

        while not shutdown_flag:

            msg = consumer.poll(timeout=5.0)  # Adjust the timeout as needed
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print(f'{msg.topic()} [{msg.partition()}] reached end at offset {msg.offset()}')
                elif msg.error():
                    print(f'KafkaError number:{msg.error()}')
                time.sleep(60)
                continue
            elif msg is None:
                time.sleep(60)
                continue
            else:
                #um novo modelo foi encontrado no topico.
                print(f'Received message: {msg.value().decode("utf-8")} from topic {msg.topic()}')
                content = msg.value().decode('utf-8')
                data = json.loads(content)
                model = data.get('model', None)
                testcases = []
                if model and gw_helper.isGWOnline():
                    #validate model
                    if gw_helper.validateJSON(model):
                        gw_helper.model = model
                        while gw_helper.hasNext():
                            testcases.append(gw_helper.getNext())
                        #test case selection
                        #submit test case set to TestExecuter



                #É necessario carregar o modelo no GW e fazer a criação de casos de testes
                # Os casos de testes são um conjunto topicos onde uma mensagem pode ser propagada dentro do cluster
                # O MTP deveria usar uma coleçao de BPMNs que serveriam como referencia as funcionalidades que composição
                # deveria ter como referencia.



    except Exception as exc:
        print(exc)
        print("Stopping consumer...")
    finally:
        if consumer:
            consumer.close()
        print("Consumer closed")


def start_monitor_loop():
    try:
        consume_messages()
    except Exception as exc:
        print("Error in start_monitor_loop:", exc)
    finally:
        print("Monitor loop terminated")


'''
[gracious shutdown]
This section is responsible to signal the end/start of the application and trigger changes in variables that will
affect the execution of Threads not demonic
'''


def signal_handler(signal, frame):
    global shutdown_flag
    shutdown_flag = True


# set up signal handlers
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


@app.on_event("startup")
async def startup_event():
    #thread = Thread(target=start_monitor_loop)
    #thread.start()
    print('MTP started')


@app.on_event("shutdown")
async def shutdown_event():
    global shutdown_flag
    shutdown_flag = True
    print('Application is shutting down...')


'''
[WS endpoints]
'''


@app.get("/")
def read_root():
    return {"Hello": "Model-based Test Planner"}


@app.get("/test")
def test():

    #todo deploy graphwalker e MTP on docker

    model_file = open("./test_models/example.json", "r")
    gw_helper = GraphWalkerHelper(ip="localhost", port=8887)
    result = []
    if gw_helper.isGWOnline():
        gw_helper.model = json.loads(model_file.read())
        gw_helper.load_model_to_graphwalker()
        while gw_helper.hasNext():
            result.append(gw_helper.getNext())

    return result

