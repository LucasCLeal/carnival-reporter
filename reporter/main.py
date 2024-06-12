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
from datetime import datetime
from EventGraphModel.eventGraphModel import EventGraphModelManager
import os
import signal


'''
The goal of this service is to monitor periodicaly the information in tha kafka topics in the cluster
use this information to Generate an EventGraphModel.
It must store the models in a topic to keep a track of if evolution.
It must notify the the service responsible for the test case generation
'''

# start globa objects
app = FastAPI()
shutdown_flag = False
modelManager = EventGraphModelManager()

load_dotenv()  # This method will load environment variables from .env file

'''
[Cluster monitor functionality]
This section stores the code responsible to manage the topics on the target kafka cluster
listing the topics, defining which topics to be ignored, manage the runtime EventGraphModel
'''

# Setup kafka consumer loop
def get_kafka_consumer():
    # Consumer configuration
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    conf = {
        'bootstrap.servers': os.getenv('BOOTSTRAP_SERVER'),  # Change this to your Kafka server configuration
        #'group.id': 'full_cluster_monitor',
        'group.id': 'new_group',
        'auto.offset.reset': 'earliest'
    }
    return Consumer(conf)


def consume_messages():
    """
    Continuously consumes messages from two control topics.
    1st premise - All WS in the cluster will infor these two topics about the topics it is consuming and producing
    """
    try:
        # create kafka consumer
        consumer = get_kafka_consumer()
    except Exception as err:
        raise err

    try:

        topic_list = consumer.list_topics(timeout=5.0)  # timeout in seconds
        ctrl_topics = ['ConsumerLog', 'ProducerLog']
        if ctrl_topics not in list(topic_list.topics.keys()):
            raise Exception('[fail] ctrl_topics not available in kafka cluster')
        else:
            consumer.subscribe(ctrl_topics)

        while not shutdown_flag:

            msg = consumer.poll(timeout=5.0)  # Adjust the timeout as needed
            if msg is None:

                #caso não existam mais mensagens em ambos os topicos, deve-se gerar uma foto grafia do sistema
                #só é possivel gerar o modelo caso exitam dados sobre os topicos do cluster
                #usar os dados levantadado até agora para gerar um GWModel
                if modelManager.cluster_topic_data != {}:
                    modelManager.name = 'teste_model'
                    modelManager.generator = 'teste_generator'
                    #gera novo modelo. caso novo modelo diferente do anterior ou anterior nulo.
                    #salvar nova versão do modelo no historico do model manager, incluindo o timestamp da mudança
                    current_model = modelManager.create_gw_model()
                    if modelManager.model_history == []:
                        modelManager.model_history.append({"timestamp": datetime.now().isoformat(),"model":current_model})
                    else:
                        report = modelManager.compare_models(current_model,modelManager.model_history[-1]["model"])
                        if report["has_differences"]:
                            modelManager.model_history.append({"timestamp": datetime.now().isoformat(), "model": current_model})

            elif msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print(f'{msg.topic()} [{msg.partition()}] reached end at offset {msg.offset()}')
                elif msg.error():
                    print(f'{msg.error()}')
                    continue
            else:
                if msg.topic() in ['ConsumerLog']:
                    # todo send to modelManager - assign consumer to topic
                    print(f'Received message: {msg.value().decode("utf-8")} from topic {msg.topic()}')
                    modelManager.update_from_consumer_log_topic(msg.value().decode('utf-8'))
                elif msg.topic() in ['ProducerLog']:
                    # todo send to modelManager - assign consumer to topic
                    print(f'Received message: {msg.value().decode("utf-8")} from topic {msg.topic()}')
                    modelManager.update_from_producer_log_topic(msg.value().decode('utf-8'))

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

'''
[gracious shutdown]
This section is responsible to signal the end/start of the application and trigger changes in variables that will
affect the execution of Threads not demonic
'''

def signal_handler(signal, frame):
    global shutdown_flag
    #print('Signal received, shutting down...')
    shutdown_flag = True


# set up signal handlers
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

@app.on_event("startup")
async def startup_event():
    thread = Thread(target=start_monitor_loop)
    thread.start()
    print('Reporter started')


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
    return {"Hello": "World"}


@app.get("/getTopicsSubscription")
def read_root():
    consumer = get_kafka_consumer()
    cluster_metadata = consumer.list_topics(timeout=1)  # timeout in seconds
    consumer.close()
    topic_list = list(cluster_metadata.topics.keys())
    return json.dumps({"topics": topic_list})


@app.get("/GenerateModel")
def read_root():
    return {"Hello": "World"}
