from flask import Flask
import os
from dotenv import load_dotenv
from kafkalogger import KafkaLogger
import atexit

app_name = os.path.splitext(os.path.basename(__file__))[0]
app = Flask(__name__)
load_dotenv()  # This method will load environment variables from .env file
logger = KafkaLogger(bootstrap_servers=os.getenv('BOOTSTRAP_SERVER'))

@app.route('/')
def home():
    return "Flask app is running!"

def on_startup():
    try:
        logger.log_producer(topic="sorted_list",producer=app_name,action="add")
        logger.log_consumer(topic="rnd_list",consumer=app_name,action="add")
    except Exception as err:
        print(err)
def on_shutdown():
    try:
        logger.log_producer(topic="sorted_list", producer=app_name, action="remove")
        logger.log_consumer(topic="rnd_list", consumer=app_name, action="remove")
    except Exception as err:
        print(err)

# Register the shutdown function to be called when the app is exiting
atexit.register(on_shutdown)

if __name__ == '__main__':
    on_startup()
    app.run(debug=True,port=5002)
