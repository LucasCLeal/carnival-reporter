from time import sleep  
from json import dumps  
from kafka import KafkaProducer
from faker import Faker

# initializing the Kafka producer  
producer = KafkaProducer(  
    bootstrap_servers = ['localhost:9092'],  
    value_serializer = lambda x:dumps(x).encode('utf-8')  
    )

def on_send_success(record_metadata):
    print("topic: ",record_metadata.topic, "; partition: ", record_metadata.partition, "; offset: ", record_metadata.offset)

def on_send_error(excp):
    log.error('I am an errback', exc_info=excp)
    # handle exception


def main():
    
    fk = Faker()
    for id in range(10):
        name = fk.name()
        data = {'name' : name, 'id':id}  
        # produce asynchronously with callbacks
        producer.send('test', value = data).add_callback(on_send_success).add_errback(on_send_error) 
        sleep(1)

def feed_topics():
    fk = Faker()
    for id in range(10):
        name = fk.name()
        date = fk.year()

        data_name = {'id':id, 'name' : name} 
        data_date =  {'id':id, 'date' : date} 

        # produce asynchronously with callbacks
        # enviando mensagens para tópicos diferentes.
        
        producer.send('nomes', value = data_name).add_callback(on_send_success).add_errback(on_send_error) 
        sleep(1)
        producer.send('datas', value = data_date).add_callback(on_send_success).add_errback(on_send_error)
        sleep(1)

if __name__ == "__main__":
    """ This is executed when run from the command line """
    feed_topics()