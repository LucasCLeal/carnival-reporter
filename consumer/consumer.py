from kafka import KafkaConsumer,TopicPartition
from json import loads  

def main(mode:str = None):
    # To consume latest messages and auto-commit offsets
    consumer = KafkaConsumer('test',
                            group_id='test-group',
                            auto_offset_reset = 'earliest',
                            bootstrap_servers=['localhost:9092'],
                            consumer_timeout_ms=1000)

    
    # obtain the last offset value
    try:
        print("beginning_offsets",consumer.beginning_offsets(consumer.assignment()) )
    except Exception as err:
        print(err)
    else:
        print("all set")

    print("topics: ",consumer.topics())
    print("subscription: ",consumer.subscription())
    
    if mode == "all":

        for message in consumer:
            # message value and key are raw bytes -- decode if necessary!
            # e.g., for unicode: `message.value.decode('utf-8')`
            print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                                message.offset, message.key,
                                                message.value))

    else:
        message = next(consumer)
        if message is None: 
            pass
        else:
            print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                                message.offset, message.key,
                                                message.value))

if __name__ == "__main__":
    """ This is executed when run from the command line """
    main("all")