# Chan Jing Hui

from confluent_kafka import Consumer, KafkaException
import json
import threading 

def kafka_consumer():
    consumer = Consumer({
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'malay-lexicon-consumer-group',
        'auto.offset.reset': 'earliest'
    })

    consumer.subscribe(['malay-lexicon-stream'])

    try:
        print("Consumer is listening for messages...")
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())
            else:
                data = json.loads(msg.value().decode('utf-8'))
                print(f"Consumed message: {data}")
    finally:
        consumer.close()

# Start the consumer
consumer_thread = threading.Thread(target=kafka_consumer)

consumer_thread.start()

consumer_thread.join()