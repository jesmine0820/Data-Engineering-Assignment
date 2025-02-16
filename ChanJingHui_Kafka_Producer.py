# Chan Jing Hui

from confluent_kafka import Producer
import json
import time
import threading

def kafka_producer():
    producer = Producer({'bootstrap.servers': 'localhost:9092'})

    def delivery_report(err, msg):
        if err:
            print(f"Delivery failed: {err}")
        else:
            print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

    try:
        with open('raw_paragraphs.txt', 'r', encoding='utf-8') as file:
            for line in file:
                entry = {
                    "text_snippet": line.strip(),
                    "timestamp": time.time()
                }
                producer.produce('malay-lexicon-stream', value=json.dumps(entry), callback=delivery_report)
                producer.flush()
                time.sleep(1)
    except FileNotFoundError:
        print("Error: 'raw_paragraphs.txt' file not found.")
    except Exception as e:
        print(f"An error occurred: {e}")

# Start the producer
producer_thread = threading.Thread(target=kafka_producer)

producer_thread.start()

producer_thread.join()
