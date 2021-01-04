from kafka import KafkaProducer
import time
import random

def generate_stream(testFile):
    topic = 'sentiment'
    producer = KafkaProducer(bootstrap_servers='localhost:9092')

    print('streaming...')
    with open(test_file) as f:
        for line in f:
    #for line in lines:
            producer.send(topic,line.encode('utf-8'))
            print("line sent")
            time.sleep(10)

if __name__ == '__main__':
    test_file = "./data/test.txt"
    generate_stream(test_file)

