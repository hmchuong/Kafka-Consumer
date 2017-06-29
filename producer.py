from confluent_kafka import Producer
import sys
import time

if __name__ == '__main__':
    broker = 'localhost:9092'
    topic = 'test1'

    conf = {'bootstrap.servers': broker}

    p = Producer(**conf)

    def delivery_callback(err, msg):
        if err:
            sys.stderr.write('%% Message failed delivery: %s\n' % err)
        else:
            sys.stderr.write('%% Message delivered to %s [%d]\n' %
                             (msg.topic(), msg.partition()))

    fin = open("data.txt","r")
    for line in fin.readlines():
        try:
            p.produce(topic, line.strip(), callback=delivery_callback)

        except BufferError as e:
            sys.stderr.write('%% Local producer queue is full (%d messages awaiting delivery): try again\n' %
                             len(p))
        p.poll(0)
        time.sleep(1)
    fin.close()
    # Wait until all messages have been delivered
    sys.stderr.write('%% Waiting for %d deliveries\n' % len(p))
    p.flush()
