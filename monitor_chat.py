import time
import sys
import json
import argparse
import sqlite3
import traceback
import pika

import mysql.connector
from mysql.connector import errors as mysql_errors

def callback(ch, method, properties, body):
    data = json.loads(body)
    print("[%s] %s" % (data.get('partialName', data.get('riderid', '???')), data['msg']))

def main(argv):
    parser = argparse.ArgumentParser(description = 'Race Result Generator')
    parser.add_argument('--pika_url', default='amqp://guest:guest@localhost:5672/%2F')
    args = parser.parse_args()

    parameters = pika.URLParameters(args.pika_url)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    result = channel.queue_declare(exclusive=True)
    queue_name = result.method.queue
    channel.queue_bind(exchange='zlogger', queue=queue_name, routing_key='CHAT.#')

    try:
        channel.basic_consume(callback, queue=queue_name, no_ack=True)
        channel.start_consuming()
    except KeyboardInterrupt:
        connection.close()

if __name__ == '__main__':
    try:
        main(sys.argv)
    except KeyboardInterrupt:
        print "exiting"
        exit()
    except SystemExit, se:
        print "ERROR:", se
