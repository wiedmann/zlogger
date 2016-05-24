import time
import sys
import json
import argparse
import sqlite3
import traceback
import pika

import mysql.connector
from mysql.connector import errors as mysql_errors

def opendb(args):
    import mysql.connector
    return mysql.connector.connect(user=args.mysql_user, host=args.mysql_host, database=args.mysql_database,
                                   password=args.mysql_password, autocommit=True)
def callback(ch, method, properties, body):
    print(" [x] %r: %r" % (method.routing_key, body))

def main(argv):
    parser = argparse.ArgumentParser(description = 'Race Result Generator')
    parser.add_argument('topics', nargs='+')
    parser.add_argument('-D', '--mysql_database', help='mysql database')
    parser.add_argument('-H', '--mysql_host', help='mysql host')
    parser.add_argument('-U', '--mysql_user', help='mysql user')
    parser.add_argument('-P', '--mysql_password', help='mysql password')
    parser.add_argument('-d', '--debug', action='store_true', help='Debug things')
    parser.add_argument('--pika_url', default='amqp://guest:guest@localhost:5672/%2F')
    parser.add_argument('-E', '--exchange', default='zlogger')
    args = parser.parse_args()

    parameters = pika.URLParameters(args.pika_url)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    result = channel.queue_declare(exclusive=True)
    queue_name = result.method.queue
    for t in args.topics:
        channel.queue_bind(exchange=args.exchange, queue=queue_name, routing_key=t)

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
