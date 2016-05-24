import heapq
import time
import sys
import json
import argparse
import traceback
from mysql.connector import errors as mysql_errors
import datetime
import dateutil.parser
import pika
import pika.exceptions

def follow(file):
    while True:
        line = file.readline()
        if not line:
            time.sleep(0.3)
            continue
        yield line

def opendb(args):
    import mysql.connector
    return mysql.connector.connect(user=args.mysql_user, host=args.mysql_host, database=args.mysql_database,
                                   password=args.mysql_password, autocommit=True)

class ChatCallback(object):
    def __init__(self, args):
        self._args = args
        self._channel = None
        self._connection = None
        self._channel, self._connection = open_amqp(args)
        self._seen_messages = []
        self._message_signatures = {}
        self._dbh = None

    def add_message_signature(self, riderid, msg):
        self._message_signatures[str(riderid) + msg] = True

    def remove_message_signature(self, riderid, msg):
        del(self._message_signatures[str(riderid) + msg])

    def message_signature_exists(self, riderid, msg):
        return self._message_signatures.get(str(riderid) + msg, False)

    def timeout_messages(self, timestamp):
        while self._seen_messages and self._seen_messages[0][0] < timestamp - datetime.timedelta(seconds=3):
            x = heapq.heappop(self._seen_messages)
            self.remove_message_signature(x[1]['riderid'], x[1]['msg'])

    def add_message(self, time, riderid, msg):
        timestamp = dateutil.parser.parse(time)
        self.timeout_messages(timestamp)
        heapq.heappush(self._seen_messages, (timestamp, {'riderid':riderid, 'msg':msg}))
        self.add_message_signature(riderid, msg)

    def callback(self, ch, method, properties, body):
        data = json.loads(body)
        if not self.message_signature_exists(data['riderid'], data['msg']):
            self.add_message(data['time'], data['riderid'], data['msg'])
            for i in xrange(0,3):
                try:
                    self._channel.publish(exchange='zlogger', routing_key='CHAT.%s' % data['riderid'], body=body)
                    break
                except pika.exceptions.ConnectionClosed:
                    self._channel, self._connection = open_amqp(self._args)
                except:
                    print "ERROR: %s" % traceback.format_exc()
                    self._channel, self._connection = open_amqp(self._args)
            for i in xrange(0,3):
                try:
                    if not self._dbh:
                        self._dbh = opendb(self._args)
                    cursor = self._dbh.cursor()
                    cursor.execute("INSERT INTO chat (riderid, msg) values (%s, %s)",
                                   [data['riderid'], data['msg']])
                    break
                except mysql_errors.Error:
                    self._dbh = None


def main(argv):
    parser = argparse.ArgumentParser(description = 'Race Result Generator')
    #parser.add_argument('--pika_url', default='amqp://guest:guest@localhost:5672/%2F')
    parser.add_argument('--pika_url')
    parser.add_argument('-D', '--mysql_database', help='mysql database')
    parser.add_argument('-H', '--mysql_host', help='mysql host')
    parser.add_argument('-U', '--mysql_user', help='mysql user')
    parser.add_argument('-P', '--mysql_password', help='mysql password')
    args = parser.parse_args()
    cb = ChatCallback(args)

    if args.pika_url:
        channel, connection = open_amqp(args)
        result = channel.queue_declare(exclusive=True)
        queue_name = result.method.queue
        channel.queue_bind(exchange='zlogger.raw_chat', queue=queue_name, routing_key='CHAT')
    else:
        channel = None

    try:
        channel.basic_consume(cb.callback, queue=queue_name, no_ack=True)
        channel.start_consuming()
    except KeyboardInterrupt:
        connection.close()


def open_amqp(args):
    parameters = pika.URLParameters(args.pika_url)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    return channel, connection


if __name__ == '__main__':
    try:
        main(sys.argv)
    except KeyboardInterrupt:
        pass
    except SystemExit, se:
        print "ERROR:", se
