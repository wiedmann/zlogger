import time
import sys
import json
import re
import argparse
import traceback

import pika
import pika.exceptions

def follow(file):
    while True:
        line = file.readline()
        if not line:
            time.sleep(0.3)
            continue
        yield line

def main(argv):
    parser = argparse.ArgumentParser(description = 'Race Result Generator')
    parser.add_argument('chat_log', help='zlogger chat log file.')
    #parser.add_argument('--pika_url', default='amqp://guest:guest@localhost:5672/%2F')
    parser.add_argument('--pika_url')
    args = parser.parse_args()

    if args.pika_url:
        channel, connection = open_amqp(args)
    else:
        channel = None

    with open(args.chat_log) as lfile:
        chatlines = follow(lfile)
        try:
            for line in chatlines:
                line = line.strip()
                m = re.search(r'^([0-9]+:[0-9]+:[0-9]+)\s+<\s*([0-9]+)\s*>\s\[([^]]*)\]\s*(.*)$', line)
                if m:
                    data = {'time': m.group(1),
                            'riderid': m.group(2),
                            'partialName': m.group(3),
                            'msg': m.group(4)}
                    for i in xrange(0,3):
                        try:
                            channel.publish('zlogger.raw_chat', 'CHAT', json.dumps(data))
                            break
                        except pika.exceptions.ConnectionClosed:
                            channel, connection = open_amqp(args)
                        except:
                            channel, connection = open_amqp(args)
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
