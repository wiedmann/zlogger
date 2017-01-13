import os
import time
import sys
import json
import argparse
import sqlite3
import traceback

import datetime
import pika
import pika.exceptions

import mysql.connector
from mysql.connector import errors as mysql_errors

class LineMapper(object):
    def __init__(self):
        self._source_lines = {}
        self._dest_lines = {}
        self._mapping = {}

    def add_source_line(self, line_id, name):
        self._source_lines[name] = int(line_id)
        if name in self._dest_lines:
            self._mapping[int(line_id)] = self._dest_lines[name]
            return True
        return False

    def add_dest_line(self, line_id, name):
        self._dest_lines[name] = int(line_id)
        if name in self._source_lines:
            self._mapping[self._source_lines[name]] = int(line_id)
            return True
        return False

    def get_mapping(self, source_line_id):
        return self._mapping[int(source_line_id)]

def follow(file):
    partial_line = ''
    while True:
        line = file.readline()
        if not line or '\n' not in line:
            if line:
                partial_line += line
            time.sleep(0.3)
            continue
        line = partial_line + line
        partial_line = ''
        yield line

def opendb(args):
    import mysql.connector
    return mysql.connector.connect(user=args.mysql_user, host=args.mysql_host, database=args.mysql_database,
                                   password=args.mysql_password, autocommit=True)

def read_chalklines(dbh, line_mapper):
    c = dbh.cursor()
    c.execute("select line, name from chalkline")
    for d in c.fetchall():
        line_mapper.add_dest_line(d[0], d[1])

active_chalklines = {}

def mark_chalkline_active(dbh, id):
    global active_chalklines
    c = dbh.cursor()
    c.execute("update chalkline set active=1, lastmonitored=now() where line = %s", (id, ))
    dbh.commit()
    active_chalklines[id] = True

def mark_all_chalklines_inactive(dbh):
    global active_chalklines
    c = dbh.cursor()
    for id in active_chalklines.keys():
        c.execute("update chalkline set active=0 where line = %s", (id, ))
        dbh.commit()
    active_chalklines = {}

class ShutdownError(Exception):
    pass

def main(argv):
    parser = argparse.ArgumentParser(description = 'Race Result Generator')
    parser.add_argument('zlogger_file', help='zlogger log file.')
    parser.add_argument('-D', '--mysql_database', help='mysql database')
    parser.add_argument('-H', '--mysql_host', help='mysql host')
    parser.add_argument('-U', '--mysql_user', help='mysql user')
    parser.add_argument('-P', '--mysql_password', help='mysql password')
    parser.add_argument('-d', '--debug', action='store_true', help='Debug things')
    parser.add_argument('-i', '--update_interval', type=int, help='chalkline update interval', default=30)
    parser.add_argument('-r', '--rename_log', action='store_true', help='Rename input log file on shutdown')
    #parser.add_argument('--pika_url', default='amqp://guest:guest@localhost:5672/%2F')
    parser.add_argument('--pika_url')
    parser.add_argument('--stay_running_after_shutdown', action='store_true')
    args = parser.parse_args()
    line_mapper = LineMapper()

    if args.pika_url:
        channel, connection = open_amqp(args)
    else:
        channel = None

    with open(args.zlogger_file) as lfile:
        loglines = follow(lfile)
        dbh = opendb(args)
        read_chalklines(dbh, line_mapper)
        mycursor = dbh.cursor()
        last_line_update={}
        shutdown=False
        try:
            for line in loglines:
                line = line.strip()
                try:
                    data = json.loads(line)
                    while True:
                        if not dbh:
                            dbh = opendb(args)
                            mycursor = dbh.cursor()
                        try:
                            if data['e'] == 'LINE':
                                line_id = data['v']['line']
                                line_name = data['v']['name']
                                line_data = str(data['v']['data'])
                                if args.debug:
                                    print "Line id %s - %s" % (line_id, line_name)
                                if not line_mapper.add_source_line(line_id, line_name):
                                    print "Adding new chalkline to db: %s, '%s', '%s'" % (line_id, line_data, line_name)
                                    mycursor.execute("INSERT into chalkline (data, name) VALUES (%s, %s)",
                                                     (line_data, line_name))
                                    dbh.commit()
                                    mycursor.execute("SELECT line from chalkline where name = %s", (line_name, ))
                                    line_id = mycursor.fetchall()[0][0]
                                    line_mapper.add_dest_line(line_id, line_name)
                            elif data['e'] == 'NEARBY':
                                line_id = line_mapper.get_mapping(data['v']['data'])
                                mark_chalkline_active(dbh, line_id)
                                last_line_update[line_id] = time.time()
                            elif data['e'] == 'SHUTDOWN':
                                mark_all_chalklines_inactive(dbh)
                                if not args.stay_running_after_shutdown:
                                    print "Got shutdown event - shutting down."
                                    raise ShutdownError()
                            elif data['e'] == 'POS' or data['e'] == 'TELE':
                                try:
                                    value = data['v']
                                    if data['e'] == 'POS':
                                        line_id = line_mapper.get_mapping(value['line'])
                                        if line_id not in last_line_update or (time.time() - last_line_update[line_id]) > args.update_interval:
                                            mark_chalkline_active(dbh, line_id)
                                            last_line_update[line_id] = time.time()
                                        location_field = 'lineid'
                                        table = 'live_results'
                                    else:
                                        location_field = 'rad'
                                        line_id = None
                                        table = 'telemetry'
                                    params = (data['msec'], value['id'], line_id, value['fwd'], value['m'],
                                              value['mwh'], value['dur'], value['ele'], value['spd'], value['hr'],
                                              value['obs'], value.get('lpup', 0), value.get('pup', ''),
                                              value.get('cad', 0), value.get('grp', 0))
                                    if channel:
                                        for i in xrange(0,3):
                                            try:
                                                msg_data = dict(zip(('msec', 'riderid', location_field, 'fwd', 'meters', 'mwh', 'duration',
                                                             'elevation', 'speed', 'hr', 'monitorid', 'lpup', 'pup', 'cad', 'grp'), params))
                                                channel.publish('zlogger', '%s.%s.%s' % (data['e'], line_id, value['id']),
                                                                      json.dumps(msg_data))
                                                break
                                            except pika.exceptions.ConnectionClosed:
                                                try:
                                                    channel, connection = open_amqp(args)
                                                except:
                                                    print "WARNING: exception trying to reconnect to amqp: %s" % traceback.format_exc()
                                                    i = 3
                                            except:
                                                print "WARNING: exception publishing POS event: %s" % traceback.format_exc()
                                                try:
                                                    channel, connection = open_amqp(args)
                                                except:
                                                    print "WARNING: exception trying to reconnect to amqp: %s" % traceback.format_exc()
                                                    i = 3
                                    if args.debug:
                                        print "Msec=%s,ID=%s,Line=%s,fwd=%s,meters=%s,mwh=%s,duration=%s,Elevation=%s,Speed=%s,HR=%s,monitor=%s" % params
                                    SQL = ('INSERT INTO ' + table + ' (msec, riderid, ' + location_field + ', fwd, meters, mwh, duration, '
                                           + 'elevation, speed, hr, monitorid, lpup, pup, cad, grp, timestamp) '
                                           + 'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, %s, %s, %s)'
                                           + 'ON DUPLICATE KEY UPDATE msec=%s, riderid=%s,'+ location_field +'=%s, fwd=%s, meters=%s, mwh = %s,'
                                           + 'duration=%s, elevation=%s, speed=%s, hr=%s, monitorid=%s, lpup=%s, pup=%s, cad=%s, grp=%s, timestamp=%s')
                                    params = params + (datetime.datetime.now(),)
                                    mycursor.execute(SQL, params + params)
                                    dbh.commit()
                                except KeyError:
                                    print "WARNING - POS Entry for unknown line: %s" % line
                            break
                        except mysql_errors.Error:
                            print "Exception - reopening mysql connection in 3 seconds: %s" % traceback.format_exc()
                            time.sleep(3)
                            if dbh:
                                try:
                                    mycursor.close()
                                    mycursor = None
                                except:
                                    mycursor = None
                                try:
                                    dbh.close()
                                    dbh = None
                                except:
                                    dbh = None
                        except KeyError:
                            print "ERROR - unrecognized data: %s" % line
                            break
                except ValueError:
                    print "WARNING - bad log file line: '%s'" % line
        except ShutdownError:
            shutdown=True
        except:
            print "Exception - exiting %s" % traceback.format_exc()
        finally:
            try:
                connection.close()
            except:
                pass
    if shutdown and args.rename_log:
        time.sleep(1)  # avoid race condition with zlogger closing file
        newfile = args.zlogger_file + '.' + datetime.datetime.now().strftime('%Y%m%d')
        suffix = 1
        basename = newfile
        while os.path.isfile(newfile):
            newfile = basename + '.' + str(suffix)
        os.rename(args.zlogger_file, newfile)


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
