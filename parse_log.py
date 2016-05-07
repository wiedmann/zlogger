import time
import sys
import json
import argparse
import sqlite3
import traceback

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

def read_chalklines(dbh, line_mapper):
    c = dbh.cursor()
    c.execute("select line, name from chalkline")
    for d in c.fetchall():
        line_mapper.add_dest_line(d[0], d[1])

def main(argv):
    parser = argparse.ArgumentParser(description = 'Race Result Generator')
    parser.add_argument('zlogger_file', help='zlogger log file.')
    parser.add_argument('-D', '--mysql_database', help='mysql database')
    parser.add_argument('-H', '--mysql_host', help='mysql host')
    parser.add_argument('-U', '--mysql_user', help='mysql user')
    parser.add_argument('-P', '--mysql_password', help='mysql password')
    parser.add_argument('-d', '--debug', action='store_true', help='Debug things')
    args = parser.parse_args()
    line_mapper = LineMapper()

    with open(args.zlogger_file) as lfile:
        loglines = follow(lfile)
        dbh = opendb(args)
        read_chalklines(dbh, line_mapper)
        mycursor = dbh.cursor()
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
                                mycursor.execute("SELECT line from chalkline where name = %s", (line_name, ))
                                line_id = mycursor.fetchall()[0][0]
                                line_mapper.add_dest_line(line_id, line_name)
                        elif data['e'] == 'POS':
                            try:
                                value = data['v']
                                line_id = line_mapper.get_mapping(value['line'])
                                params = (data['msec'], value['id'], line_id, value['fwd'], value['m'],
                                          value['mwh'], value['dur'], value['ele'], value['spd'], value['hr'],
                                          value['obs'])
                                if args.debug:
                                    print "Msec=%s,ID=%s,Line=%s,fwd=%s,meters=%s,mwh=%s,duration=%s,Elevation=%s,Speed=%s,HR=%s,monitor=%s" % params
                                SQL = '''REPLACE INTO live_results (msec, riderid, lineid, fwd, meters, mwh, duration,
                                        elevation, speed, hr, monitorid)
                                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);'''
                                mycursor.execute(SQL, params)
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
                                pass
                            try:
                                dbh.close()
                                dbh = None
                            except:
                                pass
            except ValueError:
                print "WARNING - bad log file line: '%s'" % line

if __name__ == '__main__':
    try:
        main(sys.argv)
    except KeyboardInterrupt:
        pass
    except SystemExit, se:
        print "ERROR:", se