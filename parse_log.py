import time
import sys
import json
import argparse
import sqlite3

class LineMapper(object):
    def __init__(self):
        self._source_lines = {}
        self._deet_lines = {}
        self._mapping = {}

    def add_source_line(self, line_id, name):
        self._source_lines[name] = line_id
        if name in self._dest_lines:
            self._mapping[line_id] = self._dest_lines[name]
            return True
        return False

    def add_dest_line(self, line_id, name):
        self._dest_lines[name] = line_id
        if name in self._source_lines:
            self._mapping[self._source_lines[name]] = line_id
            return True
        return False

def follow(file):
    while True:
        line = file.readline()
        if not line:
            time.sleep(0.3)
            continue
        yield line

def main(argv):
    parser = argparse.ArgumentParser(description = 'Race Result Generator')
    parser.add_argument('--database', default='race_database.sql3',
        help='Specify destination .sql3 database')
    parser.add_argument('zlogger_file', help='zlogger log file.')
    args = parser.parse_args()
    dbh = sqlite3.connect(args.database)
    c = dbh.cursor()

    with open(args.zlogger_file) as lfile:
        loglines = follow(lfile)
        for line in loglines:
            data = json.loads(line)
            if data['e'] == 'LINE':
                print "Line id %s - %s" % (data['v']['line'], data['v']['name'])
            elif data['e'] == 'POS':
                value = data['v']
                print "Pos rider id %s - line %s - fwd: %s - m : %s" % (value['id'], value['line'], value['fwd'] == 'true', value['m'])

if __name__ == '__main__':
    try:
        main(sys.argv)
    except KeyboardInterrupt:
        pass
    except SystemExit, se:
        print "ERROR:", se
