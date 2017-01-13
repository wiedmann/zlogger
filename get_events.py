#!/usr/bin/env python
import sys, argparse, getpass
import traceback

import requests
import json
import os, time, stat
import mkresults
import mysql.connector
import datetime
import dateutil.parser
from mysql.connector import errors as mysql_errors

global args

class StravaThrottled(Exception):
    pass

class ZwiftAuthFailed(Exception):
    pass

def post_credentials(session, username, password):
    # Credentials POSTing and tokens retrieval
    # POST https://secure.zwift.com/auth/realms/zwift/tokens/access/codes

    try:
        response = session.post(
            url="https://secure.zwift.com/auth/realms/zwift/tokens/access/codes",
            headers={
                "Accept": "*/*",
                "Accept-Encoding": "gzip, deflate",
                "Connection": "keep-alive",
                "Content-Type": "application/x-www-form-urlencoded",
                "Host": "secure.zwift.com",
                "User-Agent": "Zwift/1.5 (iPhone; iOS 9.0.2; Scale/2.00)",
                "Accept-Language": "en-US;q=1",
            },
            data={
                "client_id": "Zwift_Mobile_Link",
                "username": username,
                "password": password,
                "grant_type": "password",
            },
            allow_redirects = False,
            verify = args.verifyCert,
        )

        if args.verbose:
            print('Response HTTP Status Code: {status_code}'.format(
                status_code=response.status_code))
            print('Response HTTP Response Body: {content}'.format(
                content=response.content))

        json_dict = json.loads(response.content)

        return (json_dict["access_token"], json_dict["refresh_token"], json_dict["expires_in"])

    except requests.exceptions.RequestException, e:
        print('HTTP Request failed: %s' % e)

def retrieveEvents(session, access_token):
    # Get Player Activities
    # GET https://us-or-rly101.zwift.com/api/profiles/<player_id>/activities

    try:
        timestamp = long((time.time() - 7200) * 1000)
        response = session.post(
            url="https://us-or-rly101.zwift.com/api/events/search?use_subgroup_time=true&created_before=%s&start=0&limit=0" % timestamp,
            data='{"eventStartsAfter":%s}' % timestamp,
            headers={
                "Accept-Encoding": "gzip",
                "Content-type": "application/json",
                "Accept": "application/json",
                "Connection": "keep-alive",
                "Host": "us-or-rly101.zwift.com",
                "User-Agent": "Zwift/115 CFNetwork/758.0.2 Darwin/15.0.0",
                "Authorization": "Bearer %s" % access_token,
                "Accept-Language": "en-us",
            },
            verify=args.verifyCert,
        )
        if args.verbose:
            print('Response HTTP Status Code: {status_code}'.format(
                status_code=response.status_code))
            print('Response HTTP Response Body: {content}'.format(
                content=response.content))
        if response.status_code != 200:
            if response.status_code == 401:
                raise ZwiftAuthFailed()
            print('WARNING: response %s retrieving activities %s' % (response.status_code, response.content))

        try:
            return timestamp, json.loads(response.content)
        except ValueError:
            return None

    except requests.exceptions.RequestException, e:
        print('HTTP Request failed: %s' % e)


def logout(session, refresh_token):
    # Logout
    # POST https://secure.zwift.com/auth/realms/zwift/tokens/logout
    try:
        response = session.post(
            url="https://secure.zwift.com/auth/realms/zwift/tokens/logout",
            headers={
                "Accept": "*/*",
                "Accept-Encoding": "gzip, deflate",
                "Connection": "keep-alive",
                "Content-Type": "application/x-www-form-urlencoded",
                "Host": "secure.zwift.com",
                "User-Agent": "Zwift/1.5 (iPhone; iOS 9.0.2; Scale/2.00)",
                "Accept-Language": "en-US;q=1",
            },
            data={
                "client_id": "Zwift_Mobile_Link",
                "refresh_token": refresh_token,
            },
            verify = args.verifyCert,
        )
        if args.verbose:
            print('Response HTTP Status Code: {status_code}'.format(
                status_code=response.status_code))
            print('Response HTTP Response Body: {content}'.format(
                content=response.content))
    except requests.exceptions.RequestException, e:
        print('HTTP Request failed: %s' % e)


def login(session, user, password):
    access_token, refresh_token, expired_in = post_credentials(session, user, password)
    return access_token, refresh_token


def writeEntry(dbh, table, fields, data):
    c = dbh.cursor()

    numCols = len(fields)
    query = "replace into %s (%s) VALUES (%s)" % (table, ', '.join(fields), '%s, ' * (numCols - 1) + '%s')
    values = [data[f] for f in fields]
    c.execute(query, tuple(values))
    dbh.commit()
    c.close()


def main(argv):
    global args

    strava_access_token = None

    parser = argparse.ArgumentParser(description = 'Zwift Strava ID retriaval')
    parser.add_argument('-v', '--verbose', action='store_true',
            help='Verbose output')
    parser.add_argument('--dont-check-certificates', action='store_false',
            dest='verifyCert', default=True)
    parser.add_argument('-u', '--user', help='Zwift user name')
    parser.add_argument('-D', '--mysql_database', help='mysql database (overrides --database)', required=True)
    parser.add_argument('-H', '--mysql_host', help='mysql host')
    parser.add_argument('-U', '--mysql_user', help='mysql user')
    parser.add_argument('-P', '--mysql_password', help='mysql password')
    args = parser.parse_args()

    if args.user:
        password = getpass.getpass("Password for %s? " % args.user)
    else:
        file = 'zwift_creds.json'
        with open(file) as f:
            try:
                cred = json.load(f)
            except ValueError, se:
                sys.exit('"%s": %s' % (args.output, se))
        f.close
        args.user = cred['user']
        password = cred['pass']

    session = requests.session()

    ev_columns = ['id', 'name', 'shortName', 'routeId', 'laps', 'durationInSeconds', 'distanceInMeters', 'rulesId',
                  'rulesSet', 'visible', 'recurring', 'worldId', 'description', 'eventStart', 'sport', 'imageUrl',
                  'auxiliaryUrl', 'jerseyHash', 'totalEntrantCount', 'retrievalTime']
    sg_columns = ['id', 'zwift_event_id', 'name', 'routeId', 'laps', 'durationInSeconds', 'distanceInMeters', 'rulesId',
                  'rulesSet', 'description', 'eventSubgroupStart', 'auxiliaryUrl', 'paceType', 'startLocation',
                  'registrationStatus', 'signupStatus', 'fromPaceValue', 'toPaceValue', 'label', 'fieldLimit',
                  'jerseyHash', 'totalEntrantCount', 'retrievalTime']
    dbh = None
    while True:
        try:
            if not dbh:
                dbh = open_mysql(args)
            access_token, refresh_token = login(session, args.user, password)
            retrievalTime, data = retrieveEvents(session, access_token)
            for event in data:
                event['rulesSet'] = json.dumps(event['rulesSet'])
                event['eventStart'] = dateutil.parser.parse(event['eventStart'], ignoretz=True)
                event['retrievalTime'] = retrievalTime
                writeEntry(dbh, 'zwift_events', ev_columns, event)
                for subgroup in event['eventSubgroups']:
                    subgroup['zwift_event_id'] = event['id']
                    subgroup['rulesSet'] = json.dumps(subgroup['rulesSet'])
                    subgroup['eventSubgroupStart'] = dateutil.parser.parse(subgroup['eventSubgroupStart'], ignoretz=True)
                    subgroup['retrievalTime'] = retrievalTime
                    writeEntry(dbh, 'zwift_event_subgroups', sg_columns, subgroup)

            c = dbh.cursor()
            c.execute("set time_zone='+00:00'")
            c.execute(
                'DELETE FROM zwift_events WHERE retrievalTime < %s AND eventStart > FROM_UNIXTIME(%s)',
                (retrievalTime, retrievalTime / 1000))
            c.execute(
                'DELETE sg FROM zwift_event_subgroups sg, zwift_events ev WHERE sg.zwift_event_id = ev.id AND sg.retrievalTime < %s AND ev.eventStart > FROM_UNIXTIME(%s)',
                (retrievalTime, retrievalTime / 1000))
            print "retrieval complete - sleeping for 10 minutes."
            time.sleep(600)
        except mysql_errors.Error:
            print "Mysql exception %s" % traceback.format_exc()
            try:
                dbh.close()
            except:
                pass
            dbh = None
            time.sleep(10)
        except:
            print event
            print "WARNING - exception encountered: %s" % traceback.format_exc()
            time.sleep(10)


def open_mysql(args):
    return mysql.connector.connect(user=args.mysql_user, password=args.mysql_password, database=args.mysql_database,
                                   host=args.mysql_host, autocommit=True)


if __name__ == '__main__':
    try:
        main(sys.argv)
    except KeyboardInterrupt:
        pass
    except SystemExit, se:
        print "ERROR:",se
