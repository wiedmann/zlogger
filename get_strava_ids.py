#!/usr/bin/env python
import sys, argparse, getpass
import traceback

import requests
import json
import sqlite3
import os, time, stat
import mkresults
import mysql.connector
import datetime
import dateutil.parser
from collections import namedtuple
from mysql.connector import errors as mysql_errors

global args
global dbh
global mysqldbh

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

def query_player_profile(session, access_token, player_id):
    # Query Player Profile
    # GET https://us-or-rly101.zwift.com/api/profiles/<player_id>
    try:
        response = session.get(
            url="https://us-or-rly101.zwift.com/api/profiles/%s" % player_id,
            headers={
                "Accept-Encoding": "gzip, deflate",
                "Accept": "application/json",
                "Connection": "keep-alive",
                "Host": "us-or-rly101.zwift.com",
                "User-Agent": "Zwift/115 CFNetwork/758.0.2 Darwin/15.0.0",
                "Authorization": "Bearer %s" % access_token,
                "Accept-Language": "en-us",
            },
            verify = args.verifyCert,
        )

        if args.verbose:
            print('Response HTTP Status Code: {status_code}'.format(
                status_code=response.status_code))
            print('Response HTTP Response Body: {content}'.format(
                content=response.content))

        json_dict = json.loads(response.content)

        return json_dict

    except requests.exceptions.RequestException, e:
        print('HTTP Request failed: %s' % e)

def query_profile_activities(session, access_token, player_id, limit):
    # Get Player Activities
    # GET https://us-or-rly101.zwift.com/api/profiles/<player_id>/activities

    try:
        response = session.get(
            url="https://us-or-rly101.zwift.com/api/profiles/%s/activities" % player_id,
            params={
                "before": str(int(time.time()*1000)),
                "limit": str(limit),
            },
            headers={
                "Accept-Encoding": "gzip, deflate",
                "Accept": "application/json",
                "Connection": "keep-alive",
                "Host": "us-or-rly101.zwift.com",
                "User-Agent": "Zwift/115 CFNetwork/758.0.2 Darwin/15.0.0",
                "Authorization": "Bearer %s" % access_token,
                "Accept-Language": "en-us",
            },
            verify = args.verifyCert,
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
            return json.loads(response.content)
        except ValueError:
            return None

    except requests.exceptions.RequestException, e:
        print('HTTP Request failed: %s' % e)

def query_strava_athlete_id_from_activity(session, strava_access_token, activityId):
    # Requires strava access token
    # GET https://www.strava.com/api/v3/activities/<activityId>

    try:
        response = requests.get(
            url="https://www.strava.com/api/v3/activities/%s" % activityId,
            headers={
                "Authorization": "Bearer %s" % strava_access_token,
            },
        )
        if args.verbose:
            print('Response HTTP Status Code: {status_code}'.format(
                status_code=response.status_code))
            print('Response HTTP Response Body: {content}'.format(
                content=response.content))

        if response.status_code != 200:
            if response.status_code == 403:
                raise StravaThrottled()
            print "WARNING: status code %s from Strava: %s" % (response.status_code, response.content)
        try:
            json_dict = json.loads(response.content)
        except ValueError:
            json_dict = None
        return (json_dict["athlete"] if "athlete" in json_dict else None)

    except requests.exceptions.RequestException:
        print('HTTP Request failed')

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


def updateStravaId(racedbh, session, access_token, strava_access_token, rider_ids):
    c = racedbh.cursor()
    query = "SELECT zwift_id, strava_id FROM athlete_names WHERE zwift_id in (%s)"
    format_strings = ','.join(['%s'] * len(rider_ids))
    c.execute(query % format_strings, tuple(rider_ids))
    strava_ids = {r[0]: r[1] for r in c.fetchall()}
    for zwift_rider_id in rider_ids:
        if zwift_rider_id not in strava_ids or not strava_ids[zwift_rider_id]:
            json_dict = query_profile_activities(session, access_token, zwift_rider_id, 1)
            if args.verbose:
                print ("\n")
                print (json_dict)

            if json_dict == None or len(json_dict) < 1:
                print "Error fetching rider's last zwift activity. Rider ID: %s - %s" % (zwift_rider_id, json_dict)
                continue

            lastStravaActivityId = json_dict[0]["stravaActivityId"]
            athlete_json = query_strava_athlete_id_from_activity(session, strava_access_token, lastStravaActivityId)

            # what to do if rider not connected to strava?
            if athlete_json == None:
                print "Error fetching rider's last strava activity. Rider ID: %s" % zwift_rider_id
                c.execute("replace into athlete_names (strava_id, zwift_id) VALUES (%s, %s)",
                          (0, zwift_rider_id))
                continue

            fname = athlete_json.get("firstname", '')
            lname = athlete_json.get("lastname", '')
            city = athlete_json.get("city", '')
            state = athlete_json.get("state", '')
            country = athlete_json.get("country", '')
            sex = athlete_json.get("sex", '')
            premium = athlete_json.get("premium", '')

            try:
                print "zwift_rider_id: %s, strava_athlete_id: %s, fname: %s, lname: %s" %  \
                    (zwift_rider_id, athlete_json["id"], fname.encode('ascii','ignore'), lname.encode('ascii','ignore'))
            except:
                pass

            c.execute("replace into athlete_names (strava_id, zwift_id) VALUES (%s, %s)",
                      (athlete_json["id"], zwift_rider_id))
        else:
            print "athlete_id (%s) already populated in db - skipping..." % zwift_rider_id

def main(argv):
    global args
    global dbh

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
        strava_access_token = cred['strava_access_token']

    session = requests.session()

    # test the credentials - token will expire, so we'll log in again after sleeping
    access_token, refresh_token = login(session, args.user, password)
    logout(session, refresh_token)

    racedbh = open_mysql(args)

    while True:
        try:
            c = racedbh.cursor()
            c.execute('''select distinct rider_id from rider_names left join athlete_names on zwift_id=rider_id
                         where strava_id is null and retrievaldate > %s
    ''', (datetime.datetime.utcnow() - datetime.timedelta(days=1),))
            riders = [row[0] for row in c.fetchall()]
            if riders:
                updateStravaId(racedbh, session, access_token, strava_access_token, riders)
            time.sleep(60)
        except mysql_errors.Error:
            print "Exception - reopening mysql connection in 3 seconds: %s" % traceback.format_exc()
            time.sleep(3)
            racedbh = open_mysql(args)
        except StravaThrottled:
            fifteen_minutes = 900
            sleeptime = fifteen_minutes - (time.time() % fifteen_minutes)
            if sleeptime > 0 and sleeptime < (fifteen_minutes - 5):
                print "Strava throttled - sleeping %s seconds" % sleeptime
                time.sleep(sleeptime)
        except ZwiftAuthFailed:
            access_token, refresh_token = login(session, args.user, password)

    logout(session, refresh_token)


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
