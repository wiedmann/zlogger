#!/usr/bin/env python
import sys, argparse, getpass
import traceback
from heapq import heappush, heappop

import requests
import json
import os, time, stat
import mkresults
import mysql.connector
import datetime
import dateutil.parser
from collections import namedtuple
from mysql.connector import errors as mysql_errors

global args
global mysqldbh

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
        print('HTTP Request failed: %s' % traceback.format_exc())

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

def query_subgroup_profiles(session, access_token, subgroup_id):
    # Query Player Profile
    # GET https://us-or-rly101.zwift.com/api/profiles/<player_id>
    try:
        response = session.get(
            url="https://us-or-rly101.zwift.com/api/events/subgroups/entrants/%s?participation=signed_up&registered_before=0&start=0&limit=0&type=all" % (subgroup_id,),
            headers={
                "Accept-Encoding": "gzip, deflate",
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
            print('entrants Response HTTP Status Code: {status_code}'.format(
                status_code=response.status_code))
            print('entrants Response HTTP Response Body: {content}'.format(
                content=response.content))

        json_dict = []
        try:
            json_dict = json.loads(response.content)
        except ValueError:
            print "Can't parse response for subgroup %s:\n%s" % (subgroup_id, response.content)

        return json_dict

    except requests.exceptions.RequestException, e:
        print('HTTP Request failed: %s' % e)

def query_event_info(session, access_token, event_id):
    # Query Player Profile
    # GET https://us-or-rly101.zwift.com/api/profiles/<player_id>
    try:
        response = session.get(
            url="https://us-or-rly101.zwift.com/api/events/%s" % (event_id,),
            headers={
                "Accept-Encoding": "gzip, deflate",
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
            print('event Response HTTP Status Code: {status_code}'.format(
                status_code=response.status_code))
            print('event Response HTTP Response Body: {content}'.format(
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

        return json.loads(response.content)

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

        json_dict = json.loads(response.content)
        return (json_dict["athlete"] if "athlete" in json_dict else None)

    except requests.exceptions.RequestException:
        print('HTTP Request failed: %s' % traceback.format_exc())

def logout(session, refresh_token):
    # Logout
    # POST https://secure.zwift.com/auth/realms/zwift/tokens/logouts
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
        print('HTTP Request failed: %s' % traceback.format_exc())

def login(session, user, password):
    access_token, refresh_token, expired_in = post_credentials(session, user, password)
    return access_token, refresh_token

def updateRidersForSubgroup(mysqldbh, session, access_token, event_id=None, race_id=None, grp=0, zwift_event_id=None):
    json_dict = query_subgroup_profiles(session, access_token, grp)
    for entry in json_dict:
        updateRider(mysqldbh, entry, event_id, race_id, grp, zwift_event_id)

def updateRidersForEvent(mysqldbh, session, access_token, event_id=None, race_id=None, zwift_event_id=0):
    json_dict = query_event_info(session, access_token, zwift_event_id)
    for subgroup in json_dict['eventSubgroups']:
        updateRidersForSubgroup(mysqldbh, session, access_token, event_id, race_id, subgroup['id'])

def queryAndUpdateRider(mysqldbh, session, access_token, user, event_id=None, race_id=None, grp=0):
    json_dict = query_player_profile(session, access_token, user)
    return updateRider(mysqldbh, json_dict, event_id, race_id, grp)


def updateRider(mysqldbh, json_dict, event_id = None, race_id = None, grp=0, zwift_event_id=None):
    # Query Player Profile
    if args.verbose:
        print ("\n")
        print (json_dict)
    male = 1 if json_dict["male"] else 0
    # Power Meter, Smart Trainer, zPower
    if (json_dict["powerSourceModel"] == "zPower"):
        power = 1
    elif (json_dict["powerSourceModel"] == "Smart Trainer"):
        power = 2
    else:
        power = 3
    fname = json_dict["firstName"].strip()
    lname = json_dict["lastName"].strip()
    if race_id and not race_id.lower() in lname.lower():
        lname = lname + ' ' + race_id
    try:
        print ("id=%s wt=%s m=%s [%s] <%s %s>\n" %
            (json_dict["id"], json_dict["weight"], json_dict["male"],
             json_dict["powerSourceModel"], fname.encode('ascii', 'ignore'), lname.encode('ascii', 'ignore')))
    except:
        pass
    if mysqldbh:
        mycursor=mysqldbh.cursor()
    else:
        mycursor = None
    if mycursor:
        SQL = '''REPLACE INTO rider_names (rider_id, fname, lname, age, weight, height, male, zpower, country_code, event, virtualBikeModel, achievementLevel, totalDistance, totalDistanceClimbed, totalTimeInMinutes, totalInKomJersey,totalInSprintersJersey, totalInOrangeJersey, totalWattHours, totalExperiencePoints, grp, zwift_event_id)
                  VALUES (%s,%s,TRIM(%s),%s,%s,%s,%s,%s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);'''
        mycursor.execute(SQL, (json_dict["id"], fname.encode('utf-8', 'ignore'), lname.encode('utf-8', 'ignore'), json_dict["age"],
                               json_dict["weight"], json_dict["height"], male, power, json_dict["countryCode"],
                               event_id, json_dict["virtualBikeModel"],json_dict["achievementLevel"],
                               json_dict["totalDistance"],json_dict["totalDistanceClimbed"],json_dict["totalTimeInMinutes"],
                               json_dict["totalInKomJersey"],json_dict["totalInSprintersJersey"],json_dict["totalInOrangeJersey"],
                               json_dict["totalWattHours"],json_dict["totalExperiencePoints"], grp, zwift_event_id))
        mysqldbh.commit()
        mycursor.close()

def valueIfExists(dict, key, defValue):
    if key in dict:
        return dict[key]
    else:
        return defValue

def updateStravaId(racedbh, session, access_token, strava_access_token, rider_ids, required_tag):
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
                print "Error fetching rider's last zwift activity. Rider ID: %s" % zwift_rider_id
                continue

            if required_tag:
                fname = json_dict[0]["profile"]["firstName"]
                lname = json_dict[0]["profile"]["lastName"]
                if not (required_tag in lname.upper()):
                    print "Skipping non %s rider: %s %s" % (required_tag, fname.encode('ascii','ignore') , lname.encode('ascii','ignore'))
                    continue

            lastStravaActivityId = json_dict[0]["stravaActivityId"]
            athlete_json = query_strava_athlete_id_from_activity(session, strava_access_token, lastStravaActivityId)

            # what to do if rider not connected to strava?
            if athlete_json == None:
                print "Error fetching rider's last strava activity. Rider ID: %s" % zwift_rider_id
                continue

            fname = valueIfExists(athlete_json, "firstname", '')
            lname = valueIfExists(athlete_json, "lastname", '')
            city = valueIfExists(athlete_json, "city", '')
            state = valueIfExists(athlete_json, "state", '')
            country = valueIfExists(athlete_json, "country", '')
            sex = valueIfExists(athlete_json, "sex", '')
            premium = valueIfExists(athlete_json, "premium", '')

            print "zwift_rider_id: %s, strava_athlete_id: %s, fname: %s, lname: %s" %  \
                (zwift_rider_id, athlete_json["id"], fname.encode('ascii','ignore'), lname.encode('ascii','ignore'))

            c = racedbh.cursor()
            c.execute("replace into athlete_names (strava_id, zwift_id) VALUES (%s, %s)",
                      (athlete_json["id"], zwift_rider_id))
        else:
            print "athlete_id (%s) already populated in db - skipping..." % zwift_rider_id


def get_rider_list(dbh):
    mkresults.dbh = dbh
    conf = mkresults.config(args.config)
    mkresults.conf = conf
    mkresults.args = namedtuple('Args', 'no_cat debug')(no_cat=False, debug=args.verbose)

    startTime = conf.start_ms / 1000
    retrievalTime = startTime + conf.start_window_ms / 1000
    sleepTime = retrievalTime - time.time()
    while sleepTime > 0:
        print "Sleeping %s seconds" % sleepTime
        time.sleep(sleepTime)
        sleepTime = retrievalTime - time.time()
    conf.load_chalklines()
    R, all_pos = mkresults.get_riders(conf.start_ms - conf.lookback_ms,
            conf.finish_ms)
    return [ r.id for r in R.values() if mkresults.filter_start(r) ]

def get_rider_list2(dbh, line_id, startDate, window):
    if type(startDate) == int:
        startTime = startDate - window
    else:
        startTime = time.mktime(startDate.timetuple()) - window
    retrievalTime = startTime + (2 * window)
    sleepTime = retrievalTime - time.time()
    while sleepTime > 0:
        print "Sleeping %s seconds (%s - %s)" % (sleepTime, retrievalTime, time.time())
        time.sleep(sleepTime)
        sleepTime = retrievalTime - time.time()
    c = dbh.cursor()
    query = '''select riderid, grp from live_results
            where msec between %s and %s and lineid = %s order by msec asc'''
    c.execute(query, (startTime * 1000, retrievalTime * 1000, line_id))
    riders = {d[0]: d[1] for d in c.fetchall()}
    c.close()
    return riders

def get_event_module_riders(dbh, startDate, window):
    if type(startDate) == int:
        startTime = startDate - window
    else:
        startTime = time.mktime(startDate.timetuple()) - window
    retrievalTime = startTime + (2 * window)
    sleepTime = retrievalTime - time.time()
    while sleepTime > 0:
        print "Sleeping %s seconds (%s - %s)" % (sleepTime, retrievalTime, time.time())
        time.sleep(sleepTime)
        sleepTime = retrievalTime - time.time()
    c = dbh.cursor()
    query = '''select distinct riderid, grp from live_results
        where msec between %s and %s and grp > 0 order by msec asc'''
    c.execute(query, (startTime * 1000, retrievalTime * 1000))
    riders = {d[0]: d[1] for d in c.fetchall()}
    c.close()
    return riders

def get_line(dbh, name):
    c = dbh.cursor()
    query = 'select line from chalkline where name like %s'
    exactquery = 'select line from chalkline where name = %s'
    c.execute(exactquery, (name,))
    data = c.fetchall()
    if not data:
        c.execute(query, (name + '%',))
        data = c.fetchall()
    if not data:
        sys.exit('Could not find line { %s }' % name)
    if len(data) > 1:
        sys.exit("More than one line matches '%s'" % name)
    return int(data[0][0])

def get_line_info(dbh, id):
    c = dbh.cursor()
    c.execute('''select name, race_corral_exit from chalkline where line = %s ''', (id,))
    data = c.fetchall()
    if data:
        return data[0][0], data[0][1]
    return None, None

def process_line(dbh, line_id, start_time, start_window, user, password, event_id = None, race_id=None, event_module=0):
    if event_module:
        L = get_event_module_riders(dbh, start_time, start_window)
    else:
        L = get_rider_list2(dbh, line_id, start_time, start_window)
    session = requests.session()
    line_name, race_corral_exit = get_line_info(dbh, line_id)
    access_token, refresh_token = login(session, user, password)
    for (id, grp) in L.items():
        queryAndUpdateRider(dbh, session, access_token, id, event_id if race_corral_exit else None,
                    race_id if race_corral_exit else None, grp=grp)
    logout(session, refresh_token)


def get_subgroup_retrieval_times(dbh, start_time):
    start_delay = 60
    retrieval_times = [0, 900, 1800, 2700, 3600, 4500, 5400]

    c = dbh.cursor()
    retrieval_time = start_time - (retrieval_times[-1] + start_delay)
    query = '''select zes.id, unix_timestamp(zes.eventSubgroupStart) as start_time, ze.name as event_name, ze.id
               from zwift_event_subgroups zes, zwift_events ze where zes.zwift_event_id = ze.id
               and zes.eventSubgroupStart > from_unixtime(%s) and zes.eventSubgroupStart < from_unixtime(%s)
               order by start_time asc'''
    c.execute(query, (retrieval_time, retrieval_time + 7200))
    subgroup_retrievals = []
    for row in c.fetchall():
        (subgroup_id, subgroup_start_time, event_name, zwift_event_id) = row
        if 'race' in event_name.lower():
            for rt in retrieval_times:
                subgroup_retrieval_time = subgroup_start_time + start_delay + rt
                if subgroup_retrieval_time > start_time:
                    heappush(subgroup_retrievals, (subgroup_start_time + start_delay + rt, (subgroup_id, event_name, zwift_event_id)))
        else:
            subgroup_retrieval_time = subgroup_start_time + start_delay
            if subgroup_retrieval_time > start_time:
                heappush(subgroup_retrievals, (subgroup_start_time + start_delay, (subgroup_id, event_name, zwift_event_id)))

    c.close()
    return subgroup_retrievals


def run_server(dbh, args, user, password):
    if args.time:
        startDate = dateutil.parser.parse(args.time)
        last_retrieval = time.mktime(startDate.timetuple())
    else:
        last_retrieval = time.time()

    while True:
        try:
            now = time.time()
            retrieval_times = get_subgroup_retrieval_times(dbh, last_retrieval)
            cursor = dbh.cursor()
            cursor.execute('''select event_date, start_line_id, start_window, event_date + start_window as wake_time,
                              title, event_id, race_id, event_module, zwift_event_id
                              from event_detail where (event_date + start_window) > %s order by wake_time ASC limit 10''',
                           (last_retrieval,))
            sleep_time = 60
            for row in cursor.fetchall():
                (event_date, start_line_id, start_window, wake_time, title, event_id, race_id, event_module, zwift_event_id) = row
                if not zwift_event_id:
                    heappush(retrieval_times, (wake_time, [title, start_line_id, event_date, start_window, event_id, race_id, event_module]))
            last_processed = None
            while retrieval_times and retrieval_times[0][0] <= now:
                wake_time, data = heappop(retrieval_times)
                zwift_event_id = None
                if type(data) is list:
                    (title, start_line_id, event_date, start_window, event_id, race_id, event_module) = data
                else:
                    (grp, title, zwift_event_id) = data
                print "Getting riders for %s" % title.encode('ascii', 'ignore')
                if grp:
                    session = requests.session()
                    access_token, refresh_token = login(session, user, password)
                    updateRidersForSubgroup(dbh, session, access_token, 0,
                                         race_id if args.append_race_id else None, grp, zwift_event_id)
                else:
                    process_line(dbh, start_line_id, event_date, start_window, user, password, event_id,
                                 race_id if args.append_race_id else None, event_module)
                last_processed = wake_time
            if last_processed:
                last_retrieval = last_processed
            else:
                last_retrieval = now
            if retrieval_times:
                wake_time, data = retrieval_times[0]
                if type(data) is list:
                    (title, start_line_id, event_date, start_window, event_id, race_id, event_module) = data
                else:
                    (grp, title, zwift_event_id) = data
                sleep_time = min(sleep_time, wake_time - now)
                print("Next wake time in %s seconds for %s, sleeping %s seconds" % (wake_time - now, title.encode('ascii', 'ignore'), sleep_time))
            else:
                print("No races in near future, sleeping %s seconds" % sleep_time)
            if sleep_time > 0:
                time.sleep(sleep_time)
        except mysql_errors.Error:
            print "Mysql exception %s" % traceback.format_exc()
            try:
                dbh.close()
            except:
                pass
            dbh = open_mysql(args)


def main(argv):
    global args
    global dbh

    access_token = None
    strava_access_token = None
    cookies = None

    parser = argparse.ArgumentParser(description = 'Zwift Name Fetcher')
    parser.add_argument('-v', '--verbose', action='store_true',
            help='Verbose output')
    parser.add_argument('--dont-check-certificates', action='store_false',
            dest='verifyCert', default=True)
    parser.add_argument('-c', '--config', help='Use config file')
    parser.add_argument('-u', '--user', help='Zwift user name')
    parser.add_argument('-q', '--query_strava_athlete_id', action='store_true',
            help='populate zwift rider_id to strava athlete_id mapping')
    parser.add_argument('idlist', metavar='rider_id', type=int, nargs='*',
            help='rider ids to fetch')
    parser.add_argument('-D', '--mysql_database', help='mysql database (overrides --database)')
    parser.add_argument('-H', '--mysql_host', help='mysql host')
    parser.add_argument('-U', '--mysql_user', help='mysql user')
    parser.add_argument('-P', '--mysql_password', help='mysql password')
    parser.add_argument('-L', '--line', help="line name (partial match allowed)")
    parser.add_argument('-T', '--time', help="time to get riders")
    parser.add_argument('-W', '--window', help="time window (in seconds before and after time)", type=int,
                        default=600)
    parser.add_argument('--no_profile', help="Don't update profile (usually used with -q)", action="store_true")
    parser.add_argument('--server', help="Run in server mode, monitoring event_detail table", action="store_true")
    parser.add_argument('--append_race_id', help="In server mode, append race id to user's name if not present",
                        action="store_true")
    parser.add_argument('--race_id')
    parser.add_argument('--event_id')
    parser.add_argument('--subgroup_id', help="The zwift subgroup ID to query")
    parser.add_argument('--zwift_event_id', help="The zwift event ID to query")
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

    if args.mysql_database:
        racedbh = open_mysql(args)
        using_mysql = True
    if args.server:
        run_server(racedbh, args, args.user, password)
        exit()
    if args.config:
        L = get_rider_list(racedbh)
    elif args.idlist:
        L = {id:0 for id in args.idlist}
    elif args.line:
        if args.time:
            startDate = dateutil.parser.parse(args.time)
        else:
            startDate = datetime.datetime.now()
        L = get_rider_list2(racedbh, get_line(racedbh, args.line), startDate, args.window)
    elif args.subgroup_id or args.zwift_event_id:
        L = None
    else:
        L = {int(line):0 for line in sys.stdin}

    if args.verbose:
        print 'Selected %d riders' % len(L)

    access_token, refresh_token = login(session, args.user, password)

    if L:
        if not args.no_profile:
            for (id, grp) in L.items():
                queryAndUpdateRider(racedbh, session, access_token, id, args.event_id, args.race_id, grp)

        if args.query_strava_athlete_id:
            # query rider's strava athlete id and other details if optionally
            # matching the specified required tag in config
            required_tag = None
            if args.config:
                required_tag = mkresults.config(args.config).required_tag

            updateStravaId(racedbh, session, access_token, strava_access_token, L, required_tag)
    elif args.subgroup_id:
        updateRidersForSubgroup(racedbh, session, access_token, args.event_id, args.race_id, args.subgroup_id)
    elif args.zwift_event_id:
        updateRidersForEvent(racedbh, session, access_token, args.event_id, args.race_id, args.zwift_event_id)

    logout(session, refresh_token)


def open_mysql(args):
    return mysql.connector.connect(user=args.mysql_user, password=args.mysql_password, database=args.mysql_database,
                                   host=args.mysql_host, autocommit=True, charset="utf8mb4")


if __name__ == '__main__':
    try:
        main(sys.argv)
    except KeyboardInterrupt:
        pass
    except SystemExit, se:
        print "ERROR:",se
