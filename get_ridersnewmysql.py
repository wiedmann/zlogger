#!/usr/bin/env python
import sys, argparse, getpass
import requests
import json
import sqlite3
import os, time, stat
import mkresults
import mysql.connector
import datetime
import dateutil.parser
from collections import namedtuple

global args
global dbh
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

def updateRider(mysqldbh, session, access_token, user):
    # Query Player Profile
    json_dict = query_player_profile(session, access_token, user)
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
    print ("id=%s wt=%s m=%s [%s] <%s %s>\n" %
        (json_dict["id"], json_dict["weight"], json_dict["male"],
         json_dict["powerSourceModel"], fname.encode('ascii', 'ignore'), lname.encode('ascii', 'ignore')))
    c = dbh.cursor()
    if mysqldbh:
        mycursor=mysqldbh.cursor()
    else:
        mycursor = None
    try:
        if mycursor:
            SQL = "REPLACE INTO rider_names (rider_id, fname, lname, age, weight, height, male, zpower, country_code) VALUES (%s,%s,TRIM(%s),%s,%s,%s,%s,%s,%s);"
            mycursor.execute(SQL, (json_dict["id"],fname,lname.encode('ascii','ignore'),json_dict["age"],
                             json_dict["weight"],json_dict["height"],male,power,json_dict["countryCode"]))
        c.execute("insert into rider " +
            "(rider_id, fname, lname, age, weight, height, male, zpower," +
            " fetched_at) " +
            "values (?,?,?,?,?,?,?,?,date('now'))",
             (json_dict["id"], fname, lname, json_dict["age"],
             json_dict["weight"], json_dict["height"], male, power))

    except sqlite3.IntegrityError:
        c.execute("update rider " +
            "set fname = ?, lname = ?, age = ?, weight = ?, height = ?," +
            " male = ?, zpower = ?, fetched_at = date('now')" +
            " where rider_id = ?",
             (fname, lname, json_dict["age"],
             json_dict["weight"], json_dict["height"], male, power,
             json_dict["id"]))

def valueIfExists(dict, key, defValue):
    if key in dict:
        return dict[key]
    else:
        return defValue

def updateStravaId(session, access_token, strava_access_token, zwift_rider_id, required_tag):
    dbh.row_factory = sqlite3.Row
    c = dbh.cursor()
    try:
        c.execute("select athlete_id from strava_lookup where rider_id=?", (zwift_rider_id,))
        r = c.fetchone()

        if r == None or r["athlete_id"] == None:
            json_dict = query_profile_activities(session, access_token, zwift_rider_id, 1)
            if args.verbose:
                print ("\n")
                print (json_dict)

            if json_dict == None or len(json_dict) < 1:
                print "Error fetching rider's last zwift activity. Rider ID: %s" % zwift_rider_id
                return

            if required_tag:
                fname = json_dict[0]["profile"]["firstName"]
                lname = json_dict[0]["profile"]["lastName"]
                if not (required_tag in lname.upper()):
                    print "Skipping non %s rider: %s %s" % (required_tag, fname.encode('ascii','ignore') , lname.encode('ascii','ignore'))
                    return

            lastStravaActivityId = json_dict[0]["stravaActivityId"]
            athlete_json = query_strava_athlete_id_from_activity(session, strava_access_token, lastStravaActivityId)

            # what to do if rider not connected to strava?
            if athlete_json == None:
                print "Error fetching rider's last strava activity. Rider ID: %s" % zwift_rider_id
                return

            fname = valueIfExists(athlete_json, "firstname", None)
            lname = valueIfExists(athlete_json, "lastname", None)
            city = valueIfExists(athlete_json, "city", None)
            state = valueIfExists(athlete_json, "state", None)
            country = valueIfExists(athlete_json, "country", None)
            sex = valueIfExists(athlete_json, "sex", None)
            premium = valueIfExists(athlete_json, "premium", None)

            print "zwift_rider_id: %s, strava_athlete_id: %s, fname: %s, lname: %s" %  \
                (zwift_rider_id, athlete_json["id"], fname.encode('ascii','ignore'), lname.encode('ascii','ignore'))

            c = dbh.cursor()
            try:
                c.execute("insert into strava_lookup " +
                    "(rider_id, athlete_id, fname, lname, city, state, country, sex, premium," +
                    " fetched_at) " +
                    "values (?,?,?,?,?,?,?,?,?,date('now'))",
                     (zwift_rider_id, athlete_json["id"], fname, lname,
                     city, state, country,
                     sex, premium))
            except sqlite3.IntegrityError:
                c.execute("update strava_lookup " +
                    "set athlete_id = ?, fname = ?, lname = ?, city = ?, state = ?, country = ?," +
                    " sex = ?, premium = ?, fetched_at = date('now')" +
                    " where rider_id = ?",
                     (athlete_json["id"], fname, lname,
                     city, state, country,
                     sex, premium,
                     zwift_rider_id))
        else:
            print "athlete_id (%s) already populated in db - skipping..." % zwift_rider_id

    except sqlite3.Error as e:
        print('An error occured: %s' % e)

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
    startTime = time.mktime(startDate.timetuple()) - window
    retrievalTime = time.mktime(startDate.timetuple()) + window
    sleepTime = retrievalTime - time.time()
    while sleepTime > 0:
        print "Sleeping %s seconds" % sleepTime
        time.sleep(sleepTime)
        sleepTime = retrievalTime - time.time()
    c = dbh.cursor()
    if hasattr(dbh, '__module__') and dbh.__module__.startswith('mysql'):
        query = '''select riderid from live_results
            where msec between %s and %s and lineid = %s order by msec asc'''
    else:
        query = '''select rider_id from pos
            where time_ms between ? and ? and line_id=? order by time_ms asc'''
    c.execute(query, (startTime * 1000, retrievalTime * 1000, line_id))
    riders = {d[0]: None for d in c.fetchall()}
    return riders.keys()

def get_line(dbh, name):
    c = dbh.cursor()
    if hasattr(dbh, '__module__') and dbh.__module__.startswith('mysql'):
        query = 'select line from chalkline where name like %s'
        exactquery = 'select line from chalkline where name = %s'
    else:
        query = 'select line_id from chalkline where name like ?'
        exactquery = 'select line_id from chalkline where name = ?'
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
    parser.add_argument('--database', default='race_database.sql3')
    parser.add_argument('-D', '--mysql_database', help='mysql database (overrides --database)')
    parser.add_argument('-H', '--mysql_host', help='mysql host')
    parser.add_argument('-U', '--mysql_user', help='mysql user')
    parser.add_argument('-P', '--mysql_password', help='mysql password')
    parser.add_argument('-L', '--line', help="line name (partial match allowed)")
    parser.add_argument('-T', '--time', help="time to get riders")
    parser.add_argument('-W', '--window', help="time window (in seconds before and after time)", type=int,
                        default=600)
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
        racedbh = mysql.connector.connect(user=args.mysql_user, password=args.mysql_password, database=args.mysql_database,
                                      host=args.mysql_host, autocommit=True)
        using_mysql = True
    else:
        racedbh = sqlite3.connect(args.database)
        using_mysql = False
    if args.config:
        L = get_rider_list(racedbh)
    elif args.idlist:
        L = args.idlist
    elif args.line:
        if args.time:
            startDate = dateutil.parser.parse(args.time)
        else:
            startDate = datetime.datetime.now()
        L = get_rider_list2(racedbh, get_line(racedbh, args.line), startDate, args.window)
    else:
        L = [int(line) for line in sys.stdin]

    if args.verbose:
        print 'Selected %d riders' % len(L)

    access_token, refresh_token = login(session, args.user, password)

    dbh = sqlite3.connect('rider_names.sql3')
    for id in L:
        updateRider(racedbh if using_mysql else None, session, access_token, id)
    dbh.commit()
    dbh.close()

    if args.query_strava_athlete_id:
        # query rider's strava athlete id and other details if optionally
        # matching the specified required tag in config
        required_tag = None
        if args.config:
            required_tag = mkresults.config(args.config).required_tag

        dbh = sqlite3.connect('results_history.sql3')
        for id in L:
            updateStravaId(session, access_token, strava_access_token, id, required_tag)
        dbh.commit()
        dbh.close()

    logout(session, refresh_token)

if __name__ == '__main__':
    try:
        main(sys.argv)
    except KeyboardInterrupt:
        pass
    except SystemExit, se:
        print "ERROR:",se
