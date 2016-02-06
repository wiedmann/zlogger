#!/usr/bin/env python
import sys, argparse
import json
import sqlite3
import os, time, stat
import re

# import requests
# import lxml.html

RICHMOND_LAP = 16 * 1000                # 1 lap of richmond = 16.09km

class rider():
    def __init__(self, id):
        self.id         = id
        self.pos        = []
        self.set_info(('Rider', str(id), None, 0, 0, None, None))

        self.finish     = []
        self.end_time   = None
        self.dq_time    = None
        self.dq_reason  = None
        self.distance   = None

    # allow accessing self via r[key]
    def __getitem__(self, k):
        return getattr(self, k)

    def __str__(self):
        return '%6d %-35.35s records: %d' % (
                self.id, self.name, len(self.pos))

    def set_info(self, v):
        if not v:
            return
        self.fname      = v[0]
        self.lname      = v[1]
        self.cat        = v[2] or 'X'
        self.weight     = v[3]
        self.height     = v[4]
        self.male       = True if v[5] else False
        self._power     = v[6] or 0                     # 0 .. 3
        self.power      = [ '?', '*', ' ', ' ' ][self._power]
        self.name       = (self.fname + ' ' + self.lname).encode('utf-8')

        cat = None
        #
        # Try autodetecting cat from name.
        #  Could filter riders by race tag also.
        #

        # NAME (X)
        #   match category in parenthesis at end of name.
        m = re.match('.*[(](.)[)]$', self.lname)
        cat = m.group(1).upper() if m else None

        # NAME X
        #    match single letter at end of name. 
        if cat is None:
            m = re.match('.*\s(.)$', self.lname)
            cat = m.group(1).upper() if m else None

        # NAME RACE-X
        #   match single letter following dash at end of name. 
        if cat is None:
            m = re.match('.*[-](.)$', self.lname)
            cat = m.group(1).upper() if m else None

        # NAME (RACE X)
        #   match single letter with trailing paren at end of name. 
        if cat is None:
            m = re.match('.*\s(.)[)]$', self.lname)
            cat = m.group(1).upper() if m else None

        # NAME RACE-X INFO
        # NAME RACE-X) INFO
        #   match single letter following dash in name.
        if cat is None:
            m = re.match('.*[-](.)[ )].*', self.lname)
            cat = m.group(1).upper() if m else None

        # NAME (X) INFO
        #   match category in parenthesis in name.
        if cat is None:
            m = re.match('.*[(](.)[)].*', self.lname)
            cat = m.group(1).upper() if m else None

        # NAME RACE X) INFO
        #   match single letter following space in name.
        if cat is None:
            m = re.match('.*\s(.)[)].*', self.lname)
            cat = m.group(1).upper() if m else None

        #
        # Sanity check cat - force to known categories.
        #
        if cat is not None:
            cat = cat if cat in 'ABCDW' else None
        if (cat is not None) and (cat != 'X'):
            self.cat = cat
        
        #
        # No Database or self-classification, report by start group.
        #
        if (args.no_cat):
            self.cat = 'X'


    #
    # Rider is DQ'd at this time for the given reason.
    #  DQ is ignored if rider completes before the timepoint.
    #
    def set_dq(self, time_ms, reason):
        if (self.dq_time is None) or (time_ms < self.dq_time):
            self.dq_time = time_ms;
            self.dq_reason = reason


    def data(self):
        return {
            'id': self.id, 'fname': self.fname, 'lname': self.lname,
            'cat': self.cat, 'height': self.height / 10,
            'weight': float(self.weight) / 1000, 
            'power': self.power,
            'male': True if self.male else False }


    #
    # Ride properties for external data views.
    #
    @property
    def height_cm(self):
        return self.height / 10

    #
    # weight is kept in grams.
    #   truncate to kilograms.
    @property
    def weight_kg(self):
        return int(self.weight / 1000)

    @property
    def sex(self):
        return 'M' if self.male else 'F'

    # XXX unknown...
    @property
    def age(self):
        return 0

    @property
    def power_type(self):
        return [None, 'zpower', 'smart', 'meter'][self._power]

    # distance ridden.
    @property
    def km(self):
        return float(self.meters) / 1000

    # returns pace in km/hr.
    @property
    def pace(self):
        if ride.msec:
            return (float(self.meters) / float(self.msec)) * 3600
        else:
            return 0

    @property
    def date(self):
        return conf.date

    @property
    def start_msec(self):
        return stamp(self.pos[0].time_ms)

    @property
    def finish_msec(self):
        return stamp(self.end.time_ms)

    @property
    def ride_msec(self):
        return elapsed(self.msec)

    @property
    def start_hr(self):
        return self.pos[0].hr

    @property
    def finish_hr(self):
        return self.end.hr

    @property
    def ride_uuid(self):
        return conf.id + '.' + conf.date + '.' + str(self.id)


def summarize_ride(r):
    s = r.pos[0]
    e = r.end

    # for DNF, use last position seen.
    if r.dnf:
        e = r.pos[-1]

    r.mwh = e.mwh - s.mwh
    r.meters = e.meters - s.meters
    r.msec = e.time_ms - s.time_ms
    watts = 0
    if r.msec:
        watts = (float(r.mwh) * 3600) / r.msec
    r.wkg = 0
    if r.weight:
        wkg = (watts * 1000) / r.weight
        r.wkg = float(int(wkg * 100)) / 100
    r.watts = int(watts)

    if (r.wkg == 0):        r.ecat = 'X'
    elif (not r.male):      r.ecat = 'W'
    elif (r.wkg > 4):       r.ecat = 'A'
    elif (r.wkg > 3.2):     r.ecat = 'B'
    elif (r.wkg > 2.5):     r.ecat = 'C'
    else:                   r.ecat = 'D'


#
# Observed position record, keyed by observation time.
#
class pos():
    def __init__(self, v):
        self.time_ms    = v[0]
        self.line_id    = v[1]
        self.forward    = v[2]
        self.meters     = v[3]
        self.mwh        = v[4]
        self.duration   = v[5]
        self.elevation  = v[6]
        self.speed      = float((v[7] or 0) / 1000)     # meters/hour
        self.hr         = v[8]

    def __str__(self):
        return ("time: %d  %s  line: %d %s  metres: %d" %
                (self.time_ms, time.ctime(self.time_ms / 1000), self.line_id,
                'FWD' if self.forward else 'REV', self.meters))

    def data(self):
        return {
            'time_ms': self.time_ms, 'mwh': self.mwh, 'line': self.line_id,
            'duration': self.duration, 'meters': self.meters,
            'hr': self.hr, 'speed': self.speed,
            'forward': True if self.forward else False }


#
# Get all position events within the specified timeframe.
#  Returns a list of riders, containing their position records.
#
def get_riders(begin_ms, end_ms):
    R = {}
    c = dbh.cursor()
    for data in c.execute('select rider_id, time_ms, line_id, forward,' +
            ' meters, mwh, duration, elevation, speed, hr from pos' +
            ' where time_ms between ? and ? order by time_ms asc',
            (begin_ms, end_ms)):
        id = data[0]
        if not id in R:
            R[id] = rider(id)
        R[id].pos.append(pos(data[1:]))
        if (args.debug):
            print id, R[id].pos[-1]
    return R


#
# Maps the chalkline name into a line_id.
#
def get_line(name):
    c = dbh.cursor()
    c.execute('select line_id from chalkline where name = ?', (name,))
    data = c.fetchone()
    if not data:
        sys.exit('Could not find line { %s }' % name)
    return data[0]


def rider_info(r):
    c = dbh.cursor()
    c.execute('select fname, lname, cat, weight, height,' +
            ' male, zpower from rider' +
            ' where rider_id = ?', (r.id,))
    r.set_info(c.fetchone())


#
# XXX - sample for organized team-only rides.
#
def get_odz(R):
    ODZ = []
    c = dbh.cursor()
    for data in c.execute('select rider_id, team, cat from odz'):
        id = data[0]
        if not id in R:
            R[id] = rider(id)
        r.cat = r[2]
        r.team = r[1]
        ODZ.append(r)
    return ODZ

MSEC_PER_HOUR   = (60 * 60 * 1000)
MSEC_PER_MIN    = (60 * 1000)
MSEC_PER_SEC    = (1000)

class msec_time():
    def __init__(self, msec):
        msec = ((msec + 99) / 100) * 100                    # roundup
        self.hour = msec / MSEC_PER_HOUR
        msec = msec - (self.hour * MSEC_PER_HOUR)
        self.min = msec / MSEC_PER_MIN
        msec = msec - (self.min * MSEC_PER_MIN)
        self.sec = msec / MSEC_PER_SEC
        msec = msec - (self.sec * MSEC_PER_SEC)
        self.msec = msec / 100                              # to 1/10th sec


#
# A generator which takes a list of finishers, sorts by finish time,
#   saves the placement and time position, then yields the result.
#
def place(F):
    place = 0
    last_ms = 0
    finish = sorted(F, key = lambda r: r.end_time)
    for r in finish:
        place = place + 1
        r.place = place
        r.timepos = make_timepos(last_ms, r.pos[0].time_ms, r.end.time_ms)
        last_ms = r.end.time_ms
        yield r


base_ms = 0
def make_timepos(prev_ms, start_ms, finish_ms):
    global base_ms
    mark = ' '

    if (prev_ms == 0):
        #
        # save winner's finish timetime - all diffs are from this
        #
        base_ms = finish_ms
        cur_ms = finish_ms - start_ms;
    elif ((finish_ms - prev_ms) < 200):
        return "--- ST ---"
    else:
        cur_ms = finish_ms - base_ms
        mark = '+'

    t = msec_time(cur_ms)

    if (t.hour != 0):
        timepos = "%2d:%02d:%02d.%d" % (t.hour, t.min, t.sec, t.msec)
    elif (t.min != 0):
        timepos = "%c  %2d:%02d.%d" % (mark, t.min, t.sec, t.msec)
    elif (t.sec != 0):
        timepos = "%c    :%02d.%d" % (mark, t.sec, t.msec)
    elif (t.msec != 0):
        timepos = "%c    :00.%d" % (mark, t.msec)
    else:
        # s.t. is transitive, return first one
        timepos = "--- ST ---";

    return timepos


def show_nf(tag, finish):
    h1 = '==== %s ' % (tag)
    h1 += '=' * (54 - len(h1))
    h1 += '  km'
    print '\n' + h1
    for r in finish:
        line = ("%-15.15s  %-35.35s  %5.1f" %
                (r.dq_reason or '',
                r.name, r.km))
        if (args.ident):
            line += "  ID %6d" % r.id
            line += '  [ ' + r.start_msec + ' - '
            if r.end:
                line += r.finish_msec + ' ]'
            if (args.debug):
                line += ' start m=' + str(r.pos[0].meters)
        print line


#
# Key is really just name.  may have "team results".
# finish records, start_time, start_lead, 
#   start group AB, but finish results in A and B.
#
# Assume all riders have same start group.
#  (otherwise the race doesn't make much sense...)
#
def show_results(F, tag):
    N = 28

    if not F:
        return
    grp = F[0].grp
    h0 = ' ' * (N + 36);
    h0 =  '== START @ %8.8s by %.22s' % (stamp(grp.start_ms),
            grp.starter.name if grp.starter else 'clock')
    h0 += ' ' + '=' * (N + 18 - len(h0))
    h0 += ' ' * 16
    h0 += ' est  ht  hrtrate'
    h1 =  '== RESULTS for %s ' % (tag)
    h1 += '=' * (N + 19 - len(h1))
    h1 += '  km  avgW  W/kg cat  cm  beg end'
    h1 += '  [ split times in km/hr ]' if args.split else ''
    h1 += '      ID [  start time  -  finish time ]' if args.ident else ''
    print '\n' + h0 + '\n' + h1

    c = dbh.cursor()
    for r in place(F):
        s = r.pos[0]
        e = r.end

        line = ("%2d. %s%c  %-*.*s  %5.1f  %3ld  %4.2f  %c  %3d  %3d %3d" % (
                r.place,
                r.timepos,
                r.power,
                N, N, r.name,
                r.km, 
                r.watts, r.wkg, r.ecat, r.height_cm,
                r.start_hr, r.finish_hr))

        if (args.split):
            l = s
            split = []
            end = r.pos.index(r.end)
            for p in r.pos[1 : end + 1]:
                dist = (p.meters - l.meters)
                msec = (p.time_ms - l.time_ms)
                pace = (float(dist) / float(msec)) * 3600
                split.append("%4.1f" % (pace))
                l = p
            dist = (l.meters - s.meters)
            msec = (l.time_ms - s.time_ms)
            pace = (float(dist) / float(msec)) * 3600
            split.append("= avg %4.1f" % (pace))
            line += '  [ %s ]' % ('  '.join(split))

        if (args.ident):
            line += '  %6d' % r.id
            line += ' [ ' + r.start_msec + ' - '
            line += r.finish_msec + ' ]'

        print line

        if (args.update_cat and (r.cat == 'X')):
            c.execute('update rider set cat = ? where rider_id = ?',
                (ecat, r.id))
    dbh.commit();


def results(tag, F):
    done = set()

    t = msec_time(-time.timezone * 1000)
    tzoff = 'UTC%+03d:%02d' % (t.hour, t.min)
    print '=' * 80
    print '=' * 10,
    print '%s   %s: %s' % (conf.date, conf.id, conf.name)
    print '=' * 10,
    print '    start: %s   cutoff: %s  %s' % (hms(conf.start_ms), hms(conf.finish_ms), tzoff)
    print '=' * 80

    #
    # create a sorted list of known rider categories
    #
    C = sorted(list(set([ r.cat for r in F ])))
    for cat in C:
        L = [ r for r in F if r.cat == cat ]
        dnf = set([ r for r in L if filter_dnf(r) ])
        dq = set([ r for r in L if filter_dq(r) ]).difference(dnf)
        finish = set(L).difference(dq).difference(dnf)
        show_results(list(finish), 'CAT ' + cat)
        done = set(done).union(finish)

    #
    # Lump all DQ/dnf together...
    #
    dnf = set([ r for r in F if filter_dnf(r) ]).difference(done)
    dq = set([ r for r in F if filter_dq(r) ]).difference(dnf)
    if len(dq):
        finish = sorted(dq, key = lambda r: r.distance, reverse = True)
        finish = [ r for r in finish if r.distance > 0 ]
        show_nf('DQ, all', finish)
    if len(dnf):
        finish = sorted(dnf, key = lambda r: r.distance, reverse = True)
        finish = [ r for r in finish if r.distance > 0 ]
        show_nf('DNF, all', finish)

    if (True):
        return

    #
    # repeat - show DQ and DNF at end.
    #
    for cat in C:
        L = [ r for r in F if r.cat == cat ]
        dnf = set([ r for r in L if filter_dnf(r) ])
        dq = set([ r for r in L if filter_dq(r) ]).difference(dnf)
        
        dq = dq - done
        dnf = dnf - done

        if len(dq):
            finish = sorted(dq, key = lambda r: r.distance)
            finish = [ r for r in finish if r.distance > 0 ]
            show_nf('DQ, CAT ' + cat, finish)
        if len(dnf):
            finish = sorted(dnf, key = lambda r: r.distance)
            finish = [ r for r in finish if r.distance > 0 ]
            show_nf('DNF, CAT ' + cat, finish)


def json_cat(F, key):
    pos = 0
    last_ms = 0
    cat_finish = []
    for r in F:
        pos = pos + 1
        s = r.pos[0]
        e = r.end

        timepos = make_timepos(last_ms, s.time_ms, e.time_ms)
        last_ms = e.time_ms

        finish = {
            'timepos': timepos, 'meters': meters,
            'mwh': mwh, 'duration': e.duration - s.duration,
            'start_msec': s.time_ms, 'end_msec': e.time_ms,
            'watts': int(watts), 'est_cat': ecat, 'pos': pos,
            'wkg': float(int(wkg * 100)) / 100,
            'beg_hr': s.hr, 'end_hr': e.hr }
        entry = { 'rider': r.data(), 'finish': finish }
        cat_finish.append(entry)

        if not args.split:
            continue

        cross = []
        end = r.pos.index(r.end)
        for p in r.pos[0 : end + 1]:
            cross.append(p.data())
        entry['cross'] = cross

    # distance, start_time

    return { 'name': key, 'results': cat_finish }


def dump_json(race_name, start_ms, F):
    result = []
    C = sorted(list(set([ r.cat for r in F ])))
    dq = set([ r for r in F if filter_dq(r) ])
    for cat in C:
        L = [ r for r in F if r.cat == cat ]
        dq = set([ r for r in L if filter_dq(r) ])
        dnf = set([ r for r in L if filter_dnf(r) ])
        finish = set(L).difference(dq).difference(dnf)
        finish = sorted(finish, key = lambda r: r.end_time)
        result.append(json_cat(finish, cat))
        if len(dq):
            finish = sorted(dq, key = lambda r: r.distance)
            finish = [ r for r in finish if r.distance > 0 ]

            # take last record as finish pos... ugh.
            for r in finish:
                r.end = r.pos[-1]
                r.end_time = r.end.time_ms
            
            result.append(json_cat(finish, 'DQ-' + cat))
        if len(dnf):
            finish = sorted(dnf, key = lambda r: r.distance)
            finish = [ r for r in finish if r.distance > 0 ]

            # take last record as finish pos... ugh.
            for r in finish:
                r.end = r.pos[-1]
                r.end_time = r.end.time_ms
            
            result.append(json_cat(finish, 'DNF-' + cat))
    race = { 'race': race_name, 'date': conf.date, 'group' : result }
    print json.dumps(race)


def min2ms(x):
    return int(x * 60 * 1000)


# timestamp msec -> H:M:S
def hms(msec):
    t = time.localtime(msec / 1000)
    return time.strftime('%H:%M:%S', t)


# timestamp msec -> H:M:S.frac
def stamp(msec):
    return hms(msec) + ('.%03d' % (msec % 1000))


# elapsed msec -> H:M:S.frac
def elapsed(msec):
    t = msec_time(msec)
    return '%02d:%02d:%02d.%03d' % (t.hour, t.min, t.sec, t.msec)


def avg_pace(start_pos, end_pos):
    msec = float(end_pos.time_ms - start_pos.time_ms)
    dist = float(end_pos.meters - start_pos.meters)
    if msec:
        return (dist / msec) * 3600
    else:
        return 0


#
# Return only those riders which fall within the start window.
#   Trim the position record to the correct start.
#
# rider may have crossed start line, then gone back and restarted.
# yes - happened in KISS race.
#  find the _last_ correct line crossing in the start window.
#   (may be larger for longer, delaeyed neutrals.
#
def filter_start(r, window):
    start = None
    for idx, p in enumerate(r.pos):
        if (p.time_ms > (conf.start_ms + min2ms(window))):
            break
        if (p.line_id == conf.start_line_id) and \
                (p.forward == conf.start_forward):
            start = idx
    if start is None:
        return False

    s = r.pos[start]

    # If there is a rider corral, and rider isn't a late starter,
    # then perform further checks.  (late starters can just fly through...)
    if conf.corral_line and \
            (s.time_ms < (conf.start_ms + (20 * MSEC_PER_SEC))):
        # Find last crossing of corral line, from start.
        for p in r.pos[start::-1]:
            if (p.line_id != conf.corral_line_id):
                continue

            #
            # make sure average pace through the corral is low.
            #
            pace = avg_pace(p, s)
            if (pace > 18):
                r.set_dq(p.time_ms, 'Corral: %2d km/h' % (pace))
            break

    del(r.pos[0:start])
    if (args.debug):
        print 'START', r.id, r.pos[0]

    #
    # DQ any riders who started more than 30 seconds early.
    #  This catches people from [start-2:00m .. start-30s]
    #
    if (r.pos[0].time_ms < (conf.start_ms - min2ms(0.5))):
        t = msec_time(conf.start_ms - r.pos[0].time_ms)
        r.set_dq(r.pos[0].time_ms, 'Early: -%2d:%02d' % (t.min, t.sec))
    return True


#
# Starting with the second position (start has already been validated)
#  check position records against course.
#  (currently just checks alternate finish line crossings for w8topia)
#
# This should trim and flag any rides which do not match the course.
#  distance and correct finish is validated later.
#
def trim_course(r):
    forward = conf.start_forward
    for idx, p in enumerate(r.pos[1:]):
        if (p.line_id != conf.finish_line_id):
            continue
        if conf.alternate is not None:
            forward = not forward
        if (p.forward != forward):
            # crossed finish line in wrong direction
            # trim the ride.  idx starts at 0, so add one.
            if (args.debug):
                print 'WRONG', r.id, '%s' % ('fwd' if forward else 'rev'), p
            r.set_dq(p.time_ms, "WRONG COURSE")
            del(r.pos[idx + 1:])
            break
    return True


#
# Trims position records and sets maximum distance.
#
def trim_crash(r):
    s = r.pos[0]
    l = s
    r.distance = 0
    for idx, p in enumerate(r.pos[1:]):
        d = p.meters - s.meters

        if (p.meters < l.meters):
            r.set_dq(p.time_ms, "----CRASHED---")
            r.distance = max(r.distance, p.meters)
            del(r.pos[idx + 1:])
            break

        r.distance = d                  # distance so far

        if (p.mwh < l.mwh):
            r.set_dq(p.time_ms, "----CRASHED---")
            del(r.pos[idx + 1:])
            break
        if (p.duration < l.duration):
            r.set_dq(p.time_ms, "----CRASHED---")
            del(r.pos[idx + 1:])
            break
    return True


#
# Better off just to have a set of results for each class.
# then results just pick the "ASSIGNED" class, or the best weighted one.
#
class grp_finish():
    def __init__(self, r, grp):
        self.grp        = grp
        self.pos        = None
        self.dq_time    = None
        self.dq_reason  = None

        r.finish.append(self)

        s = r.pos[0]
        for idx, p in enumerate(r.pos[1:]):
            if (p.meters - s.meters) >= grp.distance:
                self.pos = p
                break

        # if no end position, this is a DNF. (or crash)
        if self.pos is None:
            return

        if (r.pos[0].time_ms > grp.start_ms):
            return

        d = (grp.start_ms - r.pos[0].time_ms) / 1000

        # XXX should be configurable...
        # allow 8 second jump.
        if (d < 8):
            return

        # compute penalty if needed?
        t = msec_time(d * 1000)
        self.dq_time = grp.start_ms
        self.dq_reason = 'Early:  %2d:%02d' % (t.min, t.sec)


    #
    # Start with the time delta to this group start.
    # Straight DNF == 0
    # DQ without completing the required distance: -3
    # DQ after completing ride: 2
    # Successful ride: 10
    #
    def weight(self, r):
        weight = -abs(self.grp.start_ms - r.pos[0].time_ms) / 1000
        if self.dq_reason is not None:
            weight = weight - 3
        if self.pos is not None:
            weight = weight + 10
        return weight


#
# Select the appropriate finish group.
# Finish groups determine start time / dq / dnf.
#
def select_finish(r):
    finish = max(r.finish, key = lambda f: f.weight(r))
    if (r.cat == 'X') and args.no_cat:
        r.cat = finish.grp.name         # group all finishes together.
    else:
        #
        # match any groups which have the cat letter in their name.
        # if no match (group 'all', or cat 'W'), select the best one.
        #
        F = [ f for f in r.finish if r.cat in f.grp.name ]
        finish = max(F, key = lambda f: f.weight(r)) if F else finish
    if finish.dq_reason and (r.dq_reason is None):
        r.set_dq(finish.dq_time, finish.dq_reason)
    r.grp = finish.grp
    r.end = finish.pos
    if r.end is not None:
        r.end_time = r.end.time_ms

    #
    # only one of dnf, dq should be true.
    #
    r.dnf = True if r.end is None else False
    if ((r.dq_time is not None) and (not r.dnf) and \
            (r.dq_time < r.end.time_ms)):
        r.dq = True
    else:
        r.dq = False

    # create the ride summary data for this finish.
    summarize_ride(r)


#
# DNF = valid start, but distance < full distance.
#  DQ = valid distance, but something went wrong.
#
def filter_dq(r):
    return r.dq


def filter_dnf(r):
    return r.dnf


def strT_to_sec(val):
    m = re.match('(\d+):(\d+)', val)
    if m:
        return (int(m.group(1)) * 60) + int(m.group(2))
    m = re.match('(\d+)', val)
    if m:
        return int(m.group(1))
    sys.exit('Could not parse time %s' % val)


#
# XXX
# fix -- this is really a start group, not a single cat.  
# cat (ABCDW) are determined by rider info.
#
class config_cat_group():
    def __init__(self, name, val):
        self.name       = name
        self.lead       = None
        self.starter    = None
        self.delay_ms   = None

        m = re.match('{\s+(.*)\s+}\s+(\w+)\s+(\w+)', val)
        if not m:
            sys.exit('Unable to parse category info "%s"' % val)
        i = iter(m.group(1).split())
        d = dict(zip(i, i))
        (km, dist) = (m.group(2), m.group(3))

        dist = long(dist)
        if km == 'mi':
            dist = dist * 1.60934
        elif km != 'km':
            sys.exit('Unknown distance specifier "%s" for cat %s' %
                    (km, self.name))
        self.distance = dist * 1000

        if 'id' in d:
            self.lead = int(d['id'])
        if 'delay' in d:
            self.delay_ms = strT_to_sec(d['delay']) * 1000


class config():
    def __init__(self, fname):
        self.id                 = None
        self.name               = None
        self.start_ms           = None
        self.finish_ms          = None
        self.start_forward      = None
        self.start_line         = None
        self.finish_forward     = None
        self.finish_line        = None
        self.corral_line        = None
        self.pace_kmh           = None
        self.cutoff_ms          = None
        self.alternate          = None
        self.grp                = []        # category groups

        self.init_kw(config.__dict__)
        self.parse(fname)

    def init_kw(self, dict):
        self.kw = { f._kw : f for f in dict.values() if hasattr(f, '_kw') }

    def keyword(key):
        def wrapper(f):
            f._kw = key
            return f
        return wrapper

    @keyword('ID')
    def kw_id(self, val):
        self.id = val

    @keyword('NAME')
    def kw_name(self, val):
        self.name = val

    @keyword('ALTERNATE')
    def kw_alternate(self, val):
        self.alternate = True

    @keyword('START')
    def kw_start(self, val):
        (dir, val) = val.split(None, 1)
        self.start_forward = True if dir == 'fwd' else False
        self.start_line = self.parse_line(val)

    @keyword('CORRAL')
    def kw_corral(self, val):
        (dir, val) = val.split(None, 1)
        self.corral_forward = True if dir == 'fwd' else False
        self.corral_line = self.parse_line(val)

    @keyword('FINISH')
    def kw_finish(self, val):
        (dir, val) = val.split(None, 1)
        self.finish_forward = True if dir == 'fwd' else False
        self.finish_line = self.parse_line(val)

    @keyword('BEGIN')
    def kw_begin(self, val):
        i = iter(val.split())
        d = dict(zip(i, i))
        tm = time.localtime()
        if not 'time' in d:
            sys.exit('Must specify start time')
        t_sec = time.strptime(d['time'], '%H:%M')
        if 'date' in d:
            t_day = time.strptime(d['date'], '%Y-%m-%d')
        else:
            t_day = tm
        off = 0
        dst = tm.tm_isdst
        if 'zone' in d:
            if d['zone'] == 'zulu':
                off = int(time.time() - time.mktime(time.gmtime())) / 60
                off = off * 60
                dst = 0
            elif d['zone'] != 'local':
                m = re.match('([+-]?)(\d{2}):?(\d{2})?', d['zone'])
                if not m:
                    sys.exit('Invalid timezone syntax')
                off = int(m.group(2)) * 3600 + int(m.group(3) or 0) * 60
                off = off * -1 if m.group(1) == '-' else off
        t = time.struct_time((t_day.tm_year, t_day.tm_mon, t_day.tm_mday,
                t_sec.tm_hour, t_sec.tm_min, t_sec.tm_sec,
                0, 0, dst))
        self.start_ms = (int(time.mktime(t)) - off) * 1000
        self.date = time.strftime('%Y-%m-%d',
                time.localtime(self.start_ms / 1000))

    @keyword('CUTOFF')
    def kw_cutoff(self, val):
        i = iter(val.split())
        d = dict(zip(i, i))
        if 'pace' in d:
            self.pace_kmh = long(d['pace'])
        if 'time' in d:
            self.cutoff_ms = strT_to_sec(d['time']) * 60 * 1000

    @keyword('CAT')
    def kw_cat(self, val):
        (name, val) = val.split(None, 1)
        grp = config_cat_group(name, val)
        self.grp.append(grp)

    def parse(self, fname):
        f = open(fname, "r")
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            (key, val) = line.split(None, 1)
            if key not in self.kw:
                continue
            self.kw[key](self, val)
        #
        # Parsing of all keywords complete.
        #   Calculate interdependent variables.
        #
        if self.cutoff_ms:
            self.finish_ms = self.start_ms + self.cutoff_ms
        elif self.pace_kmh:
            m = max(self.grp, key = lambda c : c.distance)
            self.finish_ms = self.start_ms + \
                    ((m.distance * 36) / (self.pace_kmh * 10)) * 1000
        else:
            self.finish_ms = self.start_ms + ((2 * 3600) * 1000)

    def parse_line(self, val):
        m = re.match('{\s+(.*)\s+}', val)
        if not m:
            sys.exit('Could not parse %s' % val)
        return m.group(1)

    def load_chalklines(self):
        self.start_line_id = get_line(self.start_line)
        self.finish_line_id = get_line(self.finish_line)
        if self.corral_line:
            self.corral_line_id = get_line(self.corral_line)


PREFIX='''
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <meta http-equiv="X-UA-Compatible" content="IE=edge,chrome=1">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <meta name="HandheldFriendly" content="True">
  <meta name="MobileOptimized" content="320">

  <title>Race Results</title>

  <link rel="stylesheet" type="text/css"
    href="http://oss.maxcdn.com/semantic-ui/2.1.8/semantic.min.css">
</head>
<body>
'''

TITLE='''
<div class="ui fixed inverted menu">
  <div class="ui container">
    <a class="launch icon item">
      <i class="content icon"></i>
    </a>
    <div class="item">
      Label goes here
    </div>
    <div class="right menu">
      <div class="vertically fitted borderless item">
        test
      </div>
    </div>
  </div>
</div>
'''

TOPEN='''
<div class="main ui container">
<h2 class="ui dividing header">Results</h2>
<table class="ui striped table">
'''

TCLOSE='''
</table>
</div>
'''

SUFFIX='''
</body>
</html>
'''

#
# HTTP output function.
#  Takes a template (in json format) describing the database,
#  and the unfiltered rider list.
#  Creates the database if it does not exist.
#
def http(T, F):

    print PREFIX
#    print TITLE
    print '<div class="main ui container">'
    print '<h2 class="ui dividing header">Results</h2>'
    print '<h3 class="ui header">%s  %s: %s</h3>' % (conf.date, conf.id, conf.name)

    hdr = [ f['name'] for f in T['fields'] ]
    cls = [ d['class'] if 'class' in d else '' for d in T['fields'] ]
    fld = [ f['value'] for f in T['fields'] ]

    dnf = set([ r for r in F if r.dnf ])
    dq  = set([ r for r in F if r.dq ])
    finish = set(F) - dnf - dq

    colors = { 'A': 'orange', 'B': 'teal', 'C': 'green', 'D': 'yellow',
               'W': 'pink', 'X': 'black' }
    C = sorted(list(set([ r.cat for r in finish ])))
    for cat in C:
        L = [ r for r in finish if r.cat == cat ]

        print '<h4 class="ui horizontal divider header">'
        print 'Cat %s' % cat
        print '</h4>'
        print '<table class="ui %s striped table">' % (colors[cat])
        print '<thead><tr>'
        for idx, f in enumerate(hdr):
            print '<th%s>%s</th>' % (cls[idx], f)
        print '</tr></thead><tbody>'

        for r in place(L):
            val = [ str(r[k]) for k in fld ]
            for idx, f in enumerate(val):
                esc = f.replace(' ', '&nbsp')
                print '<td%s>%s</td>' % (cls[idx], esc)
            print '</tr>'
        print '</tbody>'
        print '</table>'

    print SUFFIX

#
# MySQL output function.
#  Takes a template (in json format) describing the database
#  and the unfiltered rider list.
#  Creates the database if it does not exist.
#
#  XXX does not drop rows.... need to fix this.
#
def mysql(T, F):
    import MySQLdb

    msql = MySQLdb.connect(user = T['user'], db = T['db'])
    c = msql.cursor()

    c.execute('show tables like %s;', (T['table'],))
    if not c.fetchone():
        fld = []
        for f in T['fields']:
            fld.append("%s %s" % (f['name'], f['type']))
        sql = 'create table %s (%s);' % (T['table'], ', '.join(fld))
        c.execute(sql)
        msql.commit()

    fld = [ f['name'] for f in T['fields'] ]
    sql = 'insert into %s (%s) values (%s);' % \
            (T['table'], ', '.join(fld),
             ', '.join([ "%s" for f in fld]))

    fld = [ f['value'] for f in T['fields'] ]
    dnf = set([ r for r in F if r.dnf ])
    dq  = set([ r for r in F if r.dq ])
    finish = set(F) - dnf - dq
    for r in place(finish):
        val = [ str(r[k]) for k in fld ]
        c.execute(sql, val)
    msql.commit()
    msql.close()


global args
global conf
global dbh

def main(argv):
    global args
    global conf
    global dbh

    parser = argparse.ArgumentParser(description = 'Race Result Generator')
    parser.add_argument('-j', '--json', action='store_true',
            help='JSON output')
    parser.add_argument('-s', '--split', action='store_true',
            help='Generate split results')
    parser.add_argument('-I', '--idlist', action='store_true',
            help='List of IDs that need names')
    parser.add_argument('-d', '--debug', action='store_true',
            help='Debug things')
    parser.add_argument('-i', '--ident', action='store_true',
            help='Show Zwift identifiers')
    parser.add_argument('-u', '--update_cat', action='store_true',
            help='Update rider database with their estimated ride category')
    parser.add_argument('-r', '--result_file', action='store_true',
            help='Write results into correctly named file')
    parser.add_argument('--database', default='race_database.sql3',
            help='Specify source .sql3 database')
    parser.add_argument('--output', help='Output format specification')
    parser.add_argument('-n', '--no_cat', action='store_true',
            help='Do not perform automatic category assignemnts from names')
    parser.add_argument('config_file', help='Configuration file for race.')
    args = parser.parse_args()

    #
    # config needs to read the chalkline id's from the database, but
    #  the database name is configurable.  Delay loading chalklines
    #  until the after the configuration is parsed.
    #
    conf = config(args.config_file)
    dbh = sqlite3.connect(args.database)
    conf.load_chalklines()

    if (args.debug):
        print "START", 'fwd' if conf.start_forward else 'rev', \
                conf.start_line_id, conf.start_line
        print "FINISH", 'fwd' if conf.finish_forward else 'rev', \
                conf.finish_line_id, conf.finish_line

#    c = dbh.cursor()
#    c.execute('select max(time_ms) from event where event = ?', ('STARTUP',));
#    s = c.fetchone();

    if (args.debug):
        print('time: %s .. %s' %
                (time.ctime(conf.start_ms / 1000),
                time.ctime(conf.finish_ms / 1000)))
        print('time: [%d .. %d]' % (conf.start_ms, conf.finish_ms))

    #
    # Look back 2 minutes just to get riders who cross over the start line
    # really early.
    #
    R = get_riders(conf.start_ms - min2ms(2.0), conf.finish_ms)
    if (args.debug):
        print 'Selected %d riders' % len(R)

    START_WINDOW = 10.0         # from offical race start time.

    #
    # Cut rider list down to only those who crossed the start line
    # in the correct direction from the time the race started.
    #
    F = R.values()
    F = [ r for r in F if filter_start(r, START_WINDOW) ]

    # pull names from the database.
    [ rider_info(r) for r in F ]

    #
    # dump list of riders needing their names fetched.
    # this is fed into an external tool, which pulls the records from
    # Zwift and writes them into the database.
    #
    if (args.idlist):
#        L = [ r.id for r in F if r.fname == 'Rider' ]
        L = [ r.id for r in F ]
        print '\n'.join(map(str, L))
        return

    #
    # Trim position records.
    #
    [ trim_course(r) for r in F ]
    [ trim_crash(r) for r in F ]

    #
    # Create cat result records.  Riders have records for every cat group.
    #   If the rider's cat is known, the correct record is used.
    #   When autotecting cat, the highest weighted finish record is used.
    #
    for grp in conf.grp:
        if (grp.lead is not None) and (grp.lead in R):
            grp.starter = R[grp.lead]
            grp.start_ms = grp.starter.pos[0].time_ms
        elif grp.delay_ms is not None:
            grp.start_ms = conf.start_ms + grp.delay_ms
        else:
            grp.start_ms = conf.start_ms

        [ grp_finish(r, grp) for r in F ]

    #
    # Set rider cat here, in order to select the correct finish record.
    #  Cat 'X' == unknown, which autoselects the best record.
    #

    #
    # Now, select the matching finish record (or best weighted one)
    #  this also creates the ride summary information for the finish,
    #  but not the ride placement.
    #
    [ select_finish(r) for r in F ]

    if (args.result_file):
        fname = conf.id + '.' + conf.date
        fname += '.json' if args.json else '.txt'
        print "Writing results to %s" % fname
        sys.stdout = open(fname, 'w')

    if (args.output):
        f = open(args.output, "r")
        try:
            out = json.load(f)
        except ValueError, se:
            sys.exit('"%s": %s' % (args.output, se))
        f.close
        if 'output' not in out or \
                out['output'] not in globals():
            sys.exit('Unknown output function.')
        f = globals()[out['output']]
        f(out, F)
        print "Completed output for %s" % (args.output)
        return

    if (args.json):
        dump_json(conf.id, conf.start_ms, F)
    else:
        results(conf.id, F)

    dbh.close()

if __name__ == '__main__':
    try:
        main(sys.argv)
    except KeyboardInterrupt:
        pass
    except SystemExit, se:
        print "ERROR:", se
