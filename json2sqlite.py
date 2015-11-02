#!/usr/bin/env python

from __future__ import print_function

import sys
import argparse
import sqlite3
import bz2
import gzip
import json
import six
import datetime as D

if sys.version_info > (3,):
    unicode = str


def logTime(chkpoint):
    print('*** Checkpoint: {} at \x1b[31m{}\x1b[0m'.format(chkpoint, D.datetime.now()))
    sys.stdout.flush()


def JSONReader(file_obj):
    '''Returns a parsed JSON object per line of the file_obj passed.'''
    for line in file_obj:
        line = line.strip()
        if len(line) == 0:
            continue

        yield json.loads(line)


def guess_types(reader, max_sample_size=100, headers=None):
    '''Guess column types (as for SQLite) of JSON.

    Source code from: csv2sqlite.py
    '''

    if headers is None:
        _headers = sorted(six.advance_iterator(reader).keys())
    else:
        _headers = sorted(headers)

    # we default to text for each field
    num_columns = len(_headers)
    types = ['text'] * num_columns
    # order matters
    # (order in form of type you want used in case of tie to be last)

    options = [
        ('text', unicode),
        ('real', float),
        ('integer', int)
        # 'date',
        ]
    # for each column a set of bins for each type counting successful casts
    perresult = {
        'integer': 0,
        'real': 0,
        'text': 0
        }

    results = [dict(perresult) for x in range(num_columns)]
    sample_counts = [0 for x in range(num_columns)]

    for row_index,row in enumerate(([jsonElem[x] if x in jsonElem else '' for x in _headers]
                                    for jsonElem in reader)):
        for column,cell in enumerate(row):
            cell = unicode(cell).strip()
            if len(cell) == 0:
                continue

            # replace ',' with '' to improve cast accuracy for ints and floats
            if(cell.count(',') > 0):
               cell = cell.replace(',', '')
               if(cell.count('E') == 0):
                  cell = cell + "E0"

            for data_type,cast in options:
                try:
                    cast(cell)
                    results[column][data_type] += 1
                    sample_counts[column] += 1
                except ValueError:
                    pass

        have_max_samples = True
        for column,cell in enumerate(row):
            if sample_counts[column] < max_sample_size:
                have_max_samples = False

        if have_max_samples:
            break

    for column,colresult in enumerate(results):
        for _type, _ in options:
            if colresult[_type] > 0 and colresult[_type] >= colresult[types[column]]:
                types[column] = _type

    return types, _headers



argParser = argparse.ArgumentParser()
argParser.add_argument('jsonfile',
        help='The file which contains the json data fields '
             'from which to populate the sqlite3 db.')
argParser.add_argument('sqlitedb',
        help='The database file which should be populated. '
             'This file will be created if it does not exist.')
argParser.add_argument('table',
        help='The table to which to add the data.'
             'It will be created if it doe snot exist.')
argParser.add_argument('--headers',
        help='List of headers, one in each line.',
        default=None)

group = argParser.add_mutually_exclusive_group()
group.add_argument('--gzip',
        help='Assume file uses the gzip compression.',
        action='store_true')
group.add_argument('--bz2',
        help='Assume file uses bz2 compression.',
        action='store_true')

args = argParser.parse_args()

inputFile = None
if args.bz2:
    inputFile = bz2.BZ2File(args.jsonfile, 'rt')
elif args.gzip:
    inputFile = gzip.open(args.jsonfile, 'rt')
else:
    inputFile = open(args.jsonfile, 'rt')

if args.headers is not None:
    with open(args.headers, 'rt') as headersFile:
        providedHeaders = [x.strip() for x in headersFile.readlines()]
else:
    providedHeaders = None

types, headers = guess_types(JSONReader(inputFile), headers=providedHeaders)
num_columns = len(headers)
inputFile.seek(0)

columns = ','.join(
    ['"%s" %s' % (header, _type) for (header, _type) in zip(headers, types)]
    )

conn = sqlite3.connect(args.sqlitedb)

# Cannot handle non-ASCII input?
conn.text_factory = str

cur = conn.cursor()

try:
    create_query = 'CREATE TABLE %s (%s)' % (args.table, columns)
    cur.execute(create_query)
    logTime('Created table {}'.format(args.table))
except:
    logTime('Skipping creation of table {}'.format(args.table))

insert_query = 'INSERT INTO %s VALUES (%s)' % (args.table, ','.join(['?'] * num_columns))

line = 0
try:
    for jsonElem in JSONReader(inputFile):
        line += 1
        row = [jsonElem[x] if x in jsonElem else '' for x in headers]
        try:
            row = [
                None if x == ''
                else float(unicode(x).replace(',', '')) if y == 'real'
                else int(x) if y == 'integer'
                else unicode(x) for (x,y) in zip(row, types) ]
            cur.execute(insert_query, row)
        except ValueError as e:
            print("Unable to convert value '%s' to type '%s' on line %d" % (x, y, line), file=sys.stderr)
        except Exception as e:
            print("Error on line %d: %s" % (line, e), file=sys.stderr)
except Exception as e:
    print('General error on line %d: %s' % (line, e), file=sys.stderr)
    logTime('Rolling back changes')
    conn.rollback()
    cur.close()
else:
    logTime('Committing to disk')
    conn.commit()
    cur.close()

logTime('Finished')

