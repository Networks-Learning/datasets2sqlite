#!/usr/bin/env python
from __future__ import print_function
import sqlite3
import gzip
import bz2
import sys
import argparse
import datetime as D

def logTime(chkpoint):
    print('*** Checkpoint: {} at \x1b[31m{}\x1b[0m'.format(chkpoint, D.datetime.now()))
    sys.stdout.flush()


argParser = argparse.ArgumentParser()
argParser.add_argument('quotesFile',
        help='The file to read quotes from.')
argParser.add_argument('sqlitedb',
        help='The sqlite table to fill.')
argParser.add_argument('table_prefix',
        help='The prefix of table names in SQLite.')

group = argParser.add_mutually_exclusive_group()
group.add_argument('--gzip',
        help='Assume file uses the gzip compression.',
        action='store_true')
group.add_argument('--bz2',
        help='Assume file uses bz2 compression.',
        action='store_true')

args = argParser.parse_args()

if args.bz2:
    inputFile = bz2.BZ2File(args.quotesFile, 'rU')
elif args.gzip:
    inputFile = gzip.open(args.quotesFile, 'rU')
else:
    inputFile = open(args.quotesFile, 'rU')

conn = sqlite3.connect(args.sqlitedb)

# Always return bytestring instead of unicode.
conn.text_factory = str

cur = conn.cursor()

table_time = args.table_prefix + '_times'
columns_time = '"URL" TEXT, "Time" TEXT'

table_quotes = args.table_prefix + '_quotes'
columns_quotes = '"URL" TEXT, "Quote" TEXT'

table_links = args.table_prefix + '_links'
columns_links = '"URL" TEXT, "Link" TEXT'

for table, columns in [(table_time, columns_time),
                       (table_quotes, columns_quotes),
                       (table_links, columns_links)]:
    try:
        create_query = 'CREATE TABLE %s (%s)' % (table, columns)
        cur.execute(create_query)
        logTime('Created table {}'.format(table))
    except:
        logTime('Skipping creation of table {}'.format(table))

insert_time_query = 'INSERT INTO %s VALUES (?, ?)' % (table_time,)
insert_links_query = 'INSERT INTO %s VALUES (?, ?)' % (table_links,)
insert_quotes_query = 'INSERT INTO %s VALUES (?, ?)' % (table_quotes,)


def blockReader(inputFile):
    '''Read one Memetracker block from the passed file.'''
    while True:
        try:
            block = {}

            urlLine = inputFile.readline()
            if urlLine == '':
                break
            assert urlLine[0] == 'P', "First line was not a page."
            block['P'] = urlLine[1:].strip()

            timeLine = inputFile.readline()
            if timeLine == '':
                break
            assert timeLine[0] == 'T', "Second line was not a time."
            block['T'] = timeLine[1:].strip()

            # Read Quotes
            line = inputFile.readline()
            block['Q'] = []
            while line != '' and line != '\n' and line[0] == 'Q':
                block['Q'].append(line[1:].strip())
                line = inputFile.readline()

            # Read Links
            block['L'] = []
            while line != '' and line != '\n' and line[0] == 'L':
                block['L'].append(line[1:].strip())
                line = inputFile.readline()

            yield block

            if line == '':
                # Have reached the end of file
                break

        except IOError, e:
            print('Encountered error: ', e)
            break


blockNum = 0

try:
    for block in blockReader(inputFile):
        blockNum += 1
        P = block['P']
        Q = block['Q']
        L = block['L']
        T = block['T']

        try:
            cur.execute(insert_time_query, (P, T))

            for q in Q:
                cur.execute(insert_quotes_query, (P, q))

            for l in L:
                cur.execute(insert_links_query, (P, l))

        except Exception, e:
            print("Error in block %d: %s" % (blockNum, e), file=sys.stderr)

except Exception, e:
    print('General error on line %d: %s' % (blockNum, e), file=sys.stderr)
    logTime('Rolling back changes')
    conn.rollback()
    cur.close()
else:
    logTime('Committing to disk')
    conn.commit()
    cur.close()

logTime('Finished')

