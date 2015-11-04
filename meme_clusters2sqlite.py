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
argParser.add_argument('clusterFile',
        help='The file to read clusters from.')
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
    inputFile = bz2.BZ2File(args.clusterFile, 'rU')
elif args.gzip:
    inputFile = gzip.open(args.clusterFile, 'rU')
else:
    inputFile = open(args.clusterFile, 'rU')

conn = sqlite3.connect(args.sqlitedb)

# Always return bytestring instead of unicode.
conn.text_factory = str

cur = conn.cursor()

def blockReader(inputFile):
    '''Read one cluster block from the passed file.'''
    line = 0
    A_line, B_line, C_line = '', '', ''
    while True:
        try:
            block = {}

            line += 1
            A_line = inputFile.readline()
            if A_line == '':
                break

            A_data = A_line.split('\t')
            B_count = int(A_data[0])
            block['cluster_size'] = B_count

            block['total_frequency'] = int(A_data[1])
            block['root'] = A_data[2]
            block['cluster_id'] = int(A_data[3])

            block['B'] = []
            for b_cluster_num in range(B_count):
                block['B'].append({})
                B = block['B'][b_cluster_num]

                line += 1
                B_line = inputFile.readline().strip()
                B_data = B_line.split('\t')
                B['total_phrase_frequency'] = int(B_data[0])

                C_count = int(B_data[1])
                B['num_urls'] = C_count
                B['phrase'] = B_data[2]
                B['phrase_id'] = int(B_data[3])

                B['C'] = []
                for c_cluster_num in range(C_count):
                    B['C'].append({})
                    C = B['C'][c_cluster_num]

                    line += 1
                    C_line = inputFile.readline().strip()
                    C_data = C_line.split('\t')
                    C['timestamp'] = C_data[0]
                    C['frequency_in_url'] = int(C_data[1])
                    C['url_type'] = C_data[2]
                    C['url'] = C_data[3]

                line += 1
                # There is an empty line after each C block, except last one
                emptyLine = inputFile.readline().strip()
                assert emptyLine == '', "Empty line after C block not found. Found '{}' instead".format(emptyLine)

            yield block

        except IOError as e:
            print('Encountered error: ', e, ' at line: ', line)
            break
        except IndexError as e:
            print('Encountered index error: ', e, ' at line: ', line)
            print('A_line = ', A_line)
            print('B_line = ', B_line)
            print('C_line = ', C_line)
            break

# Skip the header of the file
for header_line in xrange(6):
    inputFile.readline()

table_prefix = args.table_prefix
table_root = table_prefix + '_roots'
columns_root = '"cluster_size" INTEGER, "total_frequency" INTEGER, "root" TEXT, "cluster_id" TEXT'
insert_root = 'INSERT INTO %s VALUES (?, ?, ?, ?)' % (table_root,)


table_derivative = table_prefix + '_derivatives'
columns_derivative = '"cluster_id" INTEGER, "total_phrase_frequency" INTEGER, "num_urls" INTEGER, "phrase" TEXT, "phrase_id" INTEGER'
insert_derivative = 'INSERT INTO %s VALUES (?, ?, ?, ?, ?)' % (table_derivative,)

table_phrase_info = table_prefix + '_phrase_info'
columns_phrase_info = '"cluster_id" INTEGER, "phrase_id" INTEGER, "frequency_in_url" INTEGER, "timestamp" TEXT, "url_type" TEXT, "url" TEXT'
insert_phrase_info = 'INSERT INTO %s VALUES (?, ?, ?, ?, ?, ?)' % (table_phrase_info,)

for table, columns in [(table_root, columns_root),
                       (table_derivative, columns_derivative),
                       (table_phrase_info, columns_phrase_info)]:
    create_query = 'CREATE TABLE %s (%s)' % (table, columns)
    try:
        cur.execute(create_query)
        logTime('Created table {}'.format(table))
    except:
        logTime('Skipping creation of table {}'.format(table))

blockNum = 0

try:
    for block in blockReader(inputFile):
        blockNum += 1

        try:
            cur.execute(insert_root, (block['cluster_size'],
                                      block['total_frequency'],
                                      block['root'],
                                      block['cluster_id']))

            for B in block['B']:
                cur.execute(insert_derivative, (block['cluster_id'],
                                                B['total_phrase_frequency'],
                                                B['num_urls'],
                                                B['phrase'],
                                                B['phrase_id']))
                for C in B['C']:
                    cur.execute(insert_phrase_info, (block['cluster_id'],
                                                     B['phrase_id'],
                                                     C['frequency_in_url'],
                                                     C['timestamp'],
                                                     C['url_type'],
                                                     C['url']))

        except Exception as e:
            print("Error in block %d: %s" % (blockNum, e), file=sys.stderr)

except Exception as e:
    print('General error in block %d: %s' % (blockNum, e), file=sys.stderr)
    logTime('Rolling back changes')
    conn.rollback()
    cur.close()
else:
    logTime('Committing to disk')
    conn.commit()
    cur.close()

logTime('Finished')

