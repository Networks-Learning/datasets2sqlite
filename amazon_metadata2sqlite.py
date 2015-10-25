#!/usr/bin/env python

from __future__ import print_function

import sys
import argparse
import sqlite3
import bz2
import gzip
# import json
import datetime as D

def logTime(chkpoint):
    print('*** Checkpoint: {} at \x1b[31m{}\x1b[0m'.format(chkpoint, D.datetime.now()))
    sys.stdout.flush()


def JSONReader(file_obj):
    '''Returns a parsed JSON object per line of the file_obj passed.'''
    for line in file_obj:
        yield eval(line)


argParser = argparse.ArgumentParser()
argParser.add_argument('metadata',
        help='The file which contains the json data fields '
             'from which to populate the sqlite3 db.')
argParser.add_argument('sqlitedb',
        help='The database file which should be populated. '
             'This file will be created if it does not exist.')

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
    inputFile = bz2.BZ2File(args.metadata, 'rU')
elif args.gzip:
    inputFile = gzip.open(args.metadata, 'rU')
else:
    inputFile = open(args.metadata, 'rU')


# asin - ID of the product, e.g. 0000031852
# title - name of the product
# price - price in US dollars (at time of crawl)
# imUrl - url of the product image
# related - related products (also bought, also viewed, bought together, buy after viewing)
# salesRank - sales rank information
# brand - brand name
# categories - list of categories the product belongs to

table_metadata = 'amz_metadata'
columns_metadata = '"asin" TEXT, "imUrl" TEXT, "title" TEXT, "description" TEXT, "price" REAL, "brand" TEXT'
insert_metadata = 'INSERT INTO %s VALUES (?, ?, ?, ?, ?, ?)' % (table_metadata,)

table_also_bought = 'amz_also_bought'
columns_also_bought = '"asin" TEXT, "also_bought" TEXT'
insert_also_bought = 'INSERT INTO %s VALUES (?, ?)' % (table_also_bought,)

table_also_viewed = 'amz_also_viewed'
columns_also_viewed = '"asin" TEXT, "also_viewed" TEXT'
insert_also_viewed = 'INSERT INTO %s VALUES (?, ?)' % (table_also_viewed,)

table_bought_together = 'amz_bought_together'
columns_bought_together = '"asin" TEXT, "bought_together" TEXT'
insert_bought_together = 'INSERT INTO %s VALUES (?, ?)' %(table_bought_together,)

table_buy_after_viewing = 'amz_buy_after_viewing'
columns_buy_after_viewing = '"asin" TEXT, "amz_buy_after_viewing" TEXT'
insert_buy_after_viewing = 'INSERT INTO %s VALUES (?, ?)' %(table_buy_after_viewing,)

table_categories = 'amz_categories'
columns_categories = '"asin" TEXT, "category" TEXT'
insert_categories = 'INSERT INTO %s VALUES (?, ?)' %(table_categories,)

table_sales_rank = 'amz_sales_rank'
columns_sales_rank = '"asin" TEXT, "area" TEXT, "rank" INTEGER'
insert_sales_rank = 'INSERT INTO %s VALUES (?, ?, ?)' % (table_sales_rank,)

conn = sqlite3.connect(args.sqlitedb)

# Always return bytestring
conn.text_factory = str

cur = conn.cursor()

for (table, columns) in [(table_metadata, columns_metadata),
                         (table_also_bought, columns_also_bought),
                         (table_also_viewed, columns_also_viewed),
                         (table_bought_together, columns_bought_together),
                         (table_buy_after_viewing, columns_buy_after_viewing),
                         (table_categories, columns_categories),
                         (table_sales_rank, columns_sales_rank)]:
    try:
        cur.execute('CREATE TABLE %s (%s)' % (table, columns))
        logTime('Created table {}'.format(table))
    except:
        logTime('Skipping table {}'.format(table))

def getMaybe(json, field):
    return json[field] if field in json else None

line = 0
try:
    for jsonElem in JSONReader(inputFile):
        line += 1
        try:
            metadata = [getMaybe(jsonElem, x) for x in ['asin', 'imUrl', 'title', 'description', 'price', 'brand']]
            if metadata[-2] is not None:
                metadata[-2] = float(metadata[-2])

            cur.execute(insert_metadata, metadata)
            asin = jsonElem['asin']

            related = getMaybe(jsonElem, 'related')

            if related is not None:
                if 'also_bought' in related:
                    cur.executemany(insert_also_bought, ((asin, x) for x in related['also_bought']))

                if 'also_viewed' in related:
                    cur.executemany(insert_also_viewed, ((asin, x) for x in related['also_viewed']))

                if 'bought_together' in related:
                    cur.executemany(insert_bought_together, ((asin, x) for x in related['bought_together']))

                if 'buy_after_viewing' in related:
                    cur.executemany(insert_buy_after_viewing, ((asin, x) for x in related['buy_after_viewing']))

            salesRank = getMaybe(jsonElem, 'salesRank')
            if salesRank is not None:
                cur.executemany(insert_sales_rank, ((asin, k, v) for k, v in salesRank.items()))

            categories = getMaybe(jsonElem, 'categories')
            if categories is not None:
                cur.executemany(insert_categories, ((asin, c) for c in categories[0]))
        except Exception, e:
            print("Error on line %d: %s" % (line, e), file=sys.stderr)
            raise e
except Exception, e:
    print('General error on line %d: %s' % (line, e), file=sys.stderr)
    logTime('Rolling back changes')
    conn.rollback()
    cur.close()
else:
    logTime('Committing to disk')
    conn.commit()
    cur.close()

logTime('Finished')

