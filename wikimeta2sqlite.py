#!/usr/bin/env python
from __future__ import print_function
import sqlite3
import bz2
import gzip
import sys
import argparse
import datetime as D

def logTime(chkpoint):
    print('*** Checkpoint: {} at \x1b[31m{}\x1b[0m'.format(chkpoint, D.datetime.now()))
    sys.stdout.flush()


argParser = argparse.ArgumentParser()
argParser.add_argument('inputFile',
        help='The file to read quotes from.')
argParser.add_argument('sqlitedb',
        help='The sqlite table to fill.')
argParser.add_argument('table_prefix',
        help='The prefix of table names in SQLite.')
argParser.add_argument('--min-date',
        help='Discard all timestamps below this date (ISO-8601 format).',
        default='')

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
    inputFile = bz2.BZ2File(args.inputFile, 'rU')
elif args.gzip:
    inputFile = gzip.open(args.inputFile, 'rU')
else:
    inputFile = open(args.inputFile, 'rU')

minDate = args.min_date

conn = sqlite3.connect(args.sqlitedb)
# Always return bytestrings
conn.text_factory = str
cur = conn.cursor()

table_prefix = args.table_prefix

table_revisions = table_prefix + '_revision'
columns_revision = ('"article_id" INTEGER, "rev_id" INTEGER, '
                    '"article_title" TEXT, "timestamp" TEXT, '
                    '"username" TEXT, "user_id" TEXT')
insert_revision = 'INSERT INTO %s VALUES (?, ?, ?, ?, ?, ?)' % (table_revisions,)

table_category = table_prefix + '_category'
columns_category = ('"rev_id" INTEGER, "CATEGORY" TEXT')
insert_category = 'INSERT INTO %s VALUES (?, ?)' % (table_category,)

table_image = table_prefix + '_image'
columns_image = ('"rev_id" INTEGER, "IMAGE" TEXT')
insert_image = 'INSERT INTO %s VALUES (?, ?)' % (table_image,)

table_main = table_prefix + '_main'
columns_main = ('"rev_id" INTEGER, "MAIN" TEXT')
insert_main = 'INSERT INTO %s VALUES (?, ?)' % (table_main,)

table_talk = table_prefix + '_talk'
columns_talk = ('"rev_id" INTEGER, "TALK" TEXT')
insert_talk = 'INSERT INTO %s VALUES (?, ?)' % (table_talk,)

table_user = table_prefix + '_user'
columns_user = ('"rev_id" INTEGER, "USER" TEXT')
insert_user = 'INSERT INTO %s VALUES (?, ?)' % (table_user,)

table_user_talk = table_prefix + '_user_talk'
columns_user_talk = ('"rev_id" INTEGER, "USER_TALK" TEXT')
insert_user_talk = 'INSERT INTO %s VALUES (?, ?)' % (table_user_talk,)

table_other = table_prefix + '_other'
columns_other = ('"rev_id" INTEGER, "OTHER" TEXT')
insert_other = 'INSERT INTO %s VALUES (?, ?)' % (table_other,)

table_external = table_prefix + '_external'
columns_external = ('"rev_id" INTEGER, "EXTERNAL" TEXT')
insert_external = 'INSERT INTO %s VALUES (?, ?)' % (table_external,)

table_template = table_prefix + '_template'
columns_template = ('"rev_id" INTEGER, "TEMPLATE" TEXT')
insert_template = 'INSERT INTO %s VALUES (?, ?)' % (table_template,)

table_comment = table_prefix + '_comment'
columns_comment = ('"rev_id" INTEGER, "COMMENT" TEXT')
insert_comment = 'INSERT INTO %s VALUES (?, ?)' % (table_comment,)

table_minor = table_prefix + '_minor'
columns_minor = ('"rev_id" INTEGER, "MINOR" TEXT')
insert_minor = 'INSERT INTO %s VALUES (?, ?)' % (table_minor,)

table_textdata = table_prefix + '_textdata'
columns_textdata = ('"rev_id" INTEGER, "TEXTDATA" TEXT')
insert_textdata = 'INSERT INTO %s VALUES (?, ?)' % (table_textdata,)


for table, columns in [(table_revisions, columns_revision),
                       (table_category, columns_category),
                       (table_image, columns_image),
                       (table_main, columns_main),
                       (table_talk, columns_talk),
                       (table_user, columns_user),
                       (table_user_talk, columns_user_talk),
                       (table_other, columns_other),
                       (table_external, columns_external),
                       (table_template, columns_template),
                       (table_comment, columns_comment),
                       (table_minor, columns_minor),
                       (table_textdata, columns_textdata)]:
    try:
        create_query = 'CREATE TABLE %s (%s)' % (table, columns)
        cur.execute(create_query)
        logTime('Created table {}'.format(table))
    except:
        logTime('Skipping creation of table {}'.format(table))


def assertType(lineType, kind, blockNum):
    assert lineType == kind, '{} line corrupt in {}'.find(kind, blockNum)


def getLineDataOfKind(inputFile, kind, blockNum):
    line = inputFile.readline().strip()
    if line == '':
        return None
    data = line.split(' ')
    assertType(data[0], kind, blockNum)
    return data[1:]


def blockReader(inputFile):
    '''Read one wikipedia metadata block from the passed file.'''
    blockNum = 0
    while True:
        try:
            blockNum += 1
            block = {}

            revisionData = getLineDataOfKind(inputFile, 'REVISION', blockNum)
            if revisionData is None:
                # The first block line was empty, finish reading
                break

            block['REVISION'] = {
                    'article_id': int(revisionData[0]),
                    'rev_id': int(revisionData[1]),
                    'article_title': revisionData[2],
                    'timestamp': revisionData[3],
                    'username': revisionData[4],
                    'user_id': revisionData[5]
            }

            block['CATEGORY'] = getLineDataOfKind(inputFile, 'CATEGORY', blockNum)
            block['IMAGE'] = getLineDataOfKind(inputFile, 'IMAGE', blockNum)
            block['MAIN'] = getLineDataOfKind(inputFile, 'MAIN', blockNum)
            block['TALK'] = getLineDataOfKind(inputFile, 'TALK', blockNum)
            block['USER'] = getLineDataOfKind(inputFile, 'USER', blockNum)
            block['USER_TALK'] = getLineDataOfKind(inputFile, 'USER_TALK', blockNum)
            block['OTHER'] = getLineDataOfKind(inputFile, 'OTHER', blockNum)
            block['EXTERNAL'] = getLineDataOfKind(inputFile, 'EXTERNAL', blockNum)
            block['TEMPLATE'] = getLineDataOfKind(inputFile, 'TEMPLATE', blockNum)
            block['COMMENT'] = ' '.join(getLineDataOfKind(inputFile, 'COMMENT', blockNum))
            block['MINOR'] = int(getLineDataOfKind(inputFile, 'MINOR', blockNum)[0])
            block['TEXTDATA'] = int(getLineDataOfKind(inputFile, 'TEXTDATA', blockNum)[0])

            emptyLine = inputFile.readline()
            assert emptyLine == '\n', 'No empty line after block {}'.format(blockNum)

            yield block
        except Exception, e:
            print('Encountered error: ', e)
            print('In block: ', blockNum)
            break

def insertWithRevId(cur, insert_statement, dataList, revId):
    cur.executemany(insert_statement, ((revId, x) for x in dataList))

try:
    blockCounter = 0
    for block in blockReader(inputFile):
        # Only count this block if the timestamp is greater than the
        # minimum data passed.
        if block['REVISION']['timestamp'] > minDate:
            blockCounter += 1

            try:
                revData = block['REVISION']
                revId = revData['rev_id']
                cur.execute(insert_revision,
                        (revData['article_id'], revData['rev_id'],
                         revData['article_title'], revData['timestamp'],
                         revData['username'], revData['user_id']))

                insertWithRevId(cur, insert_category, block['CATEGORY'], revId)
                insertWithRevId(cur, insert_image, block['IMAGE'], revId)
                insertWithRevId(cur, insert_main, block['MAIN'], revId)
                insertWithRevId(cur, insert_talk, block['TALK'], revId)
                insertWithRevId(cur, insert_user, block['USER'], revId)
                insertWithRevId(cur, insert_user_talk, block['USER_TALK'], revId)
                insertWithRevId(cur, insert_other, block['OTHER'], revId)
                insertWithRevId(cur, insert_external, block['EXTERNAL'], revId)
                insertWithRevId(cur, insert_template, block['TEMPLATE'], revId)

                cur.execute(insert_comment, (revId, block['COMMENT']))
                cur.execute(insert_minor, (revId, block['MINOR']))
                cur.execute(insert_textdata, (revId, block['TEXTDATA']))
            except Exception, e:
                print("Error in block %d: %s" % (blockCounter, e), file=sys.stderr)

            if blockCounter % 100000 == 0:
                logTime('{} records processed'.format(blockCounter))

except Exception, e:
    print('General error on line %d: %s' % (blockCounter, e), file=sys.stderr)
    logTime('Rolling back changes')
    conn.rollback()
    cur.close()
else:
    logTime('Committing to disk')
    conn.commit()
    cur.close()

logTime('Finished')
