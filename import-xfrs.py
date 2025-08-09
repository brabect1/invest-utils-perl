import sqlite3
import sys
import xfrs
import argparse
import datetime
import re

#TODO allowedRecords = {'buy', 'sell', 'dividend', 'fx', 'deposit', 'withdraw', 'quote'}

parser = argparse.ArgumentParser(
        description="Imports data into (Sqlite3) DB."
        )

# input file name (as positional argument)
parser.add_argument('paths', nargs='+', type=str,
        help='Input file paths to be imported.'
        )

# `db` option: Path to the XFRS database.
parser.add_argument('-d', '--db', type=str,
        default='xfrs.sqlite3.db', dest='db',
        help="Path to the DB file.",
        )

# `format` option: Type of the output format.
parser.add_argument('--format', type=str, choices = ['xfrs', 'json', 'csv'],
        default='xfrs', dest='format',
        help="Export output format.",
        )

args = parser.parse_args()

dbh = sqlite3.connect(args.db)
cursor = dbh.cursor()

if args.format not in {'xfrs'}:
    die(f'Export to \'{args.format}\' not implemented!')

# pre-compiled reg ex's
reComment = re.compile('^#.*')
reRecord = re.compile('^(\w+)\(\s*(\w+=[\w\.-]+(\s+\w+=[\w\.-]+)*)\s*\)$')

rec_indexes = {
        'type': 0,
        'date': 1,
        'amount': 2,
        'unit_price': 3,
        'unit_curr': 4,
        'source_price': 5,
        'source_curr': 6,
        'comm_price': 7,
        'comm_curr': 8,
        }

rec_templates = {
        'withdraw': [
            'withdraw',
            '1970-01-01',
            0,
            1,
            '',
            0,
            '',
            0,
            '',
            ],
        'deposit': [
            'deposit',
            '1970-01-01',
            0,
            1,
            '',
            0,
            '',
            0,
            '',
            ],
        'dividend': [
            'dividend',
            '1970-01-01',
            0,
            0,
            '',
            0,
            '',
            0,
            '',
            ],
        'fx': [
            'fx',
            '1970-01-01',
            0,
            0,
            '',
            0,
            '',
            0,
            '',
            ],
        'buy': [
            'buy',
            '1970-01-01',
            0,
            0,
            '',
            0,
            '',
            0,
            '',
            ],
        'sell': [
            'sell',
            '1970-01-01',
            0,
            0,
            '',
            0,
            '',
            0,
            '',
            ],
        'quote': [
            'quote',
            '1970-01-01',
            0,
            0,
            '',
            0,
            '',
            0,
            '',
            ],
        }


# process files
# -------------
for filename in args.paths:
    with open(filename) as file:
        lineno = 0

        for line in file:
            lineno += 1

            line = line.strip()

            # skip blank lines
            if len(line) == 0: continue

            # skip comments
            if reComment.match(line): continue

            # test proper records format
            m = reRecord.match(line)
            if not m:
                print(f"Improperly formatted record at line {lineno}: {line}", file=sys.stderr)
                continue

            # see if unknown record
            recType = m[1]
            if recType not in rec_templates:
                print(f"Unknown record '{recType}' at line {lineno}: {line}", file=sys.stderr)
                continue

            # split the attributes and put it into a hash
            attrs = dict()
            for pair in m[2].split():
                attrParts = pair.split('=')
                if len(attrParts) < 2:
                    print(f"Improprly formatted attribute '{attrParts[0]}' at line {lineno}: {line}",
                            file=sys.stderr)
                    continue

                attrs[attrParts[0]] = attrParts[1]

            # get the template and populate it with actuals
            rec = rec_templates[recType]

            #TODO
            print(f'{lineno}: ' + recType + ': ' + ', '.join([k + '=' + v for k, v in attrs.items()]), file=sys.stderr)

