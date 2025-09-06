import sqlite3
import sys
import xfrs
import argparse
import datetime
import re
import os.path


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
        help="Import input format.",
        )

# `force` option: If to overwrite/update the existing DB.
parser.add_argument('--force', action='store_true',
        default=False, dest='force',
        help="Allows overwriting/updating the existing DB.",
        )

args = parser.parse_args()

# Test arguments
# --------------
if args.format not in {'xfrs'}:
    sys.exit(f'Import from \'{args.format}\' not implemented!')

if os.path.isfile(args.db) and not args.force:
    sys.exit(f'File \'{args.db}\' already exists. Use `--force` to overwrite/update.')

# Pre-defined values
# ------------------

# pre-compiled reg ex's
reComment = re.compile('^#.*')
reRecord = re.compile('^(\w+)\(\s*(\w+=[\w\.-]+(\s+\w+=[\w\.-]+)*)\s*\)$')
rePrice = re.compile('(-?\d+(\.\d*)?)([A-Z.]+)')

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
            'real',
            ],
        }


# process files
# -------------
recs = list(); # list of collected records

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
            rec = rec_templates[recType].copy()

            if 'date' in attrs:
                rec[xfrs.REC_INDEXES['date']] = attrs['date']


            # money deposit or withdrawal
            if recType in  {'deposit', 'withdraw'}:
                # For the moment, we only assume currency transfers. There can
                # potentially be asset transfers. These can be modeled, for now,
                # as two records: A money transfer and a stock transaction.
                # Note: A direct deposit/withdraw of an asset can also be modeled
                # using the asset symbol as a currency and mandatory commision
                # (even though of zero value) with a valid currency symbol and
                # a mandatory cost basis (that would translate to `unit` currency
                # and price). The cost basis had to be hard-coded and would likely
                # represent the closing price at the date of the transfer per unit
                # of the asset.
                if 'amount' in attrs:
                    m = rePrice.match(attrs['amount'])
                    if m:
                        rec[xfrs.REC_INDEXES['amount']] = m[1]
                        rec[xfrs.REC_INDEXES['unit_curr']] = m[3]
                        rec[xfrs.REC_INDEXES['source_curr']] = m[3]
                        rec[xfrs.REC_INDEXES['comm_curr']] = m[3]

                        # some transfers may incur expenses that we account as commisions
                        if 'commission' in attrs:
                            m = rePrice.match(attrs['commission'])
                            if m:
                                rec[xfrs.REC_INDEXES['comm_price']] = m[1]
                                rec[xfrs.REC_INDEXES['comm_curr']] = m[3]

                        # TODO ---->>>> experimental
                        # direct asset transfers require cost basis so we can later
                        # evaluate gains on selling the asset
                        # TODO: Presently the case of depositing and withdrawing assets
                        # is not supported for evaluating portfolio performance (in
                        # `get-performance.pl`).
                        if 'costbasis' in attrs:
                            m = rePrice.match(attrs['costbasis'])
                            if m:
                                rec[xfrs.REC_INDEXES['unit_price']] = m[1]
                                rec[xfrs.REC_INDEXES['unit_curr']] = m[3]
                        #<<<<----
                    else:
                        print(f"Unexpected format of transaction amoount, line {lineno}: {attrs['amount']}",
                                file=sys.stderr)


            # stock buy and sell transactions
            if recType in {'buy','sell'}:
                if 'amount' in attrs:
                    rec[xfrs.REC_INDEXES['amount']] = attrs['amount']

                if 'stock' in attrs:
                    rec[xfrs.REC_INDEXES['source_curr']] = attrs['stock']
                else:
                    print("Missing stock symbol, line {lineno}: {line}", file=sys.stderr)

                if 'price' in attrs:
                    m = rePrice.match(attrs['price'])
                    if m:
                        rec[xfrs.REC_INDEXES['unit_price']] = m[1]
                        rec[xfrs.REC_INDEXES['unit_curr']] = m[3]
                    else:
                        print(f"Unexpected format of transaction price, line {lineno}: {attrs['price']}",
                                file=sys.stderr)

                if 'commission' in attrs:
                    m = rePrice.match(attrs['commission'])
                    if m:
                        rec[xfrs.REC_INDEXES['comm_price']] = m[1]
                        rec[xfrs.REC_INDEXES['comm_curr']] = m[3]
                    else:
                        print(f"Unexpected format of transaction commission, line {lineno}: {attrs['commission']}",
                                file=sys.stderr)

            # currency exchange
            if recType == 'fx':
                if 'amount' in attrs:
                    m = rePrice.match(attrs['amount'])
                    if m:
                        rec[xfrs.REC_INDEXES['amount']] = m[1]
                        rec[xfrs.REC_INDEXES['unit_curr']] = m[3]
                        rec[xfrs.REC_INDEXES['unit_price']] = '1'
                    else:
                        print(f"Unexpected format of transaction amoount, line {lineno}: {attrs['amount']}",
                                file=sys.stderr)

                if 'price' in attrs:
                    m = rePrice.match(attrs['price'])
                    if m:
                        rec[xfrs.REC_INDEXES['source_price']] = m[1]
                        rec[xfrs.REC_INDEXES['source_curr']] = m[3]
                    else:
                        print(f"Unexpected format of transaction price, line {lineno}: {attrs['price']}",
                                file=sys.stderr)

                if 'commission' in attrs:
                    m = rePrice.match(attrs['commission'])
                    if m:
                        rec[xfrs.REC_INDEXES['comm_price']] = m[1]
                        rec[xfrs.REC_INDEXES['comm_curr']] = m[3]
                    else:
                        print(f"Unexpected format of transaction commission, line {lineno}: {attrs['commission']}",
                                file=sys.stderr)

            # dividend reception
            if recType == 'dividend':
                if 'amount' in attrs:
                    m = rePrice.match(attrs['amount'])
                    if m:
                        rec[xfrs.REC_INDEXES['amount']] = m[1]
                        rec[xfrs.REC_INDEXES['unit_curr']] = m[3]
                        rec[xfrs.REC_INDEXES['unit_price']] = '1'
                    else:
                        print(f"Unexpected format of transaction amoount, line {lineno}: {attrs['amount']}",
                                file=sys.stderr)

                # for now treat stock title as the source currency
                if 'stock' in attrs:
                    rec[xfrs.REC_INDEXES['source_curr']] = attrs['stock']

                # for now treat tax as the commissions
                if 'tax' in attrs:
                    m = rePrice.match(attrs['tax'])
                    if m:
                        rec[xfrs.REC_INDEXES['comm_price']] = m[1]
                        rec[xfrs.REC_INDEXES['comm_curr']] = m[3]
                    else:
                        print(f"Unexpected format of transaction tax, line {lineno}: {attrs['tax']}",
                                file=sys.stderr)


            # quotes
            if recType == 'quote':
                # process `price` before `currency` so that we can properly
                # assemble the forex symbol
                m = None
                if 'price' in attrs:
                    m = rePrice.match(attrs['price'])
                    if m:
                        rec[xfrs.REC_INDEXES['unit_price']] = m[1]
                        rec[xfrs.REC_INDEXES['unit_curr']] = m[3]
                    else:
                        m = None
                        print(f"Unexpected format of transaction price, line {lineno}: {attrs['price']}",
                                file=sys.stderr)

                if 'stock' in attrs:
                    rec[xfrs.REC_INDEXES['source_curr']] = attrs['stock']
                elif 'currency' in attrs:
                    if m is not None:
                        rec[xfrs.REC_INDEXES['source_curr']] = xfrs.FxQuote.toQuoteSymbol(To=attrs['currency'], From=m[3])
                    else:
                        print("Missing price associated with currency quote, line {lineno}: {line}",
                                file=sys.stderr)
                else:
                    print("Missing stock or currency symbol, line {lineno}: {line}", file=sys.stderr)

                # repurpose `comm_currency` (which makes no sense for a quote) to
                # hold the quote type, if provided
                if 'type' in attrs:
                    rec[xfrs.REC_INDEXES['comm_curr']] = attrs['type']

            recs.append(rec)


# Put records into DB
# -------------------
if len(rec) > 0:
    dbh = sqlite3.connect(args.db)
    cursor = dbh.cursor()

    for rec in recs:
        xfrs.addRecord(dbh, rec)


# close DB
# --------
if len(rec) > 0:
    dbh.commit()
    dbh.close()
