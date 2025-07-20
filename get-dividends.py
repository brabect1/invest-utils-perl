import sqlite3
import sys
import xfrs
import argparse


parser = argparse.ArgumentParser(
        description="Prints records of received dividends."
        )

# `db` option: Path to the XFRS database.
parser.add_argument('-d', '--db', type=str,
        default='xfrs.sqlite3.db', dest='db',
        help="Path to the DB file.",
        )

# `order` option: Letting order records by date or by symbol.
parser.add_argument('-o', '--order', type=str, choices = ['date', 'symbol'],
        default='date', dest='order',
        help="By what to order the dividend records",
        )

# `from` option: Letting users select only records beyond this date.
parser.add_argument('-f', '--from', type=str,
        default=None, dest='fromDate',
        help="Return only records on and after this date. Format YYYY-MM-DD.",
        )

# `from` option: Letting users select only records beyond this date.
parser.add_argument('-t', '--to', type=str,
        default=None, dest='toDate',
        help="Return only records by this date. Format YYYY-MM-DD.",
        )

args = parser.parse_args()

dbh = sqlite3.connect(args.db)

divs = xfrs.getDividends(dbh, order=args.order, fromDate=args.fromDate, toDate=args.toDate)

dbh.commit()
dbh.close()

for d in divs:
    print(f'{d["symbol"]}\t{d["currency"]}\t{d["amount"]}\t{d["tax"]}\t{d["date"]}')

