import sqlite3
import sys
import xfrs
import argparse
import datetime
import yfinance


parser = argparse.ArgumentParser(
        description="Queries symbol quotes and records it in the DB."
        )

# `db` option: Path to the XFRS database.
parser.add_argument('-d', '--db', type=str,
        default='xfrs.sqlite3.db', dest='db',
        help="Path to the DB file.",
        )

# `base` option: Symbol of the base currency.
parser.add_argument('-b', '--base', type=str,
        default='', dest='baseCurrency',
        help="Symbol of the base currency.",
        )

args = parser.parse_args()

dbh = sqlite3.connect(args.db)
cursor = dbh.cursor()

# Capture present date. We will cache one quote per day as the whole
# utils package is not meant for realtime monitoring but rather for
# a one time use.
date = datetime.datetime.today().strftime("%Y-%m-%d")

## # Test if the 'quotes' table exist, or create otherwise
## stmt = "SELECT name FROM sqlite_master WHERE type='table' AND name='quotes';" 
## cursor.execute(stmt)
## 
## if len(cursor.fetchall()) == 0:
##     stmt = 'CREATE TABLE quotes (
##             id INT PRIMARY KEY,
##             symbol TEXT NOT NULL,
##             date TEXT NOT NULL,
##             price REAL,
##             curr TEXT);'
##     cursor.execute(stmt)

quotes = {}
for s in xfrs.getStocks(dbh):
    qs = yfinance.Ticker(s)
    q = { 'regularMarketPrice': None, 'currency': None }
    try:
        for a in q.keys():
            q[a] = qs.info[a]
        quotes[s] = {'last': q['regularMarketPrice'], 'currency': q['currency']}
    except:
        pass

for s,q in quotes.items():
    print(f'{s}:\t{q["last"]} {q["currency"]}')

dbh.commit()
dbh.close()

