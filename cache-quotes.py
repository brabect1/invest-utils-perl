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
        default=None, dest='baseCurrency',
        help="Symbol of the base currency.",
        )

# `symbols` option: Space separated list of stock symbols.
parser.add_argument('-s', '--symbols', type=str,
        default=None, dest='symbols',
        help="Space separated list of stock symbols to quote and cache.",
        )

args = parser.parse_args()

dbh = sqlite3.connect(args.db)
cursor = dbh.cursor()

# Capture present date. We will cache one quote per day as the whole
# utils package is not meant for realtime monitoring but rather for
# a one time use.
date = datetime.date.today().strftime("%Y-%m-%d")

if args.symbols is not None:
    # use symbols given from command line
    stocks = args.symbols.split()
else:
    # use symbols in XFRS DB
    stocks = xfrs.getStocks(dbh) or list()


# collect stock quotes
# --------------------
quotes = {}
for s in stocks:
    qs = yfinance.Ticker(s)
    q = { 'regularMarketPrice': None, 'currency': None }
    try:
        for a in q.keys():
            q[a] = qs.info[a]
        quotes[s] = {'price': q['regularMarketPrice'], 'currency': q['currency']}
    except:
        pass


# collect currency quotes
# -----------------------
if args.baseCurrency is not None:
    currencies = set([q['currency'] for q in quotes.values()])
    #TODO currencies = set(xfrs.getCurrencies(dbh) + [q['currency'] for q in quotes.values()])

    for c in currencies:
        # skip base currency
        if c == args.baseCurrency: continue

        s = c + args.baseCurrency + '=X'
        qs = yfinance.Ticker(s)
        q = { 'regularMarketPrice': None, 'currency': None }
        try:
            for a in q.keys():
                q[a] = qs.info[a]
            quotes[c + args.baseCurrency] = {'price': q['regularMarketPrice'], 'currency': q['currency']}
        except:
            pass


# print quotes
# --------------
for s,q in quotes.items():
    print(f'{s}:\t{q["price"]} {q["currency"]}')


# update DB
# ---------
for s,q in quotes.items():
    xfrs.cacheQuote(dbh, s, q)


# close DB
# --------
dbh.commit()
dbh.close()

