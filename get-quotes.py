import sqlite3
import sys
import xfrs
import argparse
import datetime
import re

# The list of stocks and currencies is given as an argument or
# else is taken from DB. Currencies are quoted only when
# the base currency is given (`-b` option).

parser = argparse.ArgumentParser(
        description="Quotes stock and currency symbols."
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

# `date` option: Letting users select only records beyond this date.
parser.add_argument('--date', type=str,
        default=None, dest='date',
        help="Get quote on this date. Format YYYY-MM-DD.",
        )

args = parser.parse_args()

dbh = sqlite3.connect(args.db)
cursor = dbh.cursor()

# Capture present date. We will cache one quote per day as the whole
# utils package is not meant for realtime monitoring but rather for
# a one time use.
if args.date is None:
    date = datetime.date.today().strftime("%Y-%m-%d")
else:
    date = args.date

# collect stock symbols and currencies
if args.symbols is not None:
    # use symbols given from command line
    symbols = set()
    currencies = set()
    for s in args.symbols.split():
        m = re.search('^(\w+)=X$', s)
        if m:
            if args.baseCurrency == m[1][3:]:
                currencies.add(s = m[1][:3])
            continue

        symbols.add(s)

else:
    # use symbols in XFRS DB
    symbols = xfrs.getStocks(dbh) or list()
    if args.baseCurrency is not None:
        currencies = set(xfrs.getCurrencies(dbh) or list())


# Quote symbols
# -------------
quotes = dict()

# obtain cached quotes
for s, q in xfrs.getCachedQuote(dbh, list(symbols), date = date).items():
    q['status'] = 'cached'
    quotes[s] = q

# obtain online quotes
if len(symbols) > len(quotes.keys()):
    for s, q in xfrs.getOnlineQuote([s for s in symbols if s not in quotes], date = date).items():
        q['status'] = 'online'
        quotes[s] = q


# Quote currencies
# ----------------
if args.baseCurrency is not None:
    # add currencies of previously quoted stocks in the basket
    currencies.update([q['currency'] for q in quotes.values()])

    # filter out the base currency
    if args.baseCurrency in currencies: currencies.remove(args.baseCurrency)

    # map currencies to forex symbols
    fxMap = {c: c + args.baseCurrency + '=X' for c in currencies}

    # quote forex symbols
    fx = xfrs.getOnlineQuote(fxMap.values(), date = date)

    # recalculate the recorded stock quotes (to base currency)
    for s in quotes.keys():
        c = quotes[s]['currency']
        if c != args.baseCurrency and fxMap[c] in fx:
            quotes[s]['currency'] = args.baseCurrency
            quotes[s]['price'] *= fx[fxMap[c]]['price']

    # add forex quotes
    for c in currencies:
        if c not in quotes:
            quotes[c] = fx[fxMap[c]]
            quotes[c]['status'] = 'online'


# Report quotes
# -------------
for s, q in quotes.items():
    print(f'{s}\t{q["price"]:.3f} {q["currency"]} ({q.get("status","???")})')


# close DB
# --------
dbh.commit()
dbh.close()
