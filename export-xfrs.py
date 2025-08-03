import sqlite3
import sys
import xfrs
import argparse
import datetime


parser = argparse.ArgumentParser(
        description="Exports DB into a text format."
        )

# `db` option: Path to the XFRS database.
parser.add_argument('-d', '--db', type=str,
        default='xfrs.sqlite3.db', dest='db',
        help="Path to the DB file.",
        )

# `format` option: Type of the output format.
parser.add_argument('-f', '--format', type=str, choices = ['xfrs', 'json', 'csv'],
        default='xfrs', dest='format',
        help="Export output format.",
        )

#TODO # `symbols` option: Space separated list of stock symbols.
#TODO parser.add_argument('-s', '--symbols', type=str,
#TODO         default=None, dest='symbols',
#TODO         help="Space separated list of stock symbols to quote and cache.",
#TODO         )

args = parser.parse_args()

dbh = sqlite3.connect(args.db)
cursor = dbh.cursor()

if args.format not in {'xfrs'}:
    die(f'Export to \'{args.format}\' not implemented!')


# Collect transfers
# -----------------
xfers = None

# Test if the 'xfrs' table exist, or create otherwise
stmt = "SELECT name FROM sqlite_master WHERE type='table' AND name='xfrs';" 
cursor.execute(stmt)

if len(cursor.fetchall()) > 0:

    # query and record transactions
    stmt = "SELECT type, date, amount, unit_price, unit_curr, source_price, source_curr, comm_price, comm_curr FROM xfrs;"
    cursor.execute(stmt)
    xfers = list()
    for row in cursor.fetchall():
        q = {
                'type': row[0],
                'date': row[1],
                'amount': row[2],
                'uprice': row[3],
                'ucurr': row[4],
                'sprice': row[5],
                'scurr': row[6],
                'cprice': row[7],
                'ccurr': row[8],
                }
        xfers.append(q)


# Collect quotes
# --------------
quotes = None

# Test if the 'quotes' table exist, or create otherwise
stmt = "SELECT name FROM sqlite_master WHERE type='table' AND name='quotes';" 
cursor.execute(stmt)

if len(cursor.fetchall()) > 0:

    # get list of stock symbols (to identify if the quote is for stock or for currency)
    stocks = xfrs.getStocks(dbh)

    # query and record quotes
    stmt = "SELECT symbol, date, type, price, curr FROM quotes;"
    cursor.execute(stmt)
    quotes = list()
    for row in cursor.fetchall():
        q = {
                'symbol': row[0],
                'date': row[1],
                'type': row[2],
                'price': row[3],
                'currency': row[4],
                }
        if row[0] in stocks: q = xfrs.StockQuote(**q)
        else: q = xfrs.FxQuote(**q)
        quotes.append(q)

# Export
# ------

# export transactions
if xfers is not None:
    print('\n# Transactions\n# -------\n')
    for q in xfers:
        print(f'{q["type"]}(source={q["scurr"]} amount={q["amount"]} date={q["date"]})')

# export quotes
if quotes is not None:
    print('\n# Quotes\n# -------\n')
    for q in quotes:
        if q.isStock():
            symname = 'stock'
        else:
            symname = 'currency'

        print(f'quote({symname}={q.getSymbol()} price={q.getPrice()} date={q.getDate("%Y-%m-%d")})')

# print export date
print('\n# export date: ' + datetime.date.today().strftime("%Y-%m-%d"))


# close DB
# --------
dbh.commit()
dbh.close()

