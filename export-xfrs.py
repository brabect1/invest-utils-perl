import sqlite3
import sys
import xfrs
import argparse
import datetime

allowedRecords = {'buy', 'sell', 'dividend', 'fx', 'deposit', 'withdraw', 'quote'}

parser = argparse.ArgumentParser(
        description="Exports DB into a text format."
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

# `symbols` option: Space separated list of stock symbols.
parser.add_argument('-s', '--symbols', type=str,
        default=None, dest='symbols',
        help="Space separated list of symbols to export.",
        )

# `order` option: Letting order records by date or by symbol.
parser.add_argument('-o', '--order', type=str, choices = ['date', 'symbol', 'type'],
        default=None, dest='order',
        help="By what to order the records.",
        )

# `from` option: Letting users select only records beyond this date.
parser.add_argument('-f', '--from', type=str,
        default=None, dest='fromDate',
        help="Return only records on and after this date. Format YYYY-MM-DD.",
        )

# `to` option: Letting users select only records beyond this date.
parser.add_argument('-t', '--to', type=str,
        default=None, dest='toDate',
        help="Return only records by this date. Format YYYY-MM-DD.",
        )

# `record` option: Space separated list of record types.
parser.add_argument('-r', '--records', type=str,
        default=None, dest='records',
        help="Space separated list of record types to export, either of " + \
        ','.join(allowedRecords) + '.',
        )

args = parser.parse_args()

dbh = sqlite3.connect(args.db)
cursor = dbh.cursor()

if args.format not in {'xfrs'}:
    die(f'Export to \'{args.format}\' not implemented!')


# Collect transfers
# -----------------
xfers = None

# see if not to limit export to quotes only
skipXfers = False
if args.records is not None:
    skipXfers = len([r for r in args.records.split() if r in allowedRecords and r != 'quote']) == 0

# Test if the 'xfrs' table exist, or create otherwise
stmt = "SELECT name FROM sqlite_master WHERE type='table' AND name='xfrs';" 
cursor.execute(stmt)

if len(cursor.fetchall()) > 0 and not skipXfers:

    # filters (to be used for `where` SQL cluase)
    filters = []

    # by what to order the records
    # (`None` means the default order, which would likely be the order
    # of entering the records into DB)
    order = None
    if args.order is not None:
        if args.order == 'symbol': order = 'source_curr'
        elif args.order == 'type': order = 'type'
        else: order = 'date'

    # see if filter by type
    if args.records is not None:
        records = ['\'' + s + '\'' for s in args.records.split() if s in allowedRecords]
        if len(records) > 0:
            filters.append('type in (' + ','.join(records) +')')

    # see if filter by from date
    if args.fromDate is not None:
        try:
            d = datetime.datetime.strptime(args.fromDate, '%Y-%m-%d')
            filters.append(f'date>=\'{d.strftime("%Y-%m-%d")}\'')
        except ValueError:
            # ignore wrong argument
            pass

    # see if filter by to date
    if args.toDate is not None:
        try:
            d = datetime.datetime.strptime(args.toDate, '%Y-%m-%d')
            filters.append(f'date<=\'{d.strftime("%Y-%m-%d")}\'')
        except ValueError:
            # ignore wrong argument
            pass

    # see if filter by symbol
    symbols = list()
    if args.symbols is not None: symbols = ['\'' + s + '\'' for s in args.symbols.split()]
    if len(symbols) > 0:
        filters.append('source_curr in (' + ','.join(symbols) +')')

    # query and record transactions
    stmt = "SELECT type, date, amount, unit_price, unit_curr, source_price, source_curr, comm_price, comm_curr FROM xfrs"
    if len(filters) > 0:
        stmt += ' where (' + ' and '.join(filters) + ')'
    if order is not None: stmt += ' order by ' + order
    stmt += ';'
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

# see if not to limit export to transfers only
skipQuotes = False
if args.records is not None:
    skipQuotes = 'quote' not in args.records.split()

# Test if the 'quotes' table exist, or create otherwise
stmt = "SELECT name FROM sqlite_master WHERE type='table' AND name='quotes';" 
cursor.execute(stmt)

if len(cursor.fetchall()) > 0 and not skipQuotes:

    # filters (to be used for `where` SQL cluase)
    filters = []

    # by what to order the records
    # (`None` means the default order, which would likely be the order
    # of entering the records into DB)
    order = None
    if args.order is not None:
        if args.order == 'symbol': order = 'symbol'
        elif args.order == 'type': order = 'type'
        else: order = 'date'

    # see if filter by from date
    if args.fromDate is not None:
        try:
            d = datetime.datetime.strptime(args.fromDate, '%Y-%m-%d')
            filters.append(f'date>=\'{d.strftime("%Y-%m-%d")}\'')
        except ValueError:
            # ignore wrong argument
            pass

    # see if filter by to date
    if args.toDate is not None:
        try:
            d = datetime.datetime.strptime(args.toDate, '%Y-%m-%d')
            filters.append(f'date<=\'{d.strftime("%Y-%m-%d")}\'')
        except ValueError:
            # ignore wrong argument
            pass

    # see if filter by symbol
    symbols = list()
    if args.symbols is not None: symbols = ['\'' + s + '\'' for s in args.symbols.split()]
    if len(symbols) > 0:
        filters.append('symbol in (' + ','.join(symbols) +')')

    # get list of stock symbols (to identify if the quote is for stock or for currency)
    stocks = xfrs.getStocks(dbh)

    # query and record quotes
    stmt = "SELECT symbol, date, type, price, curr FROM quotes"
    if len(filters) > 0:
        stmt += ' where (' + ' and '.join(filters) + ')'
    if order is not None: stmt += ' order by ' + order
    stmt += ';'
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
        if q['type'] in {'buy', 'sell'}:
            s = f'{q["type"]}(stock={q["scurr"]} amount={q["amount"]} price={q["uprice"]}{q["ucurr"]} date={q["date"]} commission={q["cprice"]}{q["ccurr"]})'
        elif q['type'] in {'deposit', 'withdraw'}:
            s = f'{q["type"]}(amount={q["amount"]}{q["ucurr"]} date={q["date"]}'
            if q['cprice'] > 0: s += f' commission={q["cprice"]}{q["ccurr"]}'
            s += ')'
        elif q['type'] == 'dividend':
            s = f'{q["type"]}(stock={q["scurr"]} amount={q["amount"]}{q["ucurr"]}'
            s += f' tax={q["cprice"]}{q["ccurr"]}'
            s += f' date={q["date"]})'
        elif q['type'] == 'fx':
            s = f'{q["type"]}(amount={q["amount"]}{q["ucurr"]} price={q["uprice"]}{q["ucurr"]} date={q["date"]}'
            if q['cprice'] > 0: s += f' commission={q["cprice"]}{q["ccurr"]}'
            s += ')'
        else:
            s = f'# {q["type"]}(source={q["scurr"]} amount={q["amount"]} date={q["date"]})'

        print(s)

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

