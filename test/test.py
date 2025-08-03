import sqlite3
import sys
import xfrs
import argparse
import datetime


parser = argparse.ArgumentParser(
        description="Pairs the sell transactions to the buy transactions."
        )

# `db` option: Path to the XFRS database.
parser.add_argument('-d', '--db', type=str,
        default='xfrs.sqlite3.db', dest='db',
        help="Path to the DB file.",
        )


args = parser.parse_args()

dbh = sqlite3.connect(args.db)

cursor = dbh.cursor()

## symbols = sorted(xfrs.getSymbols(dbh))
## for s in symbols:
##     print(f'{s}\tisStock={xfrs.isStock(dbh, s)}\tisCurrency={xfrs.isCurrency(dbh, s)}')

## d = xfrs.getOnlineQuote(['AAPL', 'MSFT'], date = '2025-07-25')
## for k, v in d.items():
##     print(f'{k} = {v["price"]} {v["currency"]} @ {v["date"]}')

## d = xfrs.getCachedQuote(dbh, ['AAPL', 'MSFT'], date = '2025-07-23', online = True)
## for k, v in d.items():
##     print(f'{k} = {v["price"]} {v["currency"]} @ {v["date"]}')

## d = xfrs.getQuoteCurrency(dbh, None, ['CZK', 'EUR', 'CAD', 'JPY'])
## for k, v in d.items():
##     print(f'{k} = {v["price"]} {v["currency"]} @ {v}')

try:
    p = xfrs.Price(1.2, 'CZK')
    q = {
            'symbol': 'AAPL',
            'price': 210.12,
            'currency': 'USD',
            'date': '2025-08-01',
            'type': 'real',
            }
    q = xfrs.StockQuote(**q)
    print(q)
    p = q.getPrice()
    print(p)

    q = {
            'symbol': 'EURCZK',
            'price': 25.12,
            'currency': 'CZK',
            'date': '2025-08-01',
            'type': 'real',
            }
    q = xfrs.FxQuote(**q)
    print(q)
    p = q.getPrice()
    print(p)
except Exception as e:
    print('Exception: ', e)
    raise e

