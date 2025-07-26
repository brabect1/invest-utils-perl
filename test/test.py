import sqlite3
import sys
import xfrs
import argparse


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

d = xfrs.getCachedQuote(dbh, ['AAPL', 'MSFT'], date = '2025-07-23', online = True)
for k, v in d.items():
    print(f'{k} = {v["price"]} {v["currency"]} @ {v["date"]}')

