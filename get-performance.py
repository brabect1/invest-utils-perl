import pyxirr
import sqlite3
import sys
import xfrs
import argparse
import datetime


parser = argparse.ArgumentParser(
        description="Calculates portfolie performance."
        )

# `db` option: Path to the XFRS database.
parser.add_argument('-d', '--db', type=str,
        default='xfrs.sqlite3.db', dest='db',
        help="Path to the DB file.",
        )

# `symbols` option: Space separated list of stock symbols.
parser.add_argument('-s', '--symbols', type=str,
        default=None, dest='symbols',
        help="Space separated list of symbols to which limit XIRR comps.",
        )

#TODO # `to` option: Letting users select only records beyond this date.
#TODO parser.add_argument('-t', '--to', type=str,
#TODO         default=None, dest='toDate',
#TODO         help="Return only records by this date. Format YYYY-MM-DD.",
#TODO         )

args = parser.parse_args()

# number of decimal places to round when printing values
np = 2

#TODO today = datetime.date.today().strftime("%Y-%m-%d")


# Open DB
# -------
dbh = sqlite3.connect(args.db)
cursor = dbh.cursor()


# Get symbol lists from DB
# ------------------------
symbols = xfrs.getSymbols(dbh)
currencies = xfrs.getCurrencies(dbh)
stocks = xfrs.getStocks(dbh)


# Get data collections from DB
# ----------------------------

print('# Getting balance data ...', file=sys.stderr)
balance = xfrs.getBalance(dbh, list(symbols) + list(currencies))

print('# Getting NAV data ...', file=sys.stderr)
nav = xfrs.getNAV(dbh, symbols)

print('# Getting dividend data ...', file=sys.stderr)
dividends = xfrs.getDividendSum(dbh, symbols)

print('# Getting invested amounts ...', file=sys.stderr)
investment = xfrs.getInvestedAmount(dbh, symbols)
#TODO xfrs::getTotalInvestedAmount( $dbh, \%investment_tot );
#TODO xfrs::getTotalSellPrice( $dbh, \%sell_val );
#TODO xfrs::getSellGain( $dbh, \%sell_gain );


for s in sorted(symbols):
    #TODO print(f'divi({s}) = {dividends[s]:,.2f}')
    print(f'invamnt({s}) = {investment[s]:,.2f}')


# close DB
# --------
dbh.commit()
dbh.close()

