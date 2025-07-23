import sqlite3
import sys
import xfrs
import argparse
import datetime
import yfinance


parser = argparse.ArgumentParser(
        description="Prints the current balance of a portfolio from DB, separately for each symbol and as a total net asset value (NAV)."
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

# Get balance
# ------------
balance = dict()
nav = dict()
symbols = xfrs.getStocks(dbh)
#TODO symbols = xfrs.getSymbols(dbh)

balance = xfrs.getBalance(dbh, symbols)
#TODO nav = xfrs.getNAV(dbh, symbols)

#TODO # Report cash balance
#TODO # --------------------
#TODO print "# Currencies\n";
#TODO my @currencies = xfrs::getCurrencies($dbh);
#TODO foreach my $s (@currencies) {
#TODO     print "\t$s = $balance{$s}\n";
#TODO }


# Report stock balance
# ---------------------
print("# Stocks (in units)")
for s in xfrs.getStocks(dbh):
    print(f"\t{s} = {balance.get(s, None)} ({nav.get(s, None)})")

# close DB
# --------
dbh.commit()
dbh.close()
