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

if args.symbols is not None:
    symbols = args.symbols.split()
else:
    symbols = xfrs.getSymbols(dbh)

currencies = xfrs.getCurrencies(dbh)
stocks = xfrs.getStocks(dbh)

balance = xfrs.getBalance(dbh, list(symbols) + list(currencies))
nav = xfrs.getNAV(dbh, symbols)


# Report cash balance
# --------------------
print("# Currencies")
for s in sorted(currencies):
    print(f'\t{s} = {balance.get(s,0):,.2f}')


# Report stock balance
# ---------------------
print("# Stocks (in units)")
for s in sorted(symbols):
    print(f"\t{s} = {balance.get(s, None)} ({nav.get(s, None)})")


# Report total NAV per currency
# -----------------------------
# initialize with cash balance
totals = {s: balance[s] for s in currencies}

# add NAV of all stocks
for s in symbols:
    vals = nav[s].split()
    if len(vals) > 1:
        totals[vals[1]] = totals.get(vals[1], 0) + float(vals[0])

# report
print("# Total NAV")
for s in sorted(totals.keys()):
    print(f'\t{s} = {totals[s]:,.2f}')


#TODO # Report total of totals NAV (in a base currency)
#TODO # -----------------------------------------------
#TODO if ($opt->base ne "") {
#TODO     my $total = 0;
#TODO     my $base = $opt->base;
#TODO     print "# Total NAV ($base)\n";
#TODO     foreach my $s (keys %totals) {
#TODO         if ($s eq $base) {
#TODO             $total += $totals{$s};
#TODO         } else {
#TODO             # query the conversion rate
#TODO             my %qs = xfrs::getQuoteCurrency($dbh,'',$base,$s);
#TODO             if (exists($qs{$s})) {
#TODO                 $total += $totals{$s} * $qs{$s}->{'price'};
#TODO             } else {
#TODO                 print "Error: Failed to obtain conversion rate $s to $base!\n";
#TODO             }
#TODO         }
#TODO     }
#TODO     print "\t$base = $total\n";


# close DB
# --------
dbh.commit()
dbh.close()
