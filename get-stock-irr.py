import pyxirr
import sqlite3
import sys
import xfrs
import argparse
import datetime


parser = argparse.ArgumentParser(
        description="Calculates XIRR (Irregular Internal Rate of Return) for stocks in DB."
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

dbh = sqlite3.connect(args.db)
cursor = dbh.cursor()


if args.symbols is not None:
    symbols = args.symbols.split()
else:
    symbols = xfrs.getStocks(dbh)

today = datetime.date.today().strftime("%Y-%m-%d")

for s in symbols:

    # get amounts that directly increase or decrease the balance
    stmt = 'select type, date, amount, unit_price, unit_curr, comm_price, comm_curr from xfrs where'
    stmt += f' source_curr = \'{s}\''
    stmt += ' order by date;'
    cursor.execute(stmt)

    cashflow = dict()

    curr = None; # stock currency, shall be same for all transactions
    units = 0; # stock units balance
    dividend = 0; # accumulated dividend
    date = None; # date of last buy/sell transaction 

    for row in cursor.fetchall():
        if curr is None: curr = row[4]

        # sanity check: transaction type
        if row[0] not in {'buy', 'sell', 'dividend'}:
            print(f'Error: Unknown transaction type for stock {s}: {row[0]}', file=sys.stderr)
            continue

        # sanity check: source currency
        if row[4] != curr:
            print(f'Error: Unexpected currency for {s} {row[0]}: act {row[4]}, exp {curr}', file=sys.stderr)
            continue

        # sanity check: commissions currency
        if row[6] != curr:
            print(f'Error: Unexpected commision currency for {s} {row[0]}: act {row[6]}, exp {curr}', file=sys.stderr);
            continue

        amount = 0;

        # "investing" transactions are counted as negative,
        # "divesting" (or profit taking) transactions are counted as positive
        if row[0] == 'buy':
            units += row[2]
            amount = -(row[2] * row[3] + row[5])
            date = row[1]
        elif row[0] == 'sell':
            units -= row[2]
            amount = row[2] * row[3] - row[5]
            date = row[1]
        elif row[0] == 'dividend':
            # Dividend does not increase the amount invested into the stock
            # and hence we keep it in a separate "account" and do not put it
            # into the cashflow. It will be added to the remaining balance/NAV.
            dividend += row[2] * row[3]
            dividend -= row[5]
            continue

        cashflow[date] = amount

    # get quote (to compute the actual balance)
    if units > 0:
        date = today
        cashflow[date] = 0
        q = xfrs.getQuoteStock(dbh, date, [s,])
        if q is None or s not in q:
            print(f'Error: Failed to quote stock {s}!', file=sys.stderr)
        else:

            # currency sanity check
            if q[s]['currency'] != curr:
                print(f'Error: Mismatch in stock {s} currency: act. {q[s]["currency"]}, exp. {curr}', file=sys.stderr)
            else:
                cashflow[date] += units * q[s]['price']

    # add dividend withdrawal (to the last date when owning the stock)
    # (this withdrawal is virtual and for IRR comp purposes)
    cashflow[date] += dividend

    #TODO:remove
    ## for k, v in cashflow.items():
    ##     print(f">>\t{s},{k},{v}")

    # calculate XIRR
    irr = pyxirr.xirr(cashflow.keys(), cashflow.values())
    if irr is not None:
        print(f"xirr({s}) = {irr*100:.2f}%")
    else:
        # in case XIRR comp failed
        print(f"xirr({s}) = ???")
        for k, v in cashflow.items():
            print("\t", '\t'.join(['#', s, k, str(v)]))

