import pyxirr
import sqlite3
import sys
import xfrs
import argparse
import datetime
import numbers
import re
import pyxirr


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
fmt = '{:.' + str(np) +'f}'

# pre-compiler regex to match `<value><currency>` patterns
rePrice = re.compile('(-?\d+(\.\d*)?)\s*([A-Z.]+)')

# today's date
today = datetime.date.today().strftime("%Y-%m-%d")


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

print('# Getting total invested amounts ...', file=sys.stderr)
investment_tot = xfrs.getTotalInvestedAmount(dbh, symbols)

print('# Getting sold values ...', file=sys.stderr)
sell_val = xfrs.getTotalSellValue(dbh, symbols)

print('# Getting sell gains ...', file=sys.stderr)
sell_gain = xfrs.getSellGain(dbh, symbols)


for s in sorted(symbols):
    #TODO print(f'divi({s}) = {dividends[s]:,.2f}')
    #TODO print(f'invamnt({s}) = {investment[s]:,.2f}')
    #TODO print(f'totinvamnt({s}) = {investment_tot[s]:,.2f}')
    #TODO print(f'sold({s}) = {sell_val[s]:,.2f}')
    #TODO print(f'sellgain({s}) = {sell_gain[s]:,.2f}')
    pass

# Report stock balance
# ---------------------
print("# Stocks")
props = dict()
for s in stocks:
    props[s] = dict()
    props[s]['sym'] = s
    props[s]['units'] = balance[s]

    m = rePrice.match(nav[s])
    if m:
        props[s]['nav'] = float(m[1])
        props[s]['curr'] = m[3]
    else:
        props['nav'] = nav[s]

    props[s]['dividend'] = dividends[s]
    props[s]['investment'] = investment[s]
    props[s]['total_investment'] = investment_tot[s]
    props[s]['sell_val'] = sell_val[s]
    props[s]['sell_gain'] = sell_gain[s]
    if 'curr' in props[s]:
        props[s]['real_gain'] =  props[s]['sell_gain'] + props[s]['dividend']
        props[s]['unreal_gain'] =  props[s]['nav'] - props[s]['investment']
        props[s]['total_gain'] =  props[s]['unreal_gain'] + props[s]['real_gain']
        props[s]['total_gain_percent'] =  props[s]['total_gain']*100/props[s]['total_investment']
    else:
        print(f"!!!cur!!! {s}")
        props[s]['curr'] = '???'
        props[s]['real_gain'] = float('NaN')
        props[s]['unreal_gain'] =  float('NaN')
        props[s]['total_gain'] =  float('NaN')
        props[s]['total_gain_percent'] =  float('NaN')

    # compute XIRR (irregular internal rate of return)
    cfDates, cfAmounts, cfSyms = zip(*xfrs.getStockTransactions(dbh, [s,]))
    cfDates = list(cfDates)
    cfAmounts = list(cfAmounts)
    cfSyms = list(cfSyms)
    if 'nav' not in props[s]:
        print(f"!!!nav!!! {s}", file=sys.stderr)
        props[s]['nav'] = 0
    if 'dividend' not in props[s]:
        print(f"!!!div!!! {s}", file=sys.stderr)
        props[s]['dividend'] = 0

    residual = props[s]['nav'] + props[s]['dividend']
    if residual > 0:
        cfDates.append(today)
        cfAmounts.append(residual)
        cfSyms.append(s)
    #TODO:debug for r in zip(cfDates, cfAmounts, cfSyms):
    #TODO:debug     print(f">> {r}", file=sys.stderr)
    xirr = pyxirr.xirr(cfDates, cfAmounts)
    props[s]['irr'] = xirr * 100 if xirr is not None else float('NaN')

    # sanity check: (total_investment - remain_investment) = (total_sell - gain_sell)
    invest_diff = (props[s]['total_investment'] - props[s]['investment'])
    sell_diff = (props[s]['sell_val'] - props[s]['sell_gain'])
    # using `sprintf` to round to avoid floating point math rounding errors
    if fmt.format(invest_diff) != fmt.format(sell_diff):
        print(f"Error: Inconsistent data for {s} (" +
            fmt.format(invest_diff) + " vs. " +
            fmt.format(sell_diff) + "): " +
            f"total_invest={props[s]['total_investment']}, " +
            f"remain_invest={props[s]['investment']}, " +
            f"sell_total={props[s]['sell_val']}, " +
            f"sell_gain={props[s]['sell_gain']}.",
            file=sys.stderr)

cols_order = [
    'sym',
    'curr',
    'total_investment',
    'investment',
    'nav',
    'sell_val',
    'sell_gain',
    'dividend',
    'total_gain',
    'total_gain_percent',
    'irr',
    ]
cols_name = {
    'sym': 'sym',
    'curr': 'curr',
    'investment': 'remain_invest',
    'total_investment': 'total_invest',
    'nav': 'nav',
    'sell_val': 'sell_val',
    'sell_gain': 'sell_gain',
    'dividend': 'dividend',
    'real_gain': 'real_gain',
    'unreal_gain': 'unreal_gain',
    'total_gain': 'total_gain',
    'total_gain_percent': 'total_gain_%',
    'irr': 'irr_%',
    }

print("\t".join([cols_name[c] for c in cols_order]))
for s in sorted(stocks):
    p = props[s]
    print("\t".join([fmt.format(p[c]) if isinstance(p[c], numbers.Number) else p[c] for c in cols_order]))


# close DB
# --------
dbh.commit()
dbh.close()

