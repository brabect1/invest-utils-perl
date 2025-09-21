import pyxirr
import sqlite3
import sys
import xfrs
import argparse
import datetime
import numbers
import re
import pyxirr

# TODO brabect1 (07-Sep-2025): Next updates to the script:
#      - Add 'since', 'to', 'duration' columns to give insight into
#        temporal aspects of performance.
#      - Add performance in individual currencies.
#      - Add performance in base curremcy. This would count only
#        'deposit' and 'withdraw' transactions and would FX convert
#        (virtually) based on the FX rate om the transaction date

parser = argparse.ArgumentParser(
        description="Calculates portfolie performance."
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
        props[s]['curr'] = xfrs.getStockCurrency(dbh, s) or '???'
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

# Report totals for stocks of the same currency
# ---------------------------------------------
# ***TODO*** This is a temporary solution. Properly, it shall be computed
#            from the results of DB queries. The reason is that for calculating
#            IRR we will need all the transactions, not just a "total sum".
print("\n# Stocks (total)")

totals = dict()
for s in stocks:
    p = props[s]

    # sanity check
    if 'curr' not in p or p['curr'] == '???':
        print(f'Error: Undefined currency for {s}!', file=sys.stderr)
        continue

    curr = p['curr']
    if curr not in totals:
        totals[curr] = {
            'sym': curr,
            'curr': curr,
            'units': 'n/a',
            'nav': 0,
            'dividend': 0,
            'investment': 0,
            'total_investment': 0,
            'sell_val': 0,
            'sell_gain': 0,
            'total_gain': 'n/a',
            'total_gain_percent': 'n/a',
            'irr': 'n/a',
            'symbols': list(),
            }

    totals[curr]['nav'] += p['nav']
    totals[curr]['dividend'] += p['dividend']
    totals[curr]['investment'] += p['investment']
    totals[curr]['total_investment'] += p['total_investment']
    totals[curr]['sell_val']  += p['sell_val']
    totals[curr]['sell_gain'] += p['sell_gain']
    totals[curr]['symbols'].append(s)

# calculate total performance figures
for curr in totals.keys():
    totals[curr]['real_gain'] =  totals[curr]['sell_gain'] + totals[curr]['dividend'];
    totals[curr]['unreal_gain'] =  totals[curr]['nav'] - totals[curr]['investment'];
    totals[curr]['total_gain'] =  totals[curr]['unreal_gain'] + totals[curr]['real_gain'];
    totals[curr]['total_gain_percent'] =  totals[curr]['total_gain']*100/totals[curr]['total_investment']

    # calculate IRR
    cfDates, cfAmounts, cfSyms = zip(*xfrs.getStockTransactions(dbh, totals[curr]['symbols']))
    cfDates = list(cfDates)
    cfAmounts = list(cfAmounts)
    cfSyms = list(cfSyms)
    residual = (totals[curr]['nav'] + totals[curr]['dividend'])
    if residual > 0:
        cfDates.append(today)
        cfAmounts.append(residual)
        cfSyms.append(s)
    #TODO:debug for r in zip(cfDates, cfAmounts, cfSyms):
    #TODO:debug     print(f">> {r}", file=sys.stderr)
    xirr = pyxirr.xirr(cfDates, cfAmounts)
    totals[curr]['irr'] = xirr * 100 if xirr is not None else float('NaN')

    # sanity check: (total_investment - remain_investment) = (total_sell - gain_sell)
    invest_diff = (totals[curr]['total_investment'] - totals[curr]['investment']);
    sell_diff = (totals[curr]['sell_val'] - totals[curr]['sell_gain']);
    if fmt.format(invest_diff) != fmt.format(sell_diff):
        print(f"Error: Inconsistent data for {curr} (" +
            fmt.format(invest_diff) + " vs. " +
            fmt.format(sell_diff) + "): " +
            f"total_invest={props[curr]['total_investment']}, " +
            f"remain_invest={props[curr]['investment']}, " +
            f"sell_total={props[curr]['sell_val']}, " +
            f"sell_gain={props[curr]['sell_gain']}.",
            file=sys.stderr)

# print `totals` data
print("\t".join([cols_name[c] for c in cols_order]))
for curr in sorted(totals.keys()):
    p = totals[curr]
    print("\t".join([fmt.format(p[c]) if isinstance(p[c], numbers.Number) else p[c] for c in cols_order]))


# Print cacsh report
# ------------------
print("\n# Cash")

for s in currencies:
    props[s] = {
        'sym': s,
        'curr': s,
        'units': balance[s],
        'nav': 0,
        'sell_val': 0,
        'sell_gain': 0,
        'dividend': 0 if s not in dividends else dividends[s],
        'investment': investment[s],
        'total_investment': investment_tot[s],
        'total_gain': 'n/a',
        'total_gain_percent': 'n/a',
        'irr': 'n/a',
        }

    m = rePrice.match(nav[s])
    if m:
        if m[3] != s: continue
        props[s]['nav'] = float(m[1])
    else:
        props['nav'] = nav[s]

# report
print("\t".join([cols_name[c] for c in cols_order]))
for curr in sorted(currencies):
    p = props[curr]
    print("\t".join([fmt.format(p[c]) if isinstance(p[c], numbers.Number) else p[c] for c in cols_order]))


# Report total for a base currency
# --------------------------------
if args.baseCurrency is not None:
    base_total = dict()
    base_total[args.baseCurrency] = {
        'sym': args.baseCurrency,
        'curr': args.baseCurrency,
        'units': 'n/a',
        'nav': 0,
        'dividend': 0,
        'investment': 0,
        'sell_val': 0,
        'sell_gain': 0,
        'total_investment': 0,
        'irr': 'n/a',
        }

    # Query FX conversion rates
    fx_rates = dict()
    for c in currencies:
        # skip base currency
        if c == args.baseCurrency:
            fx_rates[c] = 1.0
        else:
            s = xfrs.FxQuote.toQuoteSymbol(To=c, From=args.baseCurrency)
            cquotes = xfrs.getOnlineQuote([s,], date=today)
            if s in cquotes:
                fx_rates[c] = cquotes[s]['price']
            else:
                print("Error: Failed to obtain conversion rate $s to $base!\n", file=sys.stderr);

    # Add cash and stock balances together
    for s in currencies:
        if s not in totals: continue
        cols = ['nav', 'dividend', 'investment', 'sell_gain']
        for c in cols:
            props[s][c] += totals[s][c]

    # Get the sum over all currencies
    print(f"\n# Total {args.baseCurrency}")
    for s in currencies:
        if s not in fx_rates: continue
        cols = ['investment', 'nav', 'dividend', 'sell_val', 'sell_gain', 'total_investment']
        for c in cols:
            base_total[args.baseCurrency][c] += props[s][c] * fx_rates[s]

    # Calculate gain figures
    #TODO (21-Sep-2025): Need to double check (through unit test) if the following comps are
    #     right. Dividends are alreasy part of the cash balance, hence included in `investment`
    #     (as cash on hand) or `nav` (as reinvested capital). It seems that for `unreal_gain`
    #     the dividends would cancel out through the ddifference of `nav` and `investment`;
    #     the concern is should they not cancel, then the `real_gain` would cause double
    #     counting. Similar reasoning applies to `sell_gain`.
    #     Hence the action here is to create a unit test to test the comps yield the correct
    #     sums. To implement such unit test, the "performance" collections should aggregate
    #     into a class instance that would let query (and test) the computed values.
    base_total[args.baseCurrency]['real_gain'] = base_total[args.baseCurrency]['sell_gain'] + base_total[args.baseCurrency]['dividend']
    base_total[args.baseCurrency]['unreal_gain'] = base_total[args.baseCurrency]['nav'] - base_total[args.baseCurrency]['investment']
    base_total[args.baseCurrency]['total_gain'] = base_total[args.baseCurrency]['unreal_gain'] + base_total[args.baseCurrency]['real_gain']
    if base_total[args.baseCurrency]['total_investment'] == 0:
        base_total[args.baseCurrency]['total_gain_percent'] = '???'
    else:
        base_total[args.baseCurrency]['total_gain_percent'] =  base_total[args.baseCurrency]['total_gain']*100/base_total[args.baseCurrency]['total_investment']

    # Print results
    print("\t".join([cols_name[c] for c in cols_order]))
    p = base_total[args.baseCurrency]
    print("\t".join([fmt.format(p[c]) if isinstance(p[c], numbers.Number) else p[c] for c in cols_order]))


# close DB
# --------
dbh.commit()
dbh.close()

