import sqlite3
import sys

def getStocks(dbh):

    stocks = ()
    if dbh is None: return stocks

    stmt = "SELECT distinct source_curr from xfrs where type in ('sell', 'buy', 'dividend');"
    cursor = dbh.cursor()
    cursor.execute(stmt)
    stocks = [s[0] for s in cursor.fetchall()]

    return stocks

db = 'xfrs.sqlite3.db'
dbh = sqlite3.connect(db)

stocks = getStocks(dbh)

cursor = dbh.cursor()
for stock in stocks:
    #TODO print(stock)
    balance = 0
    stmt = f'select type, amount, unit_price, unit_curr, comm_price, comm_curr, date from xfrs where type in (\'buy\',\'sell\') and source_curr=\'{stock}\' order by date;'
    cursor.execute(stmt)

    # process the transactions fetched from DB
    curr = None
    trans = list()

    for row in cursor.fetchall():
        if len(row) < 6: continue
        if len(row[0]) == 0: continue;

        # set the currency based on the 1st transaction record
        if curr is None: curr = row[3]

        # skip the record if wrong unit currency
        if curr != row[3]:
            print(f'Error: Unexpected unit currency ({row[0]} {row[1]} {stock} units on {row[6]}): act={row[3]}, exp={curr}', file=sys.stderr)
            continue

        # invalidate commission if wrong currency
        if curr != row[5]:
            print(f'Error: Unexpected commision currency ({row[0]} {row[1]} {stock} units on {row[6]}): act={row[5]}, exp={curr}', file=sys.stderr)
            row[4] = 0;

        # act per the transaction type
        if row[0] == 'sell':
            units = -row[1];
            rs = {
                    'units': row[1],
                    'date': row[6],
                    'price': row[2],
                    'comm': row[4] / row[1]
                    }

            for rb in trans:
                # skip the buy transaction if already depleated
                # (i.e. no more units left from the transaction)
                if rb['units'] == 0: continue

                # recover sell units from the buy transaction
                units += rb['units']

                # prefix to be printed
                pfx = f'{stock}\t{rb["curr"]}'

                # suffix to be printed
                sfx = ''

                # buy records
                sfx += f'\t{rb["date"]}\t{rb["price"]}\t{rb["comm"]:.3f}'
                # sell records
                sfx += f'\t{rs["date"]}\t{rs["price"]}\t{rs["comm"]:.3f}'

                # discount the bought units and buy commissions if
                # the buy transaction gets fully cleared (i.e. selling
                # more units than what remains of the `rb` buy transaction)
                if units < 0:
                    print(pfx + f'\t{rb["units"]}' + sfx)

                    # clearing the whole buy transaction => discount also buy commission
                    rb['units'] = 0
                else:
                    print(pfx + f'\t{(rb["units"] - units)}' + sfx)
                    rb['units'] = units
                    break

            # sanity check:
            if units < 0:
                print(f'Error: Selling more than bought ({row[0]} {row[1]} {stock} units on {row[6]}): num={-units}', file=sys.stderr)

        elif row[0] == 'buy':
            # add a new record into the transactions list
            trans.append({
                'units': row[1],
                'price': row[2], # price per unit
                'curr': row[3],
                'comm': row[4] / row[1], # commision per unit
                'date': row[6]
                })

        else:
            print(f'Error: Unknown transaction type: {row[0]}', file=sys.stderr)


dbh.commit()
dbh.close()
