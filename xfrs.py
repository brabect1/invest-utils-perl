import sqlite3
import datetime

def getStocks(dbh):
    """Gets the list of stock symbols.

    Args:
      dbh: Reference to the open DB connection.

    Returns:
      An array of symbols.
    """

    if dbh is None: return None

    stmt = "SELECT distinct source_curr from xfrs where type in ('sell', 'buy', 'dividend');"
    cursor = dbh.cursor()
    cursor.execute(stmt)
    stocks = [s[0] for s in cursor.fetchall()]

    return stocks


def getDividends(dbh, **kwargs):
    """Gets a list of dividends, either all or those limited to a symbol subset.
    
    Args:
      dbh: Reference to the open DB connection.

    Kwargs:
      syms (list): list of symbols for which to get deividends
      order (str): results oredering (allowed: ``date`` (default), ``symbol``)
      fromDate (str): YYYY-MM-DD formatted date from which onwards to get dividends

    Returns:
      list: The return value is a list of records, where each record is a hash array of
            dividend properties: Symbol, Currency, Amount, Tax, Date. The list is ordered
            by date.
    """

    if dbh is None: return None

    order = 'date'
    if 'order' in kwargs and kwargs['order'] == 'symbol':
        order = 'source_curr'

    filters = ['type=\'dividend\'']

    if 'fromDate' in kwargs:
        try:
            d = datetime.datetime.strptime(kwargs["fromDate"], '%Y-%m-%d')
            filters.append(f'date>=\'{d.strftime("%Y-%m-%d")}\'')
        except ValueError:
            # ignore wrong argument
            pass

    if 'toDate' in kwargs:
        try:
            d = datetime.datetime.strptime(kwargs["toDate"], '%Y-%m-%d')
            filters.append(f'date<=\'{d.strftime("%Y-%m-%d")}\'')
        except ValueError:
            # ignore wrong argument
            pass

    stmt = f'select source_curr, unit_curr, amount*unit_price, comm_price, date from xfrs where (' + \
            ' and '.join(filters) + \
            f') order by {order};'
    cursor = dbh.cursor()
    cursor.execute(stmt)

    dividends = list()
    for row in cursor.fetchall():
        if len(row) < 5: continue

        dividends.append({
            'symbol': row[0],
            'currency': row[1],
            'amount': row[2],
            'tax':  row[3],
            'date': row[4],
            })

    return dividends


def getBalance(dbh, symbols = None):
    """Gets the current balance based on the transfers stored in the given DB.

    The routine acts on both stocks and currencies. For stocks it returns the
    number of securities held, for currencies it returns the remaining cash
    balance.

    Args:
      dbh: reference to the open DB connection
      symbols (list): reference to a hash array to be filled with a balance

    Returns:
      Dictionary indexed by symbol.
    """

    if dbh is None: return None
    cursor = dbh.cursor()

    if symbols is None:
        symbols = getStocks(dbh)

    balances = dict()

    for s in symbols:
        balance = 0

        # get amounts that directly increase or decrease the balance
        # (shall affect only cash balances)
        stmt = f'select type, amount*unit_price from xfrs where unit_curr=\'{s}\';'
        cursor.execute(stmt)

        for row in cursor.fetchall():
            if len(row) < 2: continue
            if row[0] is None or len(row[0])==0: continue

            if row[0] in ('deposit', 'fx', 'dividend', 'sell'):
                balance += row[1]
            elif row[0] in ('buy', 'withdraw'):
                balance -= row[1]
            else:
                print(f"Error: Unknown transaction type: {row[0]}", file=sys.stderr)

        # subtract any conversions where this was a source currency
        # (shall affect only cash balances)
        stmt = f'select type, source_price from xfrs where type=\'fx\' and source_curr=\'{s}\';'
        cursor.execute(stmt)

        for row in cursor.fetchall():
            if len(row) < 2: continue
            if row[0] is None or len(row[0])==0: continue
            balance -= row[1]

        # subtract any commissions
        # (shall affect only cash balances)
        stmt = f'select type, sum(comm_price) from xfrs where comm_curr=\'{s}\';'
        cursor.execute(stmt)

        for row in cursor.fetchall():
            if len(row) < 2: continue
            if row[0] is None or len(row[0])==0: continue
            balance -= row[1]

        # add increases/decreases of stock amount
        # (shall affect only stock balances)
        stmt = f'select type, amount from xfrs where type in (\'sell\', \'buy\') and source_curr=\'{s}\';'
        cursor.execute(stmt)

        for row in cursor.fetchall():
            if len(row) < 2: continue
            if row[0] is None or len(row[0])==0: continue
            if row[0] in ('sell'):
                balance -= row[1]
            elif row[0] in ('buy'):
                balance += row[1]
            else:
                print(f"Error: Unknown transaction type: {row[0]}", file=sys.stderr)

        balances[s] = balance

    return balances

