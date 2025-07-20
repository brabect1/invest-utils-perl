import sqlite3

def getStocks(dbh):
    """Gets the list of stock symbols.

    Args:
      dbh: Reference to the open DB connection.

    Returns:
      An array of symbols.
    """

    stocks = ()
    if dbh is None: return stocks

    stmt = "SELECT distinct source_curr from xfrs where type in ('sell', 'buy', 'dividend');"
    cursor = dbh.cursor()
    cursor.execute(stmt)
    stocks = [s[0] for s in cursor.fetchall()]

    return stocks

