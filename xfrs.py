import sqlite3
import datetime
import yfinance
import numbers
from abc import ABC, abstractmethod


ALLOWED_RECORDS = {'buy', 'sell', 'dividend', 'fx', 'deposit', 'withdraw', 'quote'}
"""(constant) List of recognized DB records.

As the module is unlikely to be used for other than the author's own purposes
there is little reason to rise barriers on altering the list. If needed in
future, see https://stackoverflow.com/a/2688086 for one solution.
"""

REC_INDEXES = {
        'type': 0,
        'date': 1,
        'amount': 2,
        'unit_price': 3,
        'unit_curr': 4,
        'source_price': 5,
        'source_curr': 6,
        'comm_price': 7,
        'comm_curr': 8,
        }
"""Look-up table to translate DB record named indices to the positional indices.

The positional indices are used in list representation of a DB record.
"""

def addRecord(dbh, record):
    """Adds a new record into DB.

    The routine automatically creates a DB table if not already exists. Record
    IDs are inferred automatically in a successive order.

    IMPORTANT: It is currently assumed the (transfer) records are unique, even
    if the transfer were of the same type, symbol/instrument and date. It seems
    possible to have two transfers of the same kind on the same date (e.g. as
    the broker executed one order as multiple transactions).
    For quotes, though, a quote should be unique for the symbol and the date.
    Hence the quote record would be updated should it already exist in DB.

    The `record` is a list of strings of the following meaning:
    0. Record type.

    Args:
        dbh: reference to the open DB connection
        record: hash array mapping record attributes to record values
    """

    if not isinstance(record, list):
        raise TypeError(f'record: list expected, got {record.__class__}')

    if len(record) < 9:
        raise ValueError(f'record: too short ({len(record)})')

    if dbh is None: return
    cursor = dbh.cursor()

    # `quote` record
    if record[0] == 'quote':

        # create and cache the quote
        c = record[REC_INDEXES['unit_curr']]
        quote = {
                'price': record[REC_INDEXES['unit_price']],
                'currency': c,
                }
        if len(record[REC_INDEXES['comm_curr']]) > 0:
            quote['type'] = record[REC_INDEXES['comm_curr']]
        cacheQuote(dbh, record[REC_INDEXES['source_curr']], quote)

    # all but `quote` records
    elif record[0] in ALLOWED_RECORDS:

        # make sure the `xfrs` table exists
        stmt = '''CREATE TABLE IF NOT EXISTS xfrs (
            id INT PRIMARY KEY,
            type TEXT NOT NULL,
            date TEXT,
            amount INT NOT NULL,
            unit_price REAL,
            unit_curr TEXT,
            source_price REAL,
            source_curr TEXT,
            comm_price REAL,
            comm_curr TEXT);
            '''
        cursor.execute(stmt)

        # get the record ID
        stmt = 'SELECT COUNT(1) FROM xfrs;'
        cursor.execute(stmt)
        row = cursor.fetchone()
        id = 0 if row is None else row[0]

        # create the INSERT statement/query
        stmt = 'INSERT INTO xfrs (id,' + ','.join(REC_INDEXES.keys()) + ') VALUES '''
        stmt += f"({id+1},"
        stmt += ','.join([f'\'{v}\'' for v in record[0:9]])
        stmt += ");"
        cursor.execute(stmt)


def getSymbols(dbh):
    """Gets the list of all symbols from the transfers DB, incl. currencies and stocks.

    Args:
      dbh: reference to the open DB connection

    Returns:
      an array of symbols
    """

    if dbh is None: return None
    cursor = dbh.cursor()

    symbols = set()

    stmt = 'SELECT unit_curr, source_curr, comm_curr from xfrs;'
    cursor.execute(stmt)
    for row in cursor.fetchall():
        if len(row) > 0: symbols.update(row)

    return symbols


def getCurrencies(dbh):
    """Gets the list of currency symbols from DB.

    Args:
      dbh: reference to the open DB connection

    Returns:
      a list of (currency) symbols
    """

    if dbh is None: return None
    cursor = dbh.cursor()

    currencies = set()

    # get all currencies from currency transactions
    stmt = 'select source_curr, unit_curr, comm_curr from xfrs where type in (\'deposit\',\'fx\',\'withdraw\');'
    cursor.execute(stmt)
    for row in cursor.fetchall():
        if len(row) > 0: currencies.update(row)

    # get all currencies from stock transactions
    stmt = 'select unit_curr, comm_curr from xfrs where type in (\'sell\',\'buy\',\'dividend\');'
    cursor.execute(stmt)
    for row in cursor.fetchall():
        if len(row) > 0: currencies.update(row)

    return currencies

def isCurrency(dbh, symbol):
    """Identifies if a symbol is a currency.

    Args:
      dbh: reference to the open DB connection
      symbol (string): symbol to test if represent a currency in DB

    Returns:
      true if the symbol is a currency defined in the DB, false otherwise
    """

    if symbol is None: return False
    if dbh is None: return False
    cursor = dbh.cursor()

    stmt = 'SELECT count(*) from xfrs where '

    # currency manip records (a currency may act as any of the currency records)
    stmt += f"(type in ('deposit','fx','withdraw') and"
    stmt += f" (source_curr='{symbol}' or unit_curr='{symbol}' or comm_curr='{symbol}')"
    stmt += f")"
    # stock manip records (a currency may act as a unit or commission currency)
    stmt += f" or "
    stmt += f"(type in ('sell','buy','dividend') and (unit_curr='{symbol}' or comm_curr='{symbol}'))"
    # end of the statement
    stmt += ";"

    cursor.execute(stmt)
    rows = cursor.fetchall()
    return len(rows) > 0 and len(rows[0]) > 0 and rows[0][0] > 0


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


def isStock(dbh, symbol):
    """Identifies if a symbol is a stock symbol.

    Args:
      dbh: reference to the open DB connection
      symbol (str): stock ticker

    Returns:
      true is the symbol is a stock symbol defined in the DB, false otherwise
    """

    if symbol is None: return False
    if dbh is None: return False
    cursor = dbh.cursor()

    stmt = f'SELECT count(*) from xfrs where type in (\'sell\', \'buy\', \'dividend\') and source_curr=\'{symbol}\';'

    cursor.execute(stmt)
    rows = cursor.fetchall()
    return len(rows) > 0 and len(rows[0]) > 0 and rows[0][0] > 0


def getDividends(dbh, **kwargs):
    """Gets a list of dividends, either all or those limited to a symbol subset.
    
    Args:
      dbh: Reference to the open DB connection.

    Kwargs:
      syms (list): list of symbols for which to get deividends
      order (str): results oredering (allowed: ``date`` (default), ``symbol``)
      fromDate (str): YYYY-MM-DD formatted date from which onwards to get dividends
      toDate (str): YYYY-MM-DD formatted latest date by which to get dividends

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

    if 'fromDate' in kwargs and kwargs['fromDate'] is not None:
        try:
            d = datetime.datetime.strptime(kwargs["fromDate"], '%Y-%m-%d')
            filters.append(f'date>=\'{d.strftime("%Y-%m-%d")}\'')
        except ValueError:
            # ignore wrong argument
            pass

    if 'toDate' in kwargs and kwargs['toDate'] is not None:
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


def cacheQuote(dbh, symbol, quote):
    """Adds a cached quote to DB.

    A *quote* is simply a price for a unit of a stock or currency. We cache
    quotes to avoid querying online quote sources, which may come with some
    delay (of querying the source and getting response) and maybe restrictions
    on the source side (e.g. how many quotes we may place, how often, etc.).

    Cached quotes are simply another table in DB. Quotes are cahed only
    explicitly through `addCahedQuote()`.

    Args:
      dbh: reference to the open DB connection
      symbol (str): ticker of the stock or currency
      quote (dict): hash array of quote attributes; the attributes
                    are ``date`, `price` and `currency`;
                    all but `date` (which defaults to the current date) are mandatory
    """

    if dbh is None or symbol is None or quote is None: return
    for attr in ['price', 'currency']:
        if attr not in quote or quote[attr] is None: return

    cursor = dbh.cursor()

    if 'date' not in quote:
        date = datetime.date.today().strftime("%Y-%m-%d")
    else:
        date = quote['date']

    # make sure the 'quotes' table exist, or create otherwise
    stmt = '''CREATE TABLE IF NOT EXISTS quotes (
            id INT PRIMARY KEY,
            symbol TEXT NOT NULL,
            date TEXT NOT NULL,
            type TEXT NOT NULL,
            price REAL,
            curr TEXT);'''
    cursor.execute(stmt)

    # query existing DB quotes
    stmt = f'SELECT * from quotes where date=\'{date}\' AND symbol=\'{symbol}\';'
    cursor.execute(stmt)

    priceType = 'real' if 'type' not in quote else quote['type']
    if len(cursor.fetchall()) == 0:
        # no quote yet => create
        stmt = "INSERT INTO quotes (symbol,date,type,price,curr) VALUES ("
        stmt += f'\'{symbol}\','
        stmt += f'\'{date}\','
        stmt += f'\'{priceType}\','
        stmt += f'\'{quote["price"]}\','
        stmt += f'\'{quote["currency"]}\');'
    else:
        # some quote already cached => update
        stmt = "UPDATE quotes SET "
        stmt += f'type=\'{priceType}\''
        stmt += f', price=\'{quote["price"]}\''
        stmt += f', curr=\'{quote["currency"]}\''
        stmt += f' where date=\'{date}\' AND symbol=\'{symbol}\''

    cursor.execute(stmt)
    dbh.commit()


def getOnlineQuote(symbols, **kwargs):
    """Gets the quoted price from Yahoo Finance.

    Args:
      symbols (list): list of symbols to quote

    Kwargs:
      date (str): YYYY-MM-DD formatted date on which to get the quote
      dbh: reference to the open DB connection
      cache (bool): when true the quote will get cached into DB, if `dbh` handle provided

    Returns:
      Returns a hash indexed by a symbol and for each the following attributes:
      'price', 'currency' and 'date'.
    """

    quotes = dict()
    if symbols is None or len(symbols) == 0: return quotes

    # date of `None` means today
    date = None
    today = datetime.date.today()
    if 'date' in kwargs and kwargs['date'] is not None:
        date = datetime.datetime.strptime(kwargs['date'], "%Y-%m-%d").date()
        if date > today: return quotes
        elif date == today: date = None

    stoday = today.strftime('%Y-%m-%d')
    lquotes = list()
    for s in symbols:
        qs = yfinance.Ticker(s)
        q = { 'regularMarketPrice': None, 'currency': None}
        try:
            if date is None:
                for a in q.keys():
                    q[a] = qs.info[a]
                lquotes.append({
                        'symbol': s,
                        'price': q['regularMarketPrice'],
                        'currency': q['currency'],
                        'date': stoday,
                        'type': 'real',
                        })
            else:
                # use `end=date+1` as the `history()` method does not include the end date
                # use `start=date-7` to span potential weekend and holiday days
                df = qs.history(start=date - datetime.timedelta(days=7), end=date + datetime.timedelta(days=1)).tail(1)

                # currency
                curr = qs.info['currency']

                # close price
                price = df['Close'].tolist()[0]

                # close date
                cdate = df.index[0].to_pydatetime().date()

                for d in [cdate, date]:
                    lquotes.append({
                            'symbol': s,
                            'price': price,
                            'currency': curr,
                            'date': d.strftime('%Y-%m-%d'),
                            'type': 'close',
                            })

        except:
            pass

    if 'cache' in kwargs and kwargs['cache']:
        if 'dbh' in kwargs and kwargs['dbh'] is not None:
            for q in lquotes:
                cacheQuote(kwargs['dbh'], q['symbol'], q)

    for q in lquotes:
        if q['symbol'] not in quotes: quotes[q['symbol']] = q

    return quotes


def getCachedQuote(dbh, symbols, **kwargs):
    """Gets the cached quoted price from DB.

    Args:
      dbh: reference to the open DB connection
      symbols (list): list of symbols to quote

    Kwargs:
      date (str): YYYY-MM-DD formatted date on which to get the quote (use None for today)
      online (bool): when true and not cached, fallback on online quote and cache it

    Returns:
      Returns a hash indexed by a symbol and for each the following attributes:
      'price', 'currency' and 'date'.
    """

    if dbh is None: return None

    quotes = dict()
    if symbols is None or len(symbols) == 0: return quotes

    today = datetime.date.today()
    date = today
    if 'date' in kwargs and kwargs['date'] is not None:
        date = datetime.datetime.strptime(kwargs['date'], "%Y-%m-%d").date()
        if date > today: return quotes

    sdate = date.strftime('%Y-%m-%d')
    cursor = dbh.cursor()

    # Test if the 'quotes' table exist, or create otherwise
    stmt = "SELECT name FROM sqlite_master WHERE type='table' AND name='quotes';"
    cursor.execute(stmt)

    cacheExists = len(cursor.fetchall()) > 0
    fallbackOnline = 'online' in kwargs and kwargs['online']

    # return empty result if no cache and no falling back online
    if not cacheExists and not fallbackOnline: return quotes

    # query existing DB quotes
    if cacheExists:
        for s in symbols:
            stmt = f'SELECT price, curr, type from quotes where date=\'{date}\' AND symbol=\'{s}\';'
            cursor.execute(stmt)
            rows = cursor.fetchall()
            if len(rows) > 0:
                quotes[s] = {
                        'symbol': s,
                        'price': rows[0][0],
                        'currency': rows[0][1],
                        'date': sdate,
                        'type': rows[0][2],
                        }

    # falling back online for missing symbols
    if fallbackOnline and len(symbols) > len(quotes.keys()):
        onlineQuotes = getOnlineQuote(
                [s for s in symbols if s not in quotes],
                dbh = dbh, date = sdate, cache = True
                )
        quotes.update(onlineQuotes)

    return quotes


def getQuoteStock(dbh, date, symbols):
    """Gets quote for given stock symbols.

    Returns a hash indexed by a symbol and for each the following attributes:
    'price' and 'currency'.

    Symbols, for which no quote was obtained, will not be included in the returned
    hash. Detecting if some symbols failed can be done by comparing the number of
    symbols in the input list and in keys of the output hash.

    Quotes are obtained from the following sources (with decreasing priority):
    - DB cache
    - Yahoo Finance

    This routine is somewhat strange for the `xfrs` package, which is intended to provide
    access routines to an XFRS DB. However, the quotes are needed for the getNAV() routine.

    Separate routines exist for stocks and currencies as the API to get real-time quotes
    is different.

    Arg:
      dbh: reference to the open DB connection
      date (str): date of the quote as YYYY-MM-DD string (use None for today)
      symbols (list): list of symbols to quote

    Returns:
      Returns a hash indexed by a symbol and for each the following attributes:
      'price' and 'currency'.
    """

    if dbh is None: return None
    if symbols is None or len(symbols) == 0: None

    if date is None:
        date = datetime.date.today().strftime("%Y-%m-%d")

    quotes = dict()

    #TODO:remove? attrs = ["last", "currency"]
    #TODO:remove? attrMap = {
    #TODO:remove?         'last': 'price',
    #TODO:remove?         'currency': 'currency'
    #TODO:remove?         }


    # obtain cached quotes
    quotes.update(getCachedQuote(dbh, symbols, date = date))

    # obtain additional quotes (if needed) from Yahoo finance
    symbols = [s for s in symbols if s not in quotes]
    if len(symbols) > 0:
        quotes.update(getOnlineQuote(symbols, date = date))

    return quotes


def getQuoteCurrency(dbh, date, symbols):
    """Gets quote for given currencies to the base one.

    Returns a hash indexed by a symbol and for each the following attributes:
    'price' and 'currency'.

    Symbols, for which no quote was obtained, will not be included in the returned
    hash. Detecting if some symbols failed can be done by comparing the number of
    symbols in the input list and in keys of the output hash.

    Quotes are obtained from the following sources (with decreasing priority):
    - DB cache
    - Yahoo Finance

    This routine is somewhat strange for the `xfrs` package, which is intended to provide
    access routines to an XFRS DB. However, the quotes are needed for the getNAV() routine.

    Separate routines exist for stocks and currencies as the API to get real-time quotes
    is different.

    Args:
      dbh: reference to the open DB connection
      date (str): date of the quote as YYYY-MM-DD string (use None for today)
      symbols (list): list of currency symbols to quote, where the first acts as the base
                      (example: a list of `EUR`,`USD`,`CAD` will quote `USDEUR` and `CADEUR` pairs)

    Returns:
      Returns a hash indexed by a symbol and for each the following attributes:
      'price' and 'currency'.
    """

    if dbh is None: return None
    if symbols is None or len(symbols) <= 1: None

    if date is None:
        date = datetime.date.today().strftime("%Y-%m-%d")

    base = symbols.pop(0)
    pairs = list({s + base for s in symbols if s != base})

    # get cached quotes first
    quotes = getCachedQuote(dbh, pairs, date = date)

    # quote conversion rates at Yahoo Finance (if needed)
    if len(pairs) > len(quotes.keys()):
        rates = getOnlineQuote([s + '=X' for s in pairs if s not in quotes], date = date)
        quotes.update({k[:-2]: v for k, v in rates.items()})

        #TODO # cache online query results
        #TODO for k, q in rates.items():
        #TODO     cacheQuote(dbh, k[:-2], q)

    return quotes


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
        symbols = getSymbols(dbh)

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


def getNAV(dbh, symbols):
    """Gets the net asset value (NAV) for the given symbols.

    The NAV value is returned with indication of the currency (e.g. 30.25USD).

    NAV is computed as the number of remaining shares (as returned by getBalance())
    times the present share value. As such it represent the actual value of
    a position.

    No commissions are involved in NAV. The commissions rather apply for analyzing
    a gain or invested amount.

    Args:
      dbh: reference to the open DB connection
      symbols (list): list of tickers for which to calculate NAV,
                      if ``None`` then all symbols in DB apply

    Returns:
      Dictionary indexed by symbols and values representing each symbol's NAV.
    """

    if dbh is None: return None
    cursor = dbh.cursor()

    if symbols is None:
        symbols = getSymbols(dbh)

    balances = getBalance(dbh, symbols)
    if balances is None: return None

    currencies = getCurrencies(dbh)
    stocks = getStocks(dbh)

    date = datetime.date.today().strftime("%Y-%m-%d")
    navs = dict()

    # currencies
    for s in [s for s in symbols if s in currencies]:
        navs[s] = ('0 ' if s not in balances else f'{balances[s]:.2f} ') + s

    # stocks
    quotes = getQuoteStock(dbh, date, [s for s in symbols if s in stocks])
    for s, q in quotes.items():
        if s in balances:
            navs[s] = f'{q["price"] * balances[s]:.2f} {q["currency"]}'
        else:
            navs[s] = f'0 {q["currency"]}'

    # undetermined
    navs.update({s: '???' for s in symbols if s not in navs})

    return navs


class Price(object):
    """Represents a price that consists of the numerical value and the currency."""

    fmt = '{:,.3f}'
    """str: Format of the `Price` string representation."""

    def __init__(self, value, currency, fmt=None):
        """Creates a new `Price` instance.

        Args:
            value (float): Price value.
            currency (str): Currency associated with the price.
            fmt (str): Float formatting string to be used for Price to string
                       conversion. `None` yields the defualt `Price` class
                       formatting.
        """
        if not isinstance(value, numbers.Number):
            raise TypeError('`value` not a number')
        if value < 0:
            raise ValueError(f'`value={value}` cannot be negative')
        if not isinstance(currency, str):
            raise TypeError('`currency` not a string')

        self.value = value
        self.currency = currency
        self.fmt = fmt

    def copy(self, fmt=None):
        """Gets the new copy of the Price instance with a provided formatting.

        Args:
            fmt (str): Float formatting string to be used for Price to string
                       conversion. `None` yields the defualt `Price` class
                       formatting.

        Returns:
            Copy of the Price instance with a new formatting.
        """
        return Price(self.value, self.currency, fmt)

    def __str__(self):
        return (self.fmt or Price.fmt).format(self.value) + self.currency


class Quote(ABC):
    """Represents a symbol quote.

    Price at a given date/time is the primary information the quote provides.

    In XFRS, quotes are represented with the date granularity as the module
    is not intended for monitoring/recorfing intraday price movements.
    """

    fmtDate = '%Y-%m-%d'
    """Date string format."""


    def __init__(self, **kwargs):
        """Creates a new quote instance.

        Kwargs:
            symbol (str): Required symbol string.
            price (float): Required price of the quote.
            currency (str): Required quoted price currency.
            date (str): Optional date of the quote as YYYY-MM-DD string. Missing or `None` mean today.
            type (str): Optional type of the quote. Missing or `None` means quote at the market close.
        """
        for a, t in {'symbol': str, 'price': float, 'currency': str}.items():
            if a not in kwargs:
                raise ValueError(f'Missing `{a}` information!')
            if not isinstance(kwargs[a], t):
                raise TypeError(f'`{a}` of wrong type, {t} expected but is {kwargs[a].__class__}.')

        if 'date' not in kwargs or kwargs['date'] is None:
            self.date = datetime.date.today()
        elif isinstance(kwargs['date'], str):
            self.date = datetime.datetime.strptime(kwargs["date"], '%Y-%m-%d').date()
        elif isinstance(kwargs['date'], datetime.date):
            self.date = kwargs['date']
        else:
            raise TypeError(f'`date` of wrong type, `datetime.date` expected but is {kwargs["date"].__class__}.')

        self.price = Price(kwargs['price'], kwargs['currency'])
        self.symbol = kwargs['symbol']

        if 'type' not in kwargs or kwargs['type'] is None:
            self.type = 'close'
        elif not isinstance(kwargs['type'], str):
            raise TypeError(f'`type` of wrong type, {str.__class__} expected but is {kwargs["type"].__class__}.')
        else:
            self.type = kwargs['type']


    def __str__(self):
        return f'{self.symbol}={self.price} @ ' + self.date.strftime("%Y-%m-%d")


    @abstractmethod
    def isStock(self):
        """Indicates if the quoted symbol is a stock ticker.

        Returns:
            `True` if the quoted symbol is stock, `False` otherwise.
        """
        pass


    @abstractmethod
    def isCurrency(self):
        """Indicates if the quoted symbol represents a currency pair quote.

        Returns:
            `True` if the quoted symbol is a currency pair, `False` otherwise.
        """
        pass


    def getType(self):
        """Gets the type of quoted price.

        Returns:
            Type of the quoted price.
        """
        return self.type


    def getPrice(self, fmt=None):
        """Gets the quoted price.

        Args:
            fmt (str): Float formatting string to be used for Price to string
                       conversion. `None` yields the defualt `Price` class
                       formatting.

        Returns:
            Quoted price as a `Price` instance.
        """
        return self.price.copy(fmt)


    def getDate(self, fmt = None):
        """Gets the date of the quote.

        Returns:
            Date of the quote as the `datetime.date` instance.
        """
        if fmt is None:
            return self.date
        elif isinstance(fmt, str):
            return self.date.strftime(fmt)
        else:
            ValueError('Wrong value for the date format.')


    @abstractmethod
    def getSymbol(self):
        """Gets the quote symbol.

        Returns:
            String representing the symbol.
        """
        pass


    @abstractmethod
    def getYticker(self):
        """Gets the yfinance ticker associated with the quote.

        Returns:
            String representing the ticker for yahoo finance.
        """
        pass


class StockQuote(Quote):
    """Represents the stock symbol quote."""

    def __init__(self, **kwargs):
        """Creates a new quote instance.

        Kwargs:
            symbol (str): Required symbol string.
            price (float): Required price of the quote.
            currency (str): Required quoted price currency.
            date (str): Optional date of the quote as YYYY-MM-DD string. Missing or `None` mean today.
            type (str): Optional type of the quote. Missing or `None` means quote at the market close.
        """
        super().__init__(**kwargs)


    def isStock(self):
        return True

    def isCurrency(self):
        return False

    def getSymbol(self):
        return self.symbol;

    def getYticker(self):
        return self.symbol;


class FxQuote(Quote):
    """Represents the currency pair quote."""


    def __init__(self, **kwargs):
        """Creates a new quote instance.

        Kwargs:
            symbol (str): Required symbol string.
            price (float): Required price of the quote.
            currency (str): Required quoted price currency.
            date (str): Optional date of the quote as YYYY-MM-DD string. Missing or `None` mean today.
            type (str): Optional type of the quote. Missing or `None` means quote at the market close.
        """
        # repurpose `symbol` and `currency` into the FX symbol
        if 'symbol' in kwargs and 'currency' in kwargs:
            self.pair = [str(kwargs['symbol']), str(kwargs['currency'])]
            kwargs['symbol'] = self.__class__.toFxSymbol(From=self.pair[1], To=self.pair[0])

        super().__init__(**kwargs)


    def isStock(self):
        return False

    def isCurrency(self):
        return True

    def getSymbol(self):
        return self.pair[0];

    def getYticker(self):
        return self.pair[0] + self.pair[1] + '=X';

    @classmethod
    def toQuoteSymbol(cls, **kwargs):
        """Composes an `yfinance` ticker representing the currency/forex (FX) pair.

        The distinction to `toFxSymbol()` is that the *FX symbol* can only include word
        characters, dash (`-`) and a dot (`.`). The *quote symbol* represents the FX pair
        ticker for `yfinance`. Hence for the "EUR/USD" pair, the *FX symbol* and *quote
        symbol* is `EUR-USD` and `EURUSD=X`, respectively.

        Kwargs:
            To (str): *To* currency of the FX pair.
            From (str): *From* currency of the FX pair.

        Returns:
            String representing the `yfinance` *quote symbol* of the FX pair.
        """

        if 'Pair' in kwargs and isinstance(kwargs['Pair'], list) and len(kwargs['Pair']) > 1:
            return kwargs['Pair'][0] + '-' + kwargs['Pair'][1]

        for k in ['To', 'From']:
            if k not in kwargs or kwargs[k] is None:
                return None
            if not isinstance(kwargs[k], str):
                raise TypeError(f'kwargs[{k}] not a string')

        return kwargs['To'] + kwargs['From'] + '=X'

    @classmethod
    def fromQuoteSymbol(cls, symbol):
        """Recovers a curryency/FX pair from an FX *quote symbol*.

        This method is a counterpart to the `toQuoteSymbol()` and returns
        the currency pair in a form of a hash array/dictionary with ``From``
        and ``To`` indices.

        Args:
            symbol (str): *Quote symbol* representing a currency pair.

        Returns:
            Dictionary with ``From`` and ``To`` indices, or None.
        """
        if symbol is None or not isinstance(symbol, str):
            raise TypeError(str(symbol))
            return None
        if symbol[-2:] == '=X':
            symbol = symbol[:-2]
            f = symbol[-3:]
            t = symbol[:-3]
            return {'From': f, 'To': t, 'Pair': [t, f]}
        else:
            raise ValueError(f'{symbol} -> {symbol[-2:]}')
            return None

    @classmethod
    def toFxSymbol(cls, **kwargs):
        """Composes a currency/forex (FX) pair symbol to use in the string representation
        of DB records.

        The distinction to `toQuoteSymbol()` is that the *FX symbol* can only include word
        characters, dash (`-`) and a dot (`.`). The *quote symbol* represents the FX pair
        ticker for `yfinance`. Hence for the "EUR/USD" pair, the *FX symbol* and *quote
        symbol* is `EUR-USD` and `EURUSD=X`, respectively.

        Kwargs:
            To (str): *To* currency of the FX pair.
            From (str): *From* currency of the FX pair.

        Returns:
            String representing the *FX symbol* of the FX pair.
        """

        if 'Pair' in kwargs and isinstance(kwargs['Pair'], list) and len(kwargs['Pair']) > 1:
            return kwargs['Pair'][0] + '-' + kwargs['Pair'][1]

        for k in ['To', 'From']:
            if k not in kwargs or kwargs[k] is None:
                return None
            if not isinstance(kwargs[k], str):
                raise TypeError(f'kwargs[{k}] not a string')

        return kwargs['To'] + '-' + kwargs['From']

