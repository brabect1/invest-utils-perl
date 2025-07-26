import sqlite3
import datetime
import yfinance


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

    if 'date' not in quote:
        date = datetime.date.today().strftime("%Y-%m-%d")
    else:
        date = quote['date']

    # Test if the 'quotes' table exist, or create otherwise
    stmt = "SELECT name FROM sqlite_master WHERE type='table' AND name='quotes';" 
    cursor.execute(stmt)

    if len(cursor.fetchall()) == 0:
        stmt = '''CREATE TABLE quotes (
                id INT PRIMARY KEY,
                symbol TEXT NOT NULL,
                date TEXT NOT NULL,
                price REAL,
                curr TEXT);'''
        cursor.execute(stmt)

    # query existing DB quotes
    stmt = f'SELECT * from quotes where date=\'{date}\' AND symbol=\'{symbol}\';'
    cursor.execute(stmt)

    if len(cursor.fetchall()) == 0:
        # no quote yet => create
        stmt = "INSERT INTO quotes (symbol,date,price,curr) VALUES ("
        stmt += f'\'{symbol}\','
        stmt += f'\'{date}\','
        stmt += f'\'{quote["price"]}\','
        stmt += f'\'{quote["currency"]}\');'
    else:
        # some quote already cached => update
        stmt = "UPDATE quotes SET "
        stmt += f'price=\'{quotes["price"]}\''
        stmt += f', curr=\'{quotes["currency"]}\''
        stmt += f' where date=\'{date}\' AND symbol=\'{symbol}\''

    cursor.execute(stmt)
    dbh.commit()


def getOnlineQuote(symbols, **kwargs):
    """Gets the quoted price from Yahoo Finance.

    Args:
      date (str): date of the quote as YYYY-MM-DD string (use None for today)
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
    if 'date' in kwargs and kwargs['date'] is not None:
        date = datetime.datetime.strptime(kwargs['date'], "%Y-%m-%d").date()
        today = datetime.date.today()
        if date > today: return quotes
        elif date == today: date = None

    for s in symbols:
        qs = yfinance.Ticker(s)
        q = { 'regularMarketPrice': None, 'currency': None }
        try:
            if date is None:
                for a in q.keys():
                    q[a] = qs.info[a]
            else:
                continue
            quotes[s] = {'price': q['regularMarketPrice'], 'currency': q['currency']}
        except:
            pass

    if 'cache' in kwargs and kwargs['cache']:
        if 'dbh' in kwargs and kwargs['dbh'] is not None:
            for k, v in quotes.items():
                cacheQuote(kwargs['dbh'], k, v)

    return quotes


def getCachedQuote(dbh, date, symbols):
    """Gets the cached quoted price from DB.

    Arg:
      dbh: reference to the open DB connection
      date (str): date of the quote as YYYY-MM-DD string (use None for today)
      symbols (list): list of symbols to quote

    Returns:
      Returns a hash indexed by a symbol and for each the following attributes:
      'price', 'currency' and 'date'.
    """

    die("not implemented")
    #TODO my $dbh = shift || return;
    #TODO my $date = shift;
    #TODO my @syms = @_;

    #TODO # get today's date if none given
    #TODO if (!defined $date || $date eq '') {
    #TODO     $date = POSIX::strftime("%Y-%m-%d",localtime);
    #TODO }

    #TODO # see if cached quotes exist
    #TODO my $sth = $dbh->prepare( "SELECT name FROM sqlite_master WHERE type='table' AND name='quotes';" );
    #TODO my $rv = $sth->execute();
    #TODO if($rv < 0) {
    #TODO     print $DBI::errstr;
    #TODO     return ();
    #TODO } else {
    #TODO     # see if we got some result
    #TODO     my @row = $sth->fetchrow_array();
    #TODO     if (scalar @row == 0) {
    #TODO         return ();
    #TODO     }
    #TODO }

    #TODO # get quote for each symbol
    #TODO my %quotes;
    #TODO for my $s (@syms) {
    #TODO     my $stmt = "SELECT price, curr from quotes where date='".$date."' AND symbol='".$s."';";
    #TODO     my $sth = $dbh->prepare( $stmt );
    #TODO     my $rv = $sth->execute();
    #TODO     if($rv < 0) {
    #TODO         print $DBI::errstr;
    #TODO         next;
    #TODO     }

    #TODO     my @row = $sth->fetchrow_array();
    #TODO     if (scalar @row > 1) {
    #TODO         $quotes{$s} = {
    #TODO             'date' => $date,
    #TODO             'price' => $row[0],
    #TODO             'currency' => $row[1]
    #TODO         };
    #TODO     }
    #TODO }

    #TODO return %quotes;


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

    attrs = ["last", "currency"]
    attrMap = {
            'last': 'price',
            'currency': 'currency'
            }


    # obtain cached quotes
    quotes += getCachedQuote(dbh, date, symbols)

    # obtain additional quotes (if needed) from Yahoo finance
    symbols = [s for s in symbols if s not in quotes]
    if len(symbols) > 0:
        quotes += getOnlineQuote(dbh, date, symbols)

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

    balances = getBallance(dbh, symbols)
    if balances is None: return None

    currencies = getCurrencies(dbh)
    stocks = getStocks(dbh)

    navs = dict()
    for s in symbols:
        if s in currencies:
            navs[s] = '???' if s not in balances else balances[s]
        elif s in stocks:
            die("not implemented")
            #TODO # get quote (to compute the actual NAV)
            #TODO my $nav='';
            #TODO my %qs  = getQuoteStock($dbh,'',$s);
            #TODO if (exists($qs{$s})) {
            #TODO     $nav = ($qs{$s}->{'price'} * $href->{$s}).$qs{$s}->{'currency'};
            #TODO }

            #TODO $href->{$s} =  ($nav eq '')  ? $href->{$s}."???" : $nav;
        else:
            navs[s] = '???'

    return navs

