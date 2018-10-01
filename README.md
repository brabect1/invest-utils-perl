# invest-utils-perl
Misc perl utils for analysis of stock portfolio.

The utils takes form of a perl module (.pm) and some perl scripts using that
module. The module and scripts have ad hoc design to yield the desire function.
More structured approach will be chosen later once getting a good understanding
of the needed functions.

The `ALPHAVANTAGE_API_KEY` environment variable shall be set to quote stock and
currency prices through AlphaVantage API. That one is used as a secondary source
where the primary is Yahoo Finance.

Examples
--------

We assume a FIFO accounting (i.e. first bought first sold) and account commissions
against a gain after selling the last share from a buy transaction being cleared.

### Portfolio 1 ###

Consider a simple example. All transaction incur 10USD commission. We made two
buys of 10 shares each (at 100USD and 150USD) and sold 10 shares at 175USD.
For simplicity we received no dividends.

A simple math gets the following:

- Total investment (three commissions plus the purchase price): `3*10 + 10*100 + 10*150 = 2530`
- Remaining investment (open position): `10 + 10*150 = 1510`
- Remaining shares: `10`
- Realized gain (sell price less purchase price and commissions for buy and sell transactions being cleared): `10*175 - 10*100 - 2*10 = 730`
- Unrealized gain (net asset value less remaining investment): `10*200 - 1510 = 490`
- Total yield (realized and unrealized gain of the total investment): `100% * (730 + 490)/2530 = 48.2`

Here are the commands:

    # Create a sample portfolio
    $ cat <<EOF > portfolio-1.txt
    buy(stock=AAPL amount=10 price=100.00USD date=2016-07-21 commission=10USD)
    #sell(stock=AAPL amount=10 price=120.00USD date=2017-01-17 commission=10USD)
    buy(stock=AAPL amount=10 price=150.00USD date=2018-02-09 commission=10USD)
    sell(stock=AAPL amount=10 price=175.00USD date=2018-03-05 commission=10USD)
    #buy(stock=AAPL amount=10 price=200.00USD date=2018-08-01 commission=10USD)
    EOF
    
    # Import into a DB
    $ perl import-xfrs.pl -s portfolio-1.txt -d portfolio-2.sqlite3.db
    
    # Fake caching quotes
    # (Normally we would use `cache-quotes.pl` to get present quotes, but we use
    # a hard-coded quote to make it easy to check reported results.)
    $ cat <<EOF > portfolio-1.cmds
    CREATE TABLE quotes (id INT PRIMARY KEY, symbol TEXT NOT NULL, date TEXT NOT NULL, price REAL, curr TEXT);
    INSERT INTO quotes (symbol,date,price,curr) VALUES ('AAPL','$(date +%Y-%m-%d)','200','USD');
    EOF
    
    $ sqlite3 portfolio-1.sqlite3.db < portfolio-1.cmds
    
    # Now query the portfolio performance
    $ perl get-performance.pl -b USD -d portfolio-1.sqlite3.db
    # Stocks
    	sym	curr	total_invest	remain_invest	nav	sell_val	sell_gain	dividend	total_gain	total_gain_%	irr_%
    	AAPL	USD	2530	1510	2000	1750	730	0	1220	48.221	43.706
    # Stocks (total)
    	sym	curr	total_invest	remain_invest	nav	sell_val	sell_gain	dividend	total_gain	total_gain_%	irr_%
    	USD	USD	2530	1510	2000	1750	730	0	1220	48.221	43.706
    ...

### Portfolio 2 ###

Let us extend the Portfolio 1 example by adding an initial deposit to finance
the transactions. Assume we put in 3000USD.

Then after the transactions in the Portfolio 1 we will end up with this balance:

- Cache balance: `3000 - 2530 + 1750 = 3000 - 1510 + 730 = 2220`
- Total gain is same as for stocks

Commands:

    # Create a sample portfolio
    $ cp portfolio-1.txt portfolio-2.txt && \
    cat <<EOF >> portfolio-2.txt
    deposit(amount=3000USD date=2016-07-10)
    EOF
    
    # Import into a DB
    $ perl import-xfrs.pl -s portfolio-2.txt -d portfolio-2.sqlite3.db
    
    # Fake caching quotes
    $ sqlite3 portfolio-2.sqlite3.db < portfolio-1.cmds
    
    # Now query the portfolio performance
    $ perl get-performance.pl -b USD -d portfolio-1.sqlite3.db
    ...
    # Stocks (total)
    	sym	curr	total_invest	remain_invest	nav	sell_val	sell_gain	dividend	total_gain	total_gain_%	irr_%
    	USD	USD	2530	1510	2257.4	1750	730	0	1477.4	58.395	51.027
    # Cash
    	USD	USD	3000	2220	2220	0	0	0	???	???	???
    # Total (USD)
    	USD	USD	3000	3730	4477.4	0	730	0	1477.4	49.247	???
