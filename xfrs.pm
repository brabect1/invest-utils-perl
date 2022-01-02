use DBI;
use Switch;
use strict;
use warnings;
use List::MoreUtils;
use Array::Utils;
use Finance::Quote;
use POSIX;

## #---->>>> 31-Oct-2017
## sell gain = sold units sell price - sold units buy price - sell commission - sold units buy commission
## invested amount = unsold units price + unsold units commission
## 
## total invested amount = 
## = total buy commission + total sell commission + all units buy price =
## = (sold units buy commission + unsold units buy commission) + sell commission + (sold units buy price + unsold units buy price) =
## = (unsold units buy price + unsold units commission) + (sold units buy price + sell commission + sold units buy commission) =
## = invested amount + (sold units sell price - sell gain)
## 
## DONE --> implement geteSellGain() and getTotalInvestedAmount() --> getTotalSellPrice can be computed from the other values
## #<<<<----

package xfrs;

# gets the list of all symbols from the transfers DB, incl. currencies and stocks
# arguments:
#   - reference to the open DB connection
# returns:
#   - an array of symbols
sub getSymbols {
    my $dbh = shift || return;
    my $stmt = qq(SELECT unit_curr, source_curr, comm_curr from xfrs;);
    my $sth = $dbh->prepare( $stmt );
    my $rv = $sth->execute();
    if($rv < 0){
        print $DBI::errstr;
        return;
    }

    my @syms = ();
    while (my @row = $sth->fetchrow_array()) {
        push( @syms, @row);

    }
    return List::MoreUtils::uniq(@syms);
}


# identifies if a symbol exists in DB
# arguments:
#   - reference to the open DB connection
#   - symbol
# returns:
#   - true is the symbol is defined in the DB, false otherwise
sub exists {
    my $dbh = shift || return 0;
    my $s = shift || return 0;

    my $stmt = qq(SELECT count(*) from xfrs where );
    # symbol used as a currency
    $stmt = $stmt." (source_curr='$s'";
    $stmt = $stmt." or unit_curr='$s'";
    $stmt = $stmt." or comm_curr='$s')";
    # end of the statement
    $stmt = $stmt.";";
#print "$stmt\n";
    my $sth = $dbh->prepare( $stmt );
    my $rv = $sth->execute();
    if($rv < 0){
        print $DBI::errstr;
        return 0;
    }

    my @row = $sth->fetchrow_array();
    return $row[0] > 0;
}


# gets the list of currency symbols
# arguments:
#   - reference to the open DB connection
# returns:
#   - an array of symbols
sub getCurrencies {
    my $dbh = shift || return;

    my @currencies = getSymbols( $dbh );
    my @stocks = getStocks( $dbh );
    return Array::Utils::array_diff(@currencies,@stocks);
}


# identifies if a symbol is a currency
# arguments:
#   - reference to the open DB connection
#   - symbol
# returns:
#   - true is the symbol is a currency defined in the DB, false otherwise
sub isCurrency {
    my $dbh = shift || return 0;
    my $s = shift || return 0;

    my $stmt = qq(SELECT count(*) from xfrs where );
    # currency manip records (a currency may act as any of the currency records)
    $stmt = $stmt."(type in ('deposit','fx','withdraw') and";
    $stmt = $stmt." (source_curr='$s'";
    $stmt = $stmt." or unit_curr='$s'";
    $stmt = $stmt." or comm_curr='$s')";
    $stmt = $stmt.")";
    # stock manip records (a currency may act as a unit or commission currency)
    $stmt = $stmt." or ";
    $stmt = $stmt."(type in ('sell','buy','dividend') and";
    $stmt = $stmt." (unit_curr='$s'";
    $stmt = $stmt." or comm_curr='$s')";
    $stmt = $stmt.")";
    # end of the statement
    $stmt = $stmt.";";
#print "$stmt\n";
    my $sth = $dbh->prepare( $stmt );
    my $rv = $sth->execute();
    if($rv < 0){
        print $DBI::errstr;
        return 0;
    }

    my @row = $sth->fetchrow_array();
    return $row[0] > 0;
}


# gets the list of stock symbols
# arguments:
#   - reference to the open DB connection
# returns:
#   - an array of symbols
sub getStocks {
    my $dbh = shift || return;

    my $stmt = qq(SELECT distinct source_curr from xfrs where type in ('sell', 'buy', 'dividend'););
    my $sth = $dbh->prepare( $stmt );
    my $rv = $sth->execute();
    if($rv < 0){
        print $DBI::errstr;
        return;
    }

    my @syms = ();
    while (my @row = $sth->fetchrow_array()) {
        push( @syms, @row);

    }
    return List::MoreUtils::uniq(@syms);
}


# gets the list of stock symbols in the given currency (or a list of currencies)
# arguments:
#   - reference to the open DB connection
#   - currency symbol (string) or a reference to an array of currencies (strings)
# returns:
#   - an array of symbols
sub getStocksInCurrency {
    my $dbh = shift || return;
    my $ref = shift || return;

    # prepare a SQL query
    my $stmt = qq(SELECT distinct source_curr from xfrs where type in ('sell', 'buy', 'dividend'));
    if (ref($ref) eq '') {
        $stmt .= " and unit_curr='$ref';";
    } elsif (ref($ref) eq 'ARRAY') {
        my $substr = '';
        for my $s (@$ref) {
            $substr .= ($substr eq '' ? '' : ', ')."'$s'";
        }
        $stmt .= " and unit_curr in ($substr);";
    } else {
        print "Error: Unsupported argument type: ".ref($ref)."\n";
        return;
    }

    # execute the SQL query
    my $sth = $dbh->prepare( $stmt );
    my $rv = $sth->execute();
    if($rv < 0){
        print $DBI::errstr;
        return;
    }

    # process the results
    my @syms = ();
    while (my @row = $sth->fetchrow_array()) {
        push( @syms, @row);

    }
    return List::MoreUtils::uniq(@syms);
}


# identifies if a symbol is a stock symbol
# arguments:
#   - reference to the open DB connection
#   - symbol
# returns:
#   - true is the symbol is a stock symbol defined in the DB, false otherwise
sub isStock {
    my $dbh = shift || return 0;
    my $s = shift || return 0;

    my $stmt = qq(SELECT count(*) from xfrs where type in ('sell', 'buy', 'dividend') and source_curr=);
    $stmt = $stmt."'$s';";
#print $stmt."\n";
    my $sth = $dbh->prepare( $stmt );
    my $rv = $sth->execute();
    if($rv < 0){
        print $DBI::errstr;
        return 0;
    }

    my @row = $sth->fetchrow_array();
    return $row[0] > 0;
}


# gets the current balance based on the transfers stored in the given DB
#
# The routine acts on both stocks and currencies. For stocks it returns the
# number of securities held, for currencies it returns the remaining cash
# balance.
#
# arguments:
#   - reference to the open DB connection
#   - reference to a hash array to be filled with a balance
sub getBalance {
    my $dbh = shift || return;
    my $href = shift || return;


    my @syms = keys %$href;
    if (scalar(@syms) == 0) {
        @syms = getSymbols($dbh);
    }

    foreach my $s (@syms) {
        my $balance = 0;
        my $stmt; # SQL statement
        my $sth; # compiled SQL statement handle
        my $rv; # SQL execution return value

        # get amounts that directly increase or decrease the balance
        # (shall affect only cash balances)
        $stmt = qq(select type, amount*unit_price from xfrs where unit_curr = );
        $stmt = $stmt."'$s';";
        $sth = $dbh->prepare( $stmt );
        $rv = $sth->execute();
        if($rv < 0){
            print $DBI::errstr;
        } else {
            while (my @row = $sth->fetchrow_array()) {
                if (scalar(@row) < 2) { next; }
                if (!defined $row[0] || length $row[0] == 0) { next; }
                switch ($row[0]) {
                    case ['deposit','fx','dividend','sell'] { $balance += $row[1]; }
                    case ['buy','withdraw'] { $balance -= $row[1]; }
                    else { print "\nError: Unknown transaction type: $row[0]\n"; }
                }
            }
        }

        # subtract any conversions where this was a source currency
        # (shall affect only cash balances)
        $stmt = qq(select type, source_price from xfrs where type='fx' and source_curr=);
        $stmt = $stmt."'$s';";
        $sth = $dbh->prepare( $stmt );
        $rv = $sth->execute();
        if($rv < 0){
            print $DBI::errstr;
        } else {
            while (my @row = $sth->fetchrow_array()) {
                if (scalar(@row) < 2) { next; }
                if (!defined $row[0] || length $row[0] == 0) { next; }
                $balance -= $row[1];
            }
        }

        # subtract any commissions
        # (shall affect only cash balances)
        $stmt = qq(select type, sum(comm_price) from xfrs where comm_curr = );
        $stmt = $stmt."'$s';";
        $sth = $dbh->prepare( $stmt );
        $rv = $sth->execute();
        if($rv < 0){
            print $DBI::errstr;
        } else {
            while (my @row = $sth->fetchrow_array()) {
                if (scalar(@row) < 2) { next; }
                if (!defined $row[0] || length $row[0] == 0) { next; }
                $balance -= $row[1];
            }
        }

        # add increases/decreases of stock amount
        # (shall affect only stock balances)
        $stmt = qq(select type, amount from xfrs where type in ('sell', 'buy') and source_curr = );
        $stmt = $stmt."'$s';";
        $sth = $dbh->prepare( $stmt );
        $rv = $sth->execute();
        if($rv < 0){
            print $DBI::errstr;
        } else {
            while (my @row = $sth->fetchrow_array()) {
                if (scalar(@row) < 2) { next; }
                if (!defined $row[0] || length $row[0] == 0) { next; }
                switch ($row[0]) {
                    case ['sell'] { $balance -= $row[1]; }
                    case ['buy'] { $balance += $row[1]; }
                    else { print "\nError: Unknown transaction type: $row[0]\n"; }
                }
            }
        }

        $href->{$s} = $balance;
    }

}


# Gets the net asset value (NAV) for the given symbols.
#
# The NAV value is returned with indication of the currency (e.g. 30.25USD).
#
# NAV is computed as the number of remaining shares (as returned by getBalance())
# times the present share value. As such it represent the actual value of
# a position.
#
# No commissions are involved in NAV. The commissions rather apply for analyzing
# a gain or invested amount.
#
# arguments:
#   - reference to the open DB connection
#   - reference to a hash array to be filled with NAV values
sub getNAV {
    my $dbh = shift || return;
    my $href = shift || return;

    # get the balance first
    getBalance( $dbh, $href );

    # get the list of symbols
    my @syms = keys %$href;
    if (scalar(@syms) == 0) {
        return
    }

    foreach my $s (@syms) {
        if (isCurrency( $dbh, $s )) {
            $href->{$s} = $href->{$s}.$s;
        } elsif (isStock( $dbh, $s )) {
            # get quote (to compute the actual NAV)
            my $nav='';
            my %qs  = getQuoteStock($dbh,'',$s);
            if (exists($qs{$s})) {
                $nav = ($qs{$s}->{'price'} * $href->{$s}).$qs{$s}->{'currency'};
            }

            $href->{$s} =  ($nav eq '')  ? $href->{$s}."???" : $nav;
        } else {
            $href->{$s} = $href->{$s}."???";
        }
    }

}


# gets the total dividend (reduced by a withholding tax) for a stock symbol in the given DB
# arguments:
#   - reference to the open DB connection
#   - reference to a hash array to be filled with dividend records
sub getDividend {
    my $dbh = shift || return;
    my $href = shift || return;


    my @syms = keys %$href;
    if (scalar(@syms) == 0) {
        @syms = getSymbols($dbh);
    }

    foreach my $s (@syms) {
        my $dividend = 0;
        my $stmt; # SQL statement
        my $sth; # compiled SQL statement handle
        my $rv; # SQL execution return value

        # get amounts that directly increase or decrease the balance
        $stmt = qq(select amount*unit_price - comm_price from xfrs where type='dividend' and source_curr = );
        $stmt = $stmt."'$s';";
        $sth = $dbh->prepare( $stmt );
        $rv = $sth->execute();
        if($rv < 0){
            print $DBI::errstr;
        } else {
            while (my @row = $sth->fetchrow_array()) {
                if (scalar(@row) < 1) { next; }
                if (!defined $row[0]) { next; }
                $dividend += $row[0];
            }
        }

        $href->{$s} = $dividend;
    }
}


# Gets a list of dividends, either all or those limited to a symbol subset.
#
# The return value is a list of records, where each record is a hash array of
# dividend properties: Symbol, Currency, Amount, Tax, Date. The list is ordered
# by date.
#
sub getDividends {
    my $dbh = shift || return;
    my $href = shift;

#    my @syms = keys %$href;
#    if (scalar(@syms) != 0) {
#        print "ERROR: Nor supported for now!\n";
#        return;
#    }

    my $stmt; # SQL statement
    my $sth; # compiled SQL statement handle
    my $rv; # SQL execution return value

    # get amounts that directly increase or decrease the balance
    $stmt = qq(select source_curr, unit_curr, amount*unit_price, comm_price, date from xfrs where type='dividend');
    $stmt .= " order by date;";
    $sth = $dbh->prepare( $stmt );
    $rv = $sth->execute();
    if($rv < 0){
        print $DBI::errstr;
        return;
    } else {
        my @dividends;

        while (my @row = $sth->fetchrow_array()) {
            if (scalar(@row) < 5) { next; }
            my %dividend;
            $dividend{'symbol'} = $row[0];
            $dividend{'currency'} = $row[1];
            $dividend{'amount'} = $row[2];
            $dividend{'tax'} = $row[3];
            $dividend{'date'} = $row[4];
            push(@dividends, \%dividend);
        }

        return @dividends;
    }
}



# Gets the (remaining) invested amount for a stock symbol in the given DB.
#
# The invested amount is calculated as the investment for all buy transactions
# less the corresponding value from sell transactions. The transactions are
# considered in stock units and the remaining number is translated into the
# remaining invested value.
#
# The invested value is computed on the FIFO basis and hence the first unit bought
# is considered the first unit sold. The remaining invested value thus represents
# the value paid for last, yet unsold stock units.
#
# Commissions for transactions of yet unsold stock units increase the investment
# value. Once all of the units of buy transaction are sold, the commission (for
# both the buy and sell transactions) is transferred to reduce the realized gain
# and hence removed from the remaining invested value.
#
# arguments:
#   - reference to the open DB connection
#   - reference to a hash array to be filled with investment price records
sub getInvestedAmount {
    my $dbh = shift || return;
    my $href = shift || return;


    my @syms = keys %$href;
    if (scalar(@syms) == 0) {
        @syms = getSymbols($dbh);
    }

    foreach my $s (@syms) {
        my $balance = 0;
        my $stmt; # SQL statement
        my $sth; # compiled SQL statement handle
        my $rv; # SQL execution return value

        if (isStock($dbh,$s)) {
            # get amounts that directly increase or decrease the balance
            $stmt = qq(select type, amount, unit_price, unit_curr, comm_price, comm_curr, date from xfrs where type in ('buy','sell'));
            $stmt = $stmt." and source_curr='$s'";
            $stmt = $stmt." order by date;";
            $sth = $dbh->prepare( $stmt );
            $rv = $sth->execute();
            if($rv < 0){
                print $DBI::errstr;
            } else {
                my $curr = '';
                my @trans;

                # process the transactions fetched from DB
                while (my @row = $sth->fetchrow_array()) {
                    if (scalar(@row) < 6) { next; }
                    if (!defined $row[0] || length $row[0] == 0) { next; }

                    # set the currency based on the 1st transaction record
                    if ($curr eq '') { $curr = $row[3]; }

                    # skip the record if wrong unit currency
                    if ($curr ne $row[3]) {
                        print "Error: Unexpected unit currency ($row[0] $row[1] $s units on $row[6]): act=$row[3], exp=$curr\n";
                        next;
                    }

                    # invalidate commission if wrong currency
                    if ($curr ne $row[5]) {
                        print "Error: Unexpected commision currency ($row[0] $row[1] $s units on $row[6]): act=$row[5], exp=$curr\n";
                        $row[4] = 0;
                    }

                    # act per the transaction type
                    switch ($row[0]) {
                        case ['sell'] {
                            my $units = -$row[1];
                            foreach my $rb (@trans) {
                                $units += $rb->{'units'};

                                # clear all the bought units if sold more
                                # than in the buy `rb` transaction, else
                                # update with what remained
                                if ($units < 0) {
                                    $rb->{'units'} = 0;
                                } else {
                                    $rb->{'units'} = $units;
                                    last;
                                }
                            }

                            # sanity check:
                            if ($units < 0) {
                                print "Error: Selling more than bought ($row[0] $row[1] $s units on $row[6]): num=".-$units."\n";
                            }
                        }
                        case ['buy'] {
                            # add a new record into the transactions list
                            push(@trans, {
                                    'units' => $row[1],
                                    'price' => $row[2],
                                    'curr' => $row[3],
                                    'comm' => $row[4]
                                }
                            );
                        }
                        else { print "\nError: Unknown transaction type: $row[0]\n"; }
                    }
                }

                # compute the invested value based on what has been left from
                # buy transactions
                foreach my $rb (@trans) {
                    $balance += $rb->{'units'} * $rb->{'price'} + ($rb->{'units'} > 0 ? 1 : 0) * $rb->{'comm'};
                } 
            }
        } elsif (isCurrency($dbh,$s)) {
            # get balance of the given symbol
            # (For a currency the actual balance represents the remaining invested amount.)
            my %cur;
            $cur{$s} = 0;
            getBalance($dbh,\%cur);
            $balance = $cur{$s};
        } else {
            # unknown symbol
            next;
        }

        $href->{$s} = $balance;
    }
}


# Gets the total price for the investment.
#
# The total invested amount consists of the price paid for all units bought and
# commissions paid for all buy and sell transactions. This total amount can be
# used as a basis to compute the percentage of realized/unrealized gain.
#
# Note that the total amount accumulates over the lifetime of the portfolio. If
# one flips the same security over and over again, it will count all the flips.
# For example, buying AAPL for 1000USD, then selling it for 1200USD, than buying
# it again for 1000USD and selling for 1500USD will yield the total invested
# amount of 2000USD.
#
# The same applies for deposits and withdrawals of cash. However, while flipping
# stocks is to provide fair performance measures, depositing and withdrawing cache
# might skew the total portfolio performance.
#
# arguments:
#   - reference to the open DB connection
#   - reference to a hash array to be filled with investment price records
sub getTotalInvestedAmount {
    my $dbh = shift || return;
    my $href = shift || return;


    my @syms = keys %$href;
    if (scalar(@syms) == 0) {
        @syms = getSymbols($dbh);
    }

    foreach my $s (@syms) {
        my $balance = 0;
        my $stmt; # SQL statement
        my $sth; # compiled SQL statement handle
        my $rv; # SQL execution return value

        if (isStock($dbh,$s)) {
            # get amounts that directly increase or decrease the balance
            $stmt = qq(select type, amount, unit_price, unit_curr, comm_price, comm_curr, date from xfrs where type in ('buy','sell'));
            $stmt = $stmt." and source_curr='$s'";
            $stmt = $stmt." order by date;";
            $sth = $dbh->prepare( $stmt );
            $rv = $sth->execute();
            if($rv < 0){
                print $DBI::errstr;
            } else {
                my $curr = '';

                # process the transactions fetched from DB
                while (my @row = $sth->fetchrow_array()) {
                    if (scalar(@row) < 6) { next; }
                    if (!defined $row[0] || length $row[0] == 0) { next; }

                    # set the currency based on the 1st transaction record
                    if ($curr eq '') { $curr = $row[3]; }

                    # skip the record if wrong unit currency
                    if ($curr ne $row[3]) {
                        print "Error: Unexpected unit currency ($row[0] $row[1] $s units on $row[6]): act=$row[3], exp=$curr\n";
                        next;
                    }

                    # invalidate commission if wrong currency
                    if ($curr ne $row[5]) {
                        print "Error: Unexpected commision currency ($row[0] $row[1] $s units on $row[6]): act=$row[5], exp=$curr\n";
                        $row[4] = 0;
                    }

                    # act per the transaction type
                    switch ($row[0]) {
                        case ['sell'] {
                            # For a sell transactions count only the commission.
                            $balance += $row[4];
                        }
                        case ['buy'] {
                            # For a buy transaction count the buy price plus the commission.
                            $balance += $row[1]*$row[2] + $row[4];
                        }
                        else { print "\nError: Unknown transaction type: $row[0]\n"; }
                    }
                }
            }
        } elsif (isCurrency($dbh,$s)) {
            # TODO 2017-12-24: This is temporary solution that only counts deposits and withdrawals.
            #                  We might also consider currency translations, but that would depend
            #                  on how the 'invested amount' is supposed to be used. If we care about
            #                  how much money we put into the account and how much we took back, then
            #                  the current approach is correct. If we were after efficiency of investments
            #                  in individual currencies (incl. both stock and cash), then we would
            #                  need to consider translations too.
            #
            #                  Also if deposits and withdrawals were in different currencies, we would
            #                  likely need to convert into a base currency to the date of the transaction,
            #                  as translating only the final balance would not correctly represent the
            #                  asset value of the investment.

            # get amounts that directly increase or decrease the balance
            $stmt = qq(select type, amount, unit_price, unit_curr, comm_price, comm_curr, date from xfrs where type in ('deposit','withdraw'));
            $stmt = $stmt." and source_curr='$s'";
            $stmt = $stmt." order by date;";
            $sth = $dbh->prepare( $stmt );
            $rv = $sth->execute();
            if($rv < 0){
                print $DBI::errstr;
            } else {
                my $curr = $s;
                my @trans;

                # process the transactions fetched from DB
                while (my @row = $sth->fetchrow_array()) {
                    if (scalar(@row) < 6) { next; }
                    if (!defined $row[0] || length $row[0] == 0) { next; }

                    # skip the record if wrong unit currency
                    if ($curr ne $row[3]) {
                        print "Error: Unexpected unit currency ($row[0] $row[1] $s units on $row[6]): act=$row[3], exp=$curr\n";
                        next;
                    }

                    # invalidate commission if wrong currency
                    if ($curr ne $row[5]) {
                        print "Error: Unexpected commision currency ($row[0] $row[1] $s units on $row[6]): act=$row[5], exp=$curr\n";
                        $row[4] = 0;
                    }

                    # act per the transaction type
                    switch ($row[0]) {
                        case ['withdraw'] {
                            # ignore the withdrawn amount as it does not change the money pushed
                            # into the system

                            # ignore the commission as it would be covered from the remaining balance and does
                            # not affect the value of the investment
                            # (Note: If deposits and withdrawals incurred any commissions, than there will be
                            # a residual investment after a withdrawal that would clear the balance, and that
                            # residual amount would equal the sum of all related commissions.)
                        }
                        case ['deposit'] {
                            # ignore the commission as it would be covered from the deposited amount
                            $balance += $row[1]*$row[2];
                        }
                        else { print "\nError: Unknown transaction type: $row[0]\n"; }
                    }
                }
            }
        } else {
            # unknown symbol
            next;
        }

        $href->{$s} = $balance;
    }
}


# Gets the price earned on all sell transactions.
#
# The price does not count commissions paid for the sell transactions. Hence
# to get a real amount earned on selling, one would need to reduce the total
# sell price by the amount paid for sell commissions.
#
# arguments:
#   - reference to the open DB connection
#   - reference to a hash array to be filled with sell price records
sub getTotalSellPrice {
    my $dbh = shift || return;
    my $href = shift || return;


    my @syms = keys %$href;
    if (scalar(@syms) == 0) {
        @syms = getSymbols($dbh);
    }

    foreach my $s (@syms) {
        my $balance = 0;
        my $stmt; # SQL statement
        my $sth; # compiled SQL statement handle
        my $rv; # SQL execution return value

#***TBD*** The code below could be reduced to a single SQL query: select sum(amount*unit_price) from ... 
        $stmt = qq(select type, amount, unit_price, unit_curr, comm_price, comm_curr, date from xfrs where type='sell');
        $stmt = $stmt." and source_curr='$s'";
        $stmt = $stmt." order by date;";
        $sth = $dbh->prepare( $stmt );
        $rv = $sth->execute();
        if($rv < 0){
            print $DBI::errstr;
        } else {
            my $curr = '';

            # process the transactions fetched from DB
            while (my @row = $sth->fetchrow_array()) {
                if (scalar(@row) < 6) { next; }
                if (!defined $row[0] || length $row[0] == 0) { next; }

                # set the currency based on the 1st transaction record
                if ($curr eq '') { $curr = $row[3]; }

                # skip the record if wrong unit currency
                if ($curr ne $row[3]) {
                    print "Error: Unexpected unit currency ($row[0] $row[1] $s units on $row[6]): act=$row[3], exp=$curr\n";
                    next;
                }

                # invalidate commission if wrong currency
                if ($curr ne $row[5]) {
                    print "Error: Unexpected commision currency ($row[0] $row[1] $s units on $row[6]): act=$row[5], exp=$curr\n";
                    $row[4] = 0;
                }

                # act per the transaction type
                switch ($row[0]) {
                    case ['sell'] {
                        # Take only the sell price and ignore commission.
                        $balance += $row[1]*$row[2];
                    }
                    else { print "\nError: Unknown transaction type: $row[0]\n"; }
                }
            }
        }

        $href->{$s} = $balance;
    }
}


# Gets the selling gain for the investment.
#
# The selling gain is computed as the price of shares sold less the buying
# price of those shares less commissions incurred for the transactions.
#
# The buy commissions are counted in for selling the last share of the
# corresponding buy transaction. Thus for example, buying ten shares and
# then selling nine of them will reduce the gain only by the selling
# commission as there is still one share being held from the buying
# transaction. This may seem a bit pessimistic, but it makes the accounting
# somewhat simpler (as opposed to using proportional commission price for
# every share).
#
# arguments:
#   - reference to the open DB connection
#   - reference to a hash array to be filled with investment gain records
sub getSellGain {
    my $dbh = shift || return;
    my $href = shift || return;


    my @syms = keys %$href;
    if (scalar(@syms) == 0) {
        @syms = getSymbols($dbh);
    }

    foreach my $s (@syms) {
        my $balance = 0;
        my $stmt; # SQL statement
        my $sth; # compiled SQL statement handle
        my $rv; # SQL execution return value

        if (isStock($dbh,$s)) {
            # get amounts that directly increase or decrease the balance
            $stmt = qq(select type, amount, unit_price, unit_curr, comm_price, comm_curr, date from xfrs where type in ('buy','sell'));
            $stmt = $stmt." and source_curr='$s'";
            $stmt = $stmt." order by date;";
            $sth = $dbh->prepare( $stmt );
            $rv = $sth->execute();
            if($rv < 0){
                print $DBI::errstr;
            } else {
                my $curr = '';
                my @trans;

                # process the transactions fetched from DB
                while (my @row = $sth->fetchrow_array()) {
                    if (scalar(@row) < 6) { next; }
                    if (!defined $row[0] || length $row[0] == 0) { next; }

                    # set the currency based on the 1st transaction record
                    if ($curr eq '') { $curr = $row[3]; }

                    # skip the record if wrong unit currency
                    if ($curr ne $row[3]) {
                        print "Error: Unexpected unit currency ($row[0] $row[1] $s units on $row[6]): act=$row[3], exp=$curr\n";
                        next;
                    }

                    # invalidate commission if wrong currency
                    if ($curr ne $row[5]) {
                        print "Error: Unexpected commision currency ($row[0] $row[1] $s units on $row[6]): act=$row[5], exp=$curr\n";
                        $row[4] = 0;
                    }

                    # act per the transaction type
                    switch ($row[0]) {
                        case ['sell'] {
                            # For a sell transaction count the sell price less the commission.
                            $balance += $row[1]*$row[2] - $row[4];

                            my $units = -$row[1];
                            foreach my $rb (@trans) {
                                # skip the buy transaction if already depleated
                                # (i.e. no more units left from the transaction)
                                next if ($rb->{'units'} == 0);

                                # recover sell units from the buy transaction
                                $units += $rb->{'units'};

                                # discount the bought units and buy commissions if
                                # the buy transaction gets fully cleared (i.e. selling
                                # more units than what remains of the `rb` buy transaction)
                                if ($units <= 0) {
                                    # clearing the whole buy transaction => discount also buy commission
                                    $balance -= $rb->{'units'}*$rb->{'price'} + $rb->{'comm'};
                                    $rb->{'units'} = 0;
                                } else {
                                    # some units remained from the buy transaction
                                    $balance -= ($rb->{'units'} - $units)*$rb->{'price'};
                                    $rb->{'units'} = $units;
                                    last;
                                }
                            }

                            # sanity check:
                            if ($units < 0) {
                                print "Error: Selling more than bought ($row[0] $row[1] $s units on $row[6]): num=".-$units."\n";
                            }
                        }
                        case ['buy'] {
                            # add a new record into the transactions list
                            push(@trans, {
                                    'units' => $row[1],
                                    'price' => $row[2],
                                    'curr' => $row[3],
                                    'comm' => $row[4]
                                }
                            );
                        }
                        else { print "\nError: Unknown transaction type: $row[0]\n"; }
                    }
                }
            }

            # record the final balance into the result hash
            $href->{$s} = $balance;
        }
    }
}

# ***tbd*** get all stock transactions
# will return a hash indexed by date and amounts decreasing (sell) and increasing (buy) net asset value
# each item includes the commissions expense such that the commission increases the value of buy and decreases value of sell
sub getStockTransactions {
    my $dbh = shift || return;
    my @syms = @_;

    for (my $i=0; $i < scalar @syms; $i++) {
        $syms[$i] = "'".$syms[$i]."'";
    }
    
    my %cashflow = ();

    # get amounts that directly increase or decrease the balance
    my $stmt = qq(select type, date, amount, unit_price, unit_curr, comm_price, comm_curr from xfrs where source_curr in );
    $stmt = $stmt."(".join(',', @syms).") order by date;";
    my $sth = $dbh->prepare( $stmt );
    my $rv = $sth->execute();
    if($rv < 0){
        print $DBI::errstr;
    } else {
        my $curr = ''; # stock currency shall be same for all transactions
        my $units = 0; # stock units balance
        my $date = ''; # date of last buy/sell transaction 
#        my $dividend = 0; # accumulated dividend

        while (my @row = $sth->fetchrow_array()) {
            if (scalar(@row) < 2) { next; }
            if (!defined $row[0] || length $row[0] == 0) { next; }

            if ($curr eq '') {
                $curr = $row[4];
            }

            # sanity check: transaction type
            if ($row[0] ne 'buy' && $row[0] ne 'sell' && $row[0] ne 'dividend') {
                print "\nError: Unknown transaction type for stock: $row[0]\n";
                next;
            }

            # sanity check: source currency
            if ($row[4] ne $curr) {
                print "\nError: Unexpected currency for $row[0]: act $row[4], exp $curr\n";
                next;
            }

            # sanity check: commissions currency
            if ($row[6] ne $curr) {
                print "\nError: Unexpected commision currency for $row[0]: act $row[6], exp $curr\n";
                next;
            }

            my $amount = 0;

            if ($row[0] eq 'buy') {
                $units += $row[2];
                $amount = $row[2]*$row[3] + $row[5];
                $date = $row[1];
            } elsif ($row[0] eq 'sell') {
                $units -= $row[2];
                $amount = -($row[2]*$row[3]) + $row[5];
                $date = $row[1];
            } elsif ($row[0] eq 'dividend') {
                # Dividend does not increase the amount invested into the stock
                # and hence we keep it in a separate "account" and do not put it
                # into the cashflow. It will be added to the remaining balance/NAV.
#                $dividend += $row[2]*$row[3];
#                $dividend -= $row[5];
                next;
            }

            if (!defined $cashflow{$row[1]}) {
                $cashflow{$row[1]} = 0;
            }
            $cashflow{$row[1]} += $amount;
        }
    }

    return %cashflow;
}


# Gets the cached quoted price from DB.
#
# Returns a hash indexed by a symbol and for each the following attributes:
# 'price', 'currency' and 'date'.
#
# Arguments:
#   - reference to the open DB connection
#   - date of the quote (use empty for today)
#   - list of symbols to quote
sub getCachedQuote {
    my $dbh = shift || return;
    my $date = shift;
    my @syms = @_;

    # get today's date if none given
    if (!defined $date || $date eq '') {
        $date = POSIX::strftime("%Y-%m-%d",localtime);
    }

    # see if cached quotes exist
    my $sth = $dbh->prepare( "SELECT name FROM sqlite_master WHERE type='table' AND name='quotes';" );
    my $rv = $sth->execute();
    if($rv < 0) {
        print $DBI::errstr;
        return ();
    } else {
        # see if we got some result
        my @row = $sth->fetchrow_array();
        if (scalar @row == 0) {
            return ();
        }
    }

    # get quote for each symbol
    my %quotes;
    for my $s (@syms) {
        my $stmt = "SELECT price, curr from quotes where date='".$date."' AND symbol='".$s."';";
        my $sth = $dbh->prepare( $stmt );
        my $rv = $sth->execute();
        if($rv < 0) {
            print $DBI::errstr;
            next;
        }

        my @row = $sth->fetchrow_array();
        if (scalar @row > 1) {
            $quotes{$s} = {
                'date' => $date,
                'price' => $row[0],
                'currency' => $row[1]
            };
        }
    }

    return %quotes;
}


# Gets quote for given stock symbols.
#
# Returns a hash indexed by a symbol and for each the following attributes:
# 'price' and 'currency'.
#
# Symbols, for which no quote was obtained, will not be included in the returned
# hash. Detecting if some symbols failed can be done by comparing the number of
# symbols in the input list and in keys of the output hash.
#
# Quotes are obtained from the following sources (with decreasing priority):
# - DB cache
# - Yahoo Finance
# - AlphaVantage
#
# This routine is somewhat strange for the `xfrs` package, which is intended to provide
# access routines to an XFRS DB. However, the quotes are needed for the getNAV() routine.
#
# Separate routines exist for stocks and currencies as the API to get real-time quotes
# is different.
#
# Arguments:
#   - reference to the open DB connection
#   - date of the quote (use empty for today)
#   - list of symbols to quote
sub getQuoteStock {
    my $dbh = shift || return ();
    my $date = shift;
    my @syms = @_;

    my %quotes;
    my @attrs = ("last","currency");
    my %attrMap = (
        'last' => 'price',
        'currency' => 'currency'
    );

    my $q = Finance::Quote->new;
    $q->timeout(30);

    # obtain cached quotes
    my %qtCacheStocks = getCachedQuote($dbh,$date,@syms);
    foreach my $s (keys %qtCacheStocks) {
        $quotes{$s} = $qtCacheStocks{$s};
    }

    # obtain additional quotes (if needed) from
    # - Yahoo finance
    # - AlphaVantage
    foreach my $qtSrc ('yahoo_json', 'alphavantage') {
        if (scalar @syms > scalar keys %quotes) {
            my @missed;
            foreach my $s (@syms) {
                push(@missed,$s) unless (exists($quotes{$s}));
            }

            my %qs = $q->fetch($qtSrc,@missed);
            foreach my $s (@missed) {
                next unless (exists($qs{$s,'success'}) && $qs{$s,'success'} == 1);
                foreach my $a (@attrs) {
                    if (exists($qs{$s,$a})) {
                        $quotes{$s}->{$attrMap{$a}} = $qs{$s,$a};
                    }
                }
            }
        }
    }

    return %quotes;
}


# Gets quote for given currencies to the base one.
#
# Returns a hash indexed by a symbol and for each the following attributes:
# 'price' and 'currency'.
#
# Symbols, for which no quote was obtained, will not be included in the returned
# hash. Detecting if some symbols failed can be done by comparing the number of
# symbols in the input list and in keys of the output hash.
#
# Quotes are obtained from the following sources (with decreasing priority):
# - DB cache
# - Yahoo Finance
# - AlphaVantage
#
# This routine is somewhat strange for the `xfrs` package, which is intended to provide
# access routines to an XFRS DB. However, the quotes are needed for the getNAV() routine.
#
# Separate routines exist for stocks and currencies as the API to get real-time quotes
# is different.
#
# Arguments:
#   - reference to the open DB connection
#   - date of the quote (use empty for today)
#   - list of currency symbols to quote, where the first acts as the base
#     (example: a list of `EUR`,`USD`,`CAD` will quote `USDEUR` and `CADEUR` pairs)
sub getQuoteCurrency {
    my $dbh = shift || return ();
    my $date = shift;
    my $base = shift || return ();
    my @curs = @_;

    my %quotes;
    my @attrs = ("last","currency");
    my %attrMap = (
        'last' => 'price',
        'currency' => 'currency'
    );

    my $q = Finance::Quote->new;
    $q->timeout(30);

    # get cached quotes first
    my @pairs;
    foreach my $c (@curs) {
        push(@pairs,$c.$base);
    }
    my %qtCacheCurs = xfrs::getCachedQuote($dbh,'',@pairs);
    foreach my $c (@curs) {
        if (exists($qtCacheCurs{$c.$base})) {
            $quotes{$c} = $qtCacheCurs{$c.$base};
        }
    }

    # quote conversion rates at Yahoo Finance (if needed)
    if (scalar @curs > scalar keys %quotes) {
        my %syms;
        foreach my $s (@curs) {
            $syms{$s}=$s.$base."=X" unless (exists($quotes{$s}));
        }

        my %qs = $q->fetch("yahoo_json",values %syms);
        foreach my $s (keys %syms) {
            next unless (exists($qs{$syms{$s},'success'}) && $qs{$syms{$s},'success'} == 1);

            foreach my $a (@attrs) {
                if (exists($qs{$syms{$s},$a})) {
                    $quotes{$s}->{$attrMap{$a}} = $qs{$syms{$s},$a};
                }
            }
        }
    }

    # quote conversion rates (if needed) using the default API (AlphaVantage as of Finance::Quote 1.47)
    if (scalar @curs > scalar keys %quotes) {
        my @syms;
        foreach my $s (@curs) {
            push(@syms,$s) unless (exists($quotes{$s}));
        }

        foreach my $c (@syms) {
            if ($base eq $c) {
                $quotes{$c} = {
                    'price' => 1.0,
                    'currency' => $c
                };
            } else {
                my $convrate = $q->currency($c,$base);
                if (! defined $convrate ) { next; }
                $quotes{$c} = {
                    'price' => $convrate,
                    'currency' => $base
                };
            }
        }
    }
    return %quotes;
}


# adds a new transaction record into DB
#
# The routine automatically creates a DB table if not already exists. Record
# IDs are inferred automatically in a successive order.
#
# Arguments:
#   - reference to the open DB connection
#   - hash array mapping record attributes to record values 
sub addTransaction {
    # arguments
    my $dbh = shift || return;
    my (%args) = @_;

    # local variables
    my $stmt; # SQL query
    my $sth; # query handle
    my $rv; # query return value

    # transaction TYPE is mandatory
    return unless defined $args{type};

    # make sure the XFRS table exists
    my $dbr = $dbh->do( qq(CREATE TABLE IF NOT EXISTS xfrs (
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
        ));
    if($dbr < 0){ warn $DBI::errstr; return; }

    # get the record ID
    $stmt = qq(SELECT COUNT(1) FROM xfrs;);
    $sth = $dbh->prepare( $stmt );
    $rv = $sth->execute();
    if($rv < 0) { warn $DBI::errstr; return; }
    my $id = ($sth->fetchrow_array())[0];

    # create the INSERT statement/query
    $stmt = qq(INSERT INTO xfrs (id,type,date,unit_curr,source_curr,comm_curr,amount,unit_price,source_price,comm_price) VALUES )."(".($id+1).",";
    $stmt .= "'$args{type}'";
    foreach my $attr (('date', 'unit_curr', 'source_curr', 'comm_curr')) {
        $stmt .= ",'".($args{$attr} || "")."'";
    }
    foreach my $attr (('amount', 'unit_price', 'source_price', 'comm_price')) {
        $stmt .= ",".($args{$attr} || 0);
    }

    $stmt = $stmt.");";
#TODO print "$stmt\n";
    $dbr = $dbh->do($stmt);
    if($dbr < 0){ warn $DBI::errstr; return; }
}


# adds a cached quote to DB
#
# A *quote* is simply a price for a unit of a stock or currency. We cache
# quotes to avoid querying online quote sources, which may come with some
# delay (of querying the source and getting response) and maybe restrictions
# on the source side (e.g. how many quotes we may place, how often, etc.).
#
# Cached quotes are simply another table in DB. Quotes are cahed only
# explicitly through `addCahedQuote()`.
#
# Arguments:
#   - reference to the open DB connection
#   - hash array mapping quote attributes to record values; the attributes
#     are `symbol`, `date`, `price` and `currency`; all but `date` (which
#     defaults to the current date) are mandatory
sub addCachedQuote{
    # arguments
    my $dbh = shift || return;
    my (%args) = @_;

    # local variables
    my $stmt; # SQL query
    my $sth; # query handle
    my $rv; # query return value
    my $id; # ID of the quote record

    # check mandatory options
    return unless defined $args{symbol} && defined $args{price} && defined $args{currency};

    # set the `date` option unless defined
    if (!defined $args{date}) { $args{date} = POSIX::strftime("%Y-%m-%d",localtime); }

    # make sure the XFRS table exists
    my $dbr = $dbh->do( qq(CREATE TABLE IF NOT EXISTS quotes (
        id INT PRIMARY KEY,
        symbol TEXT NOT NULL,
        date TEXT,
        price REAL,
        curr TEXT);
        ));
    if($dbr < 0){ warn $DBI::errstr; return; }

    # see if the same quote already exists
    $stmt = "SELECT id FROM quotes WHERE date='".$args{date}."' AND symbol='".$args{symbol}."';";
    $sth = $dbh->prepare( $stmt );
    $rv = $sth->execute();
    if($rv < 0) { warn $DBI::errstr; return; }

    $rv = $sth->fetchrow_arrayref();
    if (defined $rv) { # update the existing record
        $id = $rv->[0];

        # update the record
        $stmt = qq(UPDATE quotes SET price=$args{price}, curr='$args{currency}' ).
            "WHERE date='".$args{date}."' AND symbol='".$args{symbol}."';";
        $dbr = $dbh->do($stmt);
        if($dbr < 0){ warn $DBI::errstr; return; }

    } else { # insert a new record
        # get new record ID
        $stmt = qq(SELECT COUNT(1) FROM quotes;);
        $sth = $dbh->prepare( $stmt );
        $rv = $sth->execute();
        if($rv < 0) { warn $DBI::errstr; return; }
        $id = ($sth->fetchrow_array())[0];

        # insert the record
        $stmt = qq(INSERT INTO quotes (id,symbol,date,price,curr) VALUES ).
            "(".($id+1).",'$args{symbol}','$args{date}',$args{price},'$args{currency}');";
        $dbr = $dbh->do($stmt);
        if($dbr < 0){ warn $DBI::errstr; return; }
    }
}

1;
