use DBI;
use Switch;
use strict;
use warnings;
use List::MoreUtils;
use Array::Utils;
use Finance::Quote;

## #---->>>> 31-Oct-2017
## real gain = sold units sell price - sold units buy price - sell commision - sold units buy commision
## invested amount = unsold units price + unsold units commision
## 
## total invested amount = 
## = total buy commision + total sell commision + all units buy price =
## = (sold units buy commision + unsold units buy commision) + sell commision + (sold units buy price + unsold units buy price) =
## = (unsold units buy price + unsold units commision) + (sold units buy price + sell commision + sold units buy commision) =
## = invested amount + (sold units sell price - real gain)
## 
## --> implement getRealGain() and getTotalInvestedAmount() --> getTotalSellPrice can be computed from the other values
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
    # stock manip records (a currency may act as a unit or commision currency)
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
#   - currency symbol (string) or a an a reference to an array of currencies (strings)
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


# gets the current ballance based on the trabsfers stored in the given DB
# arguments:
#   - reference to the open DB connection
#   - reference to a hash array to be filled with a ballance
sub getBallance {
    my $dbh = shift || return;
    my $href = shift || return;


    my @syms = keys %$href;
    if (scalar(@syms) == 0) {
        @syms = getSymbols($dbh);
    }

    foreach my $s (@syms) {
        my $ballance = 0;
        my $stmt; # SQL statement
        my $sth; # compiled SQL statement handle
        my $rv; # SQL execution return value

        # get amounts that directly increase or decrease the ballance
        $stmt = qq(select type, amount*unit_price from xfrs where Unit_curr = );
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
                    case ['deposit','fx','dividend'] { $ballance += $row[1]; }
                    case ['buy','withdraw'] { $ballance -= $row[1]; }
                    else { print "\nError: Unknown transaction type: $row[0]\n"; }
                }
            }
        }

        # subtract any conversions where this was a source currency
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
                $ballance -= $row[1];
            }
        }

        # subtract any commissions
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
                $ballance -= $row[1];
            }
        }

        # add increases/decreases of stock amount
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
                    case ['sell'] { $ballance -= $row[1]; }
                    case ['buy'] { $ballance += $row[1]; }
                    else { print "\nError: Unknown transaction type: $row[0]\n"; }
                }
            }
        }

        $href->{$s} = $ballance;
    }

}


# Gets the net asset value (NAV) for the given symbols.
#
# The NAV value is returned with indication of the currency (e.g. 30.25USD).
#
# arguments:
#   - reference to the open DB connection
#   - reference to a hash array to be filled with NAV values
sub getNAV {
    my $dbh = shift || return;
    my $href = shift || return;

    # get the ballance first
    getBallance( $dbh, $href );

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
            my $q = Finance::Quote->new;
            $q->timeout(30);
#---->>>> 2017-12-23: Updated to Finance::Quote 1.47
#2017-12-23            my @attrs = ("price","currency");
#2017-12-23            my @exchgs = ("usa", "europe");
#2017-12-23            foreach my $e (@exchgs) {
#2017-12-23                my %qs  = $q->fetch($e,$s);
#2017-12-23                next if (!exists($qs{$s,$attrs[0]}));
#2017-12-23
#2017-12-23                $nav = ($qs{$s,$attrs[0]} * $href->{$s}).$qs{$s,$attrs[1]};
#2017-12-23                last;
#2017-12-23            }
            my @attrs = ("last","currency");
            my %qs  = $q->alphavantage($s);
            if (exists($qs{$s,$attrs[0]})) {
                $nav = ($qs{$s,$attrs[0]} * $href->{$s}).$qs{$s,$attrs[1]};
            }
#<<<<----

            $href->{$s} =  ($nav eq '')  ? $href->{$s}."???" : $nav;
        } else {
            $href->{$s} = $href->{$s}."???";
        }
    }

}


# gets the total dividend (reduced by a witholding tax) for a stock symbol in the given DB
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

        # get amounts that directly increase or decrease the ballance
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


# Gets the invested amount for a stock symbol in the given DB.
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
# value. Once all of the units of buy transaction are sold, the commision (for
# both the buy and sell trasactions) is transfered to reduce the realized gain
# and hence removed from the remainig invested value.
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
        my $ballance = 0;
        my $stmt; # SQL statement
        my $sth; # compiled SQL statement handle
        my $rv; # SQL execution return value

        if (isStock($dbh,$s)) {
            # get amounts that directly increase or decrease the ballance
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

                    # set the currence based on the 1st transaction record
                    if ($curr eq '') { $curr = $row[3]; }

                    # skip the record if wrong unit currency
                    if ($curr ne $row[3]) {
                        print "Error: Unexpected unit currency ($row[0] $row[1] $s units on $row[6]): act=$row[3], exp=$curr\n";
                        next;
                    }

                    # invalidate commision if wrong currency
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
                    $ballance += $rb->{'units'} * $rb->{'price'} + ($rb->{'units'} > 0 ? 1 : 0) * $rb->{'comm'};
                } 
            }
        } elsif (isCurrency($dbh,$s)) {
            # TODO 2017-12-24: This is teporary solution that only counts depositis and withdrawals.
            #                  We might also consider currency translations, but that would depend
            #                  on how the 'invested amount' is supposed to be used. If we care about
            #                  how much money we put into the account and how much we took back, then
            #                  the current approach is correct. If we were after efficiency of inbestments
            #                  in individual currencies (incl. both stock and cash), then we would
            #                  need to consider translations too.
            #
            #                  Also if deposits and withdrwals were in different currencise, we would
            #                  likely need to convert into a base currency to the date of the transaction,
            #                  as translating only the final ballance would not correctly represent the
            #                  asset value of the investment.

            # get amounts that directly increase or decrease the ballance
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

                    # invalidate commision if wrong currency
                    if ($curr ne $row[5]) {
                        print "Error: Unexpected commision currency ($row[0] $row[1] $s units on $row[6]): act=$row[5], exp=$curr\n";
                        $row[4] = 0;
                    }

                    # act per the transaction type
                    switch ($row[0]) {
                        case ['withdraw'] {
                            # ignore the commision as it would be covered from the remaining ballance and does
                            # not affect the vaue of the investment
                            # (Note: If deposits and withdrawals incured any commissions, than there will be
                            # a residual investment after a withdrawal that would clear the ballance, and that
                            # residual amount would equal the sum of all related commissions.)
                            $ballance = -($row[1]*$row[2]);
                        }
                        case ['deposit'] {
                            # ignore the commision as it would be covered from the deposited amount
                            $ballance += $row[1]*$row[2];
                        }
                        else { print "\nError: Unknown transaction type: $row[0]\n"; }
                    }
                }
            }
        } else {
            # unknown symbol
            next;
        }

        $href->{$s} = $ballance;
    }
}


# Gets the total price for the investment.
#
# The total invested amount consists of the price paid for all units bought and
# commisions paid for all buy and sell transactions. This total amount can be
# used as a basis to compute the percentage of realized/unrealized gain.
#
# arguments:
#   - reference to the open DB connection
#   - reference to a hash array to be filled with invetment price records
sub getTotalInvestedAmount {
    my $dbh = shift || return;
    my $href = shift || return;


    my @syms = keys %$href;
    if (scalar(@syms) == 0) {
        @syms = getSymbols($dbh);
    }

    foreach my $s (@syms) {
        my $ballance = 0;
        my $stmt; # SQL statement
        my $sth; # compiled SQL statement handle
        my $rv; # SQL execution return value

        # get amounts that directly increase or decrease the ballance
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

                # set the currence based on the 1st transaction record
                if ($curr eq '') { $curr = $row[3]; }

                # skip the record if wrong unit currency
                if ($curr ne $row[3]) {
                    print "Error: Unexpected unit currency ($row[0] $row[1] $s units on $row[6]): act=$row[3], exp=$curr\n";
                    next;
                }

                # invalidate commision if wrong currency
                if ($curr ne $row[5]) {
                    print "Error: Unexpected commision currency ($row[0] $row[1] $s units on $row[6]): act=$row[5], exp=$curr\n";
                    $row[4] = 0;
                }

                # act per the transaction type
                switch ($row[0]) {
                    case ['sell'] {
                        # For a sell transactions count only the commision.
                        $ballance += $row[4];
                    }
                    case ['buy'] {
                        # For a buy transaction count the buy price plus the commision.
                        $ballance += $row[1]*$row[2] + $row[4];
                    }
                    else { print "\nError: Unknown transaction type: $row[0]\n"; }
                }
            }
        }

        $href->{$s} = $ballance;
    }
}


# Gets the price earned on all sell transactions.
#
# The price does not count commisions paid for the sell transactions. Hence
# to get a real amount earned on selling, one would need to reduce the total
# sell price by the amount paid for sell commisions.
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
        my $ballance = 0;
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

                # set the currence based on the 1st transaction record
                if ($curr eq '') { $curr = $row[3]; }

                # skip the record if wrong unit currency
                if ($curr ne $row[3]) {
                    print "Error: Unexpected unit currency ($row[0] $row[1] $s units on $row[6]): act=$row[3], exp=$curr\n";
                    next;
                }

                # invalidate commision if wrong currency
                if ($curr ne $row[5]) {
                    print "Error: Unexpected commision currency ($row[0] $row[1] $s units on $row[6]): act=$row[5], exp=$curr\n";
                    $row[4] = 0;
                }

                # act per the transaction type
                switch ($row[0]) {
                    case ['sell'] {
                        # Take only the sell price and ignore commision.
                        $ballance += $row[1]*$row[2];
                    }
                    else { print "\nError: Unknown transaction type: $row[0]\n"; }
                }
            }
        }

        $href->{$s} = $ballance;
    }
}

# ***tbd*** get all stock transacrions
# will return a hash indexed by date and amounts decreasing (sell) and increasing (buy) net asset value
# each item includes the commisions expense such that the commision increases the value of buy and decreases value of sell
sub getStockTransactions {
    my $dbh = shift || return;
    my @syms = @_;

    for (my $i=0; $i < scalar @syms; $i++) {
        $syms[$i] = "'".$syms[$i]."'";
    }
    
    my %cashflow = ();

    # get amounts that directly increase or decrease the ballance
    my $stmt = qq(select type, date, amount, unit_price, unit_curr, comm_price, comm_curr from xfrs where source_curr in );
    $stmt = $stmt."(".join(',', @syms).") order by date;";
    my $sth = $dbh->prepare( $stmt );
    my $rv = $sth->execute();
    if($rv < 0){
        print $DBI::errstr;
    } else {
        my $curr = ''; # stock currency shall be same for all transactions
        my $units = 0; # stock units ballance
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

            # sanity check: commisions currency
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
                # into the cashflow. It will be added to the remaining ballance/NAV.
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

1;
