use DBI;
use Switch;
use strict;
use warnings;
use List::MoreUtils;
use Array::Utils;
use Finance::Quote;


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

    my @currencies = ();
    while (my @row = $sth->fetchrow_array()) {
        push( @currencies, @row);

    }
    return List::MoreUtils::uniq(@currencies);
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

    my @currencies = ();
    while (my @row = $sth->fetchrow_array()) {
        push( @currencies, @row);

    }
    return List::MoreUtils::uniq(@currencies);
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
            my @attrs = ("price","currency");
            my @exchgs = ("usa", "europe");
            foreach my $e (@exchgs) {
                my %qs  = $q->fetch($e,$s);
                next if (!exists($qs{$s,$attrs[0]}));

                $nav = ($qs{$s,$attrs[0]} * $href->{$s}).$qs{$s,$attrs[1]};
                last;
            }

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
#   - reference to a hash array to be filled with dividend records
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

        $href->{$s} = $ballance;
    }
}

1;
