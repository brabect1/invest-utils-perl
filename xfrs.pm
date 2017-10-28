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

1;
