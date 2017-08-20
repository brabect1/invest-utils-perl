use DBI;
use Switch;
use strict;
use warnings;
use List::MoreUtils;
use Array::Utils;


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

1;
