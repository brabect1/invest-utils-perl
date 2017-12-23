use xfrs;
use Finance::Quote;
use Finance::Math::IRR;
use DBI;
use Getopt::Long::Descriptive;
use Time::Piece;
use strict;
use warnings;

my ($opt, $usage) = describe_options(
  '%c %o',
  [ 'db|d=s',    "Sqlite3 DB file to import into",   { default  => "xfrs.sqlite3.db" } ],
  [],
  [ 'help|h',       "print usage message and exit", { shortcircuit => 1 } ],
);


my $dbh = DBI->connect("dbi:SQLite:dbname=".$opt->db,"","",{ RaiseError => 1 }) or die $DBI::errstr;;

my @stocks = xfrs::getStocks($dbh);

foreach my $s (@stocks) {
    my %cashflow = ();

    my $curr = ''; # stock currency shall be same for all transactions
    my $units = 0; # stock units ballance
    my $dividend = 0; # accumulated dividend
    my $date = ''; # date of last buy/sell transaction 
    my $stmt; # SQL statement
    my $sth; # compiled SQL statement handle
    my $rv; # SQL execution return value

    # get amounts that directly increase or decrease the ballance
    $stmt = qq(select type, date, amount, unit_price, unit_curr, comm_price, comm_curr from xfrs where source_curr = );
    $stmt = $stmt."'$s' order by date;";
    $sth = $dbh->prepare( $stmt );
    $rv = $sth->execute();
    if($rv < 0){
        print $DBI::errstr;
    } else {
        while (my @row = $sth->fetchrow_array()) {
            if (scalar(@row) < 2) { next; }
            if (!defined $row[0] || length $row[0] == 0) { next; }

            if ($curr eq '') {
                $curr = $row[4];
            }

            # sanity check: transaction type
            if ($row[0] ne 'buy' && $row[0] ne 'sell' && $row[0] ne 'dividend') {
                print "\nError: Unknown transaction type for stock $s: $row[0]\n";
                next;
            }

            # sanity check: source currency
            if ($row[4] ne $curr) {
                print "\nError: Unexpected currency for $s $row[0]: act $row[4], exp $curr\n";
                next;
            }

            # sanity check: commisions currency
            if ($row[6] ne $curr) {
                print "\nError: Unexpected commision currency for $s $row[0]: act $row[6], exp $curr\n";
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
                $dividend += $row[2]*$row[3];
                $dividend -= $row[5];
                next;
            }

            $cashflow{$row[1]} = $amount;
        }
    }

    # get quote (to compute the actual ballance)
    $date = localtime->strftime('%Y-%m-%d');
    $cashflow{$date} = 0;
    if ($units > 0) {
        my $q = Finance::Quote->new;
        $q->timeout(30);
#2017-12-23        my @attrs = ("price","currency");
#2017-12-23        my @exchgs = ("usa", "europe");
#2017-12-23        foreach my $e (@exchgs) {
#2017-12-23            my %qs  = $q->fetch($e,$s);
#2017-12-23            next if (!exists($qs{$s,$attrs[0]}));
#2017-12-23
#2017-12-23            # sanity check: currency match
#2017-12-23            if ($curr ne $qs{$s,$attrs[1]}) {
#2017-12-23                print "\nError: Unexpected currency for $s quote: act $qs{$s,$attrs[1]}, exp $curr\n";
#2017-12-23            }
#2017-12-23
#2017-12-23            $date = localtime->strftime('%Y-%m-%d');
#2017-12-23            $cashflow{$date} = -($qs{$s,$attrs[0]} * $units);
#2017-12-23            last;
#2017-12-23        }
        my @attrs = ("last","currency");
        my %qs  = $q->alphavantage($s);
        if (exists($qs{$s,$attrs[0]})) {
            $cashflow{$date} -= ($qs{$s,$attrs[0]} * $units);
        }
    }

    # add dividend withdrawal
    $cashflow{$date} -= $dividend;

    foreach my $k (sort keys %cashflow) {
        print ">>$s,$k,$cashflow{$k}\n";
    }

    # calculate IRR
    my $irr = xirr(%cashflow, precision => 0.001);
    print "$s, $irr\n";
}

