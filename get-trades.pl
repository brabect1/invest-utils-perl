use DBI;
use Getopt::Long::Descriptive;
use Switch;
use strict;
use warnings;
use List::MoreUtils;
use xfrs;
use Finance::Math::IRR;
use Time::Piece;
use experimental 'smartmatch'; # for using ``if ($var ~~ @array)``

my ($opt, $usage) = describe_options(
  '%c %o',
  [ 'db|d=s',    "Sqlite3 DB file to import into",   { default  => "xfrs.sqlite3.db" } ],
  [],
  [ 'help|h',       "print usage message and exit", { shortcircuit => 1 } ],
);


my $dbh = DBI->connect("dbi:SQLite:dbname=".$opt->db,"","",{ RaiseError => 1 }) or die $DBI::errstr;;

# ... TODO
my @stocks = xfrs::getStocks($dbh);

my %props = ();
foreach my $s (@stocks) {
    my $balance = 0;
    my $stmt; # SQL statement
    my $sth; # compiled SQL statement handle
    my $rv; # SQL execution return value

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
                print STDERR "Error: Unexpected unit currency ($row[0] $row[1] $s units on $row[6]): act=$row[3], exp=$curr\n";
                next;
            }

            # invalidate commission if wrong currency
            if ($curr ne $row[5]) {
                print STDERR "Error: Unexpected commision currency ($row[0] $row[1] $s units on $row[6]): act=$row[5], exp=$curr\n";
                $row[4] = 0;
            }

            # act per the transaction type
            switch ($row[0]) {
                case ['sell'] {
##                    # For a sell transaction count the sell price less the commission.
##                    $balance += $row[1]*$row[2] - $row[4];

                    my $units = -$row[1];
                    my $rs = {
                        'units' => $row[1],
                        'date' => $row[6],
                        'price' => $row[2],
                        'comm' => $row[4] / $row[1]
                    };
                    foreach my $rb (@trans) {
                        # skip the buy transaction if already depleated
                        # (i.e. no more units left from the transaction)
                        next if ($rb->{'units'} == 0);

                        # recover sell units from the buy transaction
                        $units += $rb->{'units'};

                        # prefix to be printed
                        my $pfx = "$s\t$rb->{'curr'}";

                        # suffix to be printed
                        my $sfx = "";

                        # buy records
                        $sfx .= "\t$rb->{'date'}\t$rb->{'price'}\t".sprintf("%.3f",$rb->{'comm'});
                        # sell records
                        $sfx .= "\t$rs->{'date'}\t$rs->{'price'}\t".sprintf("%.3f",$rs->{'comm'});

                        # discount the bought units and buy commissions if
                        # the buy transaction gets fully cleared (i.e. selling
                        # more units than what remains of the `rb` buy transaction)
                        if ($units < 0) {
                            print $pfx."\t$rb->{'units'}".$sfx."\n";

                            # clearing the whole buy transaction => discount also buy commission
                            $rb->{'units'} = 0;
                        } else {
                            print $pfx."\t".($rb->{'units'} - $units).$sfx."\n";
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
                            'price' => $row[2], # price per unit
                            'curr' => $row[3],
                            'comm' => $row[4] / $row[1], # commision per unit
                            'date' => $row[6]
                        }
                    );
                }
                else { print "\nError: Unknown transaction type: $row[0]\n"; }
            }
        }
    }
}

$dbh->disconnect();
