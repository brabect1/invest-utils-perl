use DBI;
use Getopt::Long::Descriptive;
use Switch;
use strict;
use warnings;
use List::MoreUtils;
use xfrs;

my ($opt, $usage) = describe_options(
  '%c %o',
  [ 'db|d=s',    "Sqlite3 DB file to import into",   { default  => "xfrs.sqlite3.db" } ],
  [],
  [ 'help|h',       "print usage message and exit", { shortcircuit => 1 } ],
);


my $dbh = DBI->connect("dbi:SQLite:dbname=".$opt->db,"","",{ RaiseError => 1 }) or die $DBI::errstr;;
my $stmt;
my $sth;
my $rv;

## get the list of currencies
#$stmt = qq(SELECT unit_curr, source_curr, comm_curr from xfrs;);
#$sth = $dbh->prepare( $stmt );
#$rv = $sth->execute();
#if($rv < 0){
#    print $DBI::errstr;
#}
#
#my @currencies = ();
#while (my @row = $sth->fetchrow_array()) {
#    push( @currencies, @row);
#
#}
#@currencies = List::MoreUtils::uniq(@currencies);

#foreach my $c (@currencies) {
#    print "$c\n";
#
#    my $ballance = 0;
#
#    # get amounts that directly increase or decrease the ballance
#    $stmt = qq(select type, amount*unit_price from xfrs where Unit_curr = );
#    $stmt = $stmt."'$c';";
#    $sth = $dbh->prepare( $stmt );
#    $rv = $sth->execute();
#    if($rv < 0){
#        print $DBI::errstr;
#    } else {
#        while (my @row = $sth->fetchrow_array()) {
#            if (scalar(@row) < 2) { next; }
#            if (!defined $row[0] || length $row[0] == 0) { next; }
#            switch ($row[0]) {
#                case ['deposit','fx','dividend'] { $ballance += $row[1]; }
#                case ['buy','withdraw'] { $ballance -= $row[1]; }
#                else { print "\nError: Unknown transaction type: $row[0]\n"; }
#            }
#        }
#        print "\t$ballance\n";
#    }
#
#    # subtract any conversions where this was a source currency
#    $stmt = qq(select type, source_price from xfrs where type='fx' and source_curr=);
#    $stmt = $stmt."'$c';";
#    $sth = $dbh->prepare( $stmt );
#    $rv = $sth->execute();
#    if($rv < 0){
#        print $DBI::errstr;
#    } else {
#        while (my @row = $sth->fetchrow_array()) {
#            if (scalar(@row) < 2) { next; }
#            if (!defined $row[0] || length $row[0] == 0) { next; }
#            $ballance -= $row[1];
#        }
#        print "\t$ballance\n";
#    }
#
#    # subtract any commissions
#    $stmt = qq(select type, sum(comm_price) from xfrs where comm_curr = );
#    $stmt = $stmt."'$c';";
#    $sth = $dbh->prepare( $stmt );
#    $rv = $sth->execute();
#    if($rv < 0){
#        print $DBI::errstr;
#    } else {
#        while (my @row = $sth->fetchrow_array()) {
#            if (scalar(@row) < 2) { next; }
#            if (!defined $row[0] || length $row[0] == 0) { next; }
#            $ballance -= $row[1];
#        }
#        print "\t$ballance\n";
#    }
#
#    # add increases/decreases of stock amount
#    $stmt = qq(select type, amount from xfrs where type in ('sell', 'buy') and source_curr = );
#    $stmt = $stmt."'$c';";
#    $sth = $dbh->prepare( $stmt );
#    $rv = $sth->execute();
#    if($rv < 0){
#        print $DBI::errstr;
#    } else {
#        while (my @row = $sth->fetchrow_array()) {
#            if (scalar(@row) < 2) { next; }
#            if (!defined $row[0] || length $row[0] == 0) { next; }
#            switch ($row[0]) {
#                case ['sell'] { $ballance -= $row[1]; }
#                case ['buy'] { $ballance += $row[1]; }
#                else { print "\nError: Unknown transaction type: $row[0]\n"; }
#            }
#        }
#        print "\t$ballance\n";
#    }
#}
my %ballance;
my %nav;

# Get ballance
# ------------
%ballance = ();
%nav = ();
my @symbols = xfrs::getSymbols( $dbh );

foreach my $s (@symbols) {
    $ballance{$s} = 0;
    $nav{$s} = 0;
#print "$s\n";
}

xfrs::getBallance( $dbh, \%ballance );
xfrs::getNAV( $dbh, \%nav );

# Report cash ballance
# --------------------
print "# Currencies\n";
my @currencies = xfrs::getCurrencies($dbh);
foreach my $s (@currencies) {
    print "\t$s = $ballance{$s}\n";
}


# Report stock ballance
# ---------------------
print "# Stocks (in units)\n";
my @stocks = xfrs::getStocks($dbh);
foreach my $s (@stocks) {
    print "\t$s = $ballance{$s} ($nav{$s})\n";
}

# Report total NAV per currency
# -----------------------------
my %totals;

# initialize with cash ballance
foreach my $s (@currencies) {
    $totals{$s} = $ballance{$s};
}

# add NAV of all stocks
foreach my $s (@stocks) {
    if ($nav{$s} =~ /(\d+(\.\d*)?)([A-Z]+)/) {
        if (!exists $totals{$3}) {
            $totals{$3} = $1;
        } else {
            $totals{$3} += $1;
        }
    }
}

# report
print "# Total NAV\n";
foreach my $s (keys %totals) {
    print "\t$s = $totals{$s}\n";
}

# Report total of totals NAV (in a base currency)
# -----------------------------------------------
my $total = 0;
my $base = 'CZK';
print "# Total NAV ($base)\n";
my $q = Finance::Quote->new;
foreach my $s (keys %totals) {
    if ($s eq $base) {
        $total += $totals{$s};
    } else {
        # query the conversion rate
        my $rate = $q->currency($s,$base);
        if ($rate) {
            $total += $totals{$s} * $rate;
        } else {
            print "Error: Failed to obtain conversion rate $s to $base!\n";
        }
    }
}
print "\t$base = $total\n";

$dbh->disconnect();

