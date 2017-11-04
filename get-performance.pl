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

my %ballance;
my %nav;
my %dividends;
my %investment;

my @symbols = xfrs::getSymbols( $dbh );
my @currencies = xfrs::getCurrencies($dbh);
my @stocks = xfrs::getStocks($dbh);

# Get symbol and stock properties
# -------------------------------
%ballance = ();
%nav = ();

foreach my $s (@symbols) {
    $ballance{$s} = 0;
    $nav{$s} = 0;
}

foreach my $s (@stocks) {
    $investment{$s} = 0;
    $dividends{$s} = 0;
}

xfrs::getBallance( $dbh, \%ballance );
xfrs::getNAV( $dbh, \%nav );
xfrs::getDividend( $dbh, \%dividends );
xfrs::getInvestedAmount( $dbh, \%investment );

# Report stock ballance
# ---------------------
print "# Stocks\n";
my %props = ();
foreach my $s (@stocks) {
    $props{$s} = {};
    $props{$s}->{'sym'} = $s;
    $props{$s}->{'units'} = $ballance{$s};
    if ($nav{$s}  =~ /(\d+(\.\d*)?)([A-Z]+)/) {
        $props{$s}->{'nav'} = $1;
        $props{$s}->{'curr'} = $3;
    } else {
        $props{$s}->{'nav'} = $nav{$s};
    }
    $props{$s}->{'dividend'} = $dividends{$s};
    $props{$s}->{'investment'} = $investment{$s};
    if (exists($props{$s}->{'curr'})) {
        $props{$s}->{'unreal_gain'} =  $props{$s}->{'nav'} - $props{$s}->{'investment'};
        $props{$s}->{'total_gain'} =  $props{$s}->{'unreal_gain'} + $props{$s}->{'dividend'};

        #***TBD*** This is not correct as the `investment` represents the remaining invested amount.
        $props{$s}->{'total_gain_percent'} =  $props{$s}->{'total_gain'}*100/$props{$s}->{'investment'};
    }
}

my @cols = (
    'sym',
    'curr',
    'investment',
    'nav',
    'dividend',
    'total_gain',
    'total_gain_percent'
);
foreach my $c (@cols) {
    print "\t$c";
}
print "\n";
foreach my $s (@stocks) {
    foreach my $c (@cols) {
        print "\t".(exists($props{$s}->{$c}) ? $props{$s}->{$c} : "???");
    }
    print "\n";
}

# Report totals for stocks of the same currency
# ---------------------------------------------
# ***TBD*** This is a temporary solution. Properly, it shall be computed
#           form the results of DB queries. The reason is that for calculating
#           IRR we will need all the transactions, not just a "total sum".
print "# Stocks (total)\n";

my %stocks_total = ();
foreach my $s (@stocks) {
    if (!exists($props{$s}->{'curr'})) {
        print "Error: Undefined currency for $s!\n";
        next;
    }

    my $curr = $props{$s}->{'curr'};
    if (!exists($stocks_total{$curr})) {
        $stocks_total{$curr}->{'sym'} = $curr;
        $stocks_total{$curr}->{'curr'} = $curr;
        $stocks_total{$curr}->{'units'} = 'n/a';
        $stocks_total{$curr}->{'nav'} = 0;
        $stocks_total{$curr}->{'dividend'} = 0;
        $stocks_total{$curr}->{'investment'} = 0;
    }
    $stocks_total{$curr}->{'nav'} += $props{$s}->{'nav'};
    $stocks_total{$curr}->{'dividend'} += $props{$s}->{'dividend'};
    $stocks_total{$curr}->{'investment'} += $props{$s}->{'investment'};
}

foreach my $c (@cols) {
    print "\t$c";
}
print "\n";
foreach my $s (keys %stocks_total) {
    $stocks_total{$s}->{'unreal_gain'} =  $stocks_total{$s}->{'nav'} - $stocks_total{$s}->{'investment'};
    $stocks_total{$s}->{'total_gain'} =  $stocks_total{$s}->{'unreal_gain'} + $stocks_total{$s}->{'dividend'};

    #***TBD*** This is not correct as the `investment` represents the remaining invested amount.
    $stocks_total{$s}->{'total_gain_percent'} =  $stocks_total{$s}->{'total_gain'}*100/$stocks_total{$s}->{'investment'};

    foreach my $c (@cols) {
        print "\t".(exists($stocks_total{$s}->{$c}) ? $stocks_total{$s}->{$c} : "???");
    }
    print "\n";
}

