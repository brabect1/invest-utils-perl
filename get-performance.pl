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
