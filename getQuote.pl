use DBI;
use Getopt::Long::Descriptive;
#use Switch;
use Finance::Quote;
use Array::Utils;
use xfrs;
use strict;
use warnings;


my ($opt, $usage) = describe_options(
  '%c %o',
  [ 'db|d=s',    "Sqlite3 DB file to import into",   { default  => "xfrs.sqlite3.db" } ],
  [ 'base|b=s',  "base currency",   { default  => "" } ],
  [],
  [ 'help|h',       "print usage message and exit", { shortcircuit => 1 } ],
);


my $dbh = DBI->connect("dbi:SQLite:dbname=".$opt->db,"","",{ RaiseError => 1 }) or die $DBI::errstr;;

my @stocks = xfrs::getStocks($dbh);
my @currencies = xfrs::getCurrencies($dbh);


my $q = Finance::Quote->new;

$q->timeout(30);
#my @attrs = ("price","name","currency");
my @attrs = ("price","currency");
my %quotes;

#push(@stocks);
my @exchgs = ("usa", "europe");
foreach my $s (@stocks) {
    $quotes{$s} = { 'price' => 1.0, 'currency' => $s };
    print "$s ....\n";
    foreach my $e (@exchgs) {
        my %qs  = $q->fetch($e,$s);
        if (!exists($qs{$s,$attrs[0]})) { print "no match for $e\n"; next; }
        foreach my $a (@attrs) {
            $quotes{$s}->{$a} = $qs{$s,$a};
            print "$s.$a = ".$qs{$s,$a}."\n";
        }
        last;
    }
##    foreach my $k (keys %quotes) {
##        print "$k = ".$quotes{$k}."\n";
##    }
}

my %curconv;
if ($opt->base ne "") {
    my @curs = @currencies;

    # add currencies of the stocks
    foreach my $s (keys %quotes) {
        my $rec = $quotes{$s};
        if (exists $rec->{'currency'}) {
            push(@curs,$rec->{'currency'});
        }
    }
    @curs = Array::Utils::unique(@curs);

    # quote conversion rates (to the base currency)
    foreach my $c (@curs) {
        if ($opt->base eq $c) {
            $curconv{$c} = 1.0;
            next;
        }
        my $convrate = $q->currency($c,$opt->base);
        if (! defined $convrate ) { next; }
        $curconv{$c} = $convrate;
        print "$c -> ".$opt->base.": $convrate\n";
    }
}

# get the actual ballance
my %ballance;
xfrs::getBallance($dbh, \%ballance);

# collect NAV (net asset value)
my $nav = 0;

# print the cash ballance
foreach my $c (@currencies) {
    print "$c,$ballance{$c},$c";
    if (exists($curconv{$c})) {
        my $v = $ballance{$c}*$curconv{$c};
        print ",".$v.",".$opt->base;
        $nav += $v;
    }
    print "\n";
}

# print the equity ballance
foreach my $s (@stocks) {
    my $p = $quotes{$s}->{'price'} || 0;
    my $c = $quotes{$s}->{'currency'} || "";
    print "$s,".($ballance{$s}*$p).",$c";
    if (exists $curconv{$c} && defined $curconv{$c}) {
        my $v = $ballance{$s}*$p*$curconv{$c};
        print ",".$v.",".$opt->base;
        $nav += $v;
    }
    print "\n";
}

print "\nnav = $nav".$opt->base."\n";

#my $conversion_rate = $q->currency("AUD","USD");
#$q->set_currency("EUR");  # Return all info in Euros.
#
#$q->require_labels(qw/price date high low volume/);
#
#$q->failover(1); # Set failover support (on by default).
#
#my @stocks = ["RGR", "AOBC"];
#
#my %quotes  = $q->fetch("nasdaq",@stocks);
##my $hashref = $q->fetch("nyse",@stocks);
#
#foreach my $k (keys %quotes) {
#    print "$k = $quotes{$k}\n";
#}
