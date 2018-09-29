# Copyright 2018 Tomas Brabec
#
# See LICENSE for license details.

# -----------------------------------------------------------------------------
# Description:
#   TODO
# -----------------------------------------------------------------------------
use DBI;
use Getopt::Long::Descriptive;
use Switch;
use strict;
use warnings;
use List::MoreUtils;
use xfrs;
use Finance::Math::IRR;
use Time::Piece;

my ($opt, $usage) = describe_options(
  '%c %o',
  [ 'db|d=s',    "Sqlite3 DB file to import into",   { default  => "xfrs.sqlite3.db" } ],
  [ 'base|b=s',  "base currency",   { default  => "USD" } ],
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
    $investment{$s} = 0;
}

foreach my $s (@stocks) {
#    $investment{$s} = 0;
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
        $props{$s}->{'total_gain_percent'} =  sprintf("%.3f", $props{$s}->{'total_gain'}*100/$props{$s}->{'investment'});
    }

    # compute IRR
    my %cashflow = xfrs::getStockTransactions($dbh,$s);
    $cashflow{localtime->strftime('%Y-%m-%d')} = -($props{$s}->{'nav'} + $props{$s}->{'dividend'});
    $props{$s}->{'irr'} = sprintf("%.3f", 100 * xirr(%cashflow, precision => 0.001));
}

my @cols = (
    'sym',
    'curr',
    'investment',
    'nav',
    'dividend',
    'total_gain',
    'total_gain_percent',
    'irr'
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
        $stocks_total{$curr}->{'symbols'} = '';
    }
    $stocks_total{$curr}->{'nav'} += $props{$s}->{'nav'};
    $stocks_total{$curr}->{'dividend'} += $props{$s}->{'dividend'};
    $stocks_total{$curr}->{'investment'} += $props{$s}->{'investment'};
    $stocks_total{$curr}->{'symbols'} .= $s.',';
}

foreach my $c (@cols) {
    print "\t$c";
}
print "\n";
foreach my $s (sort keys %stocks_total) {
    $stocks_total{$s}->{'unreal_gain'} =  $stocks_total{$s}->{'nav'} - $stocks_total{$s}->{'investment'};
    $stocks_total{$s}->{'total_gain'} =  $stocks_total{$s}->{'unreal_gain'} + $stocks_total{$s}->{'dividend'};

    #***TBD*** This is not correct as the `investment` represents the remaining invested amount.
    $stocks_total{$s}->{'total_gain_percent'} =  sprintf("%.3f", $stocks_total{$s}->{'total_gain'}*100/$stocks_total{$s}->{'investment'});

    # calculate IRR
    my %cashflow = xfrs::getStockTransactions($dbh,split(',',$stocks_total{$s}->{'symbols'}));
    $cashflow{localtime->strftime('%Y-%m-%d')} = -($stocks_total{$s}->{'nav'} + $stocks_total{$s}->{'dividend'});
    $stocks_total{$s}->{'irr'} = sprintf("%.3f", 100 * xirr(%cashflow, precision => 0.001));

    foreach my $c (@cols) {
        print "\t".(exists($stocks_total{$s}->{$c}) ? $stocks_total{$s}->{$c} : "???");
    }
    print "\n";
}

# Report total for a base currency
# --------------------------------
my %base_total = ();
my $base = 'USD';
if ($opt->base ne "") {
    $base = $opt->base;
} 

$base_total{$base}->{'sym'} = $base;
$base_total{$base}->{'curr'} = $base;
$base_total{$base}->{'units'} = 'n/a';
$base_total{$base}->{'nav'} = 0;
$base_total{$base}->{'dividend'} = 0;
$base_total{$base}->{'investment'} = 0;

foreach my $s (@currencies) {
    $props{$s} = {};
    $props{$s}->{'sym'} = $s;
    $props{$s}->{'curr'} = $s;
    $props{$s}->{'units'} = $ballance{$s};
    $props{$s}->{'nav'} = 0;
    $props{$s}->{'dividend'} = $dividends{$s} || 0;
    $props{$s}->{'investment'} = $investment{$s};

    if ($nav{$s}  =~ /(\d+(\.\d*)?)([A-Z]+)/) {
        next if ($s ne $3);
        $props{$s}->{'nav'} = $1;
    } else {
        $props{$s}->{'nav'} = $nav{$s};
    }

##    if (exists($props{$s}->{'curr'})) {
##        $props{$s}->{'unreal_gain'} =  $props{$s}->{'nav'} - $props{$s}->{'investment'};
##        $props{$s}->{'total_gain'} =  $props{$s}->{'unreal_gain'} + $props{$s}->{'dividend'};
##
##        #***TBD*** This is not correct as the `investment` represents the remaining invested amount.
##        $props{$s}->{'total_gain_percent'} =  sprintf("%.3f", $props{$s}->{'total_gain'}*100/$props{$s}->{'investment'});
##    }

##    # compute IRR
##    my %cashflow = xfrs::getStockTransactions($dbh,$s);
##    $cashflow{localtime->strftime('%Y-%m-%d')} = -($props{$s}->{'nav'} + $props{$s}->{'dividend'});
##    $props{$s}->{'irr'} = sprintf("%.3f", 100 * xirr(%cashflow, precision => 0.001));
}

print "# Cash\n";
foreach my $s (sort @currencies) {
    foreach my $c (@cols) {
        print "\t".(exists($props{$s}->{$c}) ? $props{$s}->{$c} : "???");
    }
    print "\n";
}


# Query FX conversion rates
my %fx_rates;
#my $q = Finance::Quote->new;
foreach my $s (sort @currencies) {
    if ($s eq $base) {
        $fx_rates{$s} = 1.0;
    } else {
        my %qs = xfrs::getQuoteCurrency($dbh,'',$base,$s);
        if (exists($qs{$s})) {
            $fx_rates{$s} = $qs{$s}->{'price'};
        } else {
            print "Error: Failed to obtain conversion rate $s to $base!\n";
        }
#        my $rate = $q->currency($s,$base);
#        if ($rate) {
#            $fx_rates{$s} = $rate;
#        } else {
#            print "Error: Failed to obtain conversion rate $s to $base!\n";
#        }
    }
}

# Add cash and stock ballances together
foreach my $s (sort @currencies) {
    next unless (exists $stocks_total{$s});
    my @cols = qw'nav dividend';
    foreach my $c (@cols) {
        $props{$s}->{$c} += $stocks_total{$s}->{$c};
    }
}

# Get the sum over all currencies
print "# Total ($base)\n";
foreach my $s (@currencies) {
    next unless (exists $fx_rates{$s});
    my @cols = qw'investment nav dividend';
    foreach my $c (@cols) {
        $base_total{$base}->{$c} += $props{$s}->{$c} * $fx_rates{$s};
    }
}

# Calculate gain figures
$base_total{$base}->{'unreal_gain'} =  $base_total{$base}->{'nav'} - $base_total{$base}->{'investment'};
$base_total{$base}->{'total_gain'} =  $base_total{$base}->{'unreal_gain'} + $base_total{$base}->{'dividend'};

#***TBD*** This is not correct as the `investment` represents the remaining invested amount.
$base_total{$base}->{'total_gain_percent'} =  sprintf("%.3f", $base_total{$base}->{'total_gain'}*100/$base_total{$base}->{'investment'});

# Print results
foreach my $c (@cols) {
    print "\t".(exists($base_total{$base}->{$c}) ? $base_total{$base}->{$c} : "???");
}
print "\n";
