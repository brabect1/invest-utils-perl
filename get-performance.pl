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
use Scalar::Util::Numeric;

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

my %balance;
my %nav;
my %dividends;
my %investment;
my %investment_tot;
my %sell_val;
my %sell_gain;

# number of decimal places to round when printing values
my $np = 2;

my @symbols = xfrs::getSymbols( $dbh );
my @currencies = xfrs::getCurrencies($dbh);
my @stocks = xfrs::getStocks($dbh);

# Get symbol and stock properties
# -------------------------------
%balance = ();
%nav = ();

foreach my $s (@symbols) {
    $balance{$s} = 0;
    $nav{$s} = 0;
    $investment{$s} = 0;
    $investment_tot{$s} = 0;
    $sell_val{$s} = 0;
    $sell_gain{$s} = 0;
}

foreach my $s (@stocks) {
#    $investment{$s} = 0;
    $dividends{$s} = 0;
}

xfrs::getBalance( $dbh, \%balance );
xfrs::getNAV( $dbh, \%nav );
xfrs::getDividend( $dbh, \%dividends );
xfrs::getInvestedAmount( $dbh, \%investment );
xfrs::getTotalInvestedAmount( $dbh, \%investment_tot );
xfrs::getTotalSellPrice( $dbh, \%sell_val );
xfrs::getSellGain( $dbh, \%sell_gain );

# Report stock balance
# ---------------------
print "# Stocks\n";
my %props = ();
foreach my $s (@stocks) {
    $props{$s} = {};
    $props{$s}->{'sym'} = $s;
    $props{$s}->{'units'} = $balance{$s};
    if ($nav{$s}  =~ /(\d+(\.\d*)?)([A-Z]+)/) {
        $props{$s}->{'nav'} = $1;
        $props{$s}->{'curr'} = $3;
    } else {
        $props{$s}->{'nav'} = $nav{$s};
    }
    $props{$s}->{'dividend'} = $dividends{$s};
    $props{$s}->{'investment'} = $investment{$s};
    $props{$s}->{'total_investment'} = $investment_tot{$s};
    $props{$s}->{'sell_val'} = $sell_val{$s};
    $props{$s}->{'sell_gain'} = $sell_gain{$s};
    if (exists($props{$s}->{'curr'})) {
        $props{$s}->{'real_gain'} =  $props{$s}->{'sell_gain'} + $props{$s}->{'dividend'};
        $props{$s}->{'unreal_gain'} =  $props{$s}->{'nav'} - $props{$s}->{'investment'};
        $props{$s}->{'total_gain'} =  $props{$s}->{'unreal_gain'} + $props{$s}->{'real_gain'};
        $props{$s}->{'total_gain_percent'} =  sprintf("%.3f", $props{$s}->{'total_gain'}*100/$props{$s}->{'total_investment'});
    }

    # compute IRR
    my %cashflow = xfrs::getStockTransactions($dbh,$s);
    $cashflow{localtime->strftime('%Y-%m-%d')} = -($props{$s}->{'nav'} + $props{$s}->{'dividend'});
    my $irr = xirr(%cashflow, precision => 0.001);
    $props{$s}->{'irr'} = $irr ? sprintf("%.".$np."f", 100 * $irr) : '???';

    # sanity check: (total_investment - remain_investment) = (total_sell - gain_sell)
    my $invest_diff = ($props{$s}->{'total_investment'} - $props{$s}->{'investment'});
    my $sell_diff = ($props{$s}->{'sell_val'} - $props{$s}->{'sell_gain'});
    # using `sprintf` to round to avoid floating point math rounding errors
    if (sprintf("%.".$np."f",$invest_diff) != sprintf("%.".$np."f",$sell_diff)) {
        print "Error: Inconsistent data for $s (".
            sprintf("%.".$np."f",$invest_diff)." vs. ".
            sprintf("%.".$np."f",$sell_diff)."): total_invest=".
            $props{$s}->{'total_investment'}.", remain_invest=".$props{$s}->{'investment'}.
            ", sell_total=".$props{$s}->{'sell_val'}.", sell_gain=".$props{$s}->{'sell_gain'}."\n";
    }
}

my @cols_order = (
    'sym',
    'curr',
    'total_investment',
    'investment',
    'nav',
    'sell_val',
    'sell_gain',
    'dividend',
    'total_gain',
    'total_gain_percent',
    'irr'
);
my %cols_name = (
    'sym' => 'sym',
    'curr' => 'curr',
    'investment' => 'remain_invest',
    'total_investment' => 'total_invest',
    'nav' => 'nav',
    'sell_val' => 'sell_val',
    'sell_gain' => 'sell_gain',
    'dividend' => 'dividend',
    'real_gain' => 'real_gain',
    'unreal_gain' => 'unreal_gain',
    'total_gain' => 'total_gain',
    'total_gain_percent' => 'total_gain_%',
    'irr' => 'irr_%'
);
foreach my $c (@cols_order) {
    print "\t$cols_name{$c}";
}
print "\n";
foreach my $s (@stocks) {
    foreach my $c (@cols_order) {
        my $val = '???';
        if (exists($props{$s}->{$c})) {
           $val = $props{$s}->{$c};
           $val = sprintf("%.".$np."f", $val) if Scalar::Util::Numeric::isfloat($val);
        }
        print "\t".$val;
    }
    print "\n";
}

# Report totals for stocks of the same currency
# ---------------------------------------------
# ***TBD*** This is a temporary solution. Properly, it shall be computed
#           from the results of DB queries. The reason is that for calculating
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
        $stocks_total{$curr}->{'total_investment'} = 0;
        $stocks_total{$curr}->{'sell_val'} = 0;
        $stocks_total{$curr}->{'sell_gain'} = 0;
        $stocks_total{$curr}->{'symbols'} = '';
    }
    $stocks_total{$curr}->{'nav'} += $props{$s}->{'nav'};
    $stocks_total{$curr}->{'dividend'} += $props{$s}->{'dividend'};
    $stocks_total{$curr}->{'investment'} += $props{$s}->{'investment'};
    $stocks_total{$curr}->{'total_investment'} += $props{$s}->{'total_investment'};
    $stocks_total{$curr}->{'sell_val'}  += $props{$s}->{'sell_val'};
    $stocks_total{$curr}->{'sell_gain'} += $props{$s}->{'sell_gain'};
    $stocks_total{$curr}->{'symbols'} .= $s.',';
}

foreach my $c (@cols_order) {
    print "\t$cols_name{$c}";
}
print "\n";
foreach my $s (sort keys %stocks_total) {
    $stocks_total{$s}->{'real_gain'} =  $stocks_total{$s}->{'sell_gain'} + $stocks_total{$s}->{'dividend'};
    $stocks_total{$s}->{'unreal_gain'} =  $stocks_total{$s}->{'nav'} - $stocks_total{$s}->{'investment'};
    $stocks_total{$s}->{'total_gain'} =  $stocks_total{$s}->{'unreal_gain'} + $stocks_total{$s}->{'real_gain'};
    $stocks_total{$s}->{'total_gain_percent'} =  sprintf("%.3f", $stocks_total{$s}->{'total_gain'}*100/$stocks_total{$s}->{'total_investment'});

    # calculate IRR
    my %cashflow = xfrs::getStockTransactions($dbh,split(',',$stocks_total{$s}->{'symbols'}));
    $cashflow{localtime->strftime('%Y-%m-%d')} = -($stocks_total{$s}->{'nav'} + $stocks_total{$s}->{'dividend'});
    $stocks_total{$s}->{'irr'} = sprintf("%.3f", 100 * xirr(%cashflow, precision => 0.001));

    # sanity check: (total_investment - remain_investment) = (total_sell - gain_sell)
    my $invest_diff = ($stocks_total{$s}->{'total_investment'} - $stocks_total{$s}->{'investment'});
    my $sell_diff = ($stocks_total{$s}->{'sell_val'} - $stocks_total{$s}->{'sell_gain'});
    if (sprintf("%.".$np."f",$invest_diff) != sprintf("%.".$np."f",$sell_diff)) {
        print "Error: Inconsistent data for $s (".
            sprintf("%.".$np."f",$invest_diff)." vs. ".
            sprintf("%.".$np."f",$sell_diff)."): total_invest=".
            $stocks_total{$s}->{'total_investment'}.", remain_invest=".$stocks_total{$s}->{'investment'}.
            ", sell_total=".$stocks_total{$s}->{'sell_val'}.", sell_gain=".$stocks_total{$s}->{'sell_gain'}."\n";
    }

    foreach my $c (@cols_order) {
        my $val = '???';
        if (exists($stocks_total{$s}->{$c})) {
           $val = $stocks_total{$s}->{$c};
           $val = sprintf("%.".$np."f", $val) if Scalar::Util::Numeric::isfloat($val);
        }
        print "\t".$val;
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
$base_total{$base}->{'sell_val'} = 0;

foreach my $s (@currencies) {
    $props{$s} = {};
    $props{$s}->{'sym'} = $s;
    $props{$s}->{'curr'} = $s;
    $props{$s}->{'units'} = $balance{$s};
    $props{$s}->{'nav'} = 0;
    $props{$s}->{'sell_val'} = 0;
    $props{$s}->{'sell_gain'} = 0;
    $props{$s}->{'dividend'} = $dividends{$s} || 0;
    $props{$s}->{'investment'} = $investment{$s};
    $props{$s}->{'total_investment'} = $investment_tot{$s};

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
    foreach my $c (@cols_order) {
        my $val = '???';
        if (exists($props{$s}->{$c})) {
           $val = $props{$s}->{$c};
           $val = sprintf("%.".$np."f", $val) if Scalar::Util::Numeric::isfloat($val);
        }
        print "\t".$val;
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
    }
}

# Add cash and stock balances together
foreach my $s (sort @currencies) {
    next unless (exists $stocks_total{$s});
    my @cols = qw'nav dividend investment sell_gain';
    foreach my $c (@cols) {
        $props{$s}->{$c} += $stocks_total{$s}->{$c};
    }
}

# Get the sum over all currencies
print "# Total ($base)\n";
foreach my $s (@currencies) {
    next unless (exists $fx_rates{$s});
    my @cols = qw'investment nav dividend sell_val sell_gain total_investment';
    foreach my $c (@cols) {
        $base_total{$base}->{$c} += $props{$s}->{$c} * $fx_rates{$s};
    }
}

# Calculate gain figures
$base_total{$base}->{'real_gain'} =  $base_total{$base}->{'sell_gain'} + $base_total{$base}->{'dividend'};
$base_total{$base}->{'unreal_gain'} =  $base_total{$base}->{'nav'} - $base_total{$base}->{'investment'};
$base_total{$base}->{'total_gain'} =  $base_total{$base}->{'unreal_gain'} + $base_total{$base}->{'real_gain'};
if ($base_total{$base}->{'total_investment'} == 0) {
    $base_total{$base}->{'total_gain_percent'} = '???'; 
} else {
    $base_total{$base}->{'total_gain_percent'} =  sprintf("%.3f", $base_total{$base}->{'total_gain'}*100/$base_total{$base}->{'total_investment'});
}

# Print results
foreach my $c (@cols_order) {
    my $val = '???';
    if (exists($base_total{$base}->{$c})) {
       $val = $base_total{$base}->{$c};
       $val = sprintf("%.".$np."f", $val) if Scalar::Util::Numeric::isfloat($val);
    }
    print "\t".$val;
}
print "\n";
