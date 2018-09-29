# Copyright 2018 Tomas Brabec
#
# See LICENSE for license details.

# -----------------------------------------------------------------------------
# Description:
#   Prints the current ballance of a portfolio from DB, separately for each
#   symbol and as a total net asset value (NAV).
#
#   This script is similar to `get-quotes.pl` but groups the data differently
#   and also computes NAV differently.
# -----------------------------------------------------------------------------
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
  [ 'base|b=s',  "base currency",   { default  => "" } ],
  [],
  [ 'help|h',       "print usage message and exit", { shortcircuit => 1 } ],
);


my $dbh = DBI->connect("dbi:SQLite:dbname=".$opt->db,"","",{ RaiseError => 1 }) or die $DBI::errstr;;
my $stmt;
my $sth;
my $rv;

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
if ($opt->base ne "") {
    my $total = 0;
    my $base = $opt->base;
    print "# Total NAV ($base)\n";
    foreach my $s (keys %totals) {
        if ($s eq $base) {
            $total += $totals{$s};
        } else {
            # query the conversion rate
            my %qs = xfrs::getQuoteCurrency($dbh,'',$base,$s);
            if (exists($qs{$s})) {
                $total += $totals{$s} * $qs{$s}->{'price'};
            } else {
                print "Error: Failed to obtain conversion rate $s to $base!\n";
            }
        }
    }
    print "\t$base = $total\n";
}

$dbh->disconnect();

