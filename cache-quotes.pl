# Copyright 2018 Tomas Brabec
#
# See LICENSE for license details.

# -----------------------------------------------------------------------------
# Description:
#   Quotes stock and currency symbols and updates the quotes in DB.
#
#   The list of stocks and currencies is taken from DB. Currencies are
#   quoted only when the base currency is given (`-b` option).
#
#   Quotes are obtained from Yahoo Finance.
# -----------------------------------------------------------------------------
use DBI;
use Getopt::Long::Descriptive;
use xfrs;
use POSIX;
use strict;
use warnings;

my ($opt, $usage) = describe_options(
  '%c %o',
  [ 'db|d=s',    "Sqlite3 DB file to import into",   { default  => "xfrs.sqlite3.db" } ],
  [ 'base|b=s',  "base currency",   { default  => "" } ],
  [],
  [ 'help|h',       "print usage message and exit", { shortcircuit => 1 } ],
);

# Capture present date. We will cache one quote per day as the whole
# utils package is not meant for realtime monitoring but rather for
# a one time use.
my $date = POSIX::strftime("%Y-%m-%d",localtime);

# Hash of quotes. Will be used to update DB.
my %quotes;

# Put the records into DB
my $dbh = DBI->connect("dbi:SQLite:dbname=".$opt->db,"","",{ RaiseError => 1 }) or die $DBI::errstr;;
my $sth;
my $rv;


# Test if the 'quotes' table exist, or create otherwise
$sth = $dbh->prepare( "SELECT name FROM sqlite_master WHERE type='table' AND name='quotes';" );
$rv = $sth->execute();
if($rv < 0) {
    print $DBI::errstr;
} else {
    my @row = $sth->fetchrow_array();
    if (scalar @row == 0) {
        my $dbr = $dbh->do( qq(CREATE TABLE quotes (
            id INT PRIMARY KEY,
            symbol TEXT NOT NULL,
            date TEXT NOT NULL,
            price REAL,
            curr TEXT);
            ));
        if($dbr < 0){ print $DBI::errstr; }
    }
}

# create an instance of Finance::Quote
my $q = Finance::Quote->new;
$q->timeout(30);

# quote stock symbols
# (Note: Presently we use Yahoo Finance engine as it turns more reliable
# than the AlphaVantage one.)
my @stocks = xfrs::getStocks($dbh);
if (scalar @stocks > 0) {
    my @attrs = ("last","currency");
    my %qs = $q->fetch("yahoo_json",@stocks);
    foreach my $s (@stocks) {
        if (!exists($qs{$s,'success'})) {
            print "$s no quote\n";
        } elsif ($qs{$s,'success'} != 1) {
            print "$s no success\n";
        } else {
            $quotes{$s} = { 'last' => 1.0, 'currency' => $s };
            foreach my $a (@attrs) {
                if (exists($qs{$s,$a})) {
                    $quotes{$s}->{$a} = $qs{$s,$a};
                }
            }
        }
    }
}

# quote currency symbols
# (Note: Presently we use Yahoo Finance engine as it turns more reliable
# than the AlphaVantage one.)
if ($opt->base ne "") {
    my %currencies;
    my $base = $opt->base;

    # get currencies from DB
    foreach my $c (xfrs::getCurrencies($dbh)) {
        $currencies{$c} = $c;
    }

    # get currencies from quoted stocks
    foreach my $s (keys %quotes) {
        my $c = $quotes{$s}->{'currency'};
        if (!exists($currencies{$c})) {
            $currencies{$c} = $c;
        }
    }

    # create quote symbols understood by Yahoo Finance
    my @syms;
    foreach my $c (keys %currencies) {
        # skip the base currency
        next if ($c eq $base);
        push(@syms, $c.$base."=X");
    }

    # get quotes
    my @attrs = ("last","currency");
    my %qs = $q->fetch("yahoo_json",@syms);
    foreach my $c (keys %currencies) {
        # skip the base currency
        next if ($c eq $base);

        # capture results of the quote
        my $s = $c.$base."=X";
        if (!exists($qs{$s,'success'})) {
            print "$s no quote\n";
        } elsif ($qs{$s,'success'} != 1) {
            print "$s no success\n";
        } else {
            $quotes{$c.$base} = { 'last' => 1.0, 'currency' => $c };
            foreach my $a (@attrs) {
                if (exists($qs{$s,$a})) {
                    $quotes{$c.$base}->{$a} = $qs{$s,$a};
                }
            }
        }
    }
}

# update DB
foreach my $s (keys %quotes) {
    my $stmt;

    $stmt = "SELECT * from quotes where date='".$date."' AND symbol='".$s."';";
    $sth = $dbh->prepare( $stmt );
    $rv = $sth->execute();
    if($rv < 0) {
        print $DBI::errstr;
        last;
    }
    my @row = $sth->fetchrow_array();
    $sth->finish();

    if (scalar @row != 0) {
        # some quote already cached => update
        $stmt = "UPDATE quotes SET ";
        $stmt .= "price='".$quotes{$s}->{'last'}."'";
        $stmt .= ", curr='".$quotes{$s}->{'currency'}."'";
        $stmt .= " where date='".$date."' AND symbol='".$s."';";
    } else {
        # no quote yet => create
        $stmt = "INSERT INTO quotes (symbol,date,price,curr) VALUES (";
        $stmt .= "'".$s."',";
        $stmt .= "'".$date."',";
        $stmt .= "'".$quotes{$s}->{'last'}."',";
        $stmt .= "'".$quotes{$s}->{'currency'}."');";
    }
    $rv = $dbh->do($stmt);
    if($rv < 0){ print $DBI::errstr; }
}

# close DB
$dbh->disconnect();
