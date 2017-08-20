use xfrs;
use Finance::Math::IRR;
use DBI;
use Getopt::Long::Descriptive;
use strict;
use warnings;

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

my @stocks = xfrs::getStocks($dbh);

foreach my $s (@stocks) {
    my %cashflow = ();
    my $irr = xirr(%cashflow, precision => 0.001);

    print "$s, $irr\n";
}

