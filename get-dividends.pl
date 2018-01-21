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
  [],
  [ 'help|h',       "print usage message and exit", { shortcircuit => 1 } ],
);


my $dbh = DBI->connect("dbi:SQLite:dbname=".$opt->db,"","",{ RaiseError => 1 }) or die $DBI::errstr;;

my @divs = xfrs::getDividends($dbh);

foreach my $d (@divs) {
    print "$d->{'symbol'}\t$d->{'currency'}\t$d->{'amount'}\t$d->{'tax'}\t$d->{'date'}\n";
}

$dbh->disconnect();
