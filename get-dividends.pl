use DBI;
use Getopt::Long::Descriptive;
use Switch;
use strict;
use warnings;
use List::MoreUtils;
use xfrs;
use Finance::Math::IRR;
use Time::Piece;
use experimental 'smartmatch'; # for using ``if ($var ~~ @array)``

my ($opt, $usage) = describe_options(
  '%c %o',
  [ 'db|d=s',    "Sqlite3 DB file to import into",   { default  => "xfrs.sqlite3.db" } ],
  [ 'order|o=s', "Records order (`date`, `symbol`)", { default  => "date" } ],
  [],
  [ 'help|h',       "print usage message and exit", { shortcircuit => 1 } ],
);


my $dbh = DBI->connect("dbi:SQLite:dbname=".$opt->db,"","",{ RaiseError => 1 }) or die $DBI::errstr;;
my $order = 'date';
if ($opt->order ~~ ['date', 'symbol']) {
    $order = $opt->order;
} else {
    print STDERR "Warning: Unknown ordering ('$opt->order'). Ignored ...\n";
}

my @divs = xfrs::getDividends($dbh,('order' => $order));

foreach my $d (@divs) {
    print "$d->{'symbol'}\t$d->{'currency'}\t$d->{'amount'}\t$d->{'tax'}\t$d->{'date'}\n";
}

$dbh->disconnect();
