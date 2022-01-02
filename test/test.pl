# Copyright 2021 Tomas Brabec
#
# See LICENSE for license details.

use DBI;
use POSIX;
use Test::More;
use File::Temp;
use DateTime;
use strict;
use warnings;
use xfrs;

my (undef, $filename) = File::Temp::tempfile( undef, OPEN => 0, SUFFIX => '.sqlite3.db');

ok( defined $filename );
ok( $filename ne "" );
#TODO print ">>$filename";

my $dbh = DBI->connect("dbi:SQLite:dbname=".$filename,"","",{ RaiseError => 1 }) or die $DBI::errstr;

# Test Cached Quotes
# ------------------

my $date;
my %h;

$date = POSIX::strftime("%Y-%m-%d",localtime);

# check with empty quotes
%h = xfrs::getCachedQuote($dbh,$date,('AAPL'));
ok(!defined $h{AAPL});

# insert a quote and check
xfrs::addCachedQuote($dbh, symbol => 'AAPL', date => $date, price => 100, currency => 'USD');

%h = xfrs::getCachedQuote($dbh,$date,('AAPL'));
ok(defined $h{AAPL});
ok($h{AAPL}->{date} eq $date);
ok($h{AAPL}->{price} == 100);
ok($h{AAPL}->{currency} eq 'USD');

# check with yesterdays date
my $yesterday = DateTime->today(time_zone => 'local')->add( days => -1 )->ymd();
%h = xfrs::getCachedQuote($dbh,$yesterday,('AAPL'));
ok(!defined $h{AAPL});

# add the yesterday's quote
xfrs::addCachedQuote($dbh, symbol => 'AAPL', date => $yesterday, price => 99, currency => 'EUR');
%h = xfrs::getCachedQuote($dbh,$yesterday,('AAPL'));
ok(defined $h{AAPL});
ok($h{AAPL}->{date} eq $yesterday);
ok($h{AAPL}->{price} == 99);
ok($h{AAPL}->{currency} eq 'EUR');

%h = xfrs::getCachedQuote($dbh,$date,('AAPL'));
ok(defined $h{AAPL});
ok($h{AAPL}->{date} eq $date);
ok($h{AAPL}->{price} == 100);
ok($h{AAPL}->{currency} eq 'USD');

$dbh->disconnect();

done_testing();

# Test cleanup
# ------------
if (Test::More->builder->is_passing) {
    # remove the temporary DB file
    unlink($filename) if (-e $filename);
} else {
    diag("Keeping DB file: '$filename'");
}

