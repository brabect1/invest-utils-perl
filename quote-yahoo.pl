use Data::Dumper;
use Finance::Quote;
use Array::Utils;
use xfrs;
use POSIX;
use strict;
use warnings;

print "".POSIX::strftime("%Y-%m-%d",localtime)."\n\n";

my $q = Finance::Quote->new;
#my %info = $q->fetch("yahoo_json",("USDCZK=X","AAPL","MSFT"));
my %info = $q->fetch("yahoo_json",("CZKEUR=X"));

print Dumper(\%info);

# foreach my $k (keys %info) {
#     print "$k -> $info{$k}\n";
# }
