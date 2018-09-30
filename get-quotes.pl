# Copyright 2018 Tomas Brabec
#
# See LICENSE for license details.

# -----------------------------------------------------------------------------
# Description:
#   Quotes stock and currency symbols from DB.
#
#   The list of stocks and currencies is taken from DB. Currencies are
#   quoted only when the base currency is given (`-b` option).
#
#   Quotes are obtained from the following sources with decreasing priority:
#   - Cached quotes from DB
#   - Yahoo Finance
#   - AlphaVantage.
# -----------------------------------------------------------------------------
use DBI;
use Getopt::Long::Descriptive;
#use Switch;
#use Data::Dumper;
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
my @attrs = ("last","currency");
my %quotes;

# Remaps attributes from the info structure returned by a quote.
my %attrMap = (
    'last' => 'price',
    'currency' => 'currency'
);


# Quote stocks
# ------------
# obtain cached quotes
my %qtCacheStocks = xfrs::getCachedQuote($dbh,'',@stocks);
foreach my $s (keys %qtCacheStocks) {
    $quotes{$s} = $qtCacheStocks{$s};
    print "$s (cache) ....\n";
    foreach my $a (@attrs) {
        print "\t$s.$a = ".$quotes{$s}->{$attrMap{$a}}."\n";
    }
}

# obtain additional quotes (if needed) from
# - Yahoo finance
# - AlphaVantage
foreach my $qtSrc ('yahoo_json', 'alphavantage') {
    if (scalar @stocks > scalar keys %quotes) {
        my @syms;
        foreach my $s (@stocks) {
            push(@syms,$s) unless (exists($quotes{$s}));
        }
    
        my %qs = $q->fetch($qtSrc,@syms);
        foreach my $s (@syms) {
            next unless (exists($qs{$s,'success'}) && $qs{$s,'success'} == 1);
    
            print "$s ($qtSrc) ....\n";
            foreach my $a (@attrs) {
                if (exists($qs{$s,$a})) {
                    $quotes{$s}->{$attrMap{$a}} = $qs{$s,$a};
                    print "\t$s.$a = ".$qs{$s,$a}."\n";
                }
            }
        }
    }
}

# report failed stock quotes
if (scalar @stocks > scalar keys %quotes) {
    foreach my $s (@stocks) {
        print "$s ... failed\n" unless (exists($quotes{$s}));
    }
}

# Quote currencies
# ----------------
my %curconv;
if ($opt->base ne "") {
    my %curQuotes;
    my @curs = @currencies;
    my $base = $opt->base;

    # add currencies of the stocks
    foreach my $s (keys %quotes) {
        my $rec = $quotes{$s};
        if (exists $rec->{'currency'}) {
            push(@curs,$rec->{'currency'});
        }
    }
    @curs = Array::Utils::unique(@curs);

    # get cached quotes first
    my @cacheSyms;
    foreach my $c (@curs) {
        push(@cacheSyms,$c.$base);
    }
    my %qtCacheCurs = xfrs::getCachedQuote($dbh,'',@cacheSyms);
    foreach my $c (@curs) {
        if (exists($qtCacheCurs{$c.$base})) {
            $curQuotes{$c} = $qtCacheCurs{$c.$base};
            print "$c (cache) ....\n";
            foreach my $a (@attrs) {
                print "\t$c.$a = ".$curQuotes{$c}->{$attrMap{$a}}."\n";
            }
        }
    }

    # append the base currency (if needed)
    if (!exists($curQuotes{$base})) {
        $curQuotes{$base} = {
            'price' => 1.0,
            'currency' => $base
        };
    }

    # quote conversion rates at Yahoo Finance (if needed)
    if (scalar @curs > scalar keys %curQuotes) {
        my %syms;
        foreach my $s (@curs) {
            $syms{$s}=$s.$base."=X" unless (exists($curQuotes{$s}));
        }
   
        my %qs = $q->fetch("yahoo_json",values %syms);
#print Dumper(\%qs);
        foreach my $s (keys %syms) {
            next unless (exists($qs{$syms{$s},'success'}) && $qs{$syms{$s},'success'} == 1);
    
            print "$s (yahoo_json) ....\n";
            foreach my $a (@attrs) {
                if (exists($qs{$syms{$s},$a})) {
                    $curQuotes{$s}->{$attrMap{$a}} = $qs{$syms{$s},$a};
                    print "\t$s.$a = ".$qs{$syms{$s},$a}."\n";
                }
            }
        }
    }

    # quote conversion rates (if needed) using the default API (AlphaVantage as of Finance::Quote 1.47)
    if (scalar @curs > scalar keys %curQuotes) {
        my @syms;
        foreach my $s (@curs) {
            push(@syms,$s) unless (exists($curQuotes{$s}));
        }

        foreach my $c (@syms) {
            if ($base eq $c) {
                $curQuotes{$c} = {
                    'price' => 1.0,
                    'currency' => $c
                };
            } else {
                my $convrate = $q->currency($c,$base);
                if (! defined $convrate ) { next; }
                $curQuotes{$c} = {
                    'price' => $convrate,
                    'currency' => $base
                };
            }
            print "$c/$base (defualt) ....\n";
            foreach my $a (@attrs) {
                print "\t$c.$a = ".$curQuotes{$c}->{$attrMap{$a}}."\n";
            }
        }
    }

    # populate the conversion rate hash
    foreach my $c (keys %curQuotes) {
        $curconv{$c} = $curQuotes{$c}->{'price'};
    }
}

# Print balance
# --------------
# get the actual balance
my %balance;
xfrs::getBalance($dbh, \%balance);

# collect NAV (net asset value)
my $nav = 0;

# print the cash balance
foreach my $c (@currencies) {
    print "$c,$balance{$c},$c";
    if (exists($curconv{$c})) {
        my $v = $balance{$c}*$curconv{$c};
        print ",".$v.",".$opt->base;
        $nav += $v;
    }
    print "\n";
}

# print the equity balance
foreach my $s (@stocks) {
    my $p = $quotes{$s}->{'price'} || 0;
    my $c = $quotes{$s}->{'currency'} || "";
    print "$s,".($balance{$s}*$p).",$c";
    if (exists $curconv{$c} && defined $curconv{$c}) {
        my $v = $balance{$s}*$p*$curconv{$c};
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
