# Copyright 2018 Tomas Brabec
#
# See LICENSE for license details.

# -----------------------------------------------------------------------------
# Description:
#   Imports data from a text file into XFRS DB.
# -----------------------------------------------------------------------------
use DBI;
use Getopt::Long::Descriptive;
use strict;
use warnings;
use xfrs;

my ($opt, $usage) = describe_options(
  '%c %o',
  [ 'input|s=s', "the text file to import from", { required => 1  } ],
  [ 'db|d=s',    "Sqlite3 DB file to import into",   { default  => "xfrs.sqlite3.db" } ],
  [],
  [ 'help|h',       "print usage message and exit", { shortcircuit => 1 } ],
);

my $rec_indexes = {
    'type' => 0,
    'date' => 1,
    'amount' => 2,
    'unit_price' => 3,
    'unit_curr' => 4,
    'source_price' => 5,
    'source_curr' => 6,
    'comm_price' => 7,
    'comm_curr' => 8
};

my $rec_templates = {
    'withdraw' => [
        'withdraw',
        '1970-01-01',
        0,
        1,
        '',
        0,
        '',
        0,
        ''
    ],
    'deposit' => [
        'deposit',
        '1970-01-01',
        0,
        1,
        '',
        0,
        '',
        0,
        ''
    ],
    'dividend' => [
        'dividend',
        '1970-01-01',
        0,
        0,
        '',
        0,
        '',
        0,
        ''
    ],
    'fx' => [
        'fx',
        '1970-01-01',
        0,
        0,
        '',
        0,
        '',
        0,
        ''
    ],
    'buy' => [
        'buy',
        '1970-01-01',
        0,
        0,
        '',
        0,
        '',
        0,
        ''
    ],
    'sell' => [
        'sell',
        '1970-01-01',
        0,
        0,
        '',
        0,
        '',
        0,
        ''
    ],
};

my $rec_list = [];

open(my $fd, $opt->input) or die "Cannot open file '".$opt->input."': ...";

my $lineno = 0;
while (my $line = <$fd>) {

    $lineno++;

    # skip blank lines
    if ($line =~ /^\s*$/) { next; }

    # skip comments
    if ($line =~ /^\s*#/) { next; }

    unless ($line =~ /(\w+)\(\s*(\w+=[\w\.-]+(\s+\w+=[\w\.-]+)*)\s*\)/) {
        print "Improperly formatted record at line $lineno: $line";
        next;
    }

##    print "$1 >> $2\n";
    if (!exists $rec_templates->{$1}) {
        print "Unknown transaction '$1' at line $lineno: $line";
        next;
    }

    # split the attributes and put it into a hash
    my @attrs = split(/\s/,$2);
    my $attrs_hash = {};
    foreach my $pair (@attrs) {
        my @attr_parts = split(/=/,$pair);
        if (scalar(@attr_parts) < 2) { next; }
        $attrs_hash->{$attr_parts[0]} = $attr_parts[1];
    }

    # get the template and populate it with actuals
    my $rec = $rec_templates->{$1}; 
    my @reca = @$rec;
    $rec = \@reca;

    if (exists $attrs_hash->{'date'}) {
        $$rec[$rec_indexes->{'date'}] = $attrs_hash->{'date'};
    }

    if ($$rec[0] eq 'deposit' || $$rec[0] eq 'withdraw') {
        # For the moment, we only assume currency transfers. There can
        # potentially be asset transfers. These can be modeled, for now,
        # as two records: A money transfer and a stock transaction.
        # Note: A direct deposit/withdraw of an asset can also be modeled
        # using the asset symbol as a currency and mandatory commision
        # (even though of zero value) with a valid currency symbol and
        # a mandatory cost basis (that would translate to `unit` currency
        # and price). The cost basis had to be hard-coded and would likely
        # represent the closing price at the date of the transfer per unit
        # of the asset.
        if (exists $attrs_hash->{'amount'}) {
            if ($attrs_hash->{'amount'} =~ /(\d+(\.\d*)?)([A-Z.]+)/) {
                $$rec[$rec_indexes->{'amount'}] = $1;
                $$rec[$rec_indexes->{'unit_curr'}] = $3;
                $$rec[$rec_indexes->{'source_curr'}] = $3;
                $$rec[$rec_indexes->{'comm_curr'}] = $3;

                # some transfers may incur expenses that we account as commisions
                if (exists  $attrs_hash->{'commission'} && 
                    $attrs_hash->{'commission'} =~ /(\d+(\.\d*)?)([A-Z.]+)/) {
                    $$rec[$rec_indexes->{'comm_price'}] = $1;
                    $$rec[$rec_indexes->{'comm_curr'}] = $3;
                }

                # TODO ---->>>> experimental
                # direct asset transfers require cost basis so we can later
                # evaluate gains on selling the asset
                # TODO: Presently the case of depositing and withdrawing assets
                # is not supported for evaluating portfolio performance (in
                # `get-performance.pl`).
                if (exists  $attrs_hash->{'costbasis'} &&
                    $attrs_hash->{'costbasis'} =~ /(\d+(\.\d*)?)([A-Z.]+)/) {
                    $$rec[$rec_indexes->{'unit_price'}] = $1;
                    $$rec[$rec_indexes->{'unit_curr'}] = $3;
                }
                #<<<<----
            } else {
                print "Unexpected format of transaction amoount, line $lineno: $attrs_hash->{'amount'}";
            }
        }
    }

    if ($$rec[0] eq 'buy' || $$rec[0] eq 'sell') {
        if (exists $attrs_hash->{'amount'}) {
            $$rec[$rec_indexes->{'amount'}] = $attrs_hash->{'amount'};
        }

        if (exists $attrs_hash->{'stock'}) {
            $$rec[$rec_indexes->{'source_curr'}] = $attrs_hash->{'stock'};
        } else {
            print "Missing stock symbol, line $lineno.";
        }

        if (exists $attrs_hash->{'price'}) {
            if ($attrs_hash->{'price'} =~ /(\d+(\.\d*)?)([A-Z]+)/) {
                $$rec[$rec_indexes->{'unit_price'}] = $1;
                $$rec[$rec_indexes->{'unit_curr'}] = $3;
            } else {
                print "Unexpected format of transaction amoount, line $lineno: $attrs_hash->{'price'}";
            }
        }

        if (exists $attrs_hash->{'commission'}) {
            if ($attrs_hash->{'commission'} =~ /(\d+(\.\d*)?)([A-Z]+)/) {
                $$rec[$rec_indexes->{'comm_price'}] = $1;
                $$rec[$rec_indexes->{'comm_curr'}] = $3;
            } else {
                print "Unexpected format of transaction amoount, line $lineno: $attrs_hash->{'commission'}";
            }
        }
    }

    if ($$rec[0] eq 'fx') {
        if (exists $attrs_hash->{'amount'}) {
            if ($attrs_hash->{'amount'} =~ /(\d+(\.\d*)?)([A-Z]+)/) {
                $$rec[$rec_indexes->{'amount'}] = $1;
                $$rec[$rec_indexes->{'unit_curr'}] = $3;
                $$rec[$rec_indexes->{'unit_price'}] = '1';
            } else {
                print "Unexpected format of transaction amoount, line $lineno: $attrs_hash->{'amount'}";
            }
        }

        if (exists $attrs_hash->{'price'}) {
            if ($attrs_hash->{'price'} =~ /(\d+(\.\d*)?)([A-Z]+)/) {
                $$rec[$rec_indexes->{'source_price'}] = $1 * $$rec[$rec_indexes->{'amount'}];
                $$rec[$rec_indexes->{'source_curr'}] = $3;
            } else {
                print "Unexpected format of transaction amoount, line $lineno: $attrs_hash->{'price'}";
            }
        }

        if (exists $attrs_hash->{'commission'}) {
            if ($attrs_hash->{'commission'} =~ /(\d+(\.\d*)?)([A-Z]+)/) {
                $$rec[$rec_indexes->{'comm_price'}] = $1;
                $$rec[$rec_indexes->{'comm_curr'}] = $3;
            } else {
                print "Unexpected format of transaction amoount, line $lineno: $attrs_hash->{'commission'}";
            }
        }
    }

    if ($$rec[0] eq 'dividend') {
        if (exists $attrs_hash->{'amount'}) {
            if ($attrs_hash->{'amount'} =~ /(\d+(\.\d*)?)([A-Z]+)/) {
                $$rec[$rec_indexes->{'amount'}] = $1;
                $$rec[$rec_indexes->{'unit_curr'}] = $3;
                $$rec[$rec_indexes->{'unit_price'}] = '1';
            } else {
                print "Unexpected format of transaction amoount, line $lineno: $attrs_hash->{'amount'}";
            }
        }

        # for now treat stock title as the source currency
        if (exists $attrs_hash->{'stock'}) {
            $$rec[$rec_indexes->{'source_curr'}] = $attrs_hash->{'stock'};
        }

        # for now treat tax as the commissions
        if (exists $attrs_hash->{'tax'}) {
            if ($attrs_hash->{'tax'} =~ /(\d+(\.\d*)?)([A-Z]+)/) {
                $$rec[$rec_indexes->{'comm_price'}] = $1;
                $$rec[$rec_indexes->{'comm_curr'}] = $3;
            } else {
                print "Unexpected format of transaction amoount, line $lineno: $attrs_hash->{'tax'}";
            }
        }
    }

    push( @$rec_list, $rec );
}

close($fd);

# Print records
foreach my $rec (@$rec_list) {
    print "$$rec[$rec_indexes->{'type'}]\n";
    foreach my $attr (('date', 'amount', 'unit_price', 'unit_curr', 'source_price', 'source_curr', 'comm_price', 'comm_curr')) {
        print "\t$attr:\t$$rec[$rec_indexes->{$attr}]\n";
    }
}


# Put the records into DB
my $dbh = DBI->connect("dbi:SQLite:dbname=".$opt->db,"","",{ RaiseError => 1 }) or die $DBI::errstr;;

## my $dbr = $dbh->do( qq(CREATE TABLE xfrs (
##     id INT PRIMARY KEY,
##     type TEXT NOT NULL,
##     date TEXT,
##     amount INT NOT NULL,
##     unit_price REAL,
##     unit_curr TEXT,
##     source_price REAL,
##     source_curr TEXT,
##     comm_price REAL,
##     comm_curr TEXT);
##     ));
## if($dbr < 0){ print $DBI::errstr; }
## 
## for(my $i=0; $i < @$rec_list; $i++) {
## 
##     my $stmt = qq(INSERT INTO xfrs (id,type,date,unit_curr,source_curr,comm_curr,amount,unit_price,source_price,comm_price) VALUES )."($i,";
##     
##     my $rec = $rec_list->[$i];
##     $stmt = $stmt."'$$rec[$rec_indexes->{'type'}]'";
##     foreach my $attr (('date', 'unit_curr', 'source_curr', 'comm_curr')) {
##         $stmt = $stmt.",'$$rec[$rec_indexes->{$attr}]'";
##     }
##     foreach my $attr (('amount', 'unit_price', 'source_price', 'comm_price')) {
##         $stmt = $stmt.",$$rec[$rec_indexes->{$attr}]";
##     }
## 
##     $stmt = $stmt.");";
## #print "$stmt\n";
##     $dbr = $dbh->do($stmt);
##     if($dbr < 0){ print $DBI::errstr; }
## }
foreach my $rec (@$rec_list) {
    my %h;
    foreach my $attr (keys %$rec_indexes) {
        $h{$attr} = $$rec[$rec_indexes->{$attr}];
    }
    xfrs::addTransaction($dbh,%h);
}

$dbh->disconnect();

