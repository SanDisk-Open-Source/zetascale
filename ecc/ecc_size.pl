#! /usr/bin/perl

=pod

=begin description

Print table of data sizes and the size in bits required for
the ECC data for that size.

=end description

=cut

require 5.0;
use strict;
use warnings;
use Carp;

my $debug;
my $verbose;

sub eprint {
	print {*STDERR} @_;
}

sub eprintf {
	printf {*STDERR} @_;
}

# Ceiling of log base 2 of n.
#
sub log2_ceiling($) {
	my ($n) = @_;
	my $i;
	my $p;

	$p = 1;
	for ($i = 0; $p < $n; ++$i) {
		$p *= 2;
	}
	return $i;
}


$debug = 0;
$verbose = 0;
while (defined($ARGV[0]) && $ARGV[0] =~ /^-/) {
	$_ = $ARGV[0];
	if    ($_ eq '-d') {
		$debug = 1;
	}
	elsif (m/^-d(\d+)$/) {
		$debug = $1;
	}
	elsif ($_ eq '-v') {
		$verbose = 1;
	}
	elsif ($_ eq '-') {
		last;
	}
	elsif ($_ eq '--') {
		shift;
		last;
	}
	else {
		die "Unknown option, '$_'\n ";
	}
	shift;
}
shift  if (scalar(@ARGV) > 0 && !defined($ARGV[0]));

my $szc;
my $sz;

print '              ECC',  "\n";
print 'p2 data-size  bits', "\n";
print '-- ---------- ----', "\n";
for ($szc = 0; $szc <= 32; ++$szc) {
	$sz = 1 << $szc;
	printf "%2u %10u %4u\n", $szc, $sz, 2 * log2_ceiling($sz * 8);
}

exit 0;
