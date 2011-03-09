# $Id: verify_od.pl 2473 2008-07-29 11:37:55Z drew $
#
# Sanity check od -i output with physmem_driver compiled with PHYSMEM_DEBUG_FILL
# 
# dd if=/dev/physmem | od -i | perl verify_od.pl

my $debug;

my $expected = 0;
my $address;
my $number;
my $rest;


while (<>) {
    chop;
    if  (/^([0-9a-f]+)\s+(.*)/) {
	$address = $1;
	$rest = $2;
	print STDERR "address $address rest $rest\n" if ($debug);

	while ($rest ne "") {
	    if ($rest =~ /(^\d+)\s+(.*)/) {
		$number = $1;
		$rest = $2;
	    } else {
		$number = $rest;
		$rest = "";
	    }

	    die "got $number not $expected" if ($number != $expected);
	    $expected = $number + 1;
	}
    } elsif (! /^[0-9a-f]+$/) {
	die "unexpected line: $_";
    }
}
