#! /usr/bin/perl

=pod

=begin description

Read hex dump of ECC page and convert to binary.

=end description

=cut

require 5.0;
use strict;
use warnings;
use IO::File;
use IO::Dir;
use Getopt::Std;
use Carp;

my $debug;
my $verbose;

sub eprint {
	print {*STDERR} @_;
}

sub eprintf {
	printf {*STDERR} @_;
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

my $raw_data;
my $page_4k;
my $ecc_data;
my $data_bytes;
my $ecc_bytes;
my $base_addr;
my $prev_addr;
my $addr;
my $err;

$data_bytes = 0;
$ecc_bytes = 0;
$err = 0;
while (<>) {
    chomp;
    if (m/^0[Xx]/) {
        if (m/^0[Xx]([\dA-Fa-f]+):\s+/) {
            my $hex_data;
            my @xf;
            my $x;

            $addr = hex(substr($1, -8, 8));
            $hex_data = $';
            if (defined($prev_addr)) {
                if ($addr > $prev_addr + 16) {
                    eprintf("Jump in address (%x to %x):\n",
                        $prev_addr + 16, $addr);
                    eprint('  ', $_, "\n");
                    ++$err;
                }
                elsif ($addr < $prev_addr + 16) {
                    eprintf("Overlap (%x to %x):\n",
                        $prev_addr + 16, $addr);
                    eprint('  ', $_, "\n");
                    ++$err;
                }
            }

            @xf = split(/\s+/, $hex_data);
            for $x (@xf) {
                $raw_data .= pack('H2', substr($x, 6, 2));
                $raw_data .= pack('H2', substr($x, 4, 2));
                $raw_data .= pack('H2', substr($x, 2, 2));
                $raw_data .= pack('H2', substr($x, 0, 2));
            }
            $prev_addr = $addr;
        }
        else {
            eprint("Invalid hex dump data:\n", '  ', $_, "\n");
            ++$err;
        }
    }

    if ($err > 20) {
        eprint("Too many errors -- bailing out.\n");
        last;
    }
}

if ($err) {
    exit 1;
}

my $expect_length = 4096 + 128;
if (length($raw_data) != $expect_length) {
    eprintf("Expecting %u bytes of data (4k data + 128 ECC), got %u bytes.\n",
        $expect_length, length($raw_data));
    if (length($raw_data) < $expect_length) {
        eprint("Not enough data -- bailing out.\n");
        exit 1;
    }
    eprintf("Trailing %u bytes will be ignored.\n",
        length($raw_data) - $expect_length);
}

my $pos = 0;
print substr($raw_data, $pos, 2048);
$pos = 2048 + 64;
print substr($raw_data, $pos, 2048);

$pos = 2048;
print substr($raw_data, $pos, 64);
$pos = 2048 + 64 + 2048;
print substr($raw_data, $pos, 64);

eprint('ECC 1: ', unpack('H64', substr($raw_data, 2048, 64)), "\n");
eprint('ECC 2: ', unpack('H64', substr($raw_data, 2048 + 64 + 2048, 64)), "\n");

eprint('Test pattern:', "\n");
eprint('  ', unpack('H64', substr($raw_data, 0, 64)), "\n");

exit 0;
