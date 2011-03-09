#!/usr/bin/perl
use strict;

my @actions = ('NO_ACTION', 'ACTION_COUNT');
my @backoff_stratagies = ('NO_BACKOFF', 'BACKOFF_CONSTANT', 'BACKOFF_LINEAR', 'BACKOFF_EXPONENTIAL', 'BACKOFF_RANDOM');
my @delay_operations = ('DELAY_WITH_REP_NOP', 'DELAY_WITH_MOD', 'DELAY_WITH_EMPTY_LOOP');
my @lock_types = ('LOCK_TYPE_XCHG', 'LOCK_TYPE_BOOL_CAS', 'LOCK_TYPE_CAS', 'LOCK_TYPE_FIXED_QUEUE', 'LOCK_TYPE_DYNAMIC_QUEUE');
my @fast_checks = ('NO_FAST_CHECK', 'FAST_CHECK_LOCK');
my @padding = ('DONT_PAD_LOCKS', 'PAD_LOCKS');
my @data_points =  (2);

for my $action (@actions) {
    next if ($action eq 'NO_ACTION'); # XXX 

    for my $backoff (@backoff_stratagies) {

        for my $delay_op (@delay_operations) {
            next if ($delay_op ne 'DELAY_WITH_MOD'); # XXX
            next if ($backoff eq 'NO_BACKOFF' && $delay_op ne 'DELAY_WITH_EMPTY_LOOP');

            for my $lock_type (@lock_types) {
                next if ($lock_type ne 'LOCK_TYPE_CAS'); # XXX
                # this lock type is broken
                next if ($lock_type eq 'LOCK_TYPE_BOOL_CAS'); 
                # queue locks can only be used if we can guarantee the threads won't be preempted
                next if (($lock_type eq 'LOCK_TYPE_FIXED_QUEUE' || $lock_type eq 'LOCK_TYPE_DYNAMIC_QUEUE'));

                for my $fast_check (@fast_checks) {
                    next if ($fast_check ne 'NO_FAST_CHECK'); # XXX

                    for my $pad (@padding) {
                        next if ($pad eq 'PAD_LOCKS'); # XXX

                        my $config = "$action $backoff $delay_op $lock_type $fast_check $pad"; 
                        system "gcc -O3 -m64 -std=gnu99 -Wall -o t -lpthread -D$action -D$backoff -D$delay_op -D$lock_type -D$fast_check -D$pad lock_test.c";

                        for my $num_threads (@data_points) {
                            for my $affinity_offset (-1..1) {
                                for my $affinity_stride (1..2) {
                                    system "./t $num_threads $affinity_offset $affinity_stride \"$config\"";
                                }
                            }
                        }
                        print "\n";
                    }
                }
            }
        }
    }
}
