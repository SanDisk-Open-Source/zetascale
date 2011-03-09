#!/usr/bin/perl

# performance evaluation for hotkey configures.


$ip 	= "127.0.0.1";
$port	= "11211";

# define configures for hotkey
@items= ( "N_BUCKET_PER_SLAB", "MIN_WINNERS_PER_LIST", "MIN_CANDIDATES_PER_LIST", "CANDIDATE_RATIO", "THRESHOLD");

@configures = ([16, 16, 4, 25, 1], # default
			   [16, 16, 4, 25, 100], 
			   [16, 16, 16, 25, 1],
			   [16, 16, 16, 25, 100], # just have a look
			   [16, 16, 32, 25, 1],
			   [16, 32, 4, 25, 1],   # larger winners
			   [16, 32, 16, 25, 1],  # this one?
			   [16, 32, 32, 25, 1],
			   [32, 16, 4, 25, 1], 	# less slabs
			   [32, 16, 8, 25, 1],
			   [32, 16, 16, 25, 1],
			   [32, 16, 32, 25, 1], # just have a look
			   [64, 16, 8, 25, 1],	# less slabs
			   [64, 64, 32, 25, 1],	# this one?
			   [64, 64, 8, 12, 1],	# less slabs
			   );

# generate configure file
sub gen_config() {
	print "loop = $count\n";
	open FILE, ">hotkey.config" or die "create configure file failed";
	$configure = $configures[$count];
	printf "item=@items\n config=[@{$configures[$count]}]\n";
	for $i (0..$#items) {
		print FILE "$items[$i] $configure->[$i]\n";
	}
	close FILE;
	`cp hotkey.config config.$count`;
}


# start mcd
sub startmcd() {
	`sh runperf.sh > server_$count.log 2>&1 &`;
	`sleep 5`;
}

# shutdown mcd
sub shutdownmcd() {
	`echo "shutdown" | nc $ip $port`;
	`sleep 5`;
}

# enable hotkey
sub enablehotkey() {
	`echo "schooner hotkey on" | nc $ip $port`;
}

# start ms
sub startms() {
	`sh runms.sh > perf_$count`;
}

# run all loops
sub runall() {
	$count = 0;
	for $count (0..$#configures) {
		&gen_config();
		&startmcd();
		&enablehotkey();
		&startms();
		&shutdownmcd();
	}
}

&runall();
