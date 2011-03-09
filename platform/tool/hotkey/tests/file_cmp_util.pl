#!usr/bin/perl

# compare file with special format
# file0 : 
# ref-count key
# file1:
# head_flag
# ip ref-count key
# END^M


sub cmp_pair_file_ntop() {
	my $dismatch = 0, $line=0, $dif_ref=0;

	($fd0, $fd1, $top, $log)  =  @_;

	open (STDOUT, ">$log");
	open (FILE_0, "$fd0") or die "can't open file:$fd0\n";
	open (FILE_1, "$fd1") or die "can't open file:$fd1\n";

	<FILE_1>; # omit head
	while (defined($x[$line] = <FILE_0>) && 
           defined($y[$line] = <FILE_1>)) {
		if( $y[$line] =~ m/^END/){ # end flag
			last;
		}

	   	++$line;
	}
    if ($line == 0) {
        print "OMG, no effective line in stander/snapshot.\n";
        return;
    }
    # we don't need to care the rest log lines with the same ref-count
    # just to make the real scenario to MAKESURE the keys are HOT
=pod

	@t =  split(/\s+/, $y[$line_y-1]);
	$ref = $t[1];
	while (defined($y[$line_y] = <FILE_1>)) {
		@t = split(/\s+/, $y[$line_y]);
		$line_y += 1;
		$temp = $t[1];
		if($temp !=  $ref) {
			last;
		}
	}
	$line_y -= 1;
=cut

    # select keys in stander key set, and then search them in snapshot
    # to verify they exist in dumped hot keys
	foreach my $i(0..$line-1) {
		my $ok = 0;
		my @xx = split(/\s+/, $x[$i]);
		foreach my $j (0..$line-1) {
			@yy = split(/\s+/, $y[$j]);
			if($xx[1] eq $yy[2]) {
				$dif_ref += ($xx[0]-$yy[1])*($xx[0]-$yy[1]);
				$ok = 1;
				last;
			}
		}
		if($ok == 0) {
			$dismatch += 1;
		} 
	}

    if ($line == $dismatch) {
        printf "OMG, no matched. please check two files.\n";
        return;
    }
    $dif_ref = $dif_ref * 1.0 / ($line - $dismatch);
    $dif_ref = sqrt($dif_ref);
    $missrate = $dismatch * 1.0 / $line;

	print "$dismatch differences between $fd0 and $fd1\n";
	print "dismatch: $missrate\%   in $top hot keys\n";
	print "square difference in ref: $dif_ref\n";

	close (STDOUT);
	close (FILE_0);
	close (FILE_1);
	return ($missrate, $dif_ref);
}
1;
