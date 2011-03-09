#!/usr/bin/perl

# hotkey daemon to send hotkey command and then dump the recv buffer
# with configured interval.
# it's intended to run with the same machine with schooner-memcached.

$|=1;
use strict;
use Time::HiRes qw(gettimeofday tv_interval);

# advance config for admin
my $maxtop     = "300";    # init for hot key
my $interval   = "1";      # with hour
my $capacity   = 8;        # with GB

# updated with image, don't change them unless built image modified.
my $ip                  = "127.0.0.1";
my $nctnr               = 8;

my $LOG        = "/var/log/schooner/hotkey.log";   # log location
my $DIR        = "/opt/schooner/data/hotkey";      # snapshot location
my $PROP       = "/opt/schooner/memcached/config/memcached.properties";
my $schnc      = "/opt/schooner/bin/schnc";
my @ports               = ();
my @ctnr_list           = ();
my @last_ctnr_list      = ();
my @dump_snaps          = ();
my @dump_snaps_key      = ();
my @dump_snaps_client   = ();

my $admin_port       = 0;
my $versions         = 0;
my $cur_version      = 0;
my $ntop             = $maxtop;
my $snapshot_key     = "";
my $snapshot_client  = "";

sub usage {
    print "Usage:\n";
    print "perl hotkey.pl [--ip ip] [--ports port0,port1...] [--interval n] [--maxtop n]\n";
    print "               [--capacity n] [--log log] [-d] [-h|--help] [-v|--version]\n";
    print "\t--ip:          ip address for connected server\n";
    print "\t--ports:       port lists, e.g: 11211,11212,11213, default will read all ports\n";
    print "\t--interval:    interval to dump snapshots, default is 1 (1 hour)\n";
    print "\t--maxtop:      maxtop to init hotkey, default is 300\n";
    print "\t--capacity:    capacity for holding snapshots, defult is 1 (1 GB)\n";
    print "\t--log:         hotkey service logs\n";
    print "\t-d:            debug\n";
    print "\t-h|--help:     help\n";
    print "\t-v|--version:  version\n";
    print "example: ./hotkey.pl --ip 172.16.13.26 --ports 111211,11212,11213 --interval 0.1  --capacity 1 --ntop 10000\n";
    exit;
}

sub show_version {
    print "schooner hotkey service\n";
    print "version 0.1\n";
}

sub parse_args {
    my $is_debug = 0;

    foreach my $t(0..$#ARGV) {
        if($ARGV[$t] eq '--ip') {
            $ip = $ARGV[$t+1];
        } elsif($ARGV[$t] eq '--ports') {
            my $all_ports = $ARGV[$t+1];
            @ports = split(/\s*,\s*/, $all_ports);
        } elsif($ARGV[$t] eq '--interval') {
            $interval = $ARGV[$t+1];
        } elsif($ARGV[$t] eq '--maxtop') {
            $maxtop = $ARGV[$t+1];
            $ntop   = $maxtop;
        } elsif($ARGV[$t] eq '--capacity') {
            $capacity = $ARGV[$t+1];
       } elsif($ARGV[$t] eq '-d') {
            $is_debug = 1;
        } elsif($ARGV[$t] eq '-v') {
            &show_version();
            exit;
        } elsif($ARGV[$t] eq '--version') {
            &show_version();
            exit;
        } elsif($ARGV[$t] eq '-h') {
            &usage();
        } elsif($ARGV[$t] eq '--help') {
            &usage();
        }
    }

    $versions = $capacity * 1024 * 1024 * 1024 / ($nctnr * 300 * $maxtop);
    my $vers = int( $versions / $nctnr );
    $versions = ($vers+1) * $nctnr;

    if ($is_debug == 1) {
        open (DEBUG, ">&STDOUT");
    } else {
        open (DEBUG, ">$LOG");
    }

    printf DEBUG "parameters:\n";
    printf DEBUG "interval=$interval(h), maxtop=$maxtop, capacity=$capacity(GB), log=$LOG\n";
    printf DEBUG "versions=$versions\n";
}

sub init_all_containers() {
    foreach (@ports) { 
        `echo \"schooner hotkey init $maxtop enable_ip_tracking enable_cmd_types\" | nc $ip $_ > /dev/null 2>&1`;
    }
}

sub enable_all_containers() {
    foreach (@ports) { 
        `echo \"schooner hotkey on\" | nc $ip $_ > /dev/null 2>&1 `;
    }
}

# rotoate snapshot if version is full and cycled
sub rotate_snapshots() {
    print DEBUG "\trotating snapshots: <$dump_snaps[$cur_version], $snapshot_key>\n";
    print DEBUG "\trotating snapshots: <$dump_snaps[$cur_version], $snapshot_client>\n";
    system " rm -rf $dump_snaps_key[$cur_version] ";
    system " rm -rf $dump_snaps_client[$cur_version] ";
    $dump_snaps_key[$cur_version]    = $snapshot_key;
    $dump_snaps_client[$cur_version] = $snapshot_client;
}

# stats all active containers through tcp_port
sub stat_all_containers() {

    my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = 
        localtime(time);
    my $date = localtime(time);
    $year   += 1900;
    $mon    = sprintf("%02d", 1+$mon);
    $mday   = sprintf("%02d", $mday);
    $hour   = sprintf("%02d", $hour);
    
    if ($#ports+1 == 0) {
        print DEBUG "[$date]:\nNo active container\n";
        return;   
    }

    printf DEBUG "[$date]:\nstats hotkey for ports @ports ... \n";

    for my $row_index (0..$#ctnr_list) {
        my $ctnr_name  = $ctnr_list[$row_index][0];
        my $ctnr_port  = $ctnr_list[$row_index][1];
        my $ctnr_id    = $ctnr_list[$row_index][2];
        my $ctnr_flag  = $ctnr_list[$row_index][3];
        my $ctnr_ip    = $ctnr_list[$row_index][4];

        if ($ctnr_flag eq "off") {
            print DEBUG "\tcontainer $ctnr_port is inactive, flag=$ctnr_flag\n";
            next;
        } else {
            print DEBUG "\tstart stats container through ip=$ctnr_ip port=$ctnr_port\n";
            print DEBUG "\tctnr_name=$ctnr_name, tcp_port=$ctnr_port, ctnr_id=$ctnr_id\n";
        }

        $snapshot_key = $DIR . "/snapshot_key_"; 
        $snapshot_key .= $ctnr_name . "_" . $ctnr_port . "_" . $ctnr_id . "_";
        $snapshot_key .= $year . $mon . $mday . "_";
        $snapshot_key .= $hour . "_" . $cur_version;

        $snapshot_client = $DIR . "/snapshot_client_"; 
        $snapshot_client .= $ctnr_name . "_" . $ctnr_port . "_" . $ctnr_id . "_";
        $snapshot_client .= $year . $mon . $mday . "_";
        $snapshot_client .= $hour . "_" . $cur_version;

        
        # stats hotkey/hotclient, and reset
        printf DEBUG "\tstat containers by port: $ctnr_port\n";

        system "echo \"stats hotkey $ntop\" | $schnc -w 120 $ctnr_ip $ctnr_port > $snapshot_key";
        printf DEBUG "\tstats hotkey done: $snapshot_key\n";

        system "echo \"stats hotclient $ntop\" | $schnc -w 120 $ctnr_ip $ctnr_port > $snapshot_client";
        printf DEBUG "\tstats hotclient done: $snapshot_key\n";

        system "echo \"schooner hotkey reset\" | $schnc -w 120 $ctnr_ip $ctnr_port 2>&1 > /dev/null";

        # we expect the key and client would be dumped 
        my $count = `wc -l $snapshot_key`;
        if ($count > 2) {
            rotate_snapshots();
            $cur_version = ++$cur_version % ($versions-1);
        } else {
            system "rm -rf $snapshot_key";
            system "rm -rf $snapshot_client";
            printf DEBUG "\thotkey dumped is NOT effective\n";
        }
     }

    printf DEBUG "stat containers done\n\n";
}


# retrive available container through "container list".
sub list_container_info() {
    my $ctnr_list_file = "container_list.tmp";
    my @port_list = ();
    my $nretry = 0;

    while (1) {
         system "echo \"container list\" | nc $ip $admin_port | grep -v END > $ctnr_list_file 2>&1";

         open LIST, "<$ctnr_list_file" or die "can not open $ctnr_list_file";
         @port_list = <LIST>;
         close LIST;

        `rm -rf $ctnr_list_file`;
  
         # judge whether server not startup or crashed
         if (defined($port_list[0]) && $port_list[0] =~ m/""/) {
             print DEBUG "server not startup or crashed: $port_list[0].\n";
             return;
        }

        if ($port_list[0] eq "SERVER_ERROR") {
            if (++$nretry > 20) {
                return;
            }
            print DEBUG "retring 'container list' for $nretry times...\n";
            sleep 10;
        } else {
           last;
        }
    }

    # clean last_ctnr_list and fill with cur_ctnr_list
    @last_ctnr_list = ();
    for my $i (0..$#ctnr_list) {
        $last_ctnr_list[$i] = $ctnr_list[$i];
    }

    # clean cur_ctnr_list and then fill with new info
    @ctnr_list = ();
    my %stats_head = ();
    foreach my $l (0..$#port_list) {
        $port_list[$l] =~ s/(^\s+|\s+$)//g;
        my @splits = split(/\s+/, $port_list[$l]);

        if ($l == 0) {
            # tcp_port udp_port eviction persistent size(MB)  status       id sasl name     IPs
            foreach my $c (0..$#splits) {
                $splits[$c] =~ s/(^\s+|\s+$)//g;
                $stats_head{$splits[$c]} = $c;
            }
        } else {
            # name + port + id + flag + ip
            my @ctnr_item = ();
            $splits[$stats_head{name}] =~ s/(^\s+|\s+$)//g;
            $ctnr_item[0] = $splits[$stats_head{name}];
            $splits[$stats_head{tcp_port}] =~ s/(^\s+|\s+$)//g;
            $ctnr_item[1] = $splits[$stats_head{tcp_port}];
            $splits[$stats_head{id}] =~ s/(^\s+|\s+$)//g;
            $ctnr_item[2] = $splits[$stats_head{id}];
            $splits[$stats_head{status}] =~ s/(^\s+|\s+$)//g;
            $ctnr_item[3] = $splits[$stats_head{status}];
            if (! defined($splits[$stats_head{IPs}])) {
                $ctnr_item[4] = $ip;
            } else {
                if ("0.0.0.0" =~ m/$splits[$stats_head{IPs}]/) {
                    $ctnr_item[4] = $ip;
                } else {
                    $splits[9] =~ s/(^\s+|\s+$)//g;
                    my @ips = split(/,/, $splits[$stats_head{IPs}]);
                    $ctnr_item[4] = $ips[0];
                }
            }
            push(@ctnr_list, [@ctnr_item]);
            push(@ports, $ctnr_item[1]);
        }
    }

    # print container list entries
    print DEBUG "\tlast_ctnr_list:\n";
    for my $i (0..$#last_ctnr_list) {
        my $ctnr_name  = $last_ctnr_list[$i][0];
        my $ctnr_port  = $last_ctnr_list[$i][1];
        my $ctnr_id    = $last_ctnr_list[$i][2];
        my $ctnr_flag  = $last_ctnr_list[$i][3];
        my $ctnr_ip    = $last_ctnr_list[$i][4];

        print DEBUG "\tctnr_name=$ctnr_name, ctnr_port=$ctnr_port, ctnr_id=$ctnr_id, ctnr_flag=$ctnr_flag, ctnr_ip=$ctnr_ip\n";
    }     
    
    # fill ports with effective ports
    @ports = ();
    print DEBUG "\tcur_ctnr_list:\n";
    for my $i (0..$#ctnr_list) {
        my $ctnr_name  = $ctnr_list[$i][0];
        my $ctnr_port  = $ctnr_list[$i][1];
        my $ctnr_id    = $ctnr_list[$i][2];
        my $ctnr_flag  = $ctnr_list[$i][3];
        my $ctnr_ip    = $ctnr_list[$i][4];

        print DEBUG "\tctnr_name=$ctnr_name, ctnr_port=$ctnr_port, ctnr_id=$ctnr_id, ctnr_flag=$ctnr_flag, ctnr_ip=$ctnr_ip\n";

        if ($ctnr_flag eq "on") {
            push(@ports, $ctnr_port)
        }
    } 
}

# delete the containers that not shown in the current container list.
# all the containers are listed with on/off.
sub delete_formatted_containers() {
    for my $i (0..$#last_ctnr_list) {
           
        my $exist = 0;
        for my $j (0..$#ctnr_list) {
            # ctnr_name, tcp_port, ctnr_id
            if ($last_ctnr_list[$i][0] eq $ctnr_list[$j][0] &&
                $last_ctnr_list[$i][1] == $ctnr_list[$j][1] &&
                $last_ctnr_list[$i][2] == $ctnr_list[$j][2]) {
                $exist = 1;
                last;
            }
        } 
    
        if ($exist == 0) {
            my $ctnr_name = $last_ctnr_list[$i][0];
            my $ctnr_port = $last_ctnr_list[$i][1];
            my $ctnr_id   = $last_ctnr_list[$i][2];
            my $ctnr_flag = $last_ctnr_list[$i][3];

            print DEBUG "\tcontainer need to be removed:\n";
            print DEBUG "\t\tname=$ctnr_name port=$ctnr_port id=$ctnr_id flag=$ctnr_flag\n";
            # pattern: *_name_port_id_*
            system "rm -rf `find $DIR -name \"*_${ctnr_name}_${ctnr_port}_${ctnr_id}_*\"`";
        }
    }
}

# get admin port during runtime
sub get_admin_port() {
    $admin_port = `cat $PROP | grep 'MEMCACHED_ADMIN_PORT' | awk '{print $1}'`;
    my @splits = split(/\s*=\s*/, $admin_port);
    $admin_port = $splits[1];
    $admin_port =~ s/(^\s+|\s+$)//g;
    print DEBUG "admin_port=[$admin_port]\n\n";
}

# parse args and loop to call stats
sub run_hotkey() {
    &parse_args();

    if (-d $DIR) {
        print DEBUG "$DIR already existed\n";
    } else {
        print DEBUG "mkdir failed\n" unless system  "mkdir -p $DIR";
    }

    foreach my $snap (@dump_snaps) {
        $snap = "";
    }

    $cur_version = 0;
    $interval *= 3600;

    while(1) {
        sleep $interval;

        if (! -e $PROP) {
            next;
        }

        if ($admin_port == 0) {
            &get_admin_port();
        }
        &list_container_info();
        &delete_formatted_containers();

        &stat_all_containers();
    }
}


use POSIX qw(setsid);

defined(my $pid = fork) or die "Cannot fork: $!";
unless ($pid) {
# Child process is here
   &run_hotkey();
}

# Parent process is here
#waitpid($pid, 0);

exit 0;

