# $URL: svn://svn.schoonerinfotech.net/schooner-trunk/trunk/sdf/protocol/replication/copy_replicator.gdb $
# $Id: copy_replicator.gdb 8126 2009-06-24 12:10:30Z drew $

define print_replica_meta
    print *$arg0

    set $i = 0
    while $i < $arg0->persistent.nrange
	print $arg0->ranges[$i]
	set $i = $i + 1
    end
end

document print_replica_meta
    print cr_shard_replica_meta
end

define print_shard
    print /x $arg0->sguid
    print $arg0->state
end

document print_shard
    print cr_shard
end

define print_cr
    set $shard = $arg0->shard_list.lh_first
    while $shard
	print $shard
	print_shard $shard
	set $shard = $shard->shard_list_entry.le_next
    end
end

document print_cr
    print copy_replicator
end
