# File:   $URL: svn://svn.schoonerinfotech.net/schooner-trunk/trunk/sdf/protocol/replication/key_lock.gdb $
#
# Author: drew
#
# Created April 20, 2010
#
# (c) Copyright 2010, Schooner Information Technology, Inc.
# http://www.schoonerinfotech.com/
#
# $Id: key_lock.gdb 13074 2010-04-21 23:19:39Z drew $

# Debugging help for key_lock.c

define print_key_lock_container
    print *$arg0

    echo All locks\n
    set $pklc_lock = $arg0->all_list.tqh_first
    while $pklc_lock
	print_key_lock $pklc_lock 
	set $pklc_lock = $pklc_lock->all_list_entry.tqe_next
    end
end

document print_key_lock_container
    print a replicator_key_lock_container
end

define print_key_lock
    echo Key 
    # Valid part of key
    print *$arg0.key.key@$arg0.key.len
    print *$arg0
end

document print_key_lock
    print a replicator_key_lock structure
end

define find_key_lock
    set $fkl_container = $arg0
    set $fkl_key = $arg1
    set $fkl_found = (struct replicator_key_lock *)0

    set $fkl_lock = $fkl_container->all_list.tqh_first
    while !$fkl_found && $fkl_lock
	if $fkl_lock->key.len == $fkl_key->len
	    set $fkl_i = 0
	    while $fkl_i < $arg0->$fkl_key->len && $fkl_lock->key.key[$fkl_i] == $fkl_key->key[i]
		set $fkl_i = $fkl_i + 1
	    end
	    if $fkl_i == $fkl_key->len
		set $fkl_found = $fkl_lock
	    end
	end
	set $fkl_lock = $fkl_lock->all_list_entry.tqe_next
    end

    print $fkl_found
end

document find_key_lock
    find SDF_simple_key_t arg1 in replicator_key_lock_container arg0 
end

define validate_key_lock_container
    set $vklc_container = $arg0
    set $vklc_valid = 1
    set $vklc_all_count = 0

    set $vklc_lock = $vklc_container->all_list.tqh_first
    while $vklc_lock
	if $vklc_lock->key.len > sizeof ($vklc_lock->key.key)
	    print $vklc_lock
	    printf "Key length %d > max %d at %d\n", $vklc_lock->key.len, sizeof (vklc_lock->key.key), $vklc_all_count
	    set $vklc_valid = 0
	end
	# This is legal but interacted with a hashmap bug
	if $vklc_lock->key.len < sizeof ($vklc_lock->key.key) && $vklc_lock->key.key[$vklc_lock->key.len]
	    #print $vklc_lock
	    #printf "Key not NUL terminated\n"
	end
	if $vklc_lock->container != $vklc_container
	    print $vklc_lock
	    print "Container %p not %p at %d\n", $vklc->container, $vklc_all_count
	end
	set $vklc_all_count = $vklc_all_count + 1
	set $vklc_lock = $vklc_lock->all_list_entry.tqe_next
    end

    if $vklc_all_count > $vklc_container->all_list_count
	printf "Container all_list %d entries > all_list_count %d\n", $vklc_all_count, $vklc_container->all_list_count 
	set $vklc_valid = 0
    end

    print $vklc_valid
end

document validate_key_lock_container
    Validate a replication_key_lock_container ($=0 on failure, non-zero on OK)
end
