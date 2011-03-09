# Shared 
#
# File:   sdf/platform/shmem.h
# Author: drew
#
# Created on January 24, 2008
#
# (c) Copyright 2008, Schooner Information Technology, Inc.
# http://www.schoonerinfotech.com/
#
# $Id: shmem.h 4865 2008-12-06 03:56:04Z drew $

# Shared memory debugging tools for gdb

define print_shmem
    set $local_alloc = shmem_alloc_attached.local_alloc 

    # In the non physmem cases there's a single heap segment so this is a 
    # good approximation of what's free
    printf "heap\n"
    set $left = $local_alloc->end_of_segment.int_base - \
	$local_alloc->end_of_data.int_base
    printf "0x%lx bytes remain in segment\n", $left

    set $used_bytes = 0
    set $free_bytes = 0

    set $arena = (struct sa_arena *)$local_alloc->all_arena_list.head.base.ptr

    while ($arena)
	set $i = $local_alloc->min_bucket_bits

	while ($i < $local_alloc->max_bucket_bits)
	    set $bucket = &$arena->buckets[$i]

	    set $used_bytes = $used_bytes + $bucket->used_bytes

	    set $free_bytes = $free_bytes + $bucket->free_count * \
		((1 << $i) + sizeof (struct shmem_used_entry))

	    if ($bucket->end_of_segment.int_base > \
		    $bucket->end_of_data.int_base) 
		set $free_bytes = $free_bytes + \
		    ($bucket->end_of_segment.int_base - \
		    $bucket->end_of_data.int_base)
	    end

	    set $i = $i + 1
	end

	set $arena = (struct sa_arena *)$arena->global_list_entry.next.base.ptr
    end

    printf "0x%lx used bytes 0x%lx free bytes\n", $used_bytes, $free_bytes

    set $i = 0
    while ($i < PLAT_SHMEM_ARENA_COUNT)
	# This doesn't always work, with gdb claiming it can't deal with
	# thread local variables on this target
	# set $arena = sa_thread_local.local_arena_ptrs[$i]
	set $arena = (struct sa_arena *)$local_alloc->global_arenas[$i].base.ptr

	if ($arena) 
	    set $config = &$local_alloc->config.arena_config[$i]
	    set $limit = $config->used_limit

	    # Tree size is total sub-tree size
	    print (enum plat_shmem_arena)$i
	    printf "Tree size %ld of %ld\n", $arena->tree_size, $limit
	end
	set $i = $i + 1
    end
end

document print_shmem
    print_shmem

    Display shared memory statistics and allocation information
end
