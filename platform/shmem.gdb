# $Id: shmem.gdb 2553 2008-08-01 05:39:14Z drew $

# Functions to help debug shmem programs

define get_phys
    set $ptr = $arg0

    set $size = sizeof *$ptr
    set $ret = plat_memdup_alloc($ptr, $size)
    print $ret
end

document get_phys
    get_phys <ptr>

    Copy the memory from a physical pointer to accessable memory.  The size
    of *ptr must be known.  Release with release_phys.
end

define get_phys_var
    set $ptr = $arg0
    set $size = $arg1

    set $ret = plat_memdup_alloc($ptr, $size)
    print $ret
end

document get_phys_var
    get_phys_var <ptr> <size>

    Copy the memory from physical pointer ptr ot accessable memory.
    Release with release_phys.
end

define release_phys
    set $ptr = $arg0
    call plat_memdup_free($ptr)
end

document release_phys
    release_phys <ptr>

    Release memory returned by get_phys
end
