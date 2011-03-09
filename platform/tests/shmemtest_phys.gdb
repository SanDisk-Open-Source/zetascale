# $Id: shmemtest_phys.gdb 2554 2008-08-01 05:54:18Z drew $
#
# An example on how to print structures in physical memory
define print_text
    set $text = $arg0

    get_phys $text
    set $good_header = (struct text *) $

    set $size = $good_header->data_len + sizeof(struct text)
    release_phys $good_header

    get_phys_var $text $size
    set $good_text = (struct text *) $
    print *$good_text

    release_phys $good_text
end

document print_text
    print_text <phys>

    Print text where the structure may be in physical memory.
end
