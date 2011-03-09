# $URL: svn://svn.schoonerinfotech.net/schooner-trunk/trunk/sdf/fth/fth.gdb $
# $Id: fth.gdb 14457 2010-08-04 06:55:49Z kcai $

define fth_all
    set $fba_thread = fth->allHead
    while ($fba_thread)
        print $fba_thread
        set $fba_thread = $fba_thread->nextAll
    end
end

document fth_all
    fth_all Print pointers to all fth threads into the value history
end

define backtrace_all
    thread apply all bt full
    fth_backtrace_all 20
end

document backtrace_all
    backtrace_all  Backtrace (depth 20) all known threads + fth threads
end

define fth_backtrace_all 
    set $fba_limit = $arg0

    set $fba_thread = fth->allHead
    while ($fba_thread)
	printf "---------------------------\n"
        printf "fthThread 0x%lx state %c\n", $fba_thread, $fba_thread->state
	# If it's running the information is stale so don't trace; user can
	# look at backtrace corresponding to the fthDummy(thread=thread) 
	# frame
	if ($fba_thread->state != 'R') 
	    fth_backtrace $fba_thread $fba_limit
	end
        set $fba_thread = $fba_thread->nextAll
    end
end

document fth_backtrace_all
    fth_backtrace_all <depth> Backtrace all fth stacks to depth
end

define fth_backtrace 
    set $fb_thread = $arg0
    set $fb_limit = $arg1

    set $fb_stack = $fb_thread->dispatch.stack
    set $fb_stack_end = (void *)((char *)$fb_stack + \
        $fb_thread->dispatch.stackSize)

    set $fb_i = 0
    set $fb_rbp = (void *)$fb_thread.dispatch.rbp
    set $fb_rip = $fb_thread.dispatch.pc

    while $fb_i < $fb_limit && $fb_stack <= $fb_rbp && $fb_rbp < $fb_stack_end
        printf "#%d frame 0x%ld ", $fb_i, $fb_rbp
        info line *$fb_rip

        set $fb_rip = ((unsigned long *)$fb_rbp)[1]
        set $fb_rbp = (void *)((unsigned long *)$fb_rbp)[0]

        set $fb_i = $fb_i + 1
    end
end

document fth_backtrace
    fth_backtrace <thread> <depth> Backtrace given fth thread to depth
end
