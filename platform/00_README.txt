$Id: 00_README.txt 383 2008-02-28 18:42:07Z drew $

The platform directory contains functionality normally provided by the
operating system or 'C' library but is being replaced due to operational (user
scheduled threads are needed for performance) or test concerns (each simulated
node must have its own file and unix domain socket name spaces, each process
has its own memory allocation and file descriptors).  

The crash-consistent shared memory code is also here since scheduler ready
queues must be in shared memory so that user-scheduled threads in different
unix processes can wake each other up.

Some socket messaging code is here for shared memory client side to communicate
with the recovery/growth daemon.

The logging code is here because everything must log.

It may be more reasonable to split the supporting part of this code out 
and live with circular library use -lplatform -lshmem -lplatform or whatever
(this can even be transparent with shared libraries) if nothing else it
might isolate the HT simulator work from what's going on in Menlo Park.

Or to just split the supporting chaff into its own separate directory 
sys/whatever (this might work well - make the mkdir -p targets just do 
a mkdir -p $(dir $@).

All of the header files match their POSIX/ANSI counterparts and should be 
included instead of those from /usr/include.  The platform headers 
will hide anything that is emulated and return an error :

Compiling
    #include "platform/assert.h"

    int
    main() {
	assert(0);
    }

yields

    foo.c:5:5: error: attempt to use poisoned "assert"

and 
    #include "platform/unistd.h"
    int
    main() {
	close(0);
    }

yields

    POISONED(close)

without choking on 

    struct file_ops {
	int (*close)(struct file_ops *ops);
    };


Unwrapped string functions from platform/string.h just work.
