/*
 *  By default, the fth scheduler is now always multi-queue with a floating
 *  thread.  To go back to the old scheduler and compile-time options, set
 *  "-Duse_old_scheduler" in Makefile.defs.local:
 *
 *  LOCAL_CFLAGS += -Duse_old_scheduler
 */

#ifdef use_old_scheduler
#include "fth_std.c"
#else
#include "fth_float.c"
#endif
