/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

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
