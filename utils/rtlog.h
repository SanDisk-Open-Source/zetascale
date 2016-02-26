/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 *  Light-Weight Logging
 */

#include <stdint.h>

#ifdef rt_enable_logging

extern void rtlog(const char *, uint64_t);
extern void rt_dump(void);
extern void rt_init(void);
extern void rt_set_size(int);

#else /* ! rt_enable_logging */

#define rtlog(...)
#define rt_dump(...)
#define rt_init(...)
#define rt_set_size(...)

#endif

#define rt_log(f, v)  rtlog(f, (uint64_t) v)
