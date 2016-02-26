/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * Author: Johann George.
 * (c) Copyright 2009, Schooner Information Technology, Inc.
 *
 * Attempt to run without FTH.
 */

#ifndef MSG_FTHFAKE_H
#define MSG_FTHFAKE_H

#define FTH_SPIN_INIT(a)    msgt_fake(a)
#define FTH_SPIN_LOCK(a)    msgt_fake(a)
#define FTH_SPIN_UNLOCK(a)  msgt_fake(a)
#define fthSelf()           0

typedef int fthThread_t;
typedef int fthSpinLock_t;

#endif /* MSG_FTHFAKE_H */
