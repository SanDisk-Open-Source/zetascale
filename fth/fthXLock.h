/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File:   fthXLock.h
 * Author: Jim
 *
 * Created on February 29, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http: //www.schoonerinfotech.com/
 *
 * $Id: fthXLock.h 396 2008-02-29 22:55:43Z jim $
 */

//
//  X-threading (pthread/fth) lock
//

#ifndef _FTH_XLOCK_H
#define _FTH_XLOCK_H

#include "sdfappcommon/XLock.h"

// There are two definitions of this routine - one for pthread and one for fth.  Be sure to
// include the right one or all bets are off.
void fthXLockInit(XLock_t *cross);
void fthXLock(XLock_t *cross, int write);
int fthXTryLock(XLock_t *cross, int write);
void fthXUnlock(XLock_t *cross);

#endif
