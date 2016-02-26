/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File:   fthSchedType.h
 * Author: Jim
 *
 * Created on February 29, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http: //www.schoonerinfotech.com/
 *
 * $Id: fthSchedType.h 396 2008-02-29 22:55:43Z jim $
 */
#ifndef  _FTH_SCHED_TYPE_H
#define  _FTH_SCHED_TYPE_H

// Defines the type of stack control used by the scheduler.  Pick one.
#define fthAsmDispatch 1
//#define fthSetjmpLongjmp 1

#ifndef FTH_MAX_SCHEDS
// #define FTH_MAX_SCHEDS 16
#define FTH_MAX_SCHEDS 1
#endif

#endif
