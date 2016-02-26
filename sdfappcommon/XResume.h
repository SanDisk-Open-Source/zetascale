/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File:   XResume.h
 * Author: Jim
 *
 * Created on February 29, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http:                                     //www.schoonerinfotech.com/
 *
 * $Id: XMbox.h 396 2008-02-29 22:55:43Z jim $
 */

#ifndef _SDFAPPCOMMON_X_RESUME
#define _SDFAPPCOMMON_X_RESUME

/**
 * @brief Cross-thread mailbox.  This is the common portion (both threads)
 */

#include "fth/fth.h"
#include "fth/fthThread.h"

#include "XList.h"


struct ptofThreadPtrs;

// Use a non-shmem version for now since shmem-based fthThread pointers do not exist
typedef struct ptofThreadPtrs {
    struct fthThread *head;
    struct fthThread *tail;
} ptofThreadPtrs_t;

XLIST_H(XResume, fthThread, resumeNext);

void XResume(struct fthThread *thread, uint64_t arg);
struct fthThread *XSpawn(void (*startRoutine)(uint64_t), long minStackSize);

#endif
