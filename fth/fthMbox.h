/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File:   fthMbox.h
 * Author: Jim
 *
 * Created on February 29, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http: //www.schoonerinfotech.com/
 *
 * $Id: fthMbox.h 396 2008-02-29 22:55:43Z jim $
 */

//
// Mailbox with locks

#ifndef _FTH_MBOX_H
#define _FTH_MBOX_H

#include "fth.h"
#include "fthSpinLock.h"

#define FTH_MBOX_STATS

//
// All mail and procs must have a lockListLockEl
//

struct mailLink;

typedef struct fthMbox {
    pthread_cond_t   mail_present_cv;
    pthread_mutex_t  mutex;
    struct mailLink *mail;
    struct mailLink *waiters;
    uint32_t         nmails;
    uint32_t         nwaiters;
    uint64_t         ndispatch;
    uint32_t         is_terminating;
} fthMbox_t;

void fthMboxInit(fthMbox_t *mb);

// Callable only from fth thread
uint64_t fthMboxWait(fthMbox_t *mb);
uint64_t fthMboxTry(fthMbox_t *mb);
void fthMboxPost(fthMbox_t *mb, uint64_t mail);
void fthMboxTerm(fthMbox_t *mb);
uint32_t fthMboxDispatchCount(fthMbox_t *mb);

#endif
