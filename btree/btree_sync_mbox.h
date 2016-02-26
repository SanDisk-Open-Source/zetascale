/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File:   btSyncMbox.h
 * Author: Jim
 *
 * Created on February 29, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http: //www.schoonerinfotech.com/
 *
 * $Id: btSyncMbox.h 396 2008-02-29 22:55:43Z jim $
 */

//
// Mailbox with locks

#ifndef _btSync_MBOX_H
#define _btSync_MBOX_H


#include <sys/time.h>
#include <sched.h>
#include <time.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <inttypes.h>
#include "btree_raw.h"


#include <pthread.h>
#define btSync_MBOX_STATS

//
// All mail and procs must have a lockListLockEl
//

struct mailLink;

typedef struct btSyncMbox {
    pthread_cond_t   mail_present_cv;
    pthread_mutex_t  mutex;
    struct mailLink *mail;
    struct mailLink *waiters;
    uint32_t         nmails;
    uint32_t         nwaiters;
    uint64_t         ndispatch;
    uint32_t         is_terminating;
} btSyncMbox_t;

void btSyncMboxInit(btSyncMbox_t *mb);

// Callable only from btSync thread
uint64_t btSyncMboxWait(btSyncMbox_t *mb);
uint64_t btSyncMboxTry(btSyncMbox_t *mb);
void btSyncMboxPost(btSyncMbox_t *mb, uint64_t mail);
void btSyncMboxTerm(btSyncMbox_t *mb);
uint32_t btSyncMboxDispatchCount(btSyncMbox_t *mb);

#endif
