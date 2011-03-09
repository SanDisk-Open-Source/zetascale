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

#include "fthWaitEl.h"
#include "fthThread.h"

#define FTH_MBOX_STATS

//
// All mail and procs must have a lockListLockEl
//

typedef struct fthMbox {
    fthSpinLock_t *spin;
#ifdef FTH_MBOX_STATS    
    uint32_t dispatchCount;
#endif
    uint32_t qSize;
    fthWaitQ_lll_t mailQ;
    fthThreadQ_lll_t threadQ;
} fthMbox_t;

void fthMboxInit(fthMbox_t *mb);

// Callable only from fth thread
uint64_t fthMboxWait(fthMbox_t *mb);
uint64_t fthMboxTry(fthMbox_t *mb);
void fthMboxPost(fthMbox_t *mb, uint64_t mail);
void fthMboxPostIfEmpty(fthMbox_t *mb, uint64_t mail);
void fthMboxTerm(fthMbox_t *mb);
uint32_t fthMboxDispatchCount(fthMbox_t *mb);

#endif
