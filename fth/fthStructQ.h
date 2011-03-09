/*
 * File:   fthStructQ.h
 * Author: Jim
 *
 * Created on February 29, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http: //www.schoonerinfotech.com/
 *
 * $Id: fthStructQ.h 396 2008-02-29 22:55:43Z jim $
 */

//
// Mailbox with locks

#ifndef _FTH_STRUCT_Q_H
#define _FTH_STRUCT_Q_H

#include "fthSpinLock.h"

#include "fthThread.h"

// Linked list definitions for wait elements
//
#undef LLL_NAME
#undef LLL_EL_TYPE
#undef LLL_EL_FIELD
#define LLL_NAME(suffix) fthDummyStructQ ## suffix
#define LLL_EL_TYPE struct fthDummyStruct
#define LLL_EL_FIELD waitQ

#include "fthlll.h"


//
// All mail and procs must have a lockListLockEl
//



typedef struct fthStructQ {
    fthSpinLock_t *spin;
    fthDummyStructQ_lll_t structQ;
    fthThreadQ_lll_t threadQ;
} fthStructQ_t;

// The following is a dummy that is type-cheated on top of the real struct
typedef struct fthDummyStruct {
    fthDummyStructQ_lll_el_t waitQ;
} fthDummyStruct_t;

void fthStructQInit(fthStructQ_t *sm);

// Callable only from fth thread
void * fthStructQWait(fthStructQ_t *sm);
void * fthStructQTry(fthStructQ_t *sm);
void fthStructQFree(fthStructQ_t *sm, void * ds);

#endif
