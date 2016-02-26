//----------------------------------------------------------------------------
// ZetaScale
// Copyright (c) 2016, SanDisk Corp. and/or all its affiliates.
//
// This program is free software; you can redistribute it and/or modify it under
// the terms of the GNU Lesser General Public License version 2.1 as published by the Free
// Software Foundation;
//
// This program is distributed in the hope that it will be useful, but WITHOUT
// ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
// FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License v2.1 for more details.
//
// A copy of the GNU Lesser General Public License v2.1 is provided with this package and
// can also be found at: http://opensource.org/licenses/LGPL-2.1
// You should have received a copy of the GNU Lesser General Public License along with
// this program; if not, write to the Free Software Foundation, Inc., 59 Temple
// Place, Suite 330, Boston, MA 02111-1307 USA.
//----------------------------------------------------------------------------

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
