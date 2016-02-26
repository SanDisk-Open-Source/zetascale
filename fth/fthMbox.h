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
