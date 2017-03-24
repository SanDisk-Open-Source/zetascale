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
 * File:   mbox.h
 * Author: Jim, ported to pthreads by Brian O'Krafka
 *
 * Created on February 29, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http: //www.schoonerinfotech.com/
 *
 * $Id: mbox.h 396 2008-02-29 22:55:43Z jim $
 */

//
// Mailbox with locks

#ifndef _MBOX_H
#define _MBOX_H

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <stdint.h>
#include <inttypes.h>
#include <pthread.h>
#include <assert.h>
#include <time.h>
#include <string.h>

struct mailLink;

typedef struct mbox {
    pthread_cond_t   mail_present_cv;
    pthread_mutex_t  mutex;
    struct mailLink *mail_first;
    struct mailLink *mail_last;
    struct mailLink *waiters;
    uint32_t         nmails;
    uint32_t         nwaiters;
    uint64_t         ndispatch;
    uint32_t         is_terminating;
} mbox_t;

typedef void (*dump_mail_cb_t)(FILE *f, int n, uint64_t mail, void *pstuff);

void mboxInit(mbox_t *mb);
uint64_t mboxWait(mbox_t *mb);
uint64_t mboxWaitBatch(mbox_t *mb, uint64_t *mails, uint64_t max_mails);
uint64_t mboxDump(mbox_t *mb, FILE *f, dump_mail_cb_t dump_fn, void *pstuff);
uint64_t mboxTry(mbox_t *mb);
void mboxPost(mbox_t *mb, uint64_t mail);
void mboxTerm(mbox_t *mb);
uint32_t mboxDispatchCount(mbox_t *mb);

#endif
