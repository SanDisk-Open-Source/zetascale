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

/**********************************************************************
 *
 *  threadpool.h   8/29/16   Brian O'Krafka   
 *
 *  Simple threadpool data structure.
 *
 * (c) Copyright 2016  Western Digital Corporation
 *
 **********************************************************************/

#ifndef _THREADPOOL_H
#define _THREADPOOL_H

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <stdint.h>
#include <stdarg.h>
#include <inttypes.h>
#include <pthread.h>
#include <assert.h>
#include <time.h>
#include <string.h>

typedef struct threadpool_argstuff {
    struct threadpool *ptp;
    int                myid;
    void              *pdata;
} threadpool_argstuff_t;

typedef void *(threadpool_fn_t)(threadpool_argstuff_t *as);

typedef void (*dump_cb_t)(FILE *f, int n, void *pstuff);

struct threadpool;

struct threadpool *tp_init(int nthreads, threadpool_fn_t *fn, void *pdata);
int tp_shutdown(struct threadpool *ptp);
void tp_dump(FILE *f, struct threadpool *ptp, dump_cb_t dump_fn, void *pstuff);


#endif
