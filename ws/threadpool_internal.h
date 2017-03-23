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
 *  threadpool_internal.h   8/29/16   Brian O'Krafka   
 *
 *  Simple threadpool data structure.
 *
 * (c) Copyright 2016  Western Digital Corporation
 *
 **********************************************************************/

#ifndef _THREADPOOL_INTERNAL_H
#define _THREADPOOL_INTERNAL_H

#include "threadpool.h"

struct thread_state;

typedef struct threadpool {
    int                    nthreads;
    pthread_t             *pthreads;
    struct thread_state   *thread_states;
    int                    quit;
    void                  *pdata;
} threadpool_t;

typedef struct thread_state {
    int                     myid;
    threadpool_argstuff_t   argstuff;
    threadpool_t           *thread_pool;
} thread_state_t;

#endif
