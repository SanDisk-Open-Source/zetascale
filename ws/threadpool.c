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
 *  threadpool.c   8/29/16   Brian O'Krafka   
 *
 *  Simple threadpool data structure.
 *
 * (c) Copyright 2016  Western Digital Corporation
 *
 **********************************************************************/

#include "threadpool_internal.h"

static void errmsg(char *fmt, ...);

typedef void *(thrd_fn)(void *p);

threadpool_t *tp_init(int nthreads, threadpool_fn_t *fn, void *pdata)
{
    int              i;
    threadpool_t    *ptp;
    pthread_t       *pthreads;
    thread_state_t  *thread_states;
    thread_state_t  *pts;

    pthreads = (pthread_t *) malloc(nthreads*sizeof(pthread_t));
    if (pthreads == NULL) {
        return(NULL);
    }

    thread_states = (thread_state_t *) malloc(nthreads*sizeof(thread_state_t));
    if (pthreads == NULL) {
        free(pthreads);
        return(NULL);
    }

    ptp = (threadpool_t *) malloc(sizeof(threadpool_t));
    if (ptp == NULL) {
        return(NULL);
    }
    ptp->quit          = 0;
    ptp->pdata         = pdata;
    ptp->nthreads      = nthreads;
    ptp->pthreads      = pthreads;
    ptp->thread_states = thread_states;

    for (i = 0; i < nthreads; i++) {
        pts                 = &(thread_states[i]);
	pts->myid           = i;
	pts->thread_pool    = ptp;
	pts->argstuff.ptp   = ptp;
	pts->argstuff.myid  = i;
	pts->argstuff.pdata = pdata;

        if (pthread_create(&(pthreads[i]), NULL, (thrd_fn *) fn, (void *) &(pts->argstuff)) != 0) 
	{
	    errmsg("Failed to create pthread %d\n", i);
	    return(NULL);
	}
    }

    return(ptp);
}

int tp_shutdown(threadpool_t *ptp)
{
    int   i;
    int   ret=0;
    void *value_ptr;

    // Wait for all test threads to finish
    for (i=0; i<ptp->nthreads; i++) {
        if (pthread_join(ptp->pthreads[i], &value_ptr) != 0) {
	    errmsg("pthread_join failed for thread %d\n", i);
	    ret = 1;
	}
    }
    return(ret);
}

static void errmsg(char *fmt, ...)
{
   va_list  args;

   va_start(args, fmt);
   vfprintf(stderr, fmt, args);
   va_end(args);
}

