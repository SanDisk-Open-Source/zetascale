/*
 * File:   btSync.h
 * Author: Jim
 *
 * Created on February 29, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http: //www.schoonerinfotech.com/
 *
 * $Id: btSync.h 396 2008-02-29 22:55:43Z jim $
 */

//
//  Featherweight threading structures
//

#ifndef _btSync_H
#define _btSync_H

#include <sys/time.h>
#include <sched.h>
#include <time.h>
#include <stdio.h>
#include <pthread.h>
#include "btree_raw_internal.h"
#if 0
#include "platform/defs.h"
#include "platform/types.h"
#include "platform/stdlib.h"
#endif

#define btSync_TIME_STATS                       // Collect time stats
#define btSync_TIME_MIN_MAX
#if 0
typedef struct btSyncThread {
    uint32_t          id;
    pthread_t         pthread;
    void            (*startfn)(uint64_t arg);
    pthread_mutex_t   mutex;
    pthread_cond_t    condvar;
    uint64_t          rv_wait;
    uint32_t          is_waiting;
    uint32_t          do_resume;
    struct btSyncThread *next;
    struct btSyncThread *prev;

    /*  For Drew's screwy per-btSyncread local state used by
     *  platform/attr.[ch] and his replication code.
     */
    //struct plat_attr_uthread_specific *local; // Uthread local storage

} btSyncThread_t;
#endif
typedef struct btsyncArgs {
	//btree_raw_t 		*btree;
	int			index;
}btSyncArgs_t;

void btSyncReleasePthread();
btSyncThread_t *btSyncSpawn(void *bt, int index, void (*startRoutine)(uint64_t));
btSyncThread_t *btSyncSpawnPthread(int shutdown);
void btSyncResume(btSyncThread_t *thread, uint64_t rv);
#endif
