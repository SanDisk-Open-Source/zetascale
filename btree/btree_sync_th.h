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

void btSyncReleasePthread();
btSyncThread_t *btSyncSpawn(btree_raw_t *bt, int index, void (*startRoutine)(uint64_t));
btSyncThread_t *btSyncSpawnPthread(int shutdown);
void btSyncResume(btSyncThread_t *thread, uint64_t rv);
#endif
