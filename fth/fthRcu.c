/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File:   fthRcu.c
 * Author: Josh Dybnis
 *
 * Created on September 8, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http: //www.schoonerinfotech.com/
 *
 */

#include "platform/stdio.h"
#include "platform/string.h"
#include "platform/assert.h"
#include "platform/types.h"
#include "fth.h"
#include "fthTrace.h"

#define RCU_POST_THRESHOLD 1

#define RCU_QUEUE_SCALE 18
#define RCU_QUEUE_SIZE (1 << RCU_QUEUE_SCALE)
#define RCU_QUEUE_MASK (RCU_QUEUE_SIZE - 1)
#ifdef ENABLE_FTH_RCU

typedef struct rcuFifo {
    uint32_t head;
    uint32_t tail;
    uint32_t size;
    void *pending[0];
} rcuFifo_t;

static uint64_t rcu[MAX_SCHEDS][MAX_SCHEDS] = {};
static uint64_t rcuLastPosted[MAX_SCHEDS][MAX_SCHEDS] = {};
static rcuFifo_t *rcuPending[MAX_SCHEDS] = {};

static rcuFifo_t *rcuFifoAlloc(uint64_t size)
{
    rcuFifo_t *q = (rcuFifo_t *)malloc(sizeof(rcuFifo_t) + size*sizeof(void *)); 
    plat_assert(q);
    memset(q, 0, sizeof(rcuFifo_t));
    q->size = size;
    q->head = 0;
    q->tail = 0;
    return q;
}

static uint32_t rcuFifoIndex (rcuFifo_t *q, uint32_t i)
{
    return i % q->size;
}

static void rcuFifoEnq (rcuFifo_t *q, void *x)
{
    plat_assert(rcuFifoIndex(q, q->head + 1) != rcuFifoIndex(q, q->tail));
    uint32_t i = rcuFifoIndex(q, q->head++);
    q->pending[i] = x;
}

static void *rcuFifoDeq (rcuFifo_t *q)
{
    uint32_t i = rcuFifoIndex(q, q->tail++);
    return q->pending[i];
}

static void fthRcuPost (uint64_t x)
{
    if (x - rcuLastPosted[curSchedNum][curSchedNum] < RCU_POST_THRESHOLD)
        return;

    int nextSchedNum = (curSchedNum + 1) % schedNum;

    TRACE("r0", "fthRcuPost: %llu", x, 0);
    rcu[nextSchedNum][curSchedNum] = rcuLastPosted[curSchedNum][curSchedNum] = x;
}

static void fthRcuFreeDeferred (void)
{
    while (rcuPending[curSchedNum]->tail != rcu[curSchedNum][curSchedNum]) {
        void *x = rcuFifoDeq(rcuPending[curSchedNum]);
        plat_assert(x);
        plat_free(x);
    }
}

void fthRcuSchedInit (void)
{
    int i;
    for (i = 0; i < MAX_SCHEDS; ++i) {
        rcuPending[i] = rcuFifoAlloc(RCU_QUEUE_SIZE);
    }
}

void fthRcuUpdate (void)
{
    int nextSchedNum = (curSchedNum + 1) % schedNum;
    int i;
    for (i = 0; i < schedNum; ++i) {
        if (i == curSchedNum)
            continue;

        // No need to post an update if the value hasn't changed
        if (rcu[curSchedNum][i] == rcuLastPosted[curSchedNum][i])
            continue;

        uint64_t x = rcu[curSchedNum][i];
        rcu[nextSchedNum][i] = rcuLastPosted[curSchedNum][i] = x;
    }

    fthRcuFreeDeferred();
}

void fthRcuDeferFree (void *x)
{
    rcuFifoEnq(rcuPending[curSchedNum], x);
    TRACE("r0", "fthRcuDeferFree: put %p on queue at position %llu", x, rcuPending[curSchedNum]->head);
    fthRcuPost(rcuPending[curSchedNum]->head);
}
#endif
