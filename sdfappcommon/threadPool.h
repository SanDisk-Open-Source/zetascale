/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

#include "fth/fth.h"
#include "fth/fthThread.h"
#include "fth/fthMbox.h"
#include "sdfappcommon/XMbox.h"
#include "applib/XMbox.h"
#include "sys/cdefs.h"

struct threadPool;
typedef  struct threadPool threadPool_t;

typedef void (*thread_fn_t)(threadPool_t * rock, uint64_t mail);

struct threadPool {
    uint32_t numThreads_;
    uint32_t stackSize_;
    struct waitObj *waitee_;
    thread_fn_t fn_;
    uint64_t rock_;
    fthThread_t *threads_[0];
};

typedef enum {
    PTOF_WAIT, 
    FTOF_WAIT,
    SDF_MSG_QUEUE_WAIT
} waitType_t;

struct sdf_queue;
typedef struct waitObj {
    waitType_t  waitType;
    union {
        ptofMbox_t *ptof;
        fthMbox_t  *ftof;
        struct sdf_queue *sdf_msg_queue;
    } wu;
} waitObj_t;


threadPool_t *  createThreadPool(thread_fn_t fn, uint64_t rock,
                                 struct waitObj *waitee, uint32_t maxThreads,
                                 uint32_t stackSize);

uint64_t  do_wait(waitObj_t * waitee);


