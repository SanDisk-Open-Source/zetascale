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


