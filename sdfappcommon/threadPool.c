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

#include "sdfmsg/sdf_msg_types.h"
#include "sdfmsg/sdf_msg.h"
#include "common/sdftypes.h"
#include <stdio.h>
#include "fth/fthXMbox.h"
#include "sdfappcommon/threadPool.h"

uint64_t 
do_wait(waitObj_t * waitee) {

    uint64_t ret = 0;
    shmem_void_t shmp;
    struct sdf_msg  *   sdfmp;

    switch(waitee->waitType) {
    case PTOF_WAIT:
        shmp = ptofMboxWait(waitee->wu.ptof);
        ret = shmp.base.int_base;
        break;
    case FTOF_WAIT:
        ret = fthMboxWait(waitee->wu.ftof);
        break;
    case SDF_MSG_QUEUE_WAIT:
        sdfmp = sdf_msg_receive(waitee->wu.sdf_msg_queue);
        break;
    default:
        plat_assert(0 == 1);
    }
    return ret;
}

/* execMyFunc(struct threadPool *pool, thread_fn_t fn, arg) */
void
destroyThreadPool(struct threadPool * pool){(void)pool;};

void
holdingFn(uint64_t p)
{
    uint64_t ret;
    threadPool_t * pool = uint64_to_ptr_cast(p);
    printf("holding on one more thread \n");

    while(1) {
        ret = do_wait(pool->waitee_);
        pool->fn_(pool, ret);
    }
}

threadPool_t *
createThreadPool(thread_fn_t fn, uint64_t rock,
                 waitObj_t *waitee, uint32_t maxThreads, uint32_t stackSize)
{
    threadPool_t *pool =
        (threadPool_t *)plat_alloc(sizeof(*pool) + sizeof (pool->threads_[0]) * maxThreads);
    
    pool->waitee_ = waitee;
    pool->rock_ = rock;
    pool->fn_ = fn;
    pool->stackSize_ = stackSize;

    for(uint32_t ii=0; ii<maxThreads; ii++) {
        printf("spawning fthread number %d\n", ii);
        fthResume(pool->threads_[ii]=fthSpawn(holdingFn, stackSize), ptr_to_uint64_cast(pool));
    }
    return pool;
}
