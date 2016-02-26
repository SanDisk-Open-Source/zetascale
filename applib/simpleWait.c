/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

#include <errno.h>

#include "simpleWait.h"

void
ftopSimpleWait(ftopSimpleSignal_t * sig)
{
    int ret;

    do {
        ret = sem_wait(&sig->sem);
    } while (ret && (errno == EINTR || errno == EAGAIN));

    plat_assert(ret==0);
}

void
ftopSimplePost(ftopSimpleSignal_t * sig)
{
    sem_post(&sig->sem);
}

void
ftopSimpleSignalInit(ftopSimpleSignal_t * sig)
{
    plat_assert(sig);
    /* pshared =1 => good for IPC, value = 0  =>locked state) */
    int ret = sem_init(&sig->sem, 1, 0);
    plat_assert(ret==0);
}

void
ftopSimpleSignalDestroy(ftopSimpleSignal_t * sig)
{
    plat_assert(sig);
    int ret = sem_destroy(&sig->sem);
    plat_assert(ret==0);
}
