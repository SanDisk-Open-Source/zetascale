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
