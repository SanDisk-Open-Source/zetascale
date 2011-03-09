/*
 * File:   XResume.c
 * Author: Jim
 *
 * Created on February 29, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http:                                     //www.schoonerinfotech.com/
 *
 * $Id: XResume.c 396 2008-02-29 22:55:43Z jim $
 */

/**
 * @brief Cross-thread dispatching of FTH threads
 */

#include "XResume.h"
#include "fth/fth.h"
#include "fth/fthThread.h"
#include "fth/fthIdleControl.h"

#if 0                                        // Use non-shmem version
#include "platform/shmem.h"

// Instantiate the shmem stuff
PLAT_SP_IMPL(fthThread_sp, struct fthThread);
XLIST_SHMEM_IMPL(XResume, fthThread, nextShmem);
PLAT_SP_IMPL(ptofThreadPtrs_sp, struct ptofThreadPtrs);

#else

// Use a non-shmem version for now since there is no way to get a shemem fthThread pointer
#endif

/**
 * @brief - Cross-thread (but not cross-process) safe resume for fth thread
 *
 * @param <IN> thread pointer
 * @param <IN> argument to pass to thread (return val from fthWait call)
 */

void XResume(struct fthThread *thread, uint64_t arg) {
    plat_assert(thread->dispatchable == 0);
    // Check the state and atomically change it to make sure that the thread
    // is never put on the XResume queue twice.
    char threadState;
    do {
        threadState = thread->state;
        plat_assert((threadState == 'R') || (threadState == 'W') || (threadState == 'N'));
    } while (!__sync_bool_compare_and_swap(&thread->state, threadState, 'd'));
    thread->dispatch.arg = arg;
    ptofThreadPtrs_t *ptofResumePtrs = fthResumePtrs();
    XResume_xlist_enqueue(&ptofResumePtrs->head, &ptofResumePtrs->tail, thread);

#ifdef FTH_IDLE_CONTROL    
    struct fthIdleControl *idleControl;
    idleControl = fthIdleControlPtr();
    if (idleControl) {
        fthIdleControlPokeLocal(idleControl, 1);
    }
#endif    
}

/**
 * @brief - Wrapper around fthSpawn (fthSpawn could be called directly)
 */

struct fthThread *XSpawn(void (*startRoutine)(uint64_t), long minStackSize) {
    return(fthSpawn(startRoutine, minStackSize)); // Just pass things along
}

