/*
 * File:   fthSignal.h
 * Author: drew
 *
 * Created on March 25, 2009
 *
 * (c) Copyright 2009, Schooner Information Technology, Inc.
 * http: //www.schoonerinfotech.com/
 *
 * $Id: fthSignal.c 6472 2009-03-28 03:22:37Z drew $
 */

/**
 * Posix 1003 signal handling capabilities are insufficient to 
 * do anything significant because even system libraries
 * (such as stdio in libc) aren't re-entrant to say nothing
 * of user code.
 *
 * Standard practice is to run the actual signal handlers out 
 * of a thread which is merely awoken by an allowed mechanism.
 */

#undef _XOPEN_SOURCE
#define _XOPEN_SOURCE 600
#include <semaphore.h>

#include "platform/assert.h"
#include "platform/errno.h"
#include "platform/logging.h"
#include "platform/signal.h"

#include "sdfappcommon/XResume.h"

#include "fth.h"
#include "fthMbox.h"
#include "fthMem.h"

static void fthSignalMain(uint64_t val);
static void fthSignalHandler(int sig);

enum {
    /** @brief Internal simulated signal for shutdown */
    FTH_SIGNAL_SHUTDOWN = 63,

    /** @brief Stack size for user handlers */
    FTH_SIGNAL_STACK_SIZE = 40960
};


static struct {
    fthThread_t *thread;

    /** @brief memory queue allocated to wait on */
    int memQ;

    /** @brief whether a value has been allocated in a previous init */
    int memQInitialized;

    /** @brief Which signals are pending */
    uint64_t pendingSignals;

    /** @brief Functions to call on signals, NULL for none */
    sighandler_t handlers[64];

    /** @brief Saved handlers from runtime environment */
    struct {
        struct sigaction action;
        int valid;
    } savedHandlers[64];

    /** @brief Whether the worker thread needs initializing */
    int initialized;

    /** @brief How we're shutting down */
    enum {
        /** @brief Still running */
        FTH_SIGNAL_SHUTDOWN_NONE = 0,
        /** @brief Shutdown called from signal handler; no blocking */
        FTH_SIGNAL_SHUTDOWN_SIGNAL,
        /** @brief Shutdown called from other fthread; block on mbox */
        FTH_SIGNAL_SHUTDOWN_FTHREAD,
        /** @brief Shutdown called from pthread; block on semaphore */ 
        FTH_SIGNAL_SHUTDOWN_PTHREAD
    } shutdownMode;

    /** @brief FTH_SIGNAL_SHUTDOWN_FTHREAD blocks on this mbox */
    fthMbox_t shutdownMbox;

    /** @brief FTH_SIGNAL_SHUTDOWN_PTHREAD blocks on this semaphore */
    sem_t shutdownSemaphore;
} fthSignalState;

void
fthSignalInit() {

    if (__sync_bool_compare_and_swap(&fthSignalState.initialized, 0, 1)) {
        if (!fthSignalState.memQInitialized) {
            fthSignalState.memQ = fthMemQAlloc();
            fthSignalState.memQInitialized = 1;
        }

        plat_assert(!fthSignalState.thread);
        fthSignalState.thread = fthSpawn(fthSignalMain, FTH_SIGNAL_STACK_SIZE);

        XResume(fthSignalState.thread, 0 /* thread main arg */);
    }
}

void
fthSignalShutdown() {
    fthThread_t *self;
    int status;

    plat_assert(fthSignalState.initialized);
    plat_assert(fthSignalState.shutdownMode == FTH_SIGNAL_SHUTDOWN_NONE);

    self = fthSelf();
    if (self == fthSignalState.thread) {
        fthSignalState.shutdownMode = FTH_SIGNAL_SHUTDOWN_SIGNAL;
        __sync_fetch_and_or(&fthSignalState.pendingSignals,
                            1LL << FTH_SIGNAL_SHUTDOWN);
    } else if (self) {
        fthMboxInit(&fthSignalState.shutdownMbox);
        fthSignalState.shutdownMode = FTH_SIGNAL_SHUTDOWN_FTHREAD;
        __sync_fetch_and_or(&fthSignalState.pendingSignals,
                            1LL << FTH_SIGNAL_SHUTDOWN);
        fthMboxWait(&fthSignalState.shutdownMbox);
    } else {
        status = sem_init(&fthSignalState.shutdownSemaphore,
                          0 /* pshared */, 0 /* initial value */);
        plat_assert(!status);

        fthSignalState.shutdownMode = FTH_SIGNAL_SHUTDOWN_PTHREAD;
        __sync_fetch_and_or(&fthSignalState.pendingSignals,
                            1LL << FTH_SIGNAL_SHUTDOWN);

        do {
            status = sem_wait(&fthSignalState.shutdownSemaphore);
        } while (status == -1 && errno == EINTR);
        plat_assert(!status);

        status = sem_destroy(&fthSignalState.shutdownSemaphore);
        plat_assert(!status);
    }
}

sighandler_t
fthSignal(int signum, sighandler_t handler) {
    sighandler_t old;
    int status;
    struct sigaction newAction = {};

    plat_assert_always(signum < (sizeof (fthSignalState.handlers) / 
                                 sizeof (fthSignalState.handlers[0])));
    plat_assert_always(signum < FTH_SIGNAL_SHUTDOWN);

    fthSignalInit();

    old = fthSignalState.handlers[signum];

    fthSignalState.handlers[signum] = handler;

    if (handler == SIG_DFL) {
        if (fthSignalState.savedHandlers[signum].valid) {
            status = sigaction(signum,
                               &fthSignalState.savedHandlers[signum].action,
                               NULL /* old */);
            plat_assert(!status);
            fthSignalState.savedHandlers[signum].valid = 0;
        } 
    } else if (!old || old == SIG_DFL) {
        newAction.sa_handler = &fthSignalHandler;
        status = sigaction(signum, &newAction,
                           &fthSignalState.savedHandlers[signum].action);
        plat_assert(!status);
        fthSignalState.savedHandlers[signum].valid = 1;
    }

    return (old);
}

static void
fthSignalMain(uint64_t val) {
    uint64_t remain;
    int signum;
    int mask;

    do {
        fthMemWait(&fthSignalState.pendingSignals, fthSignalState.memQ);

        remain = fthSignalState.pendingSignals;
        __sync_fetch_and_and(&fthSignalState.pendingSignals, ~remain);

        for (signum = 0; remain; ++signum) {
            mask = 1 << signum;
            if (remain & mask) {
                if (fthSignalState.handlers[signum]) {
                    (*fthSignalState.handlers[signum])(signum);
                }
            }
            remain &= ~mask;
        }
    } while (fthSignalState.shutdownMode == FTH_SIGNAL_SHUTDOWN_NONE);

    fthSignalState.thread = NULL;
    fthSignalState.shutdownMode = FTH_SIGNAL_SHUTDOWN_NONE;
    fthSignalState.initialized = 0;
}


static void
fthSignalHandler(int signum) {
    __sync_fetch_and_or(&fthSignalState.pendingSignals, 1 << signum);
}
