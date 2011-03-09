/*
 * File:   fthIdleControl.h
 * Author: drew
 *
 * Created on June 19, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http: //www.schoonerinfotech.com/
 *
 * $Id: fthIdleControl.c 10527 2009-12-12 01:55:08Z drew $
 */

#define FTH_IDLE_CONTROL_C 1

#include <pthread.h>

#include "platform/assert.h"
#include "platform/defs.h"
#include "platform/errno.h"
#include "platform/logging.h"
#include "platform/string.h"
#include "platform/time.h"

#include "fthIdleControl.h"

PLAT_SP_IMPL(fthIdleControl_sp, struct fthIdleControl);

fthIdleControl_sp_t
fthIdleControlAlloc() {
    fthIdleControl_sp_t ret;
    int failed;
    struct fthIdleControl *ret_local = NULL;
    pthread_mutexattr_t lock_attr = {};
    pthread_condattr_t signal_attr = {};

    ret = plat_shmem_alloc(fthIdleControl_sp);
    failed = fthIdleControl_sp_is_null(ret);
    if (!failed) {
        fthIdleControl_sp_rwref(&ret_local, ret);
        memset(ret_local, 0, sizeof(*ret_local));

        failed = pthread_mutexattr_init(&lock_attr);
        plat_assert(!failed);
    }
    if (!failed) {
        failed = pthread_mutexattr_setpshared(&lock_attr, 1);
        plat_assert(!failed);
    }
    if (!failed) {
        failed = pthread_condattr_init(&signal_attr);
        plat_assert(!failed);
    }
    if (!failed) {
        failed = pthread_condattr_setpshared(&signal_attr, 1);
        plat_assert(!failed);
    }
    if (!failed) {
        failed = pthread_mutex_init(&ret_local->lock, &lock_attr);
        plat_assert(!failed);
    }
    if (!failed) {
        failed = pthread_cond_init(&ret_local->signal, &signal_attr);
        plat_assert(!failed);
    }
    if (failed && ret_local) {
        pthread_mutex_destroy(&ret_local->lock);
        pthread_cond_destroy(&ret_local->signal);

        fthIdleControl_sp_rwrelease(&ret_local);

        plat_shmem_free(fthIdleControl_sp, ret);
        ret = fthIdleControl_sp_null;
    }

    if (!failed) {
        plat_log_msg(20885, PLAT_LOG_CAT_FTH_IDLE, PLAT_LOG_LEVEL_DEBUG,
                     "fthIdleConrol " PLAT_SP_FMT " at %p allocated",
                     PLAT_SP_FMT_ARG(ret), ret_local);
        fthIdleControl_sp_rwrelease(&ret_local);
    } else {
        plat_log_msg(20886, PLAT_LOG_CAT_FTH_IDLE, PLAT_LOG_LEVEL_ERROR,
                     "fthIdleControlAlloc failed");
    }

    return (ret);
}

void
fthIdleControlFree(fthIdleControl_sp_t fthIdleControl) {
    struct fthIdleControl *local = NULL;
    int failed;

    if (!fthIdleControl_sp_is_null(fthIdleControl)) {
        fthIdleControl_sp_rwref(&local, fthIdleControl);

        plat_log_msg(20887, PLAT_LOG_CAT_FTH_IDLE, PLAT_LOG_LEVEL_DEBUG,
                     "fthIdleConrol " PLAT_SP_FMT " at %p free",
                     PLAT_SP_FMT_ARG(fthIdleControl), local);

        failed = pthread_mutex_destroy(&local->lock);
        plat_assert(!failed);
        failed = pthread_cond_destroy(&local->signal);
        plat_assert(!failed);

        fthIdleControl_sp_rwrelease(&local);

        plat_shmem_free(fthIdleControl_sp, fthIdleControl);

    }
}

void
fthIdleControlAttachLocal(struct fthIdleControl *local) {
    int status;

    if (local) {
        status = pthread_mutex_lock(&local->lock);
        plat_assert(!status);
        ++local->attached;
        ++local->ceiling;
        plat_log_msg(20888, PLAT_LOG_CAT_FTH_IDLE, PLAT_LOG_LEVEL_TRACE,
                     "fthIdleConrol at %p attach attached %d ceiling %d",
                     local, local->attached, local->ceiling);
        status = pthread_mutex_unlock(&local->lock);
        plat_assert(!status);
    }
}

void
fthIdleControlAttach(fthIdleControl_sp_t fthIdleControl) {
    struct fthIdleControl *local = NULL;

    if (!fthIdleControl_sp_is_null(fthIdleControl)) {
        fthIdleControl_sp_rwref(&local, fthIdleControl);
        fthIdleControlAttachLocal(local);
        fthIdleControl_sp_rwrelease(&local);
    }
}

void
fthIdleControlDetachLocal(struct fthIdleControl *local) {
    int status;
    if (local) {
        status = pthread_mutex_lock(&local->lock);
        plat_assert(!status);
        plat_assert(local->attached > 0);
        --local->attached;
        plat_log_msg(20889, PLAT_LOG_CAT_FTH_IDLE,
                     PLAT_LOG_LEVEL_TRACE,
                     "fthIdleConrol at %p detach attached %d ceiling %d",
                     local, local->attached, local->ceiling);

        if (!local->attached) {
            plat_log_msg(20890, PLAT_LOG_CAT_FTH_IDLE,
                         PLAT_LOG_LEVEL_DEBUG,
                         "fthIdleControl at %p no thread, total sleep %ld",
                         local, local->sleep_count);
        }

        status = pthread_mutex_unlock(&local->lock);
        plat_assert(!status);

        /* Wake another attached thread incase work remains */
        fthIdleControlPokeLocal(local, 1);
    }
}

void
fthIdleControlDetach(fthIdleControl_sp_t fthIdleControl) {
    struct fthIdleControl *local = NULL;

    if (!fthIdleControl_sp_is_null(fthIdleControl)) {
        fthIdleControl_sp_rwref(&local, fthIdleControl);
        fthIdleControlDetachLocal(local);
        fthIdleControl_sp_rwrelease(&local);
    }
}
