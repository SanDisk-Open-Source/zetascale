/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

#ifndef FTH_IDLE_CONTROL_C_H
#define FTH_IDLE_CONTROL_C_H 1

/*
 * File:   fthIdleControl_c.h
 * Author: drew
 *
 * Created on August 7, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http: //www.schoonerinfotech.com/
 *
 * $Id: fthIdleControl_c.h 10527 2009-12-12 01:55:08Z drew $
 */

#include <pthread.h>

#include "platform/assert.h"
#include "platform/defs.h"
#include "platform/errno.h"
#include "platform/logging.h"
#include "platform/string.h"
#include "platform/time.h"

#include "fthIdleControl.h"

/**
 * @brief fthIdleControl implementation
 *
 * The idle controller attempts to be both independant of the current
 * fth scheduler implementation and conservative in when it puts schedulers
 * to sleep.
 *
 * It uses the ceiling field as a ceiling on the number of concurrent pthreads
 * which must run to handle scheduler events which have happened (
 * (ptofMbox mail which must be processe and threads made runnable via
 * fthResume() and ptofMbox mail).
 *
 * Each new scheduler event incerases the ceiling up to the number of attached
 * scheduler threads.  When each scheduler wants to sleep it sleeps on a zero
 * ceiling, otherwise decrementing the count and going through the loop once
 * more to check for events which have crept in.
 */

struct fthIdleControl {
    /** @brief Protects all fields; not held long */
    pthread_mutex_t lock;

    /** @brief Signal sleeping scheduler pthreads */
    pthread_cond_t signal;

    /** @brief min(Number times poked, attached) */
    int ceiling;

    /** @brief Number of scheduler loops attached */
    int attached;

    /** @brief Number of schedulers currently sleeping */
    int sleeping;

    /** @brief Number of times schedulers have gone to sleep */
    long sleep_count;
};

#endif /* def  FTH_IDLE_CONTROL_C_H */

#undef FTH_IDLE_CONTROL_INLINE

#if defined(FTH_IDLE_CONTROL_C) && defined(PLAT_NEED_OUT_OF_LINE)
#define FTH_IDLE_CONTROL_INLINE
#else 
#define FTH_IDLE_CONTROL_INLINE PLAT_INLINE
#endif

__BEGIN_DECLS


/**
 * @brief Wait until there's work to do
 * @param expires <IN> absolute time to unconditionally stop waiting.  NULL
 * for no timeout.
 * @param minSpin <IN> minimum number of schedulers which should be left
 * running to spin on memory waits.
 */
FTH_IDLE_CONTROL_INLINE void
fthIdleControlWaitLocal(struct fthIdleControl *fthIdleControl,
                        struct timespec *expires, int minSpin) {
    int done;
    int status;

    status = pthread_mutex_lock(&fthIdleControl->lock);
    plat_assert(!status);

    do {
        if (minSpin > 0 && minSpin + fthIdleControl->sleeping >= 
            fthIdleControl->attached) {
            done = 1;
        } else if (fthIdleControl->ceiling > 0) {
            --fthIdleControl->ceiling;
            done = 1;
        } else {
            ++fthIdleControl->sleeping;
            ++fthIdleControl->sleep_count;

            plat_log_msg(20891, PLAT_LOG_CAT_FTH_IDLE,
                         PLAT_LOG_LEVEL_TRACE,
                         "fthIdleConrol at %p wait sleeping"
                         " attached %d ceiling %d sleeping %d",
                         fthIdleControl, fthIdleControl->attached,
                         fthIdleControl->ceiling, fthIdleControl->sleeping);

            if (!expires) {
                status = pthread_cond_wait(&fthIdleControl->signal,
                                           &fthIdleControl->lock);
            } else {
                status = pthread_cond_timedwait(&fthIdleControl->signal,
                                                &fthIdleControl->lock,
                                                expires);
            }

            plat_assert(!status || status == ETIMEDOUT);
            done = (status == ETIMEDOUT);
            --fthIdleControl->sleeping;

            plat_log_msg(20892, PLAT_LOG_CAT_FTH_IDLE,
                         PLAT_LOG_LEVEL_TRACE,
                         "fthIdleConrol at %p wait awake"
                         " attached %d ceiling %d sleeping %d",
                         fthIdleControl, fthIdleControl->attached,
                         fthIdleControl->ceiling, fthIdleControl->sleeping);

        }
    } while (!done);

    plat_log_msg(20893, PLAT_LOG_CAT_FTH_IDLE,
                 PLAT_LOG_LEVEL_TRACE,
                 "fthIdleConrol at %p wait returning"
                 " attached %d ceiling %d sleeping %d",
                 fthIdleControl, fthIdleControl->attached,
                 fthIdleControl->ceiling, fthIdleControl->sleeping);

    pthread_mutex_unlock(&fthIdleControl->lock);
}

/**
 * @brief Wait until there's work to do
 *
 * @param expires <IN> absolute time to unconditionally stop waiting.  NULL
 * for no timeout.
 *
 * @param minSpin <IN> minimum number of schedulers which should be left
 * running to spin on memory waits.
 */
FTH_IDLE_CONTROL_INLINE void
fthIdleControlWait(fthIdleControl_sp_t fthIdleControl,
                   struct timespec *expires, int minSpin) {
    struct fthIdleControl *local = NULL;

    if (!fthIdleControl_sp_is_null(fthIdleControl)) {
        fthIdleControl_sp_rwref(&local, fthIdleControl);
        fthIdleControlWaitLocal(local, expires, minSpin);
        fthIdleControl_sp_rwrelease(&local);
    }
}

/**
 * @brief Suggest that the system is no longer idle
 * @param numEvents <IN> Number of events added
 */
FTH_IDLE_CONTROL_INLINE void
fthIdleControlPokeLocal(struct fthIdleControl *fthIdleControl,
                        int numEvents) {
    int status;

    if (fthIdleControl->ceiling < fthIdleControl->attached) {
        status = pthread_mutex_lock(&fthIdleControl->lock);
        plat_assert(!status);

        if (fthIdleControl->ceiling < fthIdleControl->attached) {
            fthIdleControl->ceiling = PLAT_MIN(fthIdleControl->ceiling +
                                               numEvents,
                                               fthIdleControl->attached);
            
            plat_log_msg(20894, PLAT_LOG_CAT_FTH_IDLE,
                         PLAT_LOG_LEVEL_TRACE,
                         "fthIdleConrol at %p poke numEvents %d"
                         " attached %d ceiling %d sleeping %d",
                         fthIdleControl, numEvents, fthIdleControl->attached,
                         fthIdleControl->ceiling, fthIdleControl->sleeping);

            if (fthIdleControl->sleeping) {
#ifdef notyet
                int need = fthIdleControl->ceiling - 
                        (fthIdleControl->attached - fthIdleControl->sleeping);
                for (status = 0; !status && need > 0; --need) {
                    status = pthread_cond_signal(&fthIdleControl->signal);
                }
#else
                status = pthread_cond_broadcast(&fthIdleControl->signal);
#endif
                plat_assert(!status);
            }
        }

        status = pthread_mutex_unlock(&fthIdleControl->lock);
        plat_assert(!status);
    }
}

/**
 * @brief Suggest that the system is no longer idle
 * @param numEvents <IN> Number of events added
 */

/*
 * The poke code increments the ceiling up to the number of schedulers
 * attached.
 */
FTH_IDLE_CONTROL_INLINE void
fthIdleControlPoke(fthIdleControl_sp_t fthIdleControl, int numEvents) {
    struct fthIdleControl *local = NULL;

    if (!fthIdleControl_sp_is_null(fthIdleControl)) {
        fthIdleControl_sp_rwref(&local, fthIdleControl);
        fthIdleControlPokeLocal(local, numEvents);
        fthIdleControl_sp_rwrelease(&local);
    }
}

__END_DECLS
