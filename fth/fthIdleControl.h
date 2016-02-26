/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File:   fthIdleControl.h
 * Author: drew
 *
 * Created on June 19, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http: //www.schoonerinfotech.com/
 *
 * $Id: fthIdleControl.h 2957 2008-08-20 22:05:57Z drew $
 */

/**
 * Control fth idle behavior.
 *
 * For normal purposes fth schedulers running in pthreads spin on
 * incoming requests (threads made runnable via fthResume() and
 * ptofMbox mail).  This produces low latencies and is not a problem
 * in production systems with more cores than schedulers although
 * it does indicate 100% CPU utilization and skew flat profiler
 * measurements.
 *
 * On development machines with fewer cores than fth schedulers +
 * MPI loops (as with simulated clusters) unit tests run faster when
 * the schedulers don't prempt each other.  Kernel sleeps while in
 * an idle state also make flat profile reports meaningful.
 *
 * The fthIdleControl structure lives in shared memory so that it
 * is visible to other processes (clients like mysql) communicating
 * with fth threads (in sdfagent).
 */

#ifndef _FTH_IDLE_CONTROL_H
#define _FTH_IDLE_CONTROL_H

#include "platform/shmem.h"

struct timespec;

struct fthIdleControl;

PLAT_SP(fthIdleControl_sp, struct fthIdleControl);

/**
 * @brief Construct fthIdleControl
 *
 * The fthIdleControl is constructed in shared memory because it must be
 * visible to other processes using the ptofMbox_t.
 */
fthIdleControl_sp_t fthIdleControlAlloc();

/** @brief Free fthIdleControl */
void fthIdleControlFree(fthIdleControl_sp_t);

/** @brief Attach scheduler to idle control */
void fthIdleControlAttach(fthIdleControl_sp_t idleControl);

/** @brief Attach scheduler to idle control */
void fthIdleControlAttachLocal(struct fthIdleControl *idleControl);

/** @brief Detach scheduler to idle control */
void fthIdleControlDetach(fthIdleControl_sp_t idleControl);

/** @brief Attach scheduler to idle control */
void fthIdleControlDetachLocal(struct fthIdleControl *idleControl);

#include "fthIdleControl_c.h"

#endif /* ndef _FTH_IDLE_CONTROL_H */
