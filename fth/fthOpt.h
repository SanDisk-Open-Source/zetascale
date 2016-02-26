/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File:   fthOpt.h
 * Author: drew
 *
 * Created on June 19, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http: //www.schoonerinfotech.com/
 *
 * $Id: fthOpt.h 8071 2009-06-20 23:33:41Z drew $
 */

#ifndef _FTH_OPT_H
#define _FTH_OPT_H

#include "platform/defs.h"
#include "misc/misc.h"

#include "fth.h"

#define PLAT_OPTS_FTH()                                                        \
    item("fth/idle_mode", "idle mode", FTH_IDLE_MODE,                          \
         fthParseIdleMode(optarg), PLAT_OPTS_ARG_REQUIRED)                     \
    item("fth/poll_interval", "busy poll interval", FTH_POLL_INTERVAL,         \
         parse_int(&fthConfig.busyPollInterval, optarg, NULL),                 \
         PLAT_OPTS_ARG_REQUIRED)                                               \
    item("fth/affinity_cpus", "set thread affinity", FTH_AFFINITY_CPUS,        \
         fthParseAffinityCpus(optarg), PLAT_OPTS_ARG_REQUIRED)                 \
    item("fth/affinity_cpu_mask", "set thread affinity using mask",            \
         FTH_AFFINITY_CPU_MASK, fthParseAffinityCpuMask(optarg),               \
         PLAT_OPTS_ARG_REQUIRED)                                               \
    item("fth/fake_clock", "use elapsed CPU time instead", FTH_FAKE_CLOCK,     \
         fthConfig.clockMode = FTH_CLOCK_FAKE, PLAT_OPTS_ARG_NO)

__BEGIN_DECLS
int fthParseIdleMode(const char *idleMode);
int fthParseAffinityCpus(const char *affinityCpus);
int fthParseAffinityCpuMask(const char *affinityCpus);
__END_DECLS

#endif /* ndef _FTH_OPT_H */
