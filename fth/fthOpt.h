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
