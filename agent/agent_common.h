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
 * File:   agent_common.h
 * Author: Xiaonan Ma
 *
 * Created on Jul 11 17:47:24 PDT 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: agent_common.h 9970 2009-10-30 23:51:23Z briano $
 */

#ifndef __AGENT_COMMON_H__
#define	__AGENT_COMMON_H__

#ifdef SDFAPI
#include "api/sdf.h"
#else
#include "common/sdf_properties.h"
#endif /* SDFAPI */
#include "fth/fthOpt.h"
#include "agent_config.h"

#define MAX_THREADS 10
#define MAX_FLASH_DEVS 128

struct sdf_agent_state {
    struct plat_opts_config_sdf_agent config;

    /*
     * These are input-output parameters for the respective subsystem's
     * initializers
     * 
     * XXX: drew 2008-06-01 It would be more intuitive if these were 
     * inputs and the respective system state was stored directly 
     * in #sdf_agent_state
     */
    SDF_action_init_t ActionInitState;
    SDF_flash_init_t FlashInitState;
    SDF_async_puts_init_t AsyncPutsInitState;
    struct sdf_replicator_config ReplicationInitState;
    struct SDF_config ContainerInitState;
    flash_settings_t  flash_settings;
    uint32_t flash_dev_count;
#ifdef MULTIPLE_FLASH_DEV_ENABLED
    struct flashDev *flash_dev[MAX_FLASH_DEVS];
#else
    struct  flashDev *flash_dev;
#endif
	/*
	 * Allow or disallow an operation based on
	 * different dynamic parameters. 
	 */   
	ZS_operational_states_t op_access;
 
    pthread_t threads[MAX_THREADS];
    int numThreads;
    pthread_mutex_t threads_mutex;
    pthread_attr_t attr;
    uint32_t rank;
};

#endif  /* __AGENT_COMMON_H__ */
