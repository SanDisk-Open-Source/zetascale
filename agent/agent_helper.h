/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File:   agent_helper.h
 * Author: Darpan Dinker
 *
 * Created on March 12, 2008, 12:23 AM
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: agent_helper.h 11701 2010-02-13 03:14:38Z drew $
 */

#ifndef _AGENT_HELPER_H
#define _AGENT_HELPER_H

#ifdef __cplusplus
extern "C" {
#endif

#include <sys/un.h>
#include "common/sdftypes.h"
#ifdef TEMP_MUTEX_SOLUTION
#include "shared/queue.h"
#elif  USE_F2P2F_XLOCK

#endif

/** Version of supported property file */
#define SDF_PROPERTY_FILE_VERSION   (1ULL)

/** Version of property file that is supported */
extern uint64_t SDFPropertyFileVersionSupported;

/** Version of property file that was loaded */
extern uint64_t SDFPropertyFileVersionFound;

/** Called from a server, may be called from a client (when processing inline) */
SDF_boolean_t init_containerCache(uint64_t maxSizeBytes);
SDF_boolean_t init_agent_sm(uint32_t rank);
/** @brief Internal flavor of #init_agent_sm() with more config opts */
SDF_boolean_t init_agent_sm_config(struct plat_shmem_config *shmem_config,
                                   int index);
/** Initialize using sdf/protocol/init_protocol.c */
SDF_boolean_t init_protocol_engine(uint32_t rank, SDF_boolean_t usingFth);

/** Report version of supported property file */
extern int property_file_report_version(char **bufp, int *lenp);

/**
 * @brief  Main initialization routine for SDF
 *
 * @return status, SDF_TRUE on success
 */

struct sdf_agent_state;
extern SDF_boolean_t sdf_init(struct sdf_agent_state *state, int argc, char *argv[]);
extern SDF_boolean_t agent_engine_pre_init(struct sdf_agent_state *state, int argc, char *argv[]);
extern SDF_boolean_t agent_engine_post_init(struct sdf_agent_state * state );

#ifdef __cplusplus
}
#endif

#endif /* _AGENT_HELPER_H */
