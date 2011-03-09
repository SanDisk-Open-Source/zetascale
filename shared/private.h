/*
 * File:   shared/private.h
 * Author: drew
 *
 * Created on June 16, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: private.h 8141 2009-06-25 00:23:53Z jbertoni $
 */

/**
 * Internal state of shared (container, name services, etc) subsystem.
 */
#ifndef _SHARED_PRIVATE_H
#define _SHARED_PRIVATE_H

#include "platform/defs.h"
#include "fth/fth.h"

#include "init_sdf.h"

/** @brief Subsystem local state */
struct SDF_shared_state {
    struct SDF_config config;
};

__BEGIN_DECLS

extern struct SDF_shared_state sdf_shared_state;
extern int (*sdf_agent_start_cb)(struct sdf_replicator *);

__END_DECLS

#endif /* _SHARED_H */
