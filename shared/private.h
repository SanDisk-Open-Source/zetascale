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
