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

#ifndef REPLICATION_COPY_REPLICATOR_H
#define REPLICATION_COPY_REPLICATOR_H 1

/*
 * File:   sdf/protocol/replication/copy_replicator.h
 *
 * Author: drew
 *
 * Created on April 18, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: copy_replicator.h 1480 2008-06-05 09:23:13Z drew $
 */

/**
 * The copy replicator is a trivial replicator provided to test the
 * replication APIs and provide performance numbers.
 *
 * It replicates. No more, no less.  All of the messaging plumbing has been
 * implemented in libsdfmsg.a and replicator_adapter.c.
 */

#include "protocol/replication/replicator.h"

__BEGIN_DECLS

/**
 * @brief Construct a trivial copy replicator for testing
 *
 * @param config <IN> configuration.  This structure is copied.
 *
 * @param api <IN> interface to the real system (provided by either
 * #sdf_replicator_adapter or the test environment)
 */
struct sdf_replicator *
sdf_copy_replicator_alloc(const struct sdf_replicator_config *config,
                          struct sdf_replicator_api *api);

__END_DECLS

#endif /* ndef REPLICATION_COPY_REPLICATOR_H */
