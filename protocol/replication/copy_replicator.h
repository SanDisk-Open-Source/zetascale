/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

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
