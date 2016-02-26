/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

#ifndef PLATFORM_AIO_LIBAIO_H
#define PLATFORM_AIO_LIBAIO_H 1

/*
 * File:   $URL: svn://svn.schoonerinfotech.net/schooner-trunk/trunk/sdf/platform/aio_libaio.h $
 * Author: drew
 *
 * Created on March 9, 2010
 *
 * (c) Copyright 2010, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: aio_libaio.h 12359 2010-03-19 04:40:48Z drew $
 */

/**
 * #paio_api implementation using Linux libaio as a back-end
 * with the exception of io_context_t being replaced by struct
 * #paio_context *.
 */

#include "platform/defs.h"

struct paio_api;

/** @brief Configuration structure */
struct paio_libaio_config {
};

#define PAIO_LIBAIO_OPTS(field)

__BEGIN_DECLS

struct paio_api *paio_libaio_create(const struct paio_libaio_config *config);

/** @brief Set default configuration */
void paio_libaio_config_init(struct paio_libaio_config *config);

void paio_libaio_config_destroy(struct paio_libaio_config *config);

__END_DECLS


#endif /* ndef PLATFORM_AIO_LIBAIO_H */
