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
