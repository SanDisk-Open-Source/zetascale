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

#ifndef PLATFORM_AIO_API_H
#define PLATFORM_AIO_API_H 1

/*
 * File:   $URL: svn://svn.schoonerinfotech.net/schooner-trunk/trunk/sdf/platform/aio_api.h $
 * Author: drew
 *
 * Created on March 8, 2010
 *
 * (c) Copyright 2010, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: aio_api.h 12398 2010-03-20 00:58:14Z drew $
 */

/**
 * API for asynchronous IO suppliers that looks like Linux libaio
 * with the exception of io_context_t being replaced by struct
 * #paio_context *.
 *
 * This allows implementations with additional features (write combining,
 * error injection for testing, in-core only for testing, etc. to be easily
 * substituted in production systems.
 */

#include <libaio.h>

#include "platform/defs.h"
#include "platform/types.h"

struct paio_api;
struct paio_context;

__BEGIN_DECLS

/*
 * FIXME: drew 2009-03-10 Track outstanding write count and total
 * and add an accessor so we can validate that write combining
 * is operating within user-specified constraints.
 */
void paio_api_destroy(struct paio_api *api);

/*
 * @return 0 on success, -1 on failure
 */
int paio_setup(struct paio_api *api, int maxevents, struct paio_context **ctxp);

/*
 * @return 0 on success, -1 on failure
 */
int paio_destroy(struct paio_context *ctx);

/*
 * @return number of submitted events, -1 on no submitted events with error
 */
int paio_submit(struct paio_context *ctx, long nr, struct iocb *ios[]);

/*
 * @return 0 on success, platform errno on failure
 */
int paio_cancel(struct paio_context *ctx, struct iocb *iocb,
                struct io_event *evt);

/**
 * @brief Return completed events
 *
 * @param min_nr <IN> When timeout is non-NULL, paio_wc_getents blocks until
 * min_nr events have been returned.
 * @param nr <IN> size of events_arg
 * @param timeout <INOUT> time remaining; NULL for no timeout; { 0, 0 }
 * to poll
 * @return Number of events returned, on on error with errno set
 */
long paio_getevents(struct paio_context *ctx_id, long min_nr, long nr,
                    struct io_event *events, struct timespec *timeout);

/** @return Number of writes not returned to #paio_getevents */
int paio_get_writes_inflight_count(struct paio_context *ctx);

/** @return  Total size of writes not returned to #paio_getevents */
long paio_get_writes_inflight_bytes(struct paio_context *ctx);

/** @return Number of writes returned to #paio_getevents */
int64_t paio_get_writes_completed_count(struct paio_context *ctx);

/** @return  Total size of writes returned to #paio_getevents */
int64_t paio_get_writes_completed_bytes(struct paio_context *ctx);

__END_DECLS


#endif /* ndef PLATFORM_AIO_API_H */
