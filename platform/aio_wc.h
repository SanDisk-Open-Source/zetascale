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

#ifndef PLATFORM_AIO_WC_H
#define PLATFORM_AIO_WC_H 1

/*
 * File:   $URL: svn://svn.schoonerinfotech.net/schooner-trunk/trunk/sdf/platform/aio_wc.h $
 * Author: drew
 *
 * Created on March 9, 2010
 *
 * (c) Copyright 2010, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: aio_wc.h 12398 2010-03-20 00:58:14Z drew $
 */

/**
 * #paio_api implementation which performs write combining
 *
 * Notes:
 * 1.  paio_wc is only usable from fthreads, except for api construction
 *     and destruction via #paio_wc_create and #paio_api_destroy.
 */

#include "platform/defs.h"

struct paio_api;

/** @brief Configuration structure */
struct paio_wc_config {
    /**
     * @brief Per file descriptor in-flight IO limit
     *
     * Since the goal is to keep the devices completely busy, both reads
     * and writes count.
     */
    int io_limit;

    /**
     * @brief Per file descriptor in-flight
     *
     * Since the goal is to keep the devices completely busy, both reads
     * and writes count.
     */
    int64_t byte_limit;

    /**
     * @brief Delay actual submit until getevents is firt called
     *
     * This is easy to implement and may allow for better write combining
     * in memcached
     */
    unsigned delay_submit_until_getevents : 1;
};

#define PAIO_WC_OPTS(field) \
    item("plat/aio/wc/io_limit",                                               \
         "write combining maximum number of write ios outstanding per fd",     \
         PLAT_AIO_WC_IO_LIMIT,                                                 \
         parse_int(&config->field.io_limit, optarg, NULL),                     \
         PLAT_OPTS_ARG_REQUIRED)                                               \
    item("plat/aio/wc/byte_limit",                                             \
         "write combining maximum total write bytes outstanding per fd",       \
         PLAT_AIO_WC_BYTE_LIMIT,                                               \
         parse_size(&config->field.byte_limit, optarg, NULL),                  \
         PLAT_OPTS_ARG_REQUIRED)                                               \
    item("plat/aio/wc/delay_submit",                                           \
         "delay submitting combined writes until getevents",                   \
         PLAT_AIO_WC_DELAY_SUBMIT,                                             \
         ({ config->field.delay_submit_until_getevents = 1; 0; }),             \
         PLAT_OPTS_ARG_NO)                                                     \
    item("plat/aio/wc/no_delay_submit",                                        \
         "do not delay submitting combined writes until getevents",            \
         PLAT_AIO_WC_NO_DELAY_SUBMIT,                                          \
         ({ config->field.delay_submit_until_getevents = 0; 0; }),             \
         PLAT_OPTS_ARG_NO)

__BEGIN_DECLS

/**
 * @brief Create a write-combining paio_api
 *
 * @param config <IN> Configuration which is not referenced after
 * function return, although config->api is referenced until
 * #paio_wc_config_destroy runs
 *
 * @return #paio_api which must be destroyed with #pai_api_destroy
 * with no pending IOs.
 */
struct paio_api *paio_wc_create(const struct paio_wc_config *config,
                                struct paio_api *wrapped_api);

/** @brief Set default configuration */
void paio_wc_config_init(struct paio_wc_config *config);

void paio_wc_config_destroy(struct paio_wc_config *config);

__END_DECLS


#endif /* ndef PLATFORM_AIO_WC_H */
