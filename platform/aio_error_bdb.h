/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

#ifndef PLATFORM_AIO_ERROR_BDB_H
#define PLATFORM_AIO_ERROR_BDB_H 1

/*
 * File:   $URL: svn://svn.schoonerinfotech.net/schooner-trunk/trunk/sdf/platform/aio_error_bdb.h $
 * Author: drew
 *
 * Created on May 19, 2010
 *
 * (c) Copyright 2010, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: aio_error_bdb.h 13882 2010-05-27 02:46:51Z drew $
 */

/**
 * #paio_api implementation which performs error injection using
 * a Berkeley DB embedded database to store persistent read error
 * state.
 *
 * Notes:
 * 1.  paio_error_bdb is only usable from fthreads, except for api construction
 *     and destruction via #paio_wc_create and #paio_api_destroy.
 */

#include "platform/defs.h"

#include "platform/aio_error_control.h"

struct paio_api;

/** @brief Configuration structure */
struct paio_error_bdb_config {
    /** Root directory of bdb files */
    char *directory;

    /** Configuration for generic error injection */
    struct paio_error_control_config error_control_config;
};

#define PAIO_ERROR_BDB_OPTS(field) \
    item("plat/aio/error_bdb/directory",                                       \
         "directory used for bdb files",                                       \
         PLAT_AIO_ERROR_BDB_DIRECTORY,                                         \
         parse_string_alloc(&config->field.directory, optarg, PATH_MAX),       \
         PLAT_OPTS_ARG_REQUIRED)                                               \
    PAIO_ERROR_CONTROL_OPTS(field.error_control_config)

__BEGIN_DECLS

/**
 * @brief Create a error injection aio
 *
 * @param error_control <OUT> #paio_error_control usable with
 * #paio_error_control_set_error is returned which remains valid until
 * #paio_api_destroy is called.
 *
 * @param config <IN> Configuration which is not referenced after function
 * returns.
 * @param wrapped_api <IN> #paio_api being wrapped, with aio_libaio making
 * the most sense.
 * @return #paio_api which must be destroyed with #pai_api_destroy
 * with no pending IOs.
 */
struct paio_api *paio_error_bdb_create(struct paio_error_control **error_control,
                                       const struct paio_error_bdb_config *config,
                                       struct paio_api *wrapped_api);

/** @brief Set default configuration */
void paio_error_bdb_config_init(struct paio_error_bdb_config *config);

void paio_error_bdb_config_destroy(struct paio_error_bdb_config *config);

void paio_error_bdb_config_dup(struct paio_error_bdb_config *dest,
                               const struct paio_error_bdb_config *src);

__END_DECLS

#endif /* ndef PLATFORM_AIO_ERROR_BDB_H */
