#ifndef PLATFORM_AIO_ERROR_CONTROL_H
#define PLATFORM_AIO_ERROR_CONTROL_H 1

#include "platform/closure.h"
#include "platform/defs.h"
#include "platform/types.h"

/*
 * File:   $URL: svn://svn.schoonerinfotech.net/schooner-trunk/trunk/sdf/platform/aio_error_control.h $
 * Author: drew
 *
 * Created on May 19, 2010
 *
 * (c) Copyright 2010, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: aio_error_control.h 13945 2010-06-02 01:01:15Z drew $
 */

/**
 * Error injection control API used by aio_error_bdb, etc.
 */

struct paio_error_control;

/** @brief Configuration structure */
struct paio_error_control_config {
};

#define PAIO_ERROR_CONTROL_OPTS(field)

#define PAIO_EC_TYPE_ITEMS() \
    /** @brief Read error, clears after next read in range  */ \
    item(PAIO_ECT_READ_ONCE, 0, read_once) \
    /** @brief Read error, clears when re-written */ \
    item(PAIO_ECT_READ_STICKY, 1, read_sticky) \
    /** @brief Error on read and write, clears after next read or write in range */ \
    item(PAIO_ECT_READ_WRITE_ONCE, 2, read_write_once) \
    /** @brief Permanant error on read and write, does not clear */ \
    item(PAIO_ECT_READ_WRITE_PERMANENT, 3, read_write_permanent)

enum paio_ec_type {
#define item(caps, value, lower) caps = value,
    PAIO_EC_TYPE_ITEMS()
#undef item
};

__BEGIN_DECLS

/**
 * @brief Set an error
 *
 * @param error_control <IN> paio_error_control returned from appropriate
 * paio_api create call like #paio_error_bdb_create.
 * @param filename <IN> name of file for error
 * @param error_type <IN> type of error
 * @param start <IN> error region start offset
 * @param len <IN> error region length
 * @return 0 on success, -errno on failure
 */
int paio_error_control_set_error(struct paio_error_control *error_control,
                                 enum paio_ec_type error_type,
                                 const char *filename,
                                 off_t start, off_t len);

/** @brief Initialize configuration defaults */
void paio_error_control_config_init(struct paio_error_control_config *config);

/**
 * @brief Destroy configuration fields
 *
 * @param config <IN> config structure owned by caller, fields consumed by
 * #paio_error_control_config_destroy
 */
void paio_error_control_config_destroy(struct paio_error_control_config *config);

/**
 * @brief Copy configuration
 *
 * @param dest <OUT> destination, which is un-initialized
 */
void paio_error_control_config_dup(struct paio_error_control_config *dest,
                                   const struct paio_error_control_config *src);

const char *paio_ec_type_to_string(enum paio_ec_type error);

/**
 * @brief parse error type from string
 * @return -errno on error, 0 on success
 */
int paio_parse_ec_type(enum paio_ec_type *out, const char *arg);

/** @brief Return usage string for #paio_ec_type parsing */
const char *paio_parse_ec_type_usage();


__END_DECLS

#endif /* ndef PLATFORM_AIO_ERROR_CONTROL_H */
