#ifndef PLATFORM_ERROR_CONTROL_INTERNAL_H
#define PLATFORM_ERROR_CONTROL_INTERNAL_H 1

/*
 * File:   $URL: svn://svn.schoonerinfotech.net/schooner-trunk/trunk/sdf/platform/aio_error_control_internal.h $
 * Author: drew
 *
 * Created on March 8, 2010
 *
 * (c) Copyright 2010, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: aio_error_control_internal.h 13831 2010-05-25 02:41:49Z drew $
 */

/**
 * @brief Entry points for error control apis
 *
 * Instances may put a #paio_error_control structure in their state
 * and translate using #PLAT_DEPROJECT to type-pun.
 */
struct paio_error_control {
    /** @brief Set an error */
    int (*set_error)(struct paio_error_control *error_control,
                     enum paio_ec_type error_type, const char *filename,
                     off_t start, off_t len);
};

#endif /* ndef PLATFORM_ERROR_CONTROL_INTERNAL_H */
