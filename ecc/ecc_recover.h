/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

#ifndef ECC_RECOVER_H
#define ECC_RECOVER_H 1

/*
 * File:   ecc_recover.h
 * Author: Guy Shaw
 *
 * Created on August 13, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: $
 */

#ifndef __KERNEL__
#include <stdint.h>	// Import uint_<n>_t, et al
#include <stddef.h>     // Import size_t
#endif

#include "discrete_math.h"

typedef unsigned int uint_t;

enum ecc_status {
    ECC_NOERROR = 0,
    ECC_SINGLE,
    ECC_MULTI,
    ECC_BAD
};
typedef enum ecc_status ecc_status_t;

/**
 * @brief Compute how many bytes of ECC data are required for a given data size.
 *
 * @param size <IN> data size (should be a power of two)
 *
 */
extern size_t
ecc_size(size_t size);

/**
 * @brief Generate ECC value for a single stripe of data
 * @param data <IN> start of data
 * @param size <IN> size in bytes of data (should be power of two)
 * @param ecc_buf <OUT> where to deposit ECC value in memory, optional
 * @param ecc_sizep <OUT> where to set size of ECC data in bytes, optional
 *
 * ecc_buf is optional because the return value is the 32-bit ECC,
 * but it may be convenient to have it returned in memory, because
 * the value placed in memory is only as many bytes as is needed,
 * which is what some hardware seems to deal with.
 *
 * ecc_sizep is optional, if it is non-NULL, then the size_t at that
 * location is set to the size in bytes of the ECC data stored in
 * ecc_buf.  More correctly, it is the size of the ECC data that
 * _would_ be stored in ecc_buf, but *ecc_sizep can be set independently,
 * even if ecc_buf is NULL.
 */
extern uint_t
ecc_gen_stripe(
    const void *data,
    size_t size,
    void *ecc_buf,
    size_t *ecc_sizep);

/**
 * @brief Generate ECC value(s) for a record, which may contain multiple
 * stripes.
 * @param data <IN> start of data
 * @param count <IN> number of stripes.
 * @param size <IN> size in bytes of each stripe (should be power of two)
 * @param ecc_buf <OUT> where to deposit ECC value in memory, optional
 * @param ecc_sizep <OUT> where to set size of ECC data in bytes, optional
 *
 * ecc_buf is optional because the return value is the 32-bit ECC,
 * but it may be convenient to have it returned in memory, because
 * the value placed in memory is only as many bytes as is needed,
 * which is what some hardware seems to deal with.
 *
 * ecc_sizep is optional, if it is non-NULL, then the size_t at that
 * location is set to the size in bytes of the ECC data stored in
 * ecc_buf.  More correctly, it is the size of the ECC data that
 * _would_ be stored in ecc_buf, but *ecc_sizep can be set independently,
 * even if ecc_buf is NULL.
 */
extern void
ecc_gen(
    const void *data,
    size_t count,
    size_t size,
    void *ecc_buf,
    size_t *ecc_sizep);

/**
 * @brief perform ECC recovery, if needed and if possible, on a single stripe.
 * @param data <IN,OUT> block of data with possible ECC error
 * @param size <IN> size of data, in bytes
 * @param ecc_buf <IN> Error-Correction Code, bytes in memory
 * @return ecc_status -- how was ECC error handled?
 */
extern ecc_status_t
ecc_recover_stripe(
    void *data,
    size_t size,
    const void *ecc);

/**
 * @brief perform error recovery, if needed and if possible,
 * on a record composed of possibly multiple stripes.
 * @param data <IN,OUT> block of data with possible ECC error
 * @param count <IN> how many blocks of data
 * @param size <IN> size of each block of data, in bytes
 * @param ecc_buf <IN> Error-Correction Code, bytes in memory
 * @param stride <IN> spacing between ECC for stripes
 * @return ecc_status -- how was ECC error handled?
 *
 * The ECC status of an array of independent blocks of data
 * is the maximum of the status of all the constituent blocks.
 */
extern ecc_status_t
ecc_recover(
    void *data,
    size_t count,
    size_t size,
    const void *ecc,
    int stride);

#endif /* ndef ECC_RECOVER_H */
