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

/*
 * File:   ecc_recover.c
 * Author: gshaw
 *
 * Created on August 13, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: $
 */

/*
 * References:
 *   See http://download.micron.com/pdf/technotes/nand/tn2905.pdf
 *       pages 15..18
 *
 */

#if defined(__GNUC__) && !defined(_lint)
#define USE_GCC_INLINE
#define USE_GCC_ASM
#else
#undef USE_GCC_INLINE
#undef USE_GCC_ASM
#endif

#undef ECC_DEBUG

#ifdef ECC_DEBUG
#include <stdio.h>
#endif

#ifndef __KERNEL__
#include <stddef.h>             // Import size_t, NULL, ...
#include <limits.h>             // Import CHAR_BIT
#endif

#include "ecc_recover.h"

/*
 * How many bytes are required to hold a given number of bits
 */
#define BITS_TO_BYTES(bits) (((bits) + ((CHAR_BIT) - 1)) / (CHAR_BIT))

#ifdef USE_GCC_ASM

static __inline__ uint_t
byte_parity(uint_t word)
{
    uint_t p;

    __asm__(
        " xorl  %%eax, %%eax\n"
        " orl   %1, %%eax\n"
        " setpo %%al\n"
        " movl  %%eax, %0\n"
        : "=r" (p) : "r" (word) : "%eax");

#if 0
    if (p != (p & 1)) {
        printf("p=%x\n", p);
        abort();
    }
#endif

    return (p);
}

#else /* def USE_GCC_ASM */

/*
 * Calculate nybble parity by table lookup.
 * Table is 16 entries of 1 bit each.
 * Relies on n being confined to the range 0..15.
 */
#define nybble_parity(n) ((0x6996 >> (n)) & 1)

/*
 * Calculate byte parity by "folding" (XOR) the upper and lower nybbles,
 * then calculating nybble parity of the folded byte.
 */
static uint_t
byte_parity(uint_t word)
{
    uint_t n;

    n = ((word >> 4) ^ word) & 0xf;
    return (nybble_parity(n));
}

#endif /* def USE_GCC_ASM */

/**
 * @brief How many bytes of ECC data are required for a given data size.
 *
 * @param size <IN> data size (should be a power of two)
 *
 */
size_t
ecc_size(size_t size)
{
    size_t ecc_bits;

    ecc_bits = 2 * log2_ceiling(size * CHAR_BIT);
    return (BITS_TO_BYTES(ecc_bits));
}

/*
 * p1_odd   0101_0101
 * p1_even  1010_1010
 * p2_odd   0011_0011
 * p2_even  1100_1100
 * p4_odd   0000_1111
 * p4_even  1111_0000
 */

#define p1_odd  0x55
#define p1_even ((~p1_odd) & 0xff)
#define p2_odd  0x33
#define p2_even ((~p2_odd) & 0xff)
#define p4_odd  0x0f
#define p4_even ((~p4_odd) & 0xff)

#define p1(b) ((byte_parity((b) & p1_odd)) | (byte_parity((b) & p1_even) << 1))
#define p2(b) ((byte_parity((b) & p2_odd)) | (byte_parity((b) & p2_even) << 1))
#define p4(b) ((byte_parity((b) & p4_odd)) | (byte_parity((b) & p4_even) << 1))

static uint_t
ecc_gen_byte(uint_t b)
{
    return (p1(b) | (p2(b) << 2) | (p4(b) << 4));
}

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

uint_t
ecc_gen_stripe(const void *data, size_t size, void *ecc_buf, size_t *ecc_sizep)
{
    size_t ecc_sz;
    uint_t ecc_bits, i, ecc, npairs;

    ecc_bits = 2 * log2_ceiling(size * CHAR_BIT);
    npairs = ecc_bits / 2;
    ecc = 0;
    for (i = 0; i < size; ++i) {
        uint_t c, byte_ecc, bpar, pair;

        c = ((unsigned char *)data)[i];
        byte_ecc = ecc_gen_byte(c);
        ecc ^= byte_ecc;
        bpar = byte_parity(c);
        if (bpar) {
            for (pair = 3; pair < npairs; ++pair) {
                uint_t mask;

                mask = 1U << (2 * pair);
                if (((i * 8) & (1U << pair)) != 0) {
                    ecc ^= mask << 1;
                } else {
                    ecc ^= mask;
                }
            }
        }
    }

    ecc_sz = BITS_TO_BYTES(ecc_bits);

    /*
     * Store ECC in memory, 1 byte at a time.
     * Be careful about endianness.
     */
    if (ecc_buf != NULL) {
        uint_t ecc_part;

        ecc_part = ecc;
        i = ecc_sz;
        for (i = 0; i < ecc_sz; i++) {
            ((unsigned char *)ecc_buf)[i] = ecc_part & 0xff;
            ecc_part >>= 8;
        }
    }

    /*
     * Store ECC size in bytes.
     */
    if (ecc_sizep != NULL) {
        *ecc_sizep = ecc_sz;
    }

    return (ecc);
}

void
ecc_gen(const void *data, size_t count, size_t size, void *ecc_buf, size_t *ecc_sizep)
{
    size_t ecc_sz;

    ecc_sz = 0;
    while (count > 0) {
        (void) ecc_gen_stripe(data, size, ecc_buf, &ecc_sz);
        data = (void *)((char *)data + size);
        ecc_buf = (void *)((char *)ecc_buf + ecc_sz);
        --count;
    }
    if (ecc_sizep != NULL) {
        *ecc_sizep = ecc_sz;
    }
}

/**
 * @param data <IN,OUT> block of data with possible ECC error
 * @param size <IN> size of data, in bytes
 * @param ecc_buf <IN> Error-Correction Code, bytes in memory
 * @return ecc_status -- how was ECC error handled?
 */
ecc_status_t
ecc_recover_stripe(void *data, size_t size, const void *ecc_buf)
{
    unsigned char *cdata;
    uint_t ecc, ecc2, diff, npairs, pair, loc, loc_byte, mask;
    uint_t ecc_bits, ecc_bytes;
    int i;

#if defined(_lint)
#endif

    /*
     * 'size' must be > 0 and a power of 2
     * We calculate ECC using 32-bit arithmetic;
     * ecc_bits == 2 * log2_ceiling(size * 8);
     * so size in bytes cannot exceed 2^28
     */
    if (size == 0 || (size & (size - 1)) != 0 || size > (1 << 28)) {
        return (ECC_BAD);
    }

    if (data == NULL || ecc_buf == NULL) {
        return (ECC_BAD);
    }

    ecc_bits = 2 * log2_ceiling(size * CHAR_BIT);
    ecc_bytes = BITS_TO_BYTES(ecc_bits);
    /*
     * Load ECC bytes from memory.
     * Need to be careful about endianness.
     */
    ecc = ((unsigned char *)ecc_buf)[ecc_bytes - 1];
    for (i = ecc_bytes - 2; i >= 0; i--) {
        ecc = (ecc << CHAR_BIT) + ((unsigned char *)ecc_buf)[i];
    }

    /*
     * The given ECC is that of the good data.
     * We need to generate our own ECC of the data in memory, for comparison.
     */
    ecc2 = ecc_gen_stripe(data, size, NULL, NULL);

    if (ecc2 == ecc) {
        return (ECC_NOERROR);
    }

    diff = ecc ^ ecc2;
    npairs = ecc_bits / 2;
#ifdef ECC_DEBUG
    printf("diff=%04x, ecc_bits=%u, npairs=%u\n", diff, ecc_bits, npairs);
#endif
    loc = 0;
    for (pair = 0; pair < npairs; ++pair) {
        uint_t pair_diff;

        pair_diff = diff & 0x3;
        if (pair_diff == 2) {
            loc |= (1U << pair);
        } else if (pair_diff == 1) {
            ;
        } else {
            return (ECC_MULTI);
        }
        diff >>= 2;
    }
    /*
     * Fix a 1-bit error
     */
    loc_byte = loc / CHAR_BIT;
    mask = 1U << (loc & 0x7);
#ifdef ECC_DEBUG
    printf("loc=%u, loc_byte = %u, mask = %02x\n", loc, loc_byte, mask);
#endif
    cdata = (unsigned char *)data;
    i = cdata[loc_byte];
    i ^= mask;
    cdata[loc_byte] = (unsigned char)i;
    return (ECC_SINGLE);
}

/**
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
ecc_status_t
ecc_recover(void *data, size_t count, size_t size, const void *ecc_buf, int stride)
{
    ecc_status_t status_all;
    ecc_status_t ret;

    status_all = ECC_NOERROR;
    while (count > 0) {
        ret = ecc_recover_stripe(data, size, ecc_buf);
        if (ret == ECC_BAD) {
            return (ret);
        }
        if (ret > status_all) {
            status_all = ret;
        }
        data = (void *)((char *)data + size);
        ecc_buf = (void *)((char *)ecc_buf + stride);
        --count;
    }
    return (status_all);
}
