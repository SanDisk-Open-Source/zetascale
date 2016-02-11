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
 * File:   ecc_test.c
 * Author: gshaw
 *
 * Created on August 13, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: ecc_test.c,v 1.1 2008/08/27 17:33:09 gshaw Exp gshaw $
 */

#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <limits.h>             // Import CHAR_BIT
#include <inttypes.h>           // Import PRI...

#include "misc/misc.h"          // Import parse_size()
/*
 * Macros for printing `size_t'
 *
 * Things that should have been defined in inttypes.h, but weren't
 */
#define __PRISIZE_PREFIX __PRIPTR_PREFIX
#define PRIuSIZE __PRISIZE_PREFIX "u"
#define PRIxSIZE __PRISIZE_PREFIX "x"
#define PRIXSIZE __PRISIZE_PREFIX "X"

#include "lfsr.h"
#include "ecc_recover.h"

#define ELEMENTS(var) (sizeof (var) / sizeof (*var))

#define eprintf(fmt, ...) \
    fprintf(stderr, fmt, ## __VA_ARGS__)

#define MAX_TEST_SIZE 8192

static  uint_t
flip_bit(unsigned char *buf, uint_t loc, uint_t mask)
{
    uint_t old_c, new_c;

    old_c = buf[loc];
    new_c = old_c ^ mask;
    if (new_c > UCHAR_MAX) {
        abort();
    }
    buf[loc] = (unsigned char)new_c;
    return (old_c);
}

// #define flip_bit(buf, index, mask) ((buf)[(index)] ^= (mask))

static unsigned char test_pat[MAX_TEST_SIZE];
static unsigned char test_buf[MAX_TEST_SIZE];
static unsigned char ecc_buf[128];    // Enough for 8 x uint64_t

static size_t max_test_size = MAX_TEST_SIZE;
static uint_t verbose;

#if defined(_lint)
//lint -esym(528, test_dummy) suppress "f() not referenced"
static void
test_dummy(void)
{
    (void) ecc_size(512);
}
#endif

/*
 * Test single-bit errors.
 *
 * Test ECC error recovery exhaustively.
 * Sweep through all sizes of data (powers of two).
 * For each test size, sweep through all possible single bit flips.
 * For each test case, ecc_recover should return ECC_SINGLE,
 * and the test buffer should compare equal to the original test data.
 */
static int
test_single(void)
{
    size_t test_size;
    uint_t byte, stride, mask;
    ecc_status_t ret;
    uint_t err;

    err = 0;
    for (test_size = 1; test_size <= max_test_size; test_size <<= 1) {
        stride = test_size >= 256 ? 253 : 1;
        if (verbose) {
            printf("@ test size = %4" PRIuSIZE "\n", test_size);
        }
        lfsr_fill(test_pat, test_size, 0, UCHAR_MAX);
        for (byte = 0; byte < test_size; byte += stride) {
            for (mask = 1; mask <= UCHAR_MAX; mask <<= 1) {
                memcpy(test_buf, test_pat, test_size);
                /*
                 * Get ECC value from ecc_buf, no need for return value.
                 */
                (void) ecc_gen_stripe(test_buf, test_size, ecc_buf, NULL);
                flip_bit(test_buf, byte, mask);
                ret = ecc_recover_stripe(test_buf, test_size, ecc_buf);
                if (ret != ECC_SINGLE) {
                    printf("Failed: status != ECC_SINGLE.\n");
                    err = 1;
                    return (-1);
                }
                if (memcmp(test_buf, test_pat, test_size) != 0) {
                    printf("Failed: compare.\n");
                    printf("test_size=%" PRIuSIZE ", byte=%u, mask=%02x\n",
                        test_size, byte, mask);
                    printf("test_pat = %02x, test_buf = %02x\n",
                        test_pat[byte], test_buf[byte]);
                    err = 1;
                    return (-2);
                }
            }
        }
    }

    if (err == 0) {
        if (verbose) {
            printf("PASSED\n");
        }
        return (0);
    }
    else {
        printf("FAILED\n");
        return (-1);
    }
}

/*
 * Test double bit errors.
 *
 * Test ECC error recovery exhaustively.
 * Sweep through all sizes of data (powers of two).
 * For each test size, sweep through all possible double bit flips.
 * For each test case, ecc_recover should return ECC_MULTI.
 * No attempt will be made to correct any of these errors.
 */
static int
test_double(void)
{
    size_t test_size, bit_size;
    uint_t bit_loc1, bit_loc2;
    uint_t byte_loc1, byte_loc2, stride, stride2, mask;
    unsigned char save_c1, save_c2;
    ecc_status_t ret;
    uint_t err;

    err = 0;
    for (test_size = 1; test_size <= max_test_size; test_size <<= 1) {
        stride = test_size >= 256 ? 253 : 1;
        if (verbose) {
            printf("@ test size = %4" PRIuSIZE "\n", test_size);
        }
        lfsr_fill(test_pat, test_size, 0, UCHAR_MAX);
        bit_size = test_size * CHAR_BIT;
        memcpy(test_buf, test_pat, test_size);
        for (bit_loc1 = 0; bit_loc1 < bit_size; bit_loc1 += stride) {
            stride2 = 1;
            for (bit_loc2 = bit_loc1 + 1; bit_loc2 < bit_size; bit_loc2 += stride2) {
                stride2 = (bit_loc2 > bit_loc1 + 256) ? 253 : 1;
                /*
                 * Get ECC value from ecc_buf, no need for return value.
                 */
                (void) ecc_gen_stripe(test_buf, test_size, ecc_buf, NULL);
                // Flip bit #bit_loc1 of test_buf
                byte_loc1 = bit_loc1 / CHAR_BIT;
                mask = 1U << (bit_loc1 - (byte_loc1 * CHAR_BIT));
                save_c1 = flip_bit(test_buf, byte_loc1, mask);
                // Flip bit #bit_loc2 of test_buf
                byte_loc2 = bit_loc2 / CHAR_BIT;
                mask = 1U << (bit_loc2 - (byte_loc2 * CHAR_BIT));
                save_c2 = flip_bit(test_buf, byte_loc2, mask);
                ret = ecc_recover_stripe(test_buf, test_size, ecc_buf);
                if (ret != ECC_MULTI) {
                    printf("Failed: status != ECC_MULTI.\n");
                    err = 1;
                    return (1);
                }
                /*
                 * Fix up test buffer for next test.
                 */
                test_buf[byte_loc1] = (unsigned char)save_c1;
                test_buf[byte_loc2] = (unsigned char)save_c2;
            }
        }
    }

    if (err == 0) {
        if (verbose) {
            printf("PASSED\n");
        }
        return (0);
    }
    else {
        printf("FAILED\n");
        return (-1);
    }
}

typedef int (*test_func_t)(void);

struct test_desc {
    char *       test_name;
    test_func_t  test_func;
};

static struct test_desc test_list[] = {
    { "single", test_single },
    { "double", test_double }
};

int
main(int argc, char * const *argv)
{
    uint_t test_nr;
    int ret;
    int64_t tmp_size;

    /*
     * Process command line options.
     */
    verbose = 0;
    for (;;) {
        ++argv;
        --argc;
        if (argc <= 0 || **argv != '-') {
            break;
        }
        if (strcmp(*argv, "-v") == 0) {
            verbose = 1;
        } else if (strcmp(*argv, "-max-test-size") == 0) {
            ++argv;
            --argc;
            ret = parse_size(&tmp_size, *argv, NULL);
            if (ret < 0) {
                eprintf("Invalid -max-test-size, '%s'\n", *argv);
                exit(1);
            }
            if (tmp_size < 0) {
                eprintf("Test size is negative.\n");
                eprintf("Test size must by in 1 .. %" PRIuSIZE ".\n",
                    (size_t)MAX_TEST_SIZE);
                exit(1);
            }
            if (tmp_size > MAX_TEST_SIZE) {
                eprintf("Test size cannot exceed %" PRIuSIZE ",\n"
                    "because ECC would require more than 32 bits.\n",
                    (size_t)MAX_TEST_SIZE);
                exit(1);
            }
            max_test_size = (size_t)tmp_size;
        } else {
            eprintf("Unknown option, '%s'\n", *argv);
            exit(1);
        }
    }

    max_test_size = p2_roundup(max_test_size);

    if (verbose) {
        uint_t i, c;

        lfsr_fill(test_pat, 64, 0, 64);
        for (i = 0; i < 64; ++i) {
            c = test_pat[i];
            test_pat[i] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789=/"[c];
        }
        test_pat[64] = '\0';
        printf("Visible Test Pattern\n  = '%s'\n", test_pat);
    }

    for (test_nr = 0; test_nr < ELEMENTS(test_list); ++test_nr) {
        test_func_t func;

        if (verbose) {
            printf("<%s>\n", test_list[test_nr].test_name);
        }
        func = test_list[test_nr].test_func;
        ret = (*func)();
        if (verbose) {
            printf("</%s>\n", test_list[test_nr].test_name);
        }
    
        if (ret < 0) {
            return (1);
        }
    }
    return (0);
}
