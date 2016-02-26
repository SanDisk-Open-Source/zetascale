/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * LFSR: Linear Feedback Shift Register
 *
 * Generate maximum length sequence.
 * Generate all numbers in given range, using a linear feedback shift register.
 *
 * Suppose you wanted to do visit every number that lies within a given
 * range, but in apparently random order.  Counting from lower bound to
 * upper bound will ensure that every number is visited, but it is
 * obviously not random enough.  Generating random numbers that lie within
 * range and selecting those that have not already been used is
 * inefficient.  It would require memory of those that have been used
 * already and time to look them up.  Primitive polynomials can be used to
 * generate a miximum length sequence for lengths of $2^n$.
 *
 * References:
 *   The Art of Computer Programming, Vol. II
 *   Seminumerical Algorithms
 *   by Donald E. Knuth
 *   Page 29
 * 
 *   Primitive Polynomials (Mod 2)
 *   by E. J. Watson
 *   Mathematics of Computation
 *   Vol XVI, Nos. 77-80, 1962
 *   Pages 368,369
 *
 */

#include <stddef.h>             // Import size_t

#include "discrete_math.h"      // Import p2_roundup(), log2_ceiling()

typedef unsigned int uint_t;

static int tap_table[] = {
    0,
    (1<<0),
    (1<<1) + (1<<0),
    (1<<2) + (1<<1),
    (1<<3) + (1<<2),
    (1<<4) + (1<<2),
    (1<<5) + (1<<4),
    (1<<6) + (1<<5),
    (1<<7) + (1<<5) + (1<<4) + (1<<3),
    (1<<8) + (1<<4),
    (1<<9) + (1<<6),
    (1<<10) + (1<<8),
    (1<<11) + (1<<10) + (1<<7) + (1<<5),
    (1<<12) + (1<<11) + (1<<9) + (1<<8),
    (1<<13) + (1<<12) + (1<<10) + (1<<8),
    (1<<14) + (1<<13),
    (1<<15) + (1<<13) + (1<<12) + (1<<10),
    (1<<16) + (1<<13),
    (1<<17) + (1<<16) + (1<<15) + (1<<12),
    (1<<18) + (1<<17) + (1<<16) + (1<<13),
    (1<<19) + (1<<16),
    (1<<20) + (1<<18),
    (1<<21) + (1<<20),
    (1<<22) + (1<<17),
    (1<<23) + (1<<22) + (1<<20) + (1<<19),
    (1<<24) + (1<<21),
    (1<<25) + (1<<24) + (1<<23) + (1<<19),
    (1<<26) + (1<<25) + (1<<24) + (1<<21),
    (1<<27) + (1<<24),
    (1<<28) + (1<<26),
    (1<<29) + (1<<28) + (1<<25) + (1<<23),
    (1<<30) + (1<<27),
};

/*
 * From Applied Cryptography: Protocols, Algoritms, and Source Code in C
 * by Bruce Schneier
 * ISBN: 0-471-11709-9
 * Edition: 2
 * Chapter 16: Pseodo-Random-Sequence generators
 * Section 16.2: Linear Feedback Shift Registers
 * Pages: 376,377
 * Table 16.2: Some Primitive Polynomials Mod 2
 *
 * 1, 0
 * 2, 1, 0
 * 3, 1, 0
 * 4, 1, 0
 * 5, 2, 0
 * 6, 1, 0
 * 7, 1, 0
 * 7, 3, 0
 * 8, 4, 3, 2, 0
 * 9, 4, 0
 * 10, 3, 0
 * 11, 2, 0
 * 12, 6, 4, 1, 0
 * 13, 4, 3, 1, 0
 * 14, 5, 3, 1, 0
 * 15, 1, 0
 * 16, 5, 3, 2, 0
 * 17, 3, 0
 * 17, 5, 0
 * 17, 6, 0
 * 18, 7, 0
 * 18, 5, 2, 1, 0
 * 19, 5, 2, 1, 0
 * 20, 3, 0
 * 21, 2, 0
 * 22, 1, 0
 * 23, 5, 0
 * 24, 4, 3, 1, 0
 * 25, 3, 0
 * 26, 6, 2, 1, 0
 * 27, 5, 2, 1, 0
 * 28, 3, 0
 * 29, 2, 0
 * 30, 6, 4, 1, 0
 * 31, 3, 0
 * 31, 6, 0
 * 31, 7, 0
 * 31, 13, 0
 * 32, 7, 6, 2, 0
 * 32, 7, 5, 3, 2, 1, 0
 * 33, 13, 0
 * 33, 16, 4, 1, 0
 * 34, 8, 4, 3, 0
 * 34, 7, 6, 5, 2, 1, 0
 * 35, 2, 0
 * 36, 11, 0
 * 36, 6, 5, 4, 2, 1, 0
 * 37, 6, 4, 1, 0
 * 37, 5, 4, 3, 2, 1, 0
 * 38, 6, 5, 1, 0
 * 39, 4, 0
 * 40, 5, 4, 3, 0
 * 41, 3, 0
 * 42, 7, 4, 3, 0
 * 42, 5, 4, 3, 2, 1, 0
 * 43, 6, 4, 3, 0
 * 44, 6, 5, 2, 0
 * 45, 4, 3, 1, 0
 * 46, 8, 7, 6, 0
 * 46, 8, 5, 3, 2, 1, 0
 * 47, 5, 0
 * 48, 9, 7, 4, 0
 * 48, 7, 5, 4, 2, 1, 0
 * 49, 9, 0
 * 49, 6, 5, 4, 0
 * 50, 4, 3, 2, 0
 * 51, 6, 3, 1, 0
 * 52, 3, 0
 * 53, 6, 2, 1, 0
 * 54, 8, 6, 3, 0
 * 54, 6, 5, 4, 3, 2, 0
 * 55, 24, 0
 * 55, 6, 2, 1, 0
 * 56, 7, 4, 2, 0
 * 57, 7, 0
 * 57, 5, 3, 2, 0
 * 58, 19, 0
 * 58, 6, 5, 1, 0
 * 59, 7, 4, 2, 0
 * 59, 6, 5, 4, 3, 1, 0
 * 60, 1, 0
 * 61, 5, 2, 1, 0
 * 62, 6, 5, 3, 0
 * 63, 1, 0
 * 64, 4, 3, 1, 0
 * 65, 18, 0
 * 65, 4, 3, 1, 0
 * 66, 9, 8, 6, 0
 * 66, 8, 6, 5, 3, 2, 0
 * 67, 5, 2, 1, 0
 * 68, 9, 0
 * 68, 7, 5, 1, 0
 * 69, 6, 5, 2, 0
 * 70, 5, 3, 1, 0
 * 71, 6, 0
 * 71, 5, 3, 1, 0
 * 72, 10, 9, 3, 0
 * 72, 6, 4, 3, 2, 1, 0
 * 73, 25, 0
 * 73, 4, 3, 2, 0
 * 74, 7, 4, 3, 0
 * 75, 6, 3, 1, 0
 * 76, 5, 4, 2, 0
 * 77, 6, 5, 2, 0
 * 78, 7, 2, 1, 0
 * 79, 9, 0
 * 79, 4, 3, 2, 0
 * 80, 9, 4, 2, 0
 * 80, 7, 5, 3, 2, 1, 0
 * 81, 4, 0
 * 82, 9, 6, 4, 0
 * 82, 8, 7, 6, 1, 0
 * 83, 7, 4, 2, 0
 * 84, 13, 0
 * 84, 8, 7, 5, 3, 1, 0
 * 85, 8, 2, 1, 0
 * 86, 6, 5, 2, 0
 * 87, 13, 0
 * 87, 7, 5, 1, 0
 * 88, 11, 9, 8, 0
 * 88, 8, 5, 4, 3, 1, 0
 * 89, 38, 0
 * 89, 51, 0
 * 89, 6, 5, 3, 0
 * 90, 5, 3, 2, 0
 * 91, 8, 5, 1, 0
 * 91, 7, 6, 5, 3, 2, 0
 * 92, 6, 5, 2, 0
 * 93, 2, 0
 * 94, 21, 0
 * 94, 6, 5, 1, 0
 * 95, 11, 0
 * 95, 6, 5, 4, 2, 1, 0
 * 96, 10, 9, 6, 0
 * 96, 7, 6, 4, 3, 2, 0
 */

/**
 * @brief Select a primitive polynomial to seed a LFSR suitable
 * for producing a sequence of the given length.
 *
 * A sequence of any length can be generated by using the polynomial
 * for the next power of 2, then ignoring excess generated values
 * that fall outside the range, 0 .. length - 1, inclusive.
 *
 */
uint_t
lfsr_seed(size_t len)
{
    return (tap_table[log2_ceiling(len)]);
}

/**
 * @brief Fill a buffer with characters generated by a maximal length LSFR.
 */
void
lfsr_fill(unsigned char *buf, size_t size, uint_t lbound, uint_t hbound)
{
    uint_t tap, n, prev_n, mask, k;

    mask = p2_roundup(hbound - lbound) - 1;
    n = tap = lfsr_seed(size);
    prev_n = 0;
    while (size > 0) {
        k = n & mask;
        if (lbound + k <= hbound) {
            *buf = (unsigned char)(lbound + k);
            ++buf;
            --size;
        }
        n /= 2;
        if (prev_n & 1) {
            n ^= tap;
        }
        prev_n = n;
    }
}
