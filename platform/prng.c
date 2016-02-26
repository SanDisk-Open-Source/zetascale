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
 * File:   sdf/platform/prng.h
 * Author: drew
 *
 * Created on March 31, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: prng.c 1073 2008-04-26 00:16:32Z drew $
 */

#include "platform/stdlib.h"
#include "platform/prng.h"

struct plat_prng {
    int64_t seed;
};

struct plat_prng *
plat_prng_alloc(int64_t seed) {
    struct plat_prng *ret;

    ret = plat_alloc(sizeof (*ret));
    if (ret) {
        ret->seed = seed;
    }

    return (ret);
}

void
plat_prng_seed(struct plat_prng *prng, int64_t seed) {
    int64_t prev;

    do {
        prev = prng->seed;
    } while (__sync_val_compare_and_swap(&prng->seed, prev, seed) != prev);
}

void
plat_prng_free(struct plat_prng *prng) {
    plat_free(prng);
}

int
plat_prng_next_bits(struct plat_prng *prng, int bits) {
    int64_t prev;
    int64_t next;

    plat_assert(bits > 0);
    plat_assert(bits <= 32);

    do {
        prev = prng->seed;
        next = (prev * 0x5deece66d + 0xb) & ((1LL <<  48) -1);
    } while (__sync_val_compare_and_swap(&prng->seed, prev, next) != prev);

    return ((int)(next >> (48 - bits)));
}

int
plat_prng_next_int(struct plat_prng *prng, int n) {
    int ret;
    int bits;

    /* n is a power of 2 */
    if ((n & -n) == n) {
        ret = (int)((n * (int64_t)plat_prng_next_bits(prng, 31)) >> 31);
    } else {
        do {
            bits = plat_prng_next_bits(prng, 31);
            ret = bits % n;
        } while (bits - ret + (n-1) < 0);
    }

    return (ret);
}
