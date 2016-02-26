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

#ifndef PLATFORM_PRNG_H
#define PLATFORM_PRNG_H 1

/*
 * File:   sdf/platform/prng.h
 * Author: drew
 *
 * Created on March 31, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: prng.h 846 2008-04-01 18:55:07Z drew $
 */

/**
 * PRNG.
 *
 * We want a PRNG for test purposes which is not affected by user & library
 * (ex qsort pivot element selection) PRNG use for consistent hashing, testing,
 * etc.
 *
 * This PRNG is cloned from the Java docs which cite
 * Donald Knuth, The Art of Computer Programming, Volume 2, Section 3.2.1.
 */
#include "platform/defs.h"

struct plat_prng;

__BEGIN_DECLS

/** @brief Allocate PRNG */
struct plat_prng *plat_prng_alloc(int64_t seed);

/** @brief Free PRNG */
void plat_prng_free(struct plat_prng *prng);

/** @brief Reseed PRNG */
void plat_prng_seed(struct plat_prng *prng, int64_t seed);

/** @brief Return bits in [1, 32] random bits as an integer */
int plat_prng_next_bits(struct plat_prng *prng, int bits);

/** @brief Return uniformly distributed int in the range [0, n) */
int plat_prng_next_int(struct plat_prng *prng, int n);

__END_DECLS

#endif /* ndef PLATFORM_PRNG_H */
