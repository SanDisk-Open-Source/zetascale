/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

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
