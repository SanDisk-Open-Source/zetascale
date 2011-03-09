#ifndef DISCRETE_MATH_H
#define DISCRETE_MATH_H 1

/*
 * File:   discrete_math.h
 * Author: Guy Shaw
 *
 * Created on August 29, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: $
 */

/**
 * @brief Round up to next power of 2.
 */
#define p2_roundup(n)                                                          \
    ({                                                                         \
        typeof(n) p2;                                                          \
        for (p2 = 1; p2 < (n); p2 *= 2) {                                      \
            /* Nothing */                                                      \
        }                                                                      \
        p2;                                                                    \
    })

/**
 * @brief ceiling log base 2 of n
 *
 * Smallest natural number, $k$, such that $2 sup k >= n$.
 */
#define log2_ceiling(n)                                                        \
    ({                                                                         \
        typeof(n) k;                                                           \
        typeof(n) p;                                                           \
        for (p = 1, k = 0; p < n; ++k) {                                       \
            p *= 2;                                                            \
        }                                                                      \
        k;                                                                     \
    })

#endif /* ndef DISCRETE_MATH_H */
