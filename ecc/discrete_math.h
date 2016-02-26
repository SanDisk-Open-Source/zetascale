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
