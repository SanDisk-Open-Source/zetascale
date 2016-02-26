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

#ifndef MISC_H
#define MISC_H 1

/*
 * File:   $HeadURL: svn://svn.schoonerinfotech.net/schooner-trunk/trunk/sdf/misc/misc.h $
 * Author: drew
 *
 * Created on January 25, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: misc.h 13945 2010-06-02 01:01:15Z drew $
 */

/*
 * Miscellaneous functions.
 */

#include <sys/cdefs.h>

#include "platform/types.h"

__BEGIN_DECLS

/**
 * Parse size.
 *
 * @param <OUT> out On success the parsed size is stored at *out.
 *
 * @param <IN> string The string parsed which may be decimal or hex
 * starting with 0x.  Values may have an optional suffix
 * (k, K, m, M, g, G, t, T, p, P) for 2^10, 20, 30, 40, and 50
 * respectively.
 *
 * @param <OUT> end_ptr_ptr This points after the last character parsed
 * @return 0 on sucess, -errno on error notably -ERANGE.
 */
int parse_size(int64_t *out, const char *string, const char **end_ptr_ptr);

/** @brief Return usage message for parse_size */
const char *parse_size_usage();

/**
 * Parse string.
 *
 * @param <OUT> out On success the string is stored at *out in
 * memory which can be released via #parse_string_free.  Any prior value is
 * passed to #parse_string_free
 *
 * @param <IN> string The string conditionally copied
 *
 * @param <IN> max_len  When > 0, max_len imposes a length
 * limit that causes longer strings to fail.  This is useful
 * to detect path length limits.
 *
 * @return 0 on success, -erno on error notably -ENAMETOOLONG
 */
int parse_string_alloc(char **out, const char *in, int max_len);

/**
 * @brief Free string returned by parse_string_alloc
 */
void parse_string_free(char *ptr);

/**
 * Parse string.
 *
 * @param out <OUT> On success the string is stored at #out in
 *
 * @param string <IN> The string conditionally copied
 *
 * @param max_len <IN>  Max_len imposes a length
 * limit that causes longer strings to fail.  This is useful
 * to detect path length limits.  It includes the terminating
 * null.
 *
 * @return 0 on success, -erno on error notably -ENAMETOOLONG
 */
int parse_string_helper(char *out, const char *in, int max_len);
#define parse_string(out, in) parse_string_helper((out), in, sizeof(out))

/*
 * Parse integer.
 *
 * @param <OUT> out On success the parsed integer is stored at *out.
 *
 * @param <IN> string The string being parsed.
 *
 * @param <OUT> end_ptr_ptr This points after the last character parsed
 *
 * @return 0 on sucess, -errno on error notably -ERANGE.
 */
int parse_int(int *out, const char *string, const char **end_ptr);

/**
 * Parse unsigned int
 *
 * @param <OUT> out On success the parsed integer is stored at *out.
 *
 * @param <IN> string The string being parsed.
 *
 * @param <OUT> end_ptr_ptr This points after the last character parsed
 *
 * @return 0 on sucess, -errno on error notably -ERANGE.
 */
int parse_uint(unsigned int *out, const char *string, const char **end_ptr);

/**
 * Parse unsigned int
 *
 * @param <OUT> out On success the parsed integer is stored at *out.
 *
 * @param <IN> string The string being parsed.
 *
 * @param <OUT> end_ptr_ptr This points after the last character parsed
 *
 * @return 0 on sucess, -errno on error notably -ERANGE.
 */
int parse_uint64(uint64_t *out, const char *string, const char **end_ptr);

/*
 * Parse a 32 bit integer.
 *
 * @param <OUT> out On success the parsed integer is stored at *out.
 *
 * @param <IN> string The string being parsed.
 *
 * @return 0 on success, -errno on error notably -EINVAL
 */
int parse_int32(int *ptr, const char *str);

/*
 * Parse time.
 *
 * @param <OUT> out The parsed time in seconds is stored in *out.
 *
 * @param <IN> string The string being parsed.
 *
 * @return 0 on success, -errno on error notably -EINVAL
 */
int parse_time(int64_t *timep, const char *string);

/**
 * Parse PRNG seed output by seed
 *
 * @param <IN> string The string being parsed.
 *
 * @return 0 on sucess, -errno on error notably -ERANGE.
 */
int parse_reseed(const char *string);

/**
 * Seed PRNG based on time, etc.
 *
 * @return 0 on success for consistency with other command line parsing code.
 */
int seed_arg();

/*
 * Return seed argument.
 */
unsigned get_seed_arg();

/**
 * Stop program so that debugger can attach before furthur argument
 * processing occurs.
 *
 * @return 0 on success
 */
int stop_arg();

/**
 * Parse timeout
 *
 * @param <IN> string The string being parsed.
 *
 * @return 0 on sucess, -errno on error notably -ERANGE.
 */
int parse_timeout(const char *string);

__END_DECLS

#endif /* ndef MISC_H */
