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

#ifndef PLATFORM_STDIO_H
#define PLATFORM_STDIO_H 1

/*
 * File:   sdf/platform/stdio.h
 * Author: drew
 *
 * Created on February 22, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: stdio.h 8107 2009-06-24 00:19:33Z drew $
 */

/*
 * Thin wrappers for unix functions to accomodate interception for
 * 1) Blocking behavior in user-scheduled threads
 * 2) The simulated cluster environment
 */

#include <sys/cdefs.h>
#include <sys/types.h>

#include <stdarg.h>
#include <stdio.h>

#include "platform/wrap.h"

#ifdef PLATFORM_INTERNAL
#define sys_asprintf asprintf
#define sys_vasprintf vasprintf
#endif

PLAT_WRAP_CPP_POISON(asprintf vasprintf)

__BEGIN_DECLS
int plat_printf(FILE *sream, const char *fmt, ...);

int plat_asprintf(char **ptr, const char *fmt, ...) __attribute__((format(printf, 2, 3)));
int plat_vasprintf(char **ptr, const char *fmt, va_list ap);

/**
 * @brief Concatenate onto a string
 *
 * @param s <INOUT> Buffer to print into when non-NULL.  Updated on completion
 * to point after the existing data.
 * @param len <INOUT> Length remaining in buffer.  Updated on completion to
 * reflect length remaining.
 * @param fmt <IN> printf format
 */
int plat_snprintfcat(char **s, int *len, const char *fmt, ...) __attribute__((format(printf, 3, 4)));

__END_DECLS

#endif /* def PLATFORM_STDIO_H */
