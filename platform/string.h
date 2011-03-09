#ifndef PLATFORM_STRING_H 
#define PLATFORM_STRING_H 1

/*
 * File:   platform/string.h
 * Author: drew
 *
 * Created on January 25, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: string.h 1611 2008-06-13 01:27:35Z drew $
 */

/*
 * Thin wrappers for unix functions to accomodate interception for
 * 1) Blocking behavior in user-scheduled threads
 * 2) The simulated cluster environment
 */

#include <sys/cdefs.h>
#include <sys/types.h>
#include <string.h>

#include "platform/wrap.h"

/* Type signatures changed so this is cleaner than the wrapping code */
#ifdef PLATFORM_INTERNAL
#define sys_strdup strdup
#define sys_strerror strerror
#define sys_strerror_r strerror_r
#endif

PLAT_WRAP_CPP_POISON(strerror strerror_r)

#ifndef PLATFORM_BYPASS
/* strdup is now a macro */ 
#undef strdup
#endif

PLAT_WRAP_CPP_POISON(strdup)

__BEGIN_DECLS

/* Re-entrant with POSIX.1-2001 semantics. */
int plat_strerror_r(int error, char *buf, int n);

/* Not-reentrant, but will become so for use in error logs */
const char *plat_strerror(int error);
char *plat_strdup(const char *string);

__END_DECLS

#endif /* ndef PLATFORM_SOCKET_H */
