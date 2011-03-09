#ifndef PLATFORM_ERRNO_H
#define PLATFORM_ERRNO_H 1

/*
 * File:   $HeaderURL:$
 * Author: drew
 *
 * Created on January 25, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: errno.h 2407 2008-07-25 13:22:32Z drew $
 */

/*
 * Thin wrappers for unix functions to accomodate interception for
 * 1) Blocking behavior in user-scheduled threads
 * 2) The simulated cluster environment
 */

#include <sys/cdefs.h>
#include <errno.h>

__BEGIN_DECLS

#define plat_errno errno


/*
 * FIXME: This needs to be a caps, description, value definition because
 * we're passing these across the wire as statuses.
 */
#define PLAT_ERRNO_ITEMS()                                                     \
    item(EEOF, "Unexpected EOF")                                               \
    item(EALREADYATTACHED, "Already attached")                                 \
    item(ENOTATTACHED, "Not attached")                                         \
    item(EBADMAGIC, "Bad magic number")                                        \
    item(EUNEXPECTEDMSG, "Unexpected message")                                 \
    item(ESHUTDOWN, "Resource shutdown")                                       \
    item(ENOPHYSMEM, "No physical memory")

enum plat_errors {
    PLAT_ERRNO_BASE = 1000,
#define item(caps, description) PLAT_ ## caps,
    PLAT_ERRNO_ITEMS()
#undef item
};

__END_DECLS

#endif /* ndef PLATFORM_ERRNO_H */
