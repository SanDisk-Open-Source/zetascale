/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

#ifndef PLATFORM_SELECT_H
#define PLATFORM_SELECT_H 1
/*
 * File:   platform/select.h
 * Author: drew
 *
 * Created on January 25, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: select.h 992 2008-04-18 17:11:06Z drew $
 */

/*
 * Thin wrappers for unix functions to accomodate interception for
 * 1) Blocking behavior in user-scheduled threads
 * 2) The simulated cluster environment
 */

#include <sys/select.h>

#include "platform/defs.h"
#include "platform/wrap.h"

#define PLAT_SELECT_WRAP_ITEMS()                                               \
    item(int, select,                                                          \
         (int __nfds, fd_set *__restrict __readfds,                            \
          fd_set *__restrict __writefds, fd_set *__restrict __exceptfds,       \
          struct timeval *__restrict __timeout),                               \
         (__nfds, __readfds, __writefds, __exceptfds, __timeout), /* */, /* */)

__BEGIN_DECLS
#define item(ret, sym, declare, call, cppthrow, attributes) \
    PLAT_WRAP(ret, sym, declare, call, cppthrow, attributes)
PLAT_SELECT_WRAP_ITEMS()
#undef item
__END_DECLS

#endif /* ndef PLATFORM_SELECT_H */
