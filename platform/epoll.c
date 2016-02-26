/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File:   epoll.c
 * Author: drew
 *
 * Created on April 14, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: epoll.c 992 2008-04-18 17:11:06Z drew $
 */

/*
 * Thin wrappers for unix functions to accomodate interception for
 * 1) Blocking behavior in user-scheduled threads
 * 2) The simulated cluster environment
 */

#define PLATFORM_INTERNAL 1

#include "platform/epoll.h"

#define item(ret, sym, declare, call, cppthrow, attributes) \
    PLAT_WRAP_IMPL(ret, sym, declare, call, cppthrow, attributes)
PLAT_EPOLL_WRAP_ITEMS()
#undef item
