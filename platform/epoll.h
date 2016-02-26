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

#ifndef PLATFORM_EPOLL_H
#define PLATFORM_EPOLL_H 1

/*
 * File:   $HeadURL: svn://svn.schoonerinfotech.net/schooner-trunk/trunk/sdf/platform/epoll.h $
 * Author: drew
 *
 * Created on April 14, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: epoll.h 992 2008-04-18 17:11:06Z drew $
 */

/*
 * Thin wrappers for unix functions to accomodate interception for
 * 1) Blocking behavior in user-scheduled threads
 * 2) The simulated cluster environment
 *
 * readlink can return int or ssize_t so it's not handled normally
 */
#include <sys/epoll.h>

#include "platform/defs.h"
#include "platform/types.h"
#include "platform/wrap.h"

#define PLAT_EPOLL_WRAP_ITEMS()                                                \
    item(int, epoll_create, (int size), (size), __THROW, /* */)                \
    item(int, epoll_ctl,                                                       \
         (int __epfd, int __op, int __fd, struct epoll_event *__event),        \
         (__epfd, __op, __fd, __event), __THROW, /* */)                        \
    item(int, epoll_wait,                                                      \
         (int __epfd, struct epoll_event *__events, int __max_events,          \
          int __timeout),                                                      \
         (__epfd, __events, __max_events, __timeout), /* */, /* */)

__BEGIN_DECLS

#define __leaf__

#define item(ret, sym, declare, call, cppthrow, attributes) \
    PLAT_WRAP(ret, sym, declare, call, cppthrow, attributes)
PLAT_EPOLL_WRAP_ITEMS()
#undef item

__END_DECLS

#endif /* ndef PLATFORM_EPOLL_H */
