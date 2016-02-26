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

#ifndef PLATFORM_MMAN_H
#define PLATFORM_MMAN_H 1

/*
 * File:   platform/mman.h
 * Author: drew
 *
 * Created on January 25, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: mman.h 587 2008-03-14 00:13:21Z drew $
 */

/*
 * Thin wrappers for unix functions to accomodate interception for
 * 1) Blocking behavior in user-scheduled threads
 * 2) The simulated cluster environment
 */

#include <sys/cdefs.h>
#include <sys/mman.h>

#include "platform/types.h"
#include "platform/wrap.h"

#define PLAT_MMAN_WRAP_MMAN_ITEMS()                                            \
    item(void *, mmap, (void *addr, size_t len, int prot, int flags, int fd,   \
                        off_t offset), (addr, len, prot, flags, fd, offset),   \
         __THROW, /**/)                                                        \
    item(int, munmap, (void *addr, size_t len), (addr, len), __THROW, /**/)    \
    item(int, mprotect, (void *addr, size_t len, int prot), (addr, len, prot), \
         __THROW, /**/)

#define PLAT_MMAN_WRAP_ITEMS()                                                 \
    PLAT_MMAN_WRAP_MMAN_ITEMS()

__BEGIN_DECLS

#define __leaf__

#define item(ret, sym, declare, call, cppthrow, attributes) \
    PLAT_WRAP(ret, sym, declare, call, cppthrow, attributes)
PLAT_MMAN_WRAP_ITEMS()
#undef item

__END_DECLS

#endif /* ndef PLATFORM_MMAN_H */
