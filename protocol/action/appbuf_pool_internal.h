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

/*
 * File:   appbuf_pool_internal.h
 * Author: Brian O'Krafka
 *
 * Created on March 3, 2009
 *
 * (c) Copyright 2009, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: appbuf_pool_internal.h 802 2008-03-29 00:44:48Z darpan $
 */

#ifndef _APPBUF_POOL_INTERNAL_H
#define _APPBUF_POOL_INTERNAL_H

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SDF_appBufState {
   SDF_appBufProps_t    props;
   uint64_t             nget;
   uint64_t             nfree;
} SDF_appBufState_t;

#ifdef	__cplusplus
}
#endif

#endif /* _APPBUF_POOL_INTERNAL_H */
