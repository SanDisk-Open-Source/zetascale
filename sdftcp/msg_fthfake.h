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
 * Author: Johann George.
 * (c) Copyright 2009, Schooner Information Technology, Inc.
 *
 * Attempt to run without FTH.
 */

#ifndef MSG_FTHFAKE_H
#define MSG_FTHFAKE_H

#define FTH_SPIN_INIT(a)    msgt_fake(a)
#define FTH_SPIN_LOCK(a)    msgt_fake(a)
#define FTH_SPIN_UNLOCK(a)  msgt_fake(a)
#define fthSelf()           0

typedef int fthThread_t;
typedef int fthSpinLock_t;

#endif /* MSG_FTHFAKE_H */
