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
 * File:   fthXLock.h
 * Author: Jim
 *
 * Created on February 29, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http: //www.schoonerinfotech.com/
 *
 * $Id: fthXLock.h 396 2008-02-29 22:55:43Z jim $
 */

//
//  X-threading (pthread/fth) lock
//

#ifndef _FTH_XLOCK_H
#define _FTH_XLOCK_H

#include "sdfappcommon/XLock.h"

// There are two definitions of this routine - one for pthread and one for fth.  Be sure to
// include the right one or all bets are off.
void fthXLockInit(XLock_t *cross);
void fthXLock(XLock_t *cross, int write);
int fthXTryLock(XLock_t *cross, int write);
void fthXUnlock(XLock_t *cross);

#endif
