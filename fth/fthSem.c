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
 * File:   fthSem.c
 * Author: drew
 *
 * Created on April 22, 2009
 *
 * (c) Copyright 2009, Schooner Information Technology, Inc.
 * http:                                     //www.schoonerinfotech.com/
 *
 * $Id: fthSem.c 6914 2009-04-23 06:05:29Z drew $
 */

/*
 * XXX: drew 2009-04-22 Not optimal, but quite simple and probably light
 * weight compared to the things that are using the semphore.  Switch
 * to a count field with an fthThreadQ for multiple waiters if it becomes
 * a problem.
 */

#include "fthSem.h"

void
fthSemInit(struct fthSem *sem, int count) {
    fthMboxInit(&sem->mbox);
    fthSemUp(sem, count);
}

void
fthSemUp(struct fthSem *sem, int count) {
    int i;
    for (i = 0; i < count; ++i) {
        fthMboxPost(&sem->mbox, 0);
    }
}

void
fthSemDown(struct fthSem *sem, int count) {
    int i;
    for (i = 0; i < count; ++i) {
        fthMboxWait(&sem->mbox);
    }
}
