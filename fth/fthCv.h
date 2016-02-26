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
 * File:   fthCv.h
 * Author: Jonathan Bertoni
 *
 * Created on February 29, 2008
 *
 * (c) Copyright 2009, Schooner Information Technology, Inc.
 * http:                                     //www.schoonerinfotech.com/
 *
 */

#ifndef __FTH_CV_H
#define __FTH_CV_H

#include "platform/types.h"
#include "fth.h"
#include "fthThread.h"
#include <pthread.h>

typedef struct fthCondVar {
    fthLock_t *lock;
    fthThreadQ_lll_t queue;
} fthCondVar_t;

#define fthCondVar_t struct fthCondVar

/*
 *  The wait and signal interfaces are similar to pthreads so that we can move
 *  to that interface easily.  The "lock" parameter to fthCvInit is used for
 *  debugging assertions.
 */

extern void fthCvInit     (fthCondVar_t *condVar, fthLock_t *lock);
extern void fthCvWait     (fthCondVar_t *condVar, fthLock_t *lock);
extern void fthCvSignal   (fthCondVar_t *condVar);
extern void fthCvBroadcast(fthCondVar_t *condVar);

#endif
