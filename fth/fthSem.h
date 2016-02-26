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
 * File:   fthSem.h
 * Author: drew
 *
 * Created on April 22, 2009
 *
 * (c) Copyright 2009, Schooner Information Technology, Inc.
 * http:                                     //www.schoonerinfotech.com/
 *
 * $Id: fthSem.h 6914 2009-04-23 06:05:29Z drew $
 */

// Semaphore

#ifndef _FTH_SEM_H
#define _FTH_SEM_H

#include "fthMbox.h"

typedef struct fthSem {
    fthMbox_t mbox;
} fthSem_t;

/** @brief Initialize sem with initial count */
void fthSemInit(struct fthSem *sem, int count);

/** @brief Add count to sem */
void fthSemUp(struct fthSem *sem, int count);

/** @brief Block until count are decremented from sem */
void fthSemDown(struct fthSem *sem, int count);

#endif
