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
 * File:   fthMem.c
 * Author: Jim
 *
 * Created on February 29, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http:                                     //www.schoonerinfotech.com/
 *
 * $Id: fthMem.c 396 2008-02-29 22:55:43Z jim $
 */


#include "platform/types.h"

#include "fthMem.h"
#include "fth.h"

extern fth_t *fth;

/**
 * @brief Active a memQ
 *
 * @return - memQ number to use
 */

uint32_t fthMemQAlloc(void) {
    return (__sync_fetch_and_add(&fth->memQCount, 1));
}

/**
 * @brief Wait for a memory location to go non-zero
 *
 * @param mem <IN> Pointer to memory location
 */

uint64_t fthMemWait(uint64_t *mem, int queueNum) {

    if (*mem == 0) {
        fthThread_t *self = fthSelf();

        self->memWait = mem;
        self->state = 'M';                   // Mem wait
        fth->memTest[queueNum] = mem;
        fth->memQ[queueNum] = self;
        fthWait();                    // Give up processor
    }

    return *mem;
}
