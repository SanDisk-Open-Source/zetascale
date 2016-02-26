/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

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
