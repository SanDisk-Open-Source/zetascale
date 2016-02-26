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
 * File:   spin_rwtest.c
 * Author: Wei,Li
 *
 * Created on July 15, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 */

/*
 * Trivial test for spin rw lock.
 */
#include <pthread.h>

#include "platform/rwlock.h"
#include "platform/spin_rw.h"

#include "platform/stdio.h"

#include "platform/types.h"

plat_spin_rwlock_t spin_rwlock=0;

#define NUM_PTHREADS 64 

int value=0;

void *pthreadReader(void *arg) {
   int local_value=0;
   plat_spin_rw_rdlock(spin_rwlock);
   local_value=value;
   plat_spin_rw_rdunlock(spin_rwlock);
   return 0;
}
void *pthreadWriter(void *arg) {
   plat_spin_rw_wrlock(spin_rwlock);
   ++value;
   plat_spin_rw_wrunlock(spin_rwlock);
   return 0;	
}
int main(int argc, char **argv) {
     pthread_t pthread_readers[NUM_PTHREADS];
     pthread_t pthread_writers[NUM_PTHREADS];
     for (uint64_t i = 0; i < NUM_PTHREADS; i++) {
        pthread_create(&pthread_readers[i], NULL, &pthreadReader, NULL);
        pthread_create(&pthread_writers[i], NULL, &pthreadWriter, NULL);
     }

    for (uint64_t i = 0; i < NUM_PTHREADS; i++) {
        pthread_join(pthread_readers[i], NULL);
	pthread_join(pthread_writers[i], NULL);
    }
    printf("%d\n",value);
    plat_assert(value==NUM_PTHREADS);
    return (0);
}
