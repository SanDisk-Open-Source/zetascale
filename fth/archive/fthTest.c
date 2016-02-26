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
 * File:   fthTest.c
 * Author: Jim
 *
 * Created on February 29, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http:                                     //www.schoonerinfotech.com/
 *
 * $Id: fthTest.h 396 2008-02-29 22:55:43Z jim $
 */

//
// Test program for many fth functions
//


#include "fth.h"
#include "fthMbox.h"
#include "fthThread.h"
#include "fthWaitEl.h"
#include "fthMem.h"
#include "fthMbox.h"
#include "fthMutex.h"
#include "fthStructQ.h"
#include "platform/assert.h"
#include "platform/errno.h"
#include "platform/fcntl.h"
#include "platform/logging.h"
#include "platform/shmem.h"
#include "platform/signal.h"
#include "platform/stdlib.h"
#include "platform/string.h"
#include "platform/time.h"
#include "platform/unistd.h"

#include "misc/misc.h"

#define pf(...) printf(__VA_ARGS__); fflush(NULL)

#include <stdio.h>

int exitRV = 0;

void llTest(void) {
    fthWaitEl_t *wait;
    fthWaitQ_lll_t *ll = (fthWaitQ_lll_t *) plat_alloc(sizeof(fthWaitQ_lll_t));
    fthWaitQ_lll_init(ll);

    // Push/pop
    for (int i = 0; i < 5; i++) {
        wait = (fthWaitEl_t *) plat_alloc(sizeof(fthWaitEl_t));
        wait->write = i;                     // Handy dummy field
        fthWaitQ_el_init(wait);
        fthWaitQ_push(ll, wait);
    }

    for (int i = 0; i < 5; i++) {
        wait = fthWaitQ_pop(ll);
        if (wait->write != 4-i) {
            exitRV = 1;
            printf("Push/Pop test - expected %i but got %i\n", i, wait->write);
        }
        plat_free(wait);
    }

    wait = fthWaitQ_pop(ll);
    if (wait != NULL) {
        exitRV = 1;
        printf("Push/Pop test - expected NULL but got %i", wait->write);
    }
    

    // push/shift
    for (int i = 0; i < 5; i++) {
        wait = (fthWaitEl_t *) plat_alloc(sizeof(fthWaitEl_t));
        wait->write = i;                     // Handy dummy field
        fthWaitQ_el_init(wait);
        fthWaitQ_push(ll, wait);
    }
    
    for (int i = 0; i < 5; i++) {
        wait = fthWaitQ_shift(ll);
        if (wait->write != i) {
            exitRV = 1;
            printf("Push/shift test - expected %i but got %i\n", i, wait->write);
        }
        plat_free(wait);
    }

    wait = fthWaitQ_shift(ll);
    if (wait != NULL) {
        exitRV = 1;
        printf("Push/shift test - expected NULL but got %i", wait->write);
    }


    // Unshift/pop
    for (int i = 0; i < 5; i++) {
        wait = (fthWaitEl_t *) plat_alloc(sizeof(fthWaitEl_t));
        wait->write = i;                     // Handy dummy field
        fthWaitQ_el_init(wait);
        fthWaitQ_unshift(ll, wait);
    }

    for (int i = 0; i < 5; i++) {
        wait = fthWaitQ_pop(ll);
        if (wait->write != i) {
            exitRV = 1;
            printf("Unshift/Pop test - expected %i but got %i\n", i, wait->write);
        }
        plat_free(wait);
    }

    wait = fthWaitQ_pop(ll);
    if (wait != NULL) {
        exitRV = 1;
        printf("Unshift/Pop test - expected NULL but got %i", wait->write);
    }
    

    // push/shift
    for (int i = 0; i < 5; i++) {
        wait = (fthWaitEl_t *) plat_alloc(sizeof(fthWaitEl_t));
        wait->write = i;                     // Handy dummy field
        fthWaitQ_el_init(wait);
        fthWaitQ_unshift(ll, wait);
    }
    
    for (int i = 0; i < 5; i++) {
        wait = fthWaitQ_shift(ll);
        if (wait->write != 4-i) {
            exitRV = 1;
            printf("Unshift/shift test - expected %i but got %i\n", i, wait->write);
        }
        plat_free(wait);
    }

    wait = fthWaitQ_shift(ll);
    if (wait != NULL) {
        exitRV = 1;
        printf("Unshift/shift test - expected NULL but got %i", wait->write);
    }

    //
    // Locking versions
    //
    
    // Push/pop
    for (int i = 0; i < 5; i++) {
        wait = (fthWaitEl_t *) plat_alloc(sizeof(fthWaitEl_t));
        wait->write = i;                     // Handy dummy field
        fthWaitQ_el_init(wait);
        fthWaitQ_push(ll, wait);
    }

    for (int i = 0; i < 5; i++) {
        wait = fthWaitQ_pop_lock(ll);
        if (wait->write != 4-i) {
            exitRV = 1;
            printf("Push/PopLock test - expected %i but got %i\n", i, wait->write);
        }
        if (wait->waitQEl.spin == 0) {
            printf("Not locked\n");
        }
        FTH_SPIN_UNLOCK(&wait->waitQEl.spin);
        plat_free(wait);
    }

    wait = fthWaitQ_pop_lock(ll);
    if (wait != NULL) {
        exitRV = 1;
        printf("Push/PopLock test - expected NULL but got %i", wait->write);
    }
    

    // push/shift
    for (int i = 0; i < 5; i++) {
        wait = (fthWaitEl_t *) plat_alloc(sizeof(fthWaitEl_t));
        wait->write = i;                     // Handy dummy field
        fthWaitQ_el_init(wait);
        fthWaitQ_push(ll, wait);
    }
    
    for (int i = 0; i < 5; i++) {
        wait = fthWaitQ_shift_lock(ll);
        if (wait->write != i) {
            exitRV = 1;
            printf("Push/shift test - expected %i but got %i\n", i, wait->write);
        }
        if (wait->waitQEl.spin == 0) {
            exitRV = 1;
            printf("Not locked\n");
        }
        FTH_SPIN_UNLOCK(&wait->waitQEl.spin);
        plat_free(wait);
    }

    wait = fthWaitQ_shift_lock(ll);
    if (wait != NULL) {
        exitRV = 1;
        printf("Push/shift test - expected NULL but got %i", wait->write);
    }


    // Unshift/pop
    for (int i = 0; i < 5; i++) {
        wait = (fthWaitEl_t *) plat_alloc(sizeof(fthWaitEl_t));
        wait->write = i;                     // Handy dummy field
        fthWaitQ_el_init(wait);
        fthWaitQ_unshift(ll, wait);
    }

    for (int i = 0; i < 5; i++) {
        wait = fthWaitQ_pop_lock(ll);
        if (wait->write != i) {
            exitRV = 1;
            printf("Unshift/PopLock test - expected %i but got %i\n", i, wait->write);
        }
        if (wait->waitQEl.spin == 0) {
            exitRV = 1;
            printf("Not locked\n");
        }
        FTH_SPIN_UNLOCK(&wait->waitQEl.spin);
        plat_free(wait);
    }

    wait = fthWaitQ_pop_lock(ll);
    if (wait != NULL) {
        exitRV = 1;
        printf("Unshift/PopLock test - expected NULL but got %i", wait->write);
    }
    

    // push/shift
    for (int i = 0; i < 5; i++) {
        wait = (fthWaitEl_t *) plat_alloc(sizeof(fthWaitEl_t));
        wait->write = i;                     // Handy dummy field
        fthWaitQ_el_init(wait);
        fthWaitQ_unshift(ll, wait);
    }
    
    for (int i = 0; i < 5; i++) {
        wait = fthWaitQ_shift_lock(ll);
        if (wait->write != 4-i) {
            exitRV = 1;
            printf("Unshift/shift test - expected %i but got %i\n", i, wait->write);
        }
        if (wait->waitQEl.spin == 0) {
            exitRV = 1;
            printf("Not locked\n");
        }
        FTH_SPIN_UNLOCK(&wait->waitQEl.spin);
        plat_free(wait);
    }

    wait = fthWaitQ_shift_lock(ll);
    if (wait != NULL) {
        exitRV = 1;
        printf("Unshift/shift test - expected NULL but got %i", wait->write);
    }

    //
    // Remove
    //
    fthWaitEl_t *list[5];

    // Remove tail
    
    for (int i = 0; i < 5; i++) {
        wait = (fthWaitEl_t *) plat_alloc(sizeof(fthWaitEl_t));
        list[i] = wait;
        wait->write = i;                     // Handy dummy field
        fthWaitQ_el_init(wait);
        fthWaitQ_unshift(ll, wait);
    }

    fthWaitQ_remove(ll, list[0]);               // Remove from the end

    for (int i = 0; i < 4; i++) {
        wait = fthWaitQ_shift(ll);
        if (wait->write != 4-i) {
            exitRV = 1;
            printf("Remove tail test - expected %i but got %i\n", i, wait->write);
        }
        FTH_SPIN_UNLOCK(&wait->waitQEl.spin);
        plat_free(wait);
    }

    wait = fthWaitQ_shift_lock(ll);
    if (wait != NULL) {
        exitRV = 1;
        printf("Remove tail test - expected NULL but got %i", wait->write);
    }

    // Remove head

    for (int i = 0; i < 5; i++) {
        wait = (fthWaitEl_t *) plat_alloc(sizeof(fthWaitEl_t));
        list[i] = wait;
        wait->write = i;                     // Handy dummy field
        fthWaitQ_el_init(wait);
        fthWaitQ_unshift(ll, wait);
    }

    fthWaitQ_remove(ll, list[4]);               // Remove from the head

    for (int i = 0; i < 4; i++) {
        wait = fthWaitQ_shift(ll);
        if (wait->write != 3-i) {
            exitRV = 1;
            printf("Remove head test - expected %i but got %i\n", i, wait->write);
        }
        FTH_SPIN_UNLOCK(&wait->waitQEl.spin);
        plat_free(wait);
    }

    wait = fthWaitQ_shift_lock(ll);
    if (wait != NULL) {
        exitRV = 1;
        printf("Remove head test - expected NULL but got %i", wait->write);
    }

    // Remove middle

    for (int i = 0; i < 5; i++) {
        wait = (fthWaitEl_t *) plat_alloc(sizeof(fthWaitEl_t));
        list[i] = wait;
        wait->write = i;                     // Handy dummy field
        fthWaitQ_el_init(wait);
        fthWaitQ_unshift(ll, wait);
    }

    fthWaitQ_remove(ll, list[2]);               // Remove from the middle

    for (int i = 0; i < 4; i++) {
        wait = fthWaitQ_shift(ll);
        if (wait->write != ((i < 2) ? (4 - i) : (3 - i))) {
            exitRV = 1;
            printf("Remove middle test - expected %i but got %i\n", i, wait->write);
        }
        FTH_SPIN_UNLOCK(&wait->waitQEl.spin);
        plat_free(wait);
    }

    wait = fthWaitQ_shift_lock(ll);
    if (wait != NULL) {
        exitRV = 1;
        printf("Remove middle test - expected NULL but got %i", wait->write);
    }


    // Remove only

    wait = (fthWaitEl_t *) plat_alloc(sizeof(fthWaitEl_t));
    wait->write = 123;                       // Handy dummy field
    fthWaitQ_el_init(wait);
    fthWaitQ_unshift(ll, wait);

    fthWaitQ_remove(ll, wait);                  // Remove from the only one

    wait = fthWaitQ_shift_lock(ll);
    if (wait != NULL) {
        exitRV = 1;
        printf("Remove only test - expected NULL but got %i", wait->write);
    }


    
}

fthMbox_t mbox;
fthLock_t lock;
uint64_t mem[3];
static pthread_mutex_t mutex;

void threadRoutine9(uint64_t arg) {
    pf("Thread 9 start - %i\n", arg);
    fthYield(-1);
    pf("Thread 9 end - %i\n", arg);
}
    
void threadRoutine8(uint64_t arg) {
    pf("Thread 8 start - %i\n", arg);
    fthYield(0);
    pf("Thread 8 Mid - %i\n", arg);
    fthYield(0);
    pf("Thread 8 end - %i\n", arg);
}
    
void threadRoutine7(uint64_t arg) {
    pf("Thread 4 start - %i\n", arg);
    fthNanoSleep(arg);
    pf("Thread 4 end - %i\n", arg);
}
    
void threadRoutine6(uint64_t arg) {
    printf("In threadRoutine6 %i\n", arg);
    fthMutexLock(&mutex);
    printf("mutex wait complete - %i\n", arg);
    fthMutexUnlock(&mutex);
}

void threadRoutine5(uint64_t arg) {
    printf("In threadRoutine5 %i\n", arg);
#ifdef UNDEF // was MULTIQ_SCHED    
    int num = fthMemWait(&mem[arg]);
    if (num != (arg+100)) {
        exitRV = 1;
        printf("Mem wait got %i but expected %i\n", num, arg);
    } else {
        printf("mem wait OK - %i/%i\n", arg, num);
    }
#endif    
}

void threadRoutine4(uint64_t arg) {
    printf("In threadRoutine4\n");
    fthWaitEl_t *wait = fthLock(&lock, 0, NULL);
    printf("Got read lock - %i\n", arg);
    fthYield(1);
    fthUnlock(wait);
    printf("Got read unlock - %i\n", arg);
    fthYield(1);
    wait = fthLock(&lock, 1, NULL);
    printf("Got write lock - %i\n", arg);
    fthUnlock(wait);
}

void threadRoutine3(uint64_t arg) {
    printf("In threadRoutine3 %i\n", arg);
    uint64_t mail = fthMboxWait(&mbox);
    if (mail != arg) {
        exitRV = 1;
        printf("Mbox head wait fail: expected %i but got %i\n", arg, mail);
    } else {
        printf("Mbox head wait OK: expected %i and got %i\n", arg, mail);
    }
}

fthStructQ_t structQ;

typedef struct dummyStruct {uint64_t one, two, three, id;} dummyStruct_t;
dummyStruct_t struct1, struct2, struct3, struct4;

void threadRoutineStruct(uint64_t arg) {
    printf("In threadRoutineStruct %i\n", arg);
    dummyStruct_t *ds = (dummyStruct_t *) fthStructQWait(&structQ);
    if (ds->id != arg) {
        exitRV = 1;
        printf("Struct wait fail: expected %i but got %i\n", arg, ds->id);
    } else {
        printf("Struct wait OK: expected %i and got %i\n", arg, ds->id);
    }
}

void threadRoutine1(uint64_t arg) {
    llTest();

    fthMboxInit(&mbox);
    
    uint64_t mail = fthMboxWait(&mbox);
    if (mail != 123) {
        exitRV = 1;
        printf("mbox wait fail: expected 123 but got %i\n", mail);
    }

    mail = fthMboxWait(&mbox);
    if (mail != 456) {
        exitRV = 1;
        printf("Mbox head wait fail: expected 456 but got %i\n", mail);
    }

    mail = fthMboxWait(&mbox);
    if (mail != 789) {
        exitRV = 1;
        printf("Mox middle wait fail: expected 789 but got %i\n", mail);
    }
    
    mail = fthMboxWait(&mbox);
    if (mail != 111) {
        exitRV = 1;
        printf("Mbox head wait fail: expected 111 but got %i\n", mail);
    }

    // Launch 3 waiters
    fthResume(fthSpawn(&threadRoutine3, 4096), 222);
    fthResume(fthSpawn(&threadRoutine3, 4096), 333);
    fthResume(fthSpawn(&threadRoutine3, 4096), 444);

    printf("Threadroutine3 should run now (3 times)\n");

    fthYield(1);                              // let them run

    printf("Back in threadroutine1\n");

    fthMboxPost(&mbox, 222);
    fthMboxPost(&mbox, 333);
    fthMboxPost(&mbox, 444);
    
    printf("Threadroutine3 should run get results (3 times)\n");

    fthYield(1);                              // let them run

    printf("MBOX Test complete\n");

    //
    //
    //

    fthStructQInit(&structQ);
    struct1.id = 1;
    struct2.id = 2;
    struct3.id = 3;
    struct4.id = 4;

    // Launch 3 waiters
    fthResume(fthSpawn(&threadRoutineStruct, 4096), 1);
    fthResume(fthSpawn(&threadRoutineStruct, 4096), 2);
    fthResume(fthSpawn(&threadRoutineStruct, 4096), 3);

    fthStructQFree(&structQ, &struct1);
    fthStructQFree(&structQ, &struct2);
    fthStructQFree(&structQ, &struct3);
    fthStructQFree(&structQ, &struct4);

    printf("ThreadRoutineStruct should run now (3 times)\n");

    fthYield(1);                              // let them run

    printf("Back in threadroutine1\n");

    fthResume(fthSpawn(&threadRoutineStruct, 4096), 4);
    
    printf("ThreadRoutineStruct should run get results (1 times)\n");

    fthYield(1);                              // let them run

    printf("structQ Test complete\n");

    fthLockInit(&lock);

    fthWaitEl_t *wait = fthLock(&lock, 1, NULL);  // Write lock
    fthResume(fthSpawn(&threadRoutine4, 4096), 1);
    fthResume(fthSpawn(&threadRoutine4, 4096), 2);
    fthResume(fthSpawn(&threadRoutine4, 4096), 3);

    fthYield(1);
    fthUnlock(wait);
    printf("Should now get 3 read locks\n");
    fthYield(1);
    printf("Should now get 3 read unlocks\n");
    wait = fthLock(&lock, 1, NULL);          // Write lock
    printf("Should now get 3 write locks\n");
    fthUnlock(wait);
    fthYield(1);
    fthYield(1);
    fthYield(1);

    printf("Lock test complete - starting mem test\n");
    mem[0] = 0;
    mem[1] = 0;
    mem[2] = 0;

    fthResume(fthSpawn(&threadRoutine5, 4096), 0);
    fthResume(fthSpawn(&threadRoutine5, 4096), 1);
    fthResume(fthSpawn(&threadRoutine5, 4096), 2);

    printf("3 threads stall on mem\n");
    fthYield(1);

    printf("Thread 0\n");
    mem[0] = 100;
    fthYield(2);                              // First yield queues, send reads mem
    printf("Thread 1\n");
    mem[1] = 101;
    fthYield(2);
    printf("Thread 2\n");
    mem[2] = 102;
    fthYield(2);

    pthread_mutex_init(&mutex, NULL);        // Init and grab the mutex
    fthMutexLock(&mutex);
    fthResume(fthSpawn(&threadRoutine6, 4096), 0);
    fthResume(fthSpawn(&threadRoutine6, 4096), 1);
    fthResume(fthSpawn(&threadRoutine6, 4096), 2);
    fthYield(0);
    printf("Should see 3 mutext wait complete messages:\n");
    fthMutexUnlock(&mutex);
    fthYield(2);

    fthResume(fthSpawn(&threadRoutine9, 4096), 0);
    fthResume(fthSpawn(&threadRoutine8, 4096), 0);
    fthResume(fthSpawn(&threadRoutine8, 4096), 0);
    fthResume(fthSpawn(&threadRoutine8, 4096), 0);
    fthYield(3);

    printf("Test complete\n");

    fthResume(fthSpawn(&threadRoutine7, 4096), 500000000);
    fthResume(fthSpawn(&threadRoutine7, 4096), 200000000);
    fthResume(fthSpawn(&threadRoutine7, 4096), 900000000);
    fthResume(fthSpawn(&threadRoutine7, 4096), 600000000);

    fthYield(0);
    
    uint64_t sleep = 3000000000;
    pf("About to sleep for %i second(s)\n", sleep/1000000000);
    fthNanoSleep(sleep);
    pf("Back from sleep\n");

    struct timeval tv;
    fthGetTimeOfDay(&tv);
    pf("Time of day %lli %lli\n", tv.tv_sec, tv.tv_usec);
    struct timespec delay;
    delay.tv_sec = 3;
    delay.tv_nsec = 0;
    nanosleep(&delay, NULL);
    fthGetTimeOfDay(&tv);
    pf("Time of day %lli %lli\n", tv.tv_sec, tv.tv_usec);    

    fthKill(100);

}

void threadRoutine2(uint64_t arg) {
    fthMboxPost(&mbox, 123);
    fthMboxPost(&mbox, 456);
    fthMboxPost(&mbox, 789);
    fthMboxPost(&mbox, 111);

}

#define SHM_SIZE 8 * 1024 *1024

int main(void) {

    struct plat_shmem_config *shmem_config = plat_alloc(sizeof(struct plat_shmem_config));
    plat_shmem_config_init(shmem_config);
    shmem_config->size = SHM_SIZE;
    plat_shmem_prototype_init(shmem_config);
    int tmp = plat_shmem_attach(shmem_config->mmap);
    if (tmp) {
        plat_log_msg(20073, PLAT_LOG_CAT_PLATFORM_TEST_SHMEM,
                     PLAT_LOG_LEVEL_FATAL, "shmem_attach(%s) failed: %s",
                     shmem_config->mmap, plat_strerror(-tmp));
        plat_abort();
    }

    
    fthInit();
    fthThread_t *thread1 = XSpawn(&threadRoutine1, 4096);
    fthThread_t *thread2 = XSpawn(&threadRoutine2, 4096);

    XResume(thread1, 0);
    XResume(thread2, 0);

    fthSchedulerPthread(0);

    printf("Scheduler halted\n");

    pf("Total scheduler idle time:         %lli microseconds\n", fthGetSchedulerIdleTime());
    pf("Total scheduler dispatch time:     %lli microseconds\n", fthGetSchedulerDispatchTime());
    pf("Total fth thread run time:         %lli microseconds\n", fthGetTotalThreadRunTime());
    pf("Total fth avg dispatch time:       %lli nanoseconds\n", fthGetSchedulerAvgDispatchNanosec());
    pf("Total fth dispatches:              %lli\n", fthGetSchedulerNumDispatches());
    pf("Total voluntary context switches   %lli\n", fthGetVoluntarySwitchCount());
    pf("Total involuntary context switches %lli\n", fthGetInvoluntarySwitchCount());

    return (0);
    
}
