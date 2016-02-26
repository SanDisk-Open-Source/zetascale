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

/* author: Mac
 *
 * Created on Apr 28, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 */

#include "fth.h"
#include "fthMbox.h"
#include "fthThread.h"
#include "fthWaitEl.h"
#include "fthMem.h"
#include "fthMbox.h"
#include "fthMutex.h"
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


#include <stdio.h>
#include <errno.h>
#include <unistd.h>
static int failed_count = 0;
#define ASSERT(expr, pass_str, failed_str) \
     if (expr) { \
         printf("*** PASSED, RESULT DESCRIPTION:%s *** \n", pass_str); \
     } \
     else { \
         printf("*** FAILED, RESULT DESCRIPTION:%s *** \n", failed_str); \
         failed_count ++; \
     }
     	
#define QUESTION(expr, question) \
     if (expr) { \
         printf("*** QUESTION, TEST DESCRIPTION:%s *** \n", question); \
     }

int exitRV = 0;
int eligibleQ_size, freeWait_size, crossWaitQ_size, allQ_size, resumeQ_size, sleepQ_size;
fthThread_t *save_fth_thread;

/*
 * the count of element in eligibleQ
 * with the different state, fth thread will be 
 * put into different queue.
 */
int get_eligibleQ_size(fthThread_t *p) {
    eligibleQ_size = 0;
    while(p) {

        eligibleQ_size ++;
        p = p->threadQ.next;
    }
    return eligibleQ_size;

}

int get_freeWait_size(fthWaitEl_t *p) {
    freeWait_size = 0;
    while(p) {
        freeWait_size ++;
        p = p->waitQEl.next;
    }
    return freeWait_size;

}

int get_crossWaitQ_size(fthThread_t *p) {
    crossWaitQ_size = 0;
    while(p) {
        crossWaitQ_size ++;
        p = p->threadQ.next;
    }
    return crossWaitQ_size;

}

int get_allQ_size(fthThread_t *p) {
    allQ_size = 0;
    while(p) {
        allQ_size ++;
        p = p->nextAll;
    }
    return allQ_size;
}

int get_resumeQ_size(fthThread_t *p) {
    resumeQ_size = 0;
    while(p) {
        resumeQ_size ++;
        p = p->resumeNext;
    }
    return resumeQ_size;
}

int get_sleepQ_size(fthThread_t *p) {
    sleepQ_size = 0;
    while(p) {
        sleepQ_size ++;
        p = p->threadQ.next;
    }
    return sleepQ_size;
}

void fth_c_test_1(uint64_t arg) {

    uint64_t wait_data;
    printf("after calling fthSchedulerPthread, test the size of main used Q/L in fth:\n");
    printf("in fth %d:\n", arg);
#ifdef MULTIQ_SCHED    
    ASSERT(get_eligibleQ_size(fthBase()->eligibleQ[0].head) == 2, "the eligibleQ size is 2. ", "the eligibleQ size isn't 2. ")
#else
    ASSERT(get_eligibleQ_size(fthBase()->eligibleQ.head) == 2, "the eligibleQ size is 2. ", "the eligibleQ size isn't 2. ")
#endif // MULTIQ_SCHED

    ASSERT(get_freeWait_size(fthBase()->freeWait.head) == 0, "the freeWait size is 0. ", "the freeWait size isn't 0. ")
    ASSERT(get_allQ_size(fthBase()->allHead) == 3, "the allQ size is 3. ", "the allQ size isn't 3. ")
    ASSERT(get_resumeQ_size(fthResumePtrs()->head) == 0, "the resumeQ size is 0. ", "the resumeQ size isn't 0. ")
    ASSERT(get_sleepQ_size(fthBase()->sleepQ.head) == 0, "the sleepQ size is 0. ", "the sleepQ size isn't 0. ")

    printf("fth %d call %s\n", arg, "fthWait()");
    save_fth_thread = fthSelf();//just save the wait fth 
    wait_data = fthWait();//where the wait fth was put ?? must the user own to save the handle of it?
    //---------------------------
    ASSERT(fthSelf()->state == 'R', "resume fth state is R. ", "resume fth state isn't R. ")
    printf("fth %d was resumed:return to the %s\n", arg, __FUNCTION__);
    ASSERT(wait_data == 10, "wait data is 10. ", "wait data isn't 10. ")
#ifdef MULTIQ_SCHED
    ASSERT(get_eligibleQ_size(fthBase()->eligibleQ[0].head) == 0, "the eligibleQ size is 0, because fth 2 still sleeping. ", "the eligibleQ size isn't 0. ")
#else
    ASSERT(get_eligibleQ_size(fthBase()->eligibleQ.head) == 0, "the eligibleQ size is 0, because fth 2 still sleeping. ", "the eligibleQ size isn't 0. ")
#endif // MULTIQ_SCHED

    ASSERT(get_freeWait_size(fthBase()->freeWait.head) == 0, "the freeWait size is 0. ", "the freeWait size isn't 0. ")
    ASSERT(get_allQ_size(fthBase()->allHead) == 2, "the allQ size is 2, because one of fth has bean killed. ", "the allQ size isn't 2. ")
    ASSERT(get_resumeQ_size(fthResumePtrs()->head) == 0, "the resumeQ size is 0. ", "the resumeQ size isn't 0. ")
    ASSERT(get_sleepQ_size(fthBase()->sleepQ.head) == 1, "the sleepQ size is 1. ", "the sleepQ size isn't 1. ")
    printf("the state sleeping fth is %c\n", fthBase()->sleepQ.head->state);
    
}

void fth_c_test_3(uint64_t arg);
void fth_c_test_2(uint64_t arg) {
    printf("in fth %d:\n", arg);
    ASSERT(save_fth_thread->state == 'W', "this is the wait fth. ", "this isn't the wait fth. ")
#ifdef MULTIQ_SCHED
    ASSERT(get_eligibleQ_size(fthBase()->eligibleQ[0].head) == 1, "the eligibleQ size is 1. ", "the eligibleQ size isn't 1. ")
#else
    ASSERT(get_eligibleQ_size(fthBase()->eligibleQ.head) == 1, "the eligibleQ size is 1. ", "the eligibleQ size isn't 1. ")
#endif // MULTIQ_SCHED

    ASSERT(get_freeWait_size(fthBase()->freeWait.head) == 0, "the freeWait size is 0. ", "the freeWait size isn't 0. ")
    ASSERT(get_allQ_size(fthBase()->allHead) == 3, "the allQ size is 3. ", "the allQ size isn't 3. ")
    ASSERT(get_resumeQ_size(fthResumePtrs()->head) == 0, "the resumeQ size is 0. ", "the resumeQ size isn't 0. ")
    ASSERT(get_sleepQ_size(fthBase()->sleepQ.head) == 0, "the sleepQ size is 0. ", "the sleepQ size isn't 0. ")
    fthResume(save_fth_thread, 10);//seems the second arg isn't need.

    printf("fth %d: resume the wait fth:\n", arg);
#ifdef MULTIQ_SCHED
    ASSERT(get_eligibleQ_size(fthBase()->eligibleQ[0].head) == 2, "the eligibleQ size is 2. ", "the eligibleQ size isn't 2. ")
#else
    ASSERT(get_eligibleQ_size(fthBase()->eligibleQ.head) == 2, "the eligibleQ size is 2. ", "the eligibleQ size isn't 2. ")
#endif // MULTIQ_SCHED

    ASSERT(get_freeWait_size(fthBase()->freeWait.head) == 0, "the freeWait size is 0. ", "the freeWait size isn't 0. ")
    ASSERT(get_allQ_size(fthBase()->allHead) == 3, "the allQ size is 3. ", "the allQ size isn't 3. ")
        //ASSERT(get_resumeQ_size(fthResumePtrs()->head) == 1, "the resumeQ size is 1. ", "the resumeQ size isn't 1. ")
    ASSERT(get_sleepQ_size(fthBase()->sleepQ.head) == 0, "the sleepQ size is 0. ", "the sleepQ size isn't 0. ")
    ASSERT(save_fth_thread->state == 'D', "after calling fthResume, the wait fth's state change into 'D'. ", "after calling fthResume, the wait fth's state not change into 'D'. ")
#ifdef MULTIQ_SCHED    
    ASSERT(fthBase()->eligibleQ[0].head->dispatch.startRoutine == fth_c_test_3, "after calling fthResume, the wait fth is in eligibleQ. ", "after calling fthResume, the wait fth isn't in eligibleQ. ")
#else
    ASSERT(fthBase()->eligibleQ.head->dispatch.startRoutine == fth_c_test_3, "after calling fthResume, the wait fth is in eligibleQ. ", "after calling fthResume, the wait fth isn't in eligibleQ. ")
#endif // MULTIQ_SCHED

    uint64_t sleep = 3000000000;
    fthNanoSleep(sleep);
    fthKill(1);
  
}

void fth_c_test_3(uint64_t arg) {

    //int wait_data;
#ifdef MULTIQ_SCHED
    ASSERT(get_eligibleQ_size(fthBase()->eligibleQ[0].head) == 1, "the eligibleQ size is 1. ", "the eligibleQ size isn't 1. ")
#else
    ASSERT(get_eligibleQ_size(fthBase()->eligibleQ.head) == 1, "the eligibleQ size is 1. ", "the eligibleQ size isn't 1. ")
#endif // MULTIQ_SCHED

    ASSERT(get_freeWait_size(fthBase()->freeWait.head) == 0, "the freeWait size is 0. ", "the freeWait size isn't 0. ")
    ASSERT(get_allQ_size(fthBase()->allHead) == 3, "the allQ size is 3. ", "the allQ size isn't 3. ")
    ASSERT(get_resumeQ_size(fthResumePtrs()->head) == 0, "the resumeQ size is 0. ", "the resumeQ size isn't 0. ")
    ASSERT(get_sleepQ_size(fthBase()->sleepQ.head) == 1, "the sleepQ size is 1. ", "the sleepQ size isn't 1. ")
    printf("fth 3 finished. \n");
    /*
    printf("fth %d call the fthWait function\n", arg);
    save_fth_thread = fthSelf();
    wait_data = fthWait();
    printf("fth %d was resumed:return to the %s\n", arg, __FUNCTION__);
    ASSERT(wait_data == 230, "get the wait data 230. ", "don't get the wait data 230. ")
    ASSERT(get_eligibleQ_size(fthBase()->eligibleQ.head) == 0, "the eligibleQ size is 0. ", "the eligibleQ size isn't 0. ")
    ASSERT(get_freeWait_size(fthBase()->freeWait.head) == 0, "the freeWait size is 0. ", "the freeWait size isn't 0. ")
    fthKill(1);
    */
}

#define SHM_SIZE 8 * 1024 *1024

int main(int argc, char **argv) {

    fthThread_t *temp_thread = NULL;
    fthWaitEl_t *temp_wait = NULL;

    plat_shmem_trivial_test_start(argc, argv);

    // In the multiQ case, the no-arg call to fthInit will generate just one scheduler Q
    fthInit();

    //create three fths
    XResume(XSpawn(fth_c_test_1, 4096), 1);
    XResume(XSpawn(fth_c_test_2, 4096), 2);
    XResume(XSpawn(fth_c_test_3, 4096), 3);

    printf("=== TEST THE FTHWAIT FUNCTION ====\n");
    printf("before calling fthSchedulerPthread, test the size of main used Q/L in fth:\n");
#ifdef MULTIQ_SCHED
    temp_thread = fthBase()->eligibleQ[0].head;//queue 1 eligibleQ
#else
    temp_thread = fthBase()->eligibleQ.head;//queue 1 eligibleQ
#endif // MULTIQ_SCHED

    ASSERT(get_eligibleQ_size(temp_thread) == 0, "the eligibleQ size is 0. ", "the eligibleQ size isn't 0. ")
    
    temp_wait = fthBase()->freeWait.head;//queue 2 freeWaitQ
    ASSERT(get_freeWait_size(temp_wait) == 0, "the freeWait size is 0. ", "the freeWait size isn't 0. ")

    temp_thread = fthBase()->allHead;//queue 4 allQ
    ASSERT(get_allQ_size(temp_thread) == 3, "the allQ size is 3. ", "the allQ size isn't 3. ")

    temp_thread = fthResumePtrs()->head;//queue 5 resumeQ
    ASSERT(get_resumeQ_size(temp_thread) == 3, "the resumeQ size is 3. ", "the resumeQ size isn't 3. ")
    
    temp_thread = fthBase()->sleepQ.head;//queue 6 sleepQ
    ASSERT(get_sleepQ_size(temp_thread) == 0, "the sleepQ size is 0. ", "the sleepQ size isn't 0. ")
    fthSchedulerPthread(0);

    plat_shmem_trivial_test_end();

    printf("test finished. \n");
    printf("in this file,the failed count is %d. \n", failed_count);
    return (failed_count);
                   
}

