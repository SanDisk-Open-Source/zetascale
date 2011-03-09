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
#include "fthSched.h"
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
  do {  \
     if (expr) { \
         printf("*** PASSED, RESULT DESCRIPTION:%s *** \n", pass_str); \
     } \
     else { \
         printf("*** FAILED, RESULT DESCRIPTION:%s *** \n", failed_str); \
         failed_count ++; \
     } \
  } while(0);

int exitRV = 0;

void fth_c_test_1(uint64_t arg) {

   fthThread_t *p;
   printf("after calling fthSchedulerPthread, test some basic structures of fth.\n");
   printf("in fth %d:\n", arg);

   ASSERT(fthResumePtrs()->head == NULL, "the head of resumeQ is null. ", "the head of resumeQ isn't null. ")

     ASSERT(fthBase()->allTail != NULL, "the tail of allQ isn't null. ", "the tail of allQ is null. ")
#ifdef MULTIQ_SCHED
   ASSERT(fthBase()->eligibleQ[0].head != NULL, "the head of eligibleQ isn't null. ", "the head of eligibleQ is null. ")   ASSERT(fthBase()->allHead != NULL, "the head of allQ isn't null. ", "the head of allQ is null. ")
#else   
   ASSERT(fthBase()->eligibleQ.head != NULL, "the head of eligibleQ isn't null. ", "the head of eligibleQ is null. ")
#endif
   p = fthSelf();
   ASSERT(p != NULL, "current fth isn't null. ", "current fth is null. ")

#ifdef MULTIQ_SCHED
   p = fthBase()->eligibleQ[0].head->threadQ.next;   
   ASSERT(p != NULL, "fth scheduler has started. ", "fth scheduler hasn't started. ")
#else  
   p = fthBase()->eligibleQ.head->threadQ.next;
   ASSERT(p != NULL, "fth scheduler has started. ", "fth scheduler hasn't started. ")
#endif
   ASSERT(p != NULL, "the second of eligibleQ isn't null. ", "the second of eligibleQ is null. ")

#ifdef MULTIQ_SCHED
   ASSERT(p == fthBase()->eligibleQ[0].tail, "the second of eligibleQ is tail. ", "the second of eligibleQ isn't tail. ")
#else
   ASSERT(p == fthBase()->eligibleQ.tail, "the second of eligibleQ is tail. ", "the second of eligibleQ isn't tail. ")
#endif
   p = p->threadQ.next;
   ASSERT(p == NULL, "the third of eligibleQ is null. ", "the third of eligibleQ is null. ")
   
}

void fth_c_test_2(uint64_t arg) {

   fthThread_t *p;
   printf("in fth %d:\n", arg);

#ifdef MULTIQ_SCHED
   ASSERT(fthBase()->eligibleQ[0].head != NULL, "the head of eligibleQ isn't null. ", "the head of eligibleQ is null. ")
#else
   ASSERT(fthBase()->eligibleQ.head != NULL, "the head of eligibleQ isn't null. ", "the head of eligibleQ is null. ")
#endif

#ifdef MULTIQ_SCHED
     ASSERT(fthBase()->eligibleQ[0].head  == fthBase()->eligibleQ[0].tail, "eligibleQ only have one element, so a fth has dead", "eligibleQ have more than one element. ")
#else
     ASSERT(fthBase()->eligibleQ.head  == fthBase()->eligibleQ.tail, "eligibleQ only have one element, so a fth has dead.", " eligibleQ have more than one element. ")
#endif
   
   p = fthSelf();
   ASSERT(p != NULL, "current fth isn't null. ", "current fth is null. ")

#ifdef MULTIQ_SCHED
   p = fthBase()->eligibleQ[0].head->threadQ.next;   ASSERT(p == NULL, "the second of eligibleQ is null. ", "the second of eligibleQ isn't null. ") 
#else
   p = fthBase()->eligibleQ.head->threadQ.next;  ASSERT(p == NULL, "the second of eligibleQ is null. ", "the second of eligibleQ isn't null. ")
#endif
  
  


}


void fth_c_test_3(uint64_t arg) {
   fthThread_t *p;
   fthSched_t *s;

#ifdef MULTIQ_SCHED
   ASSERT(fthBase()->eligibleQ[0].head == NULL, "the head of eligibleQ is null. ", "the head of eligibleQ isn't null. ") 
     printf("in fth %d:\n", arg);
#else

#endif

#ifdef MULTIQ_SCHED
   ASSERT(fthBase()->eligibleQ[0].head == NULL, "the head of eligibleQ is null. ", "the head of eligibleQ isn't null. ")
   ASSERT(fthBase()->eligibleQ[0].head == fthBase()->eligibleQ[0].tail, "the eligibleQ is empty. ", "the eligibleQ isn't empty. ")
#else
   ASSERT(fthBase()->eligibleQ.head == NULL, "the head of eligibleQ is null. ", "the head of eligibleQ isn't null. ")
   ASSERT(fthBase()->eligibleQ.head == fthBase()->eligibleQ.tail, "the eligibleQ is empty. ", "the eligibleQ isn't empty. ")
#endif
   p = fthSelf();
   ASSERT(p != NULL, "current fth isn't null. ", "current fth is null. ")
   s = p->dispatch.sched;
   ASSERT(s != NULL, "fth scheduler has started. ", "fth scheduler hasn't started. ")
   ASSERT(fthBase()->kill == 0, "fth scheduler has alive. ", "fth scheduler hasn't alive. ")
   fthKill(1);
   ASSERT(fthBase()->kill == 1, "fth scheduler was killed. ", "fth scheduler wasn't kill. ")
   printf("One of fth killed the scheduler ,so main function will exit ...\n");

}
void fth_c_test(uint64_t arg) {
    printf("Fth module test: %d ...\n",arg);
    if(arg == 1) {
        fthKill(1);// there are just one fth scheduler, so fthKill number is 1.
    }

}
#define SHM_SIZE 8 * 1024 *1024
#define FTHNUM  3
int main(void) {
    fth_t *temp = NULL;
    fthThread_t *t = NULL;
    
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

    printf("=== TESTING GLOBAL LIBRARY API ===\n");
    printf("before calling fthSchedulerPthread , test some basic structures of fth.\n");
    temp =  fthBase();
    ASSERT(temp == 0x0, "before fthInit(),fth variable is null .", "before fthInit(), fth variable isn't null .")
    
   
    printf("=== TESTINF BASIC OPERATION ===\n");
    printf("Initializing Fth system (spawns scheduler and main pthread)\n");
#ifdef MULTIQ_SCHED
    fthInitMultiQ(1,1);
#else
    fthInit();//step 2 fthInit
#endif
    ASSERT(fthBase() != NULL, "after fthInit(), fth variable isn't null . ", "after fthInit(), fth variable is null .")
    
    printf("spawn %d fths:\n", FTHNUM);
    XResume((t = XSpawn(fth_c_test_1, 4096)), 1);//step 3 XSpawn, step 4 XResume
    XResume(XSpawn(fth_c_test_2, 4096), 2);
    XResume(XSpawn(fth_c_test_3, 4096), 3);

    printf("after calling XResume, test the field of fthThread_t %d:\n", 1);
    ASSERT(t->state == 'd', "state init. ", "state init. ")
    ASSERT(t->dispatch.startRoutine == fth_c_test_1, "startRoutine init. ", "startRoutine init. ")
    ASSERT(t->dispatch.sched == NULL, "sched init. ", "sched init. ")
    ASSERT(t->spin == 0, "spin init. ", "spin init. ")
    ASSERT(t->dispatchable == 0, "dispatchable init. ", "dispatchable init. ")
    ASSERT(t->yieldCount == 0, "yieldCount init. ", "yieldCount init. ")
    ASSERT(t->defaultYield == 0, "defaultYield init. ", "defaultYield init. ")
    ASSERT(t->nextAll != NULL, "nextAll init. ", "nextAll init")

#ifdef MULTIQ_SCHED
    t = fthBase()->eligibleQ[0].head;
#else
    t = fthBase()->eligibleQ.head;
#endif
    ASSERT(t == NULL, "the first element of eligibleQ is null. ", "the first element of eligibleQ isn't null. ")

#ifdef MULTIQ_SCHED
      fthThreadQ_lll_init(&fthBase()->eligibleQ[0]);
    t = fthBase()->eligibleQ[0].head;
#else    
    fthThreadQ_lll_init(&fthBase()->eligibleQ);
    t = fthBase()->eligibleQ.head;
#endif
    ASSERT(t == NULL, "empty eligibleQ. ", "not empty eligible. ")

    t = fthBase()->allHead;
    ASSERT(t != NULL, "the head of allQ isn't null. ", "the head of allQ is null. ")
   
    t = fthBase()->allTail;
    ASSERT(t != NULL, "the tail of allQ isn't null. ", "the tail of allQ is null. ")

    ASSERT(fthBase()->allHead != fthBase()->allTail, "the head of allQ isn't equal to the tail of allQ, because i has XSpawned many fths. ", "the head of all is equal to the tail of allQ. ")
    
    t = fthBase()->sleepQ.head;
    ASSERT(t == NULL, "empty sleepQ. ", "not empty sleepQ")
 
    t = fthResumePtrs()->head;
    ASSERT(t != NULL, "the head of resumeQ isn't null. ", "the head of resumeQ is null. ")
    t = fthResumePtrs()->tail;
    ASSERT(t != NULL, "the tail of resumeQ isn't null. ", "the tail of resumeQ is null. ")
    ASSERT(fthResumePtrs()->head->resumeNext->resumeNext == fthResumePtrs()->tail, "there are three fths in resumeQ. ", "there are not three fths in resumeQ. ")
    fthSchedulerPthread(0);//step 5 fthSchedulerPthread
    printf("scheduler halted\n");
    printf("In this file,the failed count is %d. \n", failed_count);
    return (failed_count);
            
}

