/* author: Mac
 *
 * Created on Apr 29, 2008
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
#define ASSERT(expr, str) \
     if (expr) { \
         printf("*** PASSED, TEST:%s *** \n", str); \
     } \
     else { \
         printf("*** FAILED, TEST:%s *** \n", str); \
         failed_count ++; \
     }

int exitRV = 0;
int mailQ_size, threadQ_size, eligibleQ_size;
static int post_data, last_fth_id;
static fthMbox_t mbox; 


int get_mailQ_size(fthWaitEl_t *w) {
    mailQ_size = 0;
    while(w) {

        mailQ_size ++;
        w = w->waitQEl.next;
    }
    return mailQ_size;

}

int get_threadQ_size(fthThread_t *p) {

    threadQ_size = 0;
    while(p) {
        threadQ_size ++;
        p = p->threadQ.next;
    }
    return threadQ_size;


}
int get_eligibleQ_size(fthThread_t *p) {
    eligibleQ_size = 0;
    while(p) {
        eligibleQ_size ++;
        p = p->threadQ.next;
    }
    return eligibleQ_size;

}
#define MAILNUM 500
void fth_mbox_test_1(uint64_t arg) {
    int index;
    printf("fth %d: entry is %s\n", arg, __FUNCTION__);
    for(index = 0; index < MAILNUM; index ++) {
        fthMboxPost(&mbox, index);
    }
    ASSERT(get_mailQ_size(mbox.mailQ.head) == MAILNUM, "the count of mail in mailbox is 500. ")
    
}

void fth_mbox_test_2(uint64_t arg) {
    int index;
    //if you set a large array , please use dynamic malloc.
    uint64_t temp[MAILNUM];
    printf("fth %d: entry is %s\n", arg, __FUNCTION__);
    for(index = 0; index <MAILNUM; index ++) {

        temp[index] = fthMboxWait(&mbox);
    }
    ASSERT(temp[300] == 300, "get the mail. ")
}

void fth_mbox_test_3(uint64_t arg) {
    int data, index;
    printf("fth %d: entry is %s\n", arg, __FUNCTION__);
    ASSERT(get_mailQ_size(mbox.mailQ.head) == 0, "mailQ has empty. ")
    ASSERT(fthMboxTry(&mbox) == 0, "no mail. ")
    printf("other fth need put any data into mailQ can resume fth %d. \n", arg);
    ASSERT(get_threadQ_size(mbox.threadQ.head) == 0, "no fth in threadQ. ")
    data = fthMboxWait(&mbox);
    printf("fth 3 post data is %d\n",data);
  
    for(index = 0; index < 30; index ++) {
       data = fthMboxWait(&mbox);
       printf("fth %d wait %d\n", arg, data);
    }
}

void fth_mbox_test_5(uint64_t arg);
void fth_mbox_test_6(uint64_t arg);

#define FTHREADER 2
void fth_mbox_test_4(uint64_t arg) {
    int index;
    printf("fth %d: entry is %s\n", arg, __FUNCTION__);
    ASSERT(get_threadQ_size(mbox.threadQ.head) == 1, "one fth in threadQ. ")
    fthMboxPost(&mbox, 6347);
    fthYield(1);

    for(index = 0; index < 30; index ++) {
      fthMboxPost(&mbox, index);
      printf("!<Note>:fth %d post %d\n", arg, index);
      fthYield(1);
    }
    printf("@@@fth %d spawn new threads to mornitor single write multiple read:\n", arg);
    fthResume(fthSpawn(fth_mbox_test_5, 4096), 5);
    for(index = 0; index <FTHREADER; index ++)
       fthResume(fthSpawn(fth_mbox_test_6, 4096), index + 6);

}
#define WRITENUM 10
#define READNUM  WRITENUM/FTHREADER
#define FTHWRITER 2

void fth_mbox_test_7(uint64_t arg);
void fth_mbox_test_8(uint64_t arg);

void fth_mbox_test_5(uint64_t arg) {
    int index;
    printf("you are in fth %d\n", arg);
#ifdef MULTIQ_SCHED
    ASSERT(get_eligibleQ_size(fthBase()->eligibleQ[0].head) == 2, "the size of eligibleQ. ")
#else
    ASSERT(get_eligibleQ_size(fthBase()->eligibleQ.head) == 2, "the size of eligibleQ. ")
#endif // MULTIQ_SCHED


    ASSERT(get_mailQ_size(mbox.mailQ.head) == 0, "empty mail box. ")
    for(index = 0; index < WRITENUM; index ++) {
       fthMboxPost(&mbox, index);
       printf("!<Note>:fth %d post data %d\n", arg, index);
       fthYield(1);
    }

    printf("@@@fth %d spawn new threads to mornitor multiple write single read:\n", arg);
    post_data = 0;
    for(index = 0; index < FTHWRITER; index ++) 
        fthResume(fthSpawn(fth_mbox_test_7, 4096), index + 8);
    fthResume(fthSpawn(fth_mbox_test_8, 4096), FTHWRITER + 8);
    last_fth_id = FTHWRITER + 8;
    
}

void fth_mbox_test_6(uint64_t arg) {
    int index, data;
    printf("you are in fth %d\n", arg);
    for(index = 0; index < READNUM; index ++) {
       printf("fth %d want to get data:\n", arg);
       data = fthMboxWait(&mbox);
       //ASSERT(get_mailQ_size(mbox.mailQ.head) == 0, "after fth 6 get mail ,no mail in mailbox. ")
       printf("fth %d get data %d\n", arg, data);
    }
}

#ifdef WRITENUM
#undef WRITENUM
#define WRITENUM 4
#endif

void fth_mbox_test_7(uint64_t arg) {
    int index;
    printf("you are in fth %d\n", arg);
    for(index = 0; index < WRITENUM; index ++) {
       fthMboxPost(&mbox, post_data);
       printf("!<Note>:fth %d post data %d\n", arg, post_data ++);
       fthYield(1);
    }
}

#ifdef READNUM
#undef READNUM
#define READNUM WRITENUM * FTHWRITER
#endif



/*
#ifdef FTHWRITER
#undef FTHWRITER
#define FTHWRITER 2
#endif

#ifdef FTHREADER
#undef FTHREADER
#define FTHREADER 2
#endif


#ifdef WRITENUM
#undef WRITENUM
#define WRITENUM 4
#endif

#ifdef READNUM
#undef READNUM
#define READNUM  4
#endif
*/
void fth_mbox_test_9(uint64_t arg);
void fth_mbox_test_10(uint64_t arg);


void fth_mbox_test_8(uint64_t arg) {
    int index, data;
    printf("you are in fth %d\n", arg);
    for(index = 0; index < READNUM; index ++) {
       printf("fth %d want to get data:\n", arg);
       data = fthMboxWait(&mbox);
       printf("fth %d get data %d\n", arg, data);
    }

#ifdef FTHWRITER
#undef FTHWRITER
#define FTHWRITER 2
#endif

#ifdef FTHREADER
#undef FTHREADER
#define FTHREADER 2
#endif


#ifdef WRITENUM
#undef WRITENUM
#define WRITENUM 4
#endif

#ifdef READNUM
#undef READNUM
#define READNUM  4
#endif
    

    fthYield(1);

    printf("@@@fth %d spawn new threads to mornitor multiple writer multiple reader:\n", arg);
#ifdef MULTIQ_SCHED
    ASSERT(get_eligibleQ_size(fthBase()->eligibleQ[0].head) == 0, "the size of eligibleQ. ")
#else
    ASSERT(get_eligibleQ_size(fthBase()->eligibleQ.head) == 0, "the size of eligibleQ. ")
#endif // MULTIQ_SCHED

    ASSERT(get_mailQ_size(mbox.mailQ.head) == 0, "empty mail box. ")
    post_data = 0;
    for(index = 0; index < FTHWRITER; index ++)
        fthResume(fthSpawn(fth_mbox_test_9, 4096), index + last_fth_id +1);
    last_fth_id += FTHWRITER;
    for(index = 0; index < FTHREADER; index ++)
        fthResume(fthSpawn(fth_mbox_test_10, 4096), index + last_fth_id +1);
    last_fth_id += FTHREADER;
    

}

void fth_mbox_test_9(uint64_t arg) {
    int index;
    printf("you are in fth %d\n", arg);
    for(index = 0; index < WRITENUM; index ++) {
       fthMboxPost(&mbox, post_data);
       printf("!<Note>:fth %d post data %d\n", arg, post_data ++);
       fthYield(1);
    }
}

void fth_mbox_test_10(uint64_t arg) {
    int index, data;
    printf("you are in fth %d\n", arg);
    for(index = 0; index < READNUM; index ++) {
       printf("fth %d want to get data:\n", arg);
       data = fthMboxWait(&mbox);
       printf("fth %d get data %d\n", arg, data);
       fthYield(1);
    }
    if(index == READNUM)
       fthKill(1);
}

#define SHM_SIZE 8 * 1024 *1024

int main(int argc, char **argv) {

    plat_shmem_trivial_test_start(argc, argv);

    fthInit();
    
    printf("*** === TEST THE FUNCTION IN FTHMBOX.C === ***\n");
    printf("initialize the global mbox. \n");
    fthMboxInit(&mbox);
    ASSERT(mbox.spin == 0, "init spin. ")
    ASSERT(get_mailQ_size(mbox.mailQ.head) == 0, "the size of mailQ is 0. ")
    ASSERT(get_threadQ_size(mbox.threadQ.head) == 0, "the size of threadQ is 0. ")

    XResume(XSpawn(fth_mbox_test_1, 4096), 1);
    XResume(XSpawn(fth_mbox_test_2, 4096), 2);
    XResume(XSpawn(fth_mbox_test_3, 4096), 3);
    XResume(XSpawn(fth_mbox_test_4, 4096), 4);

    fthSchedulerPthread(0);

    plat_shmem_trivial_test_end();

    printf("test finished. \n");
    printf("In this file,the failed count is %d. \n", failed_count);
    return (failed_count);

                   
}

