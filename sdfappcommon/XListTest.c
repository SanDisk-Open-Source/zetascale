#include <sched.h>

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

#include "sdfappcommon/XList.h"

#include "fth/fth.h"

int status = 0;

typedef struct foo {
    int myData;
    struct foo *next_foo_ptr;
} foo_t;

XLIST_H(foo, next_foo_ptr);
XLIST_IMPL(foo, next_foo_ptr);

static  char *  backing_file = "/tmp/shmem";

#define NUM_TRIES 100000
#define NUM_PTHREADS 4


#define SHM_SIZE 8 * 1024 *1024

#include <stdio.h>

#define pf(...) printf(__VA_ARGS__); fflush(NULL);

static foo_t *check[NUM_TRIES*NUM_PTHREADS];
static int rands[NUM_TRIES*NUM_PTHREADS];

int randIndex = 0;
int saferand(void) {
    int index = __sync_fetch_and_add(&randIndex, 1) % (NUM_PTHREADS * NUM_TRIES);
    return rands[index];
}

static foo_t *head;
static foo_t *tail;

int done_count = 0;
int start_count = 0;

void threadRoutine1(uint64_t arg) {
    pf("Thread %i start\n", arg);
    (void) __sync_add_and_fetch(&start_count, 1);
    while (start_count < NUM_PTHREADS) {
        
    }
    pf("Thread %i running\n", arg);
    
    int read_count = 0;
    int write_count = 0;
    while((read_count < NUM_TRIES) || (write_count < NUM_TRIES)) {
        if ((read_count < NUM_TRIES) && (saferand() & 1)) { // Randomly read or write
            // Read
            foo_t *fp = foo_xlist_dequeue(&head, &tail);
            if (fp != NULL) {
                if (fp->myData < 0) {
                    pf("Got pointer %x with data %i\n", fp, fp->myData);
                    status = 1;
                }
                if (read_count + 10 > NUM_TRIES) {
                    pf("Reading %i, %i, %i\n", arg, read_count, fp->myData);
                }
                fp->myData= -1;
                read_count++;
            } else {
                
                //                sched_yield();
            }

        } else if (write_count < NUM_TRIES) {
            if (write_count + 10 > NUM_TRIES) {
                pf("Writing %i, %i\n", arg, write_count);
            }
            foo_xlist_enqueue(&head, &tail, check[write_count++ + (arg * NUM_TRIES)]);
        }
    }

    int num = __sync_add_and_fetch(&done_count, 1);
    pf("Thread %i done - count %i\n", arg, num);
    if (num == NUM_PTHREADS) {
        pf("Last man standing\n");
        for (int i = 0; i < NUM_TRIES * NUM_PTHREADS; i++) {
            if (check[i]->myData != -1) {
                pf("Element %i not cleared\n", i);
                status = 1;
            }
        }

        fthKill(100);
        pf("Done - %i\n", status);
        plat_exit(status);
    }

}


void *pthreadSchedRoutine(void *arg) {

    (void) sched_setscheduler(0, SCHED_RR, NULL);
    nice(1);
    

    fthResume(fthSpawn(&threadRoutine1, 8192), (uint64_t) arg);

    pf("Scheduler %i started\n", (uint64_t) arg);
    fthSchedulePthreadr();    
    pf("Scheduler %i halted\n", (uint64_t) arg);

    return (0);

}


int main(void) {

    pthread_t pthread[NUM_PTHREADS];

    int tmp;
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

    head = NULL;
    tail = NULL;
    for (int i = 0; i < NUM_TRIES * NUM_PTHREADS; i++) {
        rands[i] = rand();
        foo_t *foo = plat_alloc(sizeof(foo_t));
        foo->myData = i;
        check[i] = foo;
    }

    for (uint64_t i = 0; i < NUM_PTHREADS-1; i++) {
        pthread_create(&pthread[i], NULL, &pthreadSchedRoutine, (void *) i);
    }

    pthreadSchedRoutine((void *) NUM_PTHREADS-1);

    //    for (uint64_t i = 0; i < NUM_PTHREADS; i++) {
        //        pthread_join(pthread[i], NULL);
    //  }

}

