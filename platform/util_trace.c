/*
* File:   sdf/platform/util_shmem.c
* Author: Wei,Li
*
* Created on July 31, 2008
*
* (c) Copyright 2008, Schooner Information Technology, Inc.
* http://www.schoonerinfotech.com/
*
*/
#include <sys/types.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <memory.h>
#include <signal.h>
#include "platform/stdio.h"
#include "platform/stdlib.h"
#include "platform/spin_rw.h"
#include "platform/util_trace.h"

/**
 * two instances of the shmem buffer, now it is a circul buffer and we only
 * keep the latest data of the size "size".
 * The double buffer here is used for fast tracing, writing to one buffer and 
 * flush the other to the disk.
 **/
static struct shmem_buffer shmem_buffer_A = {
    0,                          /* flag to show if this buffer is active, for simple, only A's flag is used here, 0 for A is active, and 1 for B is active */
    -1,                         /* shmid */
    (char *)-1,                 /* buffer */
    0,                          /* buffer size */
    0,                          /* buffer real size */
    0                           /*collector's pid */
};

static struct shmem_buffer shmem_buffer_B = {
    0,                          /* not used for B */
    -1,                         /* shmid */
    (char *)-1,                 /* buffer */
    0,                          /* buffer size */
    0,                          /* buffer real size */
    0                           /*collector's pid */
};

/*the buffer ptr, would switch between A and B*/
static struct shmem_buffer *shmem_buffer_ptr = &shmem_buffer_A;

/*current buffer, 0 for A, 1 for B*/
static int *current_buffer = 0;

/*a const of the content length, 32 bits*/
static int content_len = 32;

/*a const of the trace content number of each buffer*/
static int content_number = 0;
/*the buffer size, default is 50M*/
static int SHME_CONTENT_SIZE = 50;
/*
 * a unsigned long long to record the buffer switch time for generating 
 * the log id
 */
// static unsigned long long switch_counter=0;

/*
 * global tracing offset
 */
static uint64_t Current_offset = 0;


/**
 *a common creation of the share memory.
 **/
static int
create_shmem(struct shmem_buffer *shmem, int key, int rmid)
{
    /*if the old key find, delete it first */
    if (((shmem->shmid) = shmget(key, 0, 0666)) >= 0 || rmid) {
        shmctl(shmem->shmid, IPC_RMID, 0);
    }
    /*only for the fist process to ini the share memory system */
    if (((shmem->shmid) = shmget(key, SHME_MAX_SIZE, IPC_CREAT | 0666)) < 0) {
        // Always seems to fail; should use plat_log_msg anyway
        // printf("LOG:SHMEM: shmget failed, use the stderr instead\n");
        return 0;
    }
    return 1;
}

/**attache the shmem to this process*/
static int
attach_shmem(struct shmem_buffer *shmem)
{
    if (((shmem->shm) = shmat((shmem->shmid), NULL, 0)) == (char *)-1) {
        printf("LOG:SHMEM: shmat failed, use the stderr instead\n");
        return 0;
    }
    return 1;
}

/**
 *a common init of the share memory.
 **/
static int
init_shmem(struct shmem_buffer *shmem, int key)
{
    int do_create = 0;
    if (((shmem->shmid) = shmget(key, SHME_MAX_SIZE, 0666)) < 0) {
        /*get fail, so create a new one */
        if (!create_shmem(shmem, key, 0))
            return 0;
        do_create = 1;
        attach_shmem(shmem);
    } else {
        attach_shmem(shmem);
        shmem->total_size = (unsigned int *)
            (shmem->shm + SHME_FLAG_SIZE + SHME_LEN_SIZE +
             SHME_LEN_REAL_SIZE);
        /*if the new size is smaller then old size */
        if ((*shmem->total_size) != SHME_CONTENT_SIZE) {
            /*rm the old one and create a new one */
            if (!create_shmem(shmem, key, 1))
                return 0;
            do_create = 1;
            attach_shmem(shmem);
        }
    }
    /*first time, set all to zero */
    if (do_create) {
        /*for the first allocate, ini all data to \0 */
        memset((shmem->shm), '\0', SHME_MAX_SIZE);
    }
    /*
     * after the share memory's allocation, so just ini the size and
     * flag information
     */
    shmem->flag = (int *)(shmem->shm);
    shmem->size = (unsigned int *)(shmem->shm + SHME_FLAG_SIZE);
    shmem->real_size = (unsigned int *)
        (shmem->shm + SHME_FLAG_SIZE + SHME_LEN_SIZE);
    shmem->total_size = (unsigned int *)
        (shmem->shm + SHME_FLAG_SIZE + SHME_LEN_SIZE + SHME_LEN_REAL_SIZE);
    *(shmem->total_size) = SHME_CONTENT_SIZE;
    shmem->pid = (pid_t *)
        (shmem->shm + SHME_FLAG_SIZE + SHME_LEN_SIZE + SHME_LEN_REAL_SIZE +
         SHME_LEN_TOTAL_SIZE);
    shmem->shm = shmem->shm + SHME_HEADER_SIZE;
    return 1;
}


/**
 *a common detach of the share memory, do nothing if failed.
 **/
static void
detache_shmem(char *shm)
{
    if (SHME_ENABLE && shmdt(shm - SHME_HEADER_SIZE) == -1) {
        //printf("LOG:SHMEM: shmdt failed, just a warning\n");
        return;
    }
}


#ifdef SHME_ENABLE
/**
 * a contructor for init the shmem
 **/
__attribute__ ((constructor))
     void init_shmem_buffer()
{
    /*init the buffer size from config file */
    SHME_CONTENT_SIZE = 50;
    INIT_CONFIG_PARAMETER(TRACE_CONFIG_FILE, "CONTENT_SIZE=",
                          SHME_CONTENT_SIZE, 0);
    SHME_CONTENT_SIZE = SHME_CONTENT_SIZE << 20;
    /*init both buffer A and B */
    if (SHME_ENABLE && init_shmem(&shmem_buffer_A, SHME_KEY_A)
        && init_shmem(&shmem_buffer_B, SHME_KEY_B)) {
        printf("log buffer A: flag:%d, size: %d, real_size:%d, "
               "total_size: %d, pid:%d\n",
               *shmem_buffer_A.flag, *shmem_buffer_A.size,
               *shmem_buffer_A.real_size, (*shmem_buffer_A.total_size),
               *shmem_buffer_A.pid);
        printf("log buffer B: flag:%d, size: %d, real_size:%d, "
               "total_size: %d, pid:%d\n",
               *shmem_buffer_B.flag, *shmem_buffer_B.size,
               *shmem_buffer_B.real_size, *shmem_buffer_B.total_size,
               *shmem_buffer_B.pid);
        current_buffer = shmem_buffer_A.flag;
        if ((*current_buffer) == 0) {
            shmem_buffer_ptr = &shmem_buffer_A;
        } else {
            shmem_buffer_ptr = &shmem_buffer_B;
        }
        /*init the content len and content number of each buffer */
        content_len = sizeof(trace_content_t);
        content_number = (SHME_CONTENT_SIZE / content_len);
        /*assert the SHME_CONTENT_SIZE%content_len should be zero */
        plat_assert((SHME_CONTENT_SIZE % content_len) == 0);
    }
}


/**
 * a destructor for detach the share memory.
 * notice: we CAN NOT release the share memory here as other external 
 * program would read the content of the share memory.
 */
__attribute__ ((destructor))
     void detache_shmem_buffer()
{
    detache_shmem(shmem_buffer_A.shm);
    detache_shmem(shmem_buffer_B.shm);
}
#endif /* SHME_ENABLE */


/*
 * fast binary tracing with fixed-size entries (for memcached only)
 *
 * note that for the following code to work, SHME_CONTENT_SIZE has to
 * be a multiple of content_size
 */
unsigned int
printf_fast(trace_content_t * trace)
{
    int curr_buf;
    uint64_t offset;
    struct shmem_buffer *shmem_buffer_local_ptr;

    offset = __sync_fetch_and_add(&Current_offset, content_len);

    if (0 == ((offset / SHME_CONTENT_SIZE) % 2)) {
        curr_buf = 0;
        shmem_buffer_local_ptr = &shmem_buffer_A;
    } else {
        curr_buf = 1;
        shmem_buffer_local_ptr = &shmem_buffer_B;
    }

    if (NULL == shmem_buffer_local_ptr ||
        ((char *)-1) == shmem_buffer_local_ptr->shm) {
        return 0;
    }
    /*get the trace id and real offset */
    trace->id = offset / content_len;
    offset %= SHME_CONTENT_SIZE;

    /*do the memcpy */
    memcpy(shmem_buffer_local_ptr->shm + offset, trace,
           sizeof(trace_content_t));

    /*
     * assignment should be safe even without atomic operations
     */
    *shmem_buffer_local_ptr->size = offset + content_len;

    /*
     * if trace buffer is full, signal the collector
     * this should be done for the last step to ensure when sig the collector
     * all content in buffer is filled. 
     */
    if (0 == ((offset + content_len) % SHME_CONTENT_SIZE)) {
        (*current_buffer) = (curr_buf == 0)?1:0; 
        if (0 < *(shmem_buffer_local_ptr->pid)) {
            sigqueue(*(shmem_buffer_local_ptr->pid), SHME_FULL + curr_buf,
                     (union sigval)SHME_CONTENT_SIZE);
        }
    }

    return 1;                   /* SUCCESS */
}


/**
 *a external interface to get the share memory information
 */
struct shmem_buffer *
get_shmem_info(int index)
{
    if (index == 0)
        return &shmem_buffer_A;
    return &shmem_buffer_B;
}


#ifdef NOT_NEEDED

/**
 *Signal the collector the buffer is full if the collector exsits.
 */
static void
signal_collector(int sig, int value)
{
    /*
     * the signal_collector function is thread safe, so no need to protect 
     * any shared variable here
     */

    /*get the old ptr and make the old ptr's size to zero later on */
    struct shmem_buffer *old_ptr = shmem_buffer_ptr;

    /*add the switch counter */
    switch_counter++;

    /*order is so imp here! */
    if (*current_buffer) {
        (*current_buffer) = 0;
        shmem_buffer_ptr = &shmem_buffer_A;
    } else {
        (*current_buffer) = 1;
        shmem_buffer_ptr = &shmem_buffer_B;
    }
    /*check if the collector process exsits */
    if (*(shmem_buffer_ptr->pid) > 0) {
#ifdef SYNC_ALL
        /*wait the target buffer is empty */
        while (*(shmem_buffer_ptr->size) > SHME_CONTENT_SIZE);
#endif
        sigqueue(*(shmem_buffer_ptr->pid),
                 sig + ((*current_buffer) == 0 ? 1 : 0),
                 (union sigval)(value));

    } else {
#ifdef SYNC_ALL
        *(old_ptr->size) = 0;
        *(old_ptr->real_size) = 0;
#endif
    }
#ifndef SYNC_ALL
    *(old_ptr->size) = 0;
    *(old_ptr->real_size) = 0;
#endif
}


/**
 * the method is used for memcached only and also the infomation here is fixed
 * formate, for the beta release only
 **/
unsigned int
printf_fast(trace_content_t * trace)
{
    /*make sure shmem_buffer_ptr is aviliable */
    if (shmem_buffer_ptr && shmem_buffer_ptr->shm != ((char *)-1)) {
        int offset = 0;

        /*get local copy of global variable */
        struct shmem_buffer *shmem_buffer_local_ptr = shmem_buffer_ptr;
        unsigned long long counter = switch_counter;

        /*
         * get the offset for write first, to make sure each thread write 
         * on a different range
         */
        offset = __sync_fetch_and_add(shmem_buffer_ptr->size, content_len);

        /*
         * for offset bigger than SHME_CONTENT_SIZE, need to write to the 
         * other buffer
         */
        while (offset + content_len > SHME_CONTENT_SIZE) {
            /*
             * for the first thread reach the end, he need to cut the 
             * shmem_buffer.size to zero
             */
            if (offset <= SHME_CONTENT_SIZE) {
                //printf("full\n");
                signal_collector(SHME_FULL, offset);
            }

            /*
             * wait util the buffer size not bigger that SHME_CONTENT_SIZE, 
             * which is to say, the buffer is ready
             */
            while (*(shmem_buffer_ptr->size) > SHME_CONTENT_SIZE);

            /*
             * refresh the local copy of this thread, this action must 
             * before the offset generation
             */
            shmem_buffer_local_ptr = shmem_buffer_ptr;
            counter = switch_counter;

            /*get the new offset */
            offset =
                __sync_fetch_and_add(shmem_buffer_ptr->size, content_len);
        }

        /* 
         * generate a id, using the offset and counter.content_len must
         * be a constant acturlly. This require the SHME_CONTENT_SIZE
         * must be the times of 32, 50M is OK here
         */
        trace->id = (offset % SHME_CONTENT_SIZE) / content_len
            + (content_number * counter);

        /*set the trace infor */
        memcpy(shmem_buffer_local_ptr->shm + offset, trace,
               sizeof(trace_content_t));

        offset += sizeof(trace_content_t);

        /*only add the real size after the memcpy finished */
#ifdef DEEP_SYNC_ALL
        __sync_fetch_and_add(shmem_buffer_local_ptr->real_size, content_len);
#endif
    }
    return 1;
}

#endif /* NOT_NEEDED */
