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
 * File:   fthFlashAccess.c
 * Author: Darpan Dinker
 *
 * Created on October 3, 2008, 3:40 AM
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id$
 */

// ############## File is for experimental purposes. ###############
// ############## If perplexed by the code, please contact the author. ###############

#undef ENABLE_MULTIPLE_FLASH_SUBSYSTEMS

#include <pthread.h>

#include "platform/assert.h"
#include "platform/errno.h"
#include "platform/fcntl.h"
#include "platform/logging.h"
// #define PLAT_OPTS_NAME(name) name ## _fthFlashAccess
// #define PLAT_OPTS_NO_CONFIG
// #include "platform/opts.h"

#include "platform/signal.h"
#include "platform/stdlib.h"
#include "platform/string.h"

#include "platform/unistd.h"
#include "common/sdfstats.h"

#include "flash/flash.h"

#include "fth/fth.h"
#include "fth/fthOpt.h"

#include "utils/hash.h"
#include "common/sdfstats.h"
#include "utils/properties.h"
#include "applib/XMbox.h"
#include "applib/simpleWait.h"
#include "fth/fthXMbox.h"

#include "fthFlashAccess.h"
#include <stdio.h>

/*#define PLAT_OPTS_ITEMS_fthFlashAccess()                                       \
    PLAT_OPTS_FTH()
 */


PLAT_SP_IMPL(FIOEntry_sp, struct FIOEntry);
// PLAT_SP_VAR_OPAQUE_IMPL(FIOEntryPtr_sp, void);

FIO_Q queue;

/**
 * Create a Flash-IO scoreboard entry.
 */
FIOEntry_sp_t fio_sb_entry_create();
/**
 * Destroy a Flash-IO scorebard entry.
 */
void fio_sb_entry_destroy(FIOEntry_sp_t entry);

/**
 * Called from an application Pthread per request-response.
 * <b>Blocking call</b>.
 */
int fio_sb_send_and_wait(FIOEntry_sp_t entry);

/** Called once at init */
int fio_sb_initQueue(FIO_Q *q) {
    q->mbox_shmem = ptofMbox_sp_alloc();
    if (ptofMbox_sp_is_null(q->mbox_shmem)) {
        return -1;
    }
    ptofMbox_t * ptofm = ptofMbox_sp_rwref(&ptofm, q->mbox_shmem);
    ptofMboxInit(ptofm);
    ptofMbox_sp_rwrelease(&ptofm);

    return 0;
}

/** Called once */
void fio_sb_freeQueue(FIO_Q *q) {
    ptofMbox_sp_free(q->mbox_shmem);
    q->mbox_shmem = ptofMbox_sp_null;
}

/** Called from Fthread to dequeue work enqueued by a Pthread */
FIOEntry_sp_t fio_sb_deque(FIO_Q *q) {
    ptofMbox_t *xmbox;
    FIOEntry_sp_t entry;
    ptofMbox_sp_rwref(&xmbox, q->mbox_shmem);
    entry = shmem_cast(FIOEntry_sp, ptofMboxWait(xmbox));
    ptofMbox_sp_rwrelease(&xmbox);
    return entry;
}

FIOEntry_sp_t fio_sb_entry_create() {
    FIOEntry_sp_t ret = FIOEntry_sp_null;
    struct FIOEntry *p = NULL;

    ret = plat_shmem_alloc(FIOEntry_sp);
    if (!FIOEntry_sp_is_null(ret)) {
        p = FIOEntry_sp_rwref(&p, ret);
        p->op = '\0';
        p->shard = NULL;
        p->key = NULL;
        p->keyLen = 0;
        p->dataIn = NULL;
        p->dataOut = NULL;
        p->dataLen = 0; // in or out
        p->retStatus = 0;

        ftopSimpleSignal_t *xmbox = &p->sb_wait_mbox;
        ftopSimpleSignalInit(xmbox);

        FIOEntry_sp_rwrelease(&p);
    } else {
        plat_log_msg(20866, PLAT_LOG_CAT_SDF_SHARED, PLAT_LOG_LEVEL_FATAL, "Not enough shared memory, plat_shmem_alloc() failed.");
    }

    return (ret);
}

/** Called from Pthread */
int fio_sb_send_and_wait(FIOEntry_sp_t entry) {
    struct FIOEntry *p = NULL;
    int ret = 0;

    if (!FIOEntry_sp_is_null(entry)) {
        p = FIOEntry_sp_rwref(&p, entry);

        ptofMboxPost(queue.mbox_shmem, shmem_cast(shmem_void, entry));

        /* We only care about the signal from the other end that the
         * wait is over, not the actual mail contents. */
        ftopSimpleSignal_t *xmbox = &p->sb_wait_mbox;
        ftopSimpleWait(xmbox);
        //if (p->respStatus == SDF_SM_WAITING) {
        //    fprintf(stderr, "What the heck?\n");
        //}
        // plat_assert(p->respStatus != SDF_SM_WAITING);
        FIOEntry_sp_rwrelease(&p);
    }

    /* there's a race between this release (which includes the mbox struct) and the other
     * side still working on the mbox. Hack for now to avoid hanging due to that race
     * TODO: Fix the race */

    ret = 1;

    return (ret);
}

/** Called from Fthread when work is done */
int fio_sb_notify(FIOEntry_sp_t entry) {
    struct FIOEntry *p = NULL;
    int ret = 0;

    if (!FIOEntry_sp_is_null(entry)) {
        p = FIOEntry_sp_rwref(&p, entry);
        if (NULL != p) {
            ftopSimplePost(&p->sb_wait_mbox);
            ret = 1;
        }
        FIOEntry_sp_rwrelease(&p);
    }

    return (ret);
}

extern void ftopMboxPost(ftopMbox_t *xmbox, shmem_void_t mailShmem);

int fio_sb_notify_local(struct FIOEntry *p) {
    int ret = 0;

    if (NULL != p) {
        ftopSimplePost(&p->sb_wait_mbox);
        ret = 1;
    }

    return (ret);
}

void fio_sb_entry_destroy(FIOEntry_sp_t entry) {
    struct FIOEntry *p = NULL;

    plat_assert(!FIOEntry_sp_is_null(entry));

    p = FIOEntry_sp_rwref(&p, entry);
    p->op = '\0';
    p->shard = NULL;
    p->key = NULL;
    p->keyLen = 0;
    p->dataIn = NULL;
    p->dataOut = NULL;
    p->dataLen = 0; // in or out
    p->retStatus = 0;
    FIOEntry_sp_rwrelease(&p);
    plat_shmem_free(FIOEntry_sp, entry);
    entry = FIOEntry_sp_null;
}
////////////////////////////////////////////////////////////////////////////////
static flashDev_t *dev = NULL;
static shard_t *shard = NULL;
pthread_t pthreadMain;

#define FLASH_PUT_NO_TEST          0
#define FLASH_PUT_TEST_EXIST       0x01
#define FLASH_PUT_TEST_NONEXIST    0x02

/** Pthread local metadata buffer */
__thread objMetaData_t tl_md;

/*
 * @brief read from flash
 * @return FLASH_EOK     - Succeeded
 *         FLASH_ENOENT  - Key does not exist or has expired
int flashGet(struct shard *shard, struct objMetaData *metaData, char *key, char **dataPtr, int flags)
 * @return FLASH_EOK     - Succeeded
 *         FLASH_ENOENT  - No entry
 *         FLASH_EEXIST  - Entry already exists
 *         FLASH_EDQUOT  - Put would cause shard to exceed quota
 *         FLASH_ENOSPC  - Flash device is full
 *         FLASH_EMFILE  - Too many objects in this shard
int flashPut(struct shard *shard, struct objMetaData *metaData, char *key, char *data, int flags)
 */
int readFromFlash(shard_t *shard, char *key, int keyLen, char **dataOut, int *dataLenOut) {
    tl_md.objFlags = 0;
    tl_md.expTime = 0;
    tl_md.createTime = 0;
    tl_md.keyLen = keyLen;

    if (dataOut != NULL) {
        *dataOut = NULL;
    }
    int ret = flashGet(shard, &tl_md, key, dataOut, 0);
    if (ret != FLASH_EOK && ret != FLASH_ENOENT) {
        printf("flashGet() returned unexpected value=%d\n", ret);
    }

    *dataLenOut = tl_md.dataLen;
    return (ret);
}

int writeToFlash(shard_t *shard, char *key, int keyLen, char *dataIn, int *dataLenIn) {
    tl_md.objFlags = 0;
    tl_md.expTime = 0;
    tl_md.createTime = 0;
    tl_md.keyLen = keyLen;
    tl_md.dataLen = *dataLenIn;

    int ret = flashPut(shard, &tl_md, key, dataIn, FLASH_PUT_NO_TEST);
    if (ret != FLASH_EOK) {
        printf("flashPut() returned unexpected value=%d\n", ret);
    }
    return (ret);
}
////////////////////////////////////////////////////////////////////////////////

__thread FIOEntry_sp_t tl_entry; // = FIOEntry_sp_null;

int pThreadPutObject(char *key, int keyLen, char *data, int *dataLen) {
    int ret = SDF_FAILURE;

    if (FIOEntry_sp_is_null(tl_entry)) {
        tl_entry = fio_sb_entry_create();
    }
    struct FIOEntry *p = FIOEntry_sp_rwref(&p, tl_entry);
    p->shard = shard;
    p->key = key;
    p->keyLen = keyLen;
    p->dataIn = data;
    p->dataOut = NULL;
    p->dataLen = *dataLen; // in or out
    p->op = 'P';
    // printf("Put and wait for %s\n", key);
    fio_sb_send_and_wait(tl_entry);
    // printf("Put and wait DONE for %s, ret=%d\n", key, p->retStatus);
    FIOEntry_sp_rwrelease(&p);
    if (p->retStatus == FLASH_EOK) {
        ret = SDF_SUCCESS;
    }
    FIOEntry_sp_rwrelease(&p);

    return (ret);
}

int pThreadGetObject(char *key, int keyLen, char **data, int *dataLen) {
    int ret = SDF_FAILURE;

    if (FIOEntry_sp_is_null(tl_entry)) {
        tl_entry = fio_sb_entry_create();
    }
    struct FIOEntry *p = FIOEntry_sp_rwref(&p, tl_entry);
    p->shard = shard;
    p->key = key;
    p->keyLen = keyLen;
    p->dataIn = NULL;
    p->dataOut = data;
    p->dataLen = 0; // in or out
    p->op = 'G';
    // printf("Get and wait for %s\n", key);
    fio_sb_send_and_wait(tl_entry);
    // printf("Get and wait DONE for %s, len=%d, ret=%d\n", key, p->dataLen, p->retStatus);
    *dataLen = p->dataLen;
    if (p->retStatus == FLASH_EOK) {
        ret = SDF_SUCCESS;
    }
    FIOEntry_sp_rwrelease(&p);

    return (ret);
}
////////////////////////////////////////////////////////////////////////////////

void fileIOhandlers(uint64_t arg) {
    while (1) {
        // printf("Fthread blocking on deque\n");
        FIOEntry_sp_t entry = fio_sb_deque(&queue);
        struct FIOEntry *p = FIOEntry_sp_rwref(&p, entry);
        // printf("Fthread received a message for op=%c\n", p->op);
        switch (p->op) {
            case 'P':
                p->retStatus = writeToFlash(p->shard, p->key, p->keyLen, p->dataIn, &p->dataLen);
                break;
            case 'G':
                p->retStatus = readFromFlash(p->shard, p->key, p->keyLen, p->dataOut, &p->dataLen);
                break;
            default:
                printf("Error in receiving op\n");
        }
        fio_sb_notify_local(p);
        // printf("Fthread after notify\n");
        FIOEntry_sp_rwrelease(&p);
    }

}
////////////////////////////////////////////////////////////////////////////////
// {{ PARAMETERS
unsigned thread_schedulers = 1;
unsigned numFthPerScheduler = 1;
// }} PARAMETERS

void test_enqueue_from_pthread(unsigned count) {
    char key[256];
    // {{ PUTs
    for (unsigned i = 0; i < count; i++) {
        FIOEntry_sp_t entry = fio_sb_entry_create();
        struct FIOEntry *p = FIOEntry_sp_rwref(&p, entry);
        p->shard = shard;
        p->key = key;
        p->keyLen = snprintf(key, 256, "hello-%d", i);
        p->dataIn = plat_alloc(1024);
        p->dataOut = NULL;
        p->dataLen = 1024; // in or out
        p->op = 'P';
        // FIOEntry_sp_rwrelease(&p);
        // printf("Put and wait for %s\n", key);
        fio_sb_send_and_wait(entry);
        // FIOEntry_sp_rwref(&p, entry);
        // printf("Put and wait DONE for %s, ret=%d\n", key, p->retStatus);
        FIOEntry_sp_rwrelease(&p);
        fio_sb_entry_destroy(entry);
    }
    // }} PUTs

    // {{ GETs
    for (unsigned i = 0; i < count; i++) {
        FIOEntry_sp_t entry = fio_sb_entry_create();
        struct FIOEntry *p = FIOEntry_sp_rwref(&p, entry);
        p->shard = shard;
        p->key = key;
        p->keyLen = snprintf(key, 256, "hello-%d", i);
        p->dataIn = NULL;
        p->dataOut = NULL;
        p->dataLen = 0; // in or out
        p->op = 'G';
        // FIOEntry_sp_rwrelease(&p);
        // printf("Get and wait for %s\n", key);
        fio_sb_send_and_wait(entry);
        // FIOEntry_sp_rwref(&p, entry);
        // printf("Get and wait DONE for %s, len=%d, ret=%d\n", key, p->dataLen, p->retStatus);
        FIOEntry_sp_rwrelease(&p);
        fio_sb_entry_destroy(entry);
    }
    // }} GETs
}

void *pthreadRoutine(void *arg) {

    unsigned num_new_fthreads = *((unsigned *) arg);

    printf("Starting %i fthread(s) \n", num_new_fthreads);

    for (int i = 0; i < num_new_fthreads; i++) {
        XResume(fthSpawn(&fileIOhandlers, 40960), i);
    }

    fthSchedulerPthread(0);

    printf("Scheduler %p halted\n", arg);

    return (0);
}

void *spawnSchedulerThreads(void *arg) {
    pthread_t pthread[thread_schedulers];
    for (int i = 0; i < thread_schedulers; i++) {
        pthread_create(&pthread[i], NULL, &pthreadRoutine, (void *) & numFthPerScheduler);
    }

    // test_enqueue_from_pthread(5);

    for (int i = 0; i < thread_schedulers; i++) {
        pthread_join(pthread[i], NULL);
    }

    return NULL;
}

int initFthFlashAccess(unsigned fthSchedNum, unsigned fthNum) {
    thread_schedulers = fthSchedNum;
    numFthPerScheduler = fthNum;

    struct plat_shmem_config *shmem_config = plat_alloc(sizeof (struct plat_shmem_config));
    plat_shmem_config_init(shmem_config);
    plat_shmem_prototype_init(shmem_config);
    const char *path = plat_shmem_config_get_path(shmem_config);
    int tmp = plat_shmem_attach(path);
    if (tmp) {
        plat_log_msg(20073, PLAT_LOG_CAT_PLATFORM_TEST_SHMEM,
                PLAT_LOG_LEVEL_FATAL, "shmem_attach(%s) failed: %s",
                path, plat_strerror(-tmp));
        return -1;
    }

    fthInitMultiQ(1, thread_schedulers);

    if (!(dev = flashOpen("/dev/flash0", 0))) {
        return -2;
    }

    if (!(shard = shardCreate(dev, 1001, FLASH_SHARD_INIT_TYPE_OBJECT +
            FLASH_SHARD_INIT_PERSISTENCE_YES + FLASH_SHARD_INIT_EVICTION_CACHE,
            0xffffffffffffffff, getProperty_Int("SDF_SHARD_MAX_OBJECTS", 1000000)))) {
        return -3;
    }
    plat_log_msg(20867, PLAT_LOG_CAT_PRINT_ARGS, PLAT_LOG_LEVEL_TRACE, "PROP: SDF_SHARD_MAX_OBJECTS=%d",
            getProperty_Int("SDF_SHARD_MAX_OBJECTS", 1000000));

    if (fio_sb_initQueue(&queue)) {
        return -4;
    }

    return pthread_create(&pthreadMain, NULL, &spawnSchedulerThreads, NULL);
}

int main(int argc, char **argv) {
    int ret = 0;

    // Painless way to get tracing in for diagnostics
    //    if (plat_opts_parse_strip_fthFlashAccess(&argc, argv)) {
    //        ret = 2;
    //    }

    if (!(ret = initFthFlashAccess(1, 1))) {
        pthread_join(pthreadMain, NULL);
        plat_exit(ret);
    }

    return (ret);
}

