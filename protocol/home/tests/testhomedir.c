/*
 * File:   testhomedir.c
 * Author: Darpan Dinker
 *
 * Created on March 26, 2008, 12:19 PM
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: testhomedir.c 10527 2009-12-12 01:55:08Z drew $
 */

/*
 * WHAT DOES THIS TEST DO?
 * After creating 10 entries on 1st Fth, it spawns 2nd Fth
 */

#include "agent/agent_helper.h"
#include "fth/fth.h"
#include "protocol/home/direntry.h"
#include "protocol/home/homedir.h"
#include "protocol/reqq.h"
#include "shared/container.h"
#include "utils/properties.h"
#include <inttypes.h>

extern struct plat_shmem_alloc_stats g_init_sm_stats, g_end_sm_stats;
extern void print_sm_stats(struct plat_shmem_alloc_stats init, struct plat_shmem_alloc_stats end);

HomeDir homedir;
int numBlocks = 1024;
#define threadstacksize 4096*3

void
testRoutine2(uint64_t arg)
{
    uint64_t cguid = 1;
    SDF_container_type_t ctype = SDF_BLOCK_CONTAINER;
    int blockNum;
    SDF_boolean_t bEntryCreated;

    for (blockNum = 0; blockNum < numBlocks; blockNum++) {
        local_key_t *lkey = get_local_block_key(blockNum);
        DirEntry *entry = HomeDir_get_create(homedir, cguid, ctype, lkey, &bEntryCreated);
        // {{
        fthThread_t *top = reqq_peek(entry->q);
        if (top != fthSelf()) {

            fthWaitEl_t *wait = reqq_lock(entry->q);
            uint16_t sz = reqq_enqueue(entry->q, fthSelf());
            reqq_unlock(entry->q, wait);

            printf("Thread 2: Size of request queue when second thread added itself = %u.\n", sz);

            if (sz > 1) {
                printf("Thread 2: going to wait\n");
                fthWait();
                printf("Thread 2: resumed after wait\n");
                fthWaitEl_t *wait = reqq_lock(entry->q);
                fthThread_t *top = reqq_peek(entry->q);
                if (top == fthSelf()) {
                    fthThread_t *self = reqq_dequeue(entry->q);
                    plat_assert_always(self == top);
                }
                reqq_unlock(entry->q, wait);
            }
        }
        // }}
    }
}

void testcreate()
{
    uint64_t cguid = 1;
    SDF_container_type_t ctype = SDF_BLOCK_CONTAINER;
    SDF_boolean_t bEntryCreated;

    for (int blockNum = 0; blockNum < numBlocks; blockNum++) {
        local_key_t *lkey = get_local_block_key(blockNum);
        DirEntry *entry = HomeDir_get_create(homedir, cguid, ctype, lkey, &bEntryCreated);
        plat_assert_always(SDF_TRUE == bEntryCreated && NULL != entry);
        // {{
        fthWaitEl_t *wait = reqq_lock(entry->q); // LOCK REQQ
        fthThread_t *top = reqq_peek(entry->q);
        plat_assert_always(top == fthSelf());
        fthThread_t *self = reqq_dequeue(entry->q);
        plat_assert_always(self == fthSelf());
        reqq_unlock(entry->q, wait); // UNLOCK REQQ
        // }}
        if (bEntryCreated) {
            free_local_key(lkey); // HomeDir_get_create makes a copy if created via LinkedDirList_put
        }
    }

    printf("Thread 1: Created %d blocks in the directory\n", numBlocks);
}

void testget()
{
    uint64_t cguid = 1;
    SDF_container_type_t ctype = SDF_BLOCK_CONTAINER;
    
    for (int blockNum = 0; blockNum < numBlocks*2; blockNum++) {
        local_key_t *lkey = get_local_block_key(blockNum);
        DirEntry *entry = HomeDir_get(homedir, cguid, ctype, lkey);
        // {{
        if (blockNum < numBlocks) {
            plat_assert_always(entry);
            fthWaitEl_t *wait = reqq_lock(entry->q); // LOCK REQQ
            fthThread_t *top = reqq_peek(entry->q);
            plat_assert_always(top == fthSelf());
            fthThread_t *self = reqq_dequeue(entry->q);
            plat_assert_always(self == fthSelf());
            reqq_unlock(entry->q, wait); // UNLOCK REQQ
            if (NULL != (top = reqq_peek(entry->q))) {
                fthResume(top, 0);
                printf("Thread 1: yielding after setting thread 2 to run for block=%u\n", blockNum);
                fthYield(1);
            }
        }
        // }}
        free_local_key(lkey);
    }
    printf("Thread 1: Got %d blocks from the directory\n", numBlocks);
}

void testremove()
{
    uint64_t cguid = 1;
    SDF_container_type_t ctype = SDF_BLOCK_CONTAINER;

    for (int blockNum = 0; blockNum < numBlocks; blockNum++) {
        local_key_t *lkey = get_local_block_key(blockNum);
        DirEntry *entry = HomeDir_remove(homedir, cguid, ctype, lkey);
        // {{
        plat_assert_always(entry);
        fthThread_t *top = reqq_peek(entry->q);
        if (top)
            plat_assert_always(top == fthSelf());
        fthWaitEl_t *wait = reqq_lock(entry->q);
        fthThread_t *self = reqq_dequeue(entry->q);
        if (self)
            plat_assert_always(self == fthSelf());
        reqq_unlock(entry->q, wait);
        
        reqq_destroy(entry->q);
        plat_assert_always(NULL == entry->home);
        plat_free(entry);
        // }}
        free_local_key(lkey);
    }
    printf("Thread 1: Removed %d blocks from the directory\n", numBlocks);
}

void
testRoutine1(uint64_t arg)
{
    int size=1024;
    char str[size];

    HomeDir_printStats(homedir, str, size); printf("%s\n", str);
    testcreate();
    HomeDir_printStats(homedir, str, size); printf("%s\n", str);
    testget();
    HomeDir_printStats(homedir, str, size); printf("%s\n", str);
    testremove();
    HomeDir_printStats(homedir, str, size); printf("%s\n", str);
    fthKill(222);
}

int
execute_test()
{
    fthInit();
    fthThread_t *thread = fthSpawn(&testRoutine1, threadstacksize);

    fthResume(thread, 0);

    fthSchedulerPthread(0);
    
    HomeDir_destroy(homedir);

    return 0;
}

SDF_boolean_t
internal_testhomedir_init()
{
    SDF_boolean_t ret = SDF_FALSE;
    loadProperties("/opt/schooner/config/schooner-med.properties"); // TODO get filename from command line
    // plat_log_parse_arg("platform/alloc=trace");

    if (SDF_TRUE != (ret = init_agent_sm(0))) {
        printf("init_agent_sm() failed!\n");
    } else {
        plat_shmem_alloc_get_stats(&g_init_sm_stats);
        uint64_t buckets = getProperty_uLongLong("SDF_HOME_DIR_BUCKETS", MAX_BUCKETS-1);
        plat_log_msg(21332, PLAT_LOG_CAT_PRINT_ARGS, PLAT_LOG_LEVEL_TRACE, "PROP: SDF_HOME_DIR_BUCKETS=%"PRIu64,
                     buckets);
        uint32_t lockType = getProperty_uLongInt("SDF_HOME_DIR_LOCKTYPE", HMDIR_FTH_BUCKET);
        plat_log_msg(21333, PLAT_LOG_CAT_PRINT_ARGS, PLAT_LOG_LEVEL_TRACE, "PROP: SDF_HOME_DIR_LOCKTYPE=%"PRIu32,
                     lockType);
        homedir = HomeDir_create(buckets, lockType);
    }

    return (ret);
}

int
main(int argc, char *argv[])
{
    printf("Start of %s.\n", argv[0]);
    if (SDF_TRUE != internal_testhomedir_init()) {
        return -1;
    } else if (argc > 1) {
        numBlocks = atoi(argv[1]);
    }
    
    int ret = execute_test();
    printf("End of %s.\n", argv[0]);
    // plat_log_parse_arg("sdf/shared=debug");
    plat_shmem_alloc_get_stats(&g_end_sm_stats);
    print_sm_stats(g_init_sm_stats, g_end_sm_stats);
    return (ret);
}
