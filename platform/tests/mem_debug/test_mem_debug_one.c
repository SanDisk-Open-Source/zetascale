/*
 * File:   test_mem_debug_one.c
 * Author: Mingqiang Zhuang
 *
 * Created on April 18, 2008, 9:34 AM
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 */

/** include files **/
#include "test_mem_debug_one.h"
#include "platform/mem_debug.h"
#include "platform/stdio.h"
#include "platform/unistd.h"
#include "platform/signal.h"

#define DEFAULT_BACKTRACE_DEPTH 10
#define BLOCK_COUNT 10
#define BLOCK_SIZE 1024*1024 //block size need be the multiple of page size
#define MEM_POOL_SIZE (BLOCK_SIZE*BLOCK_COUNT)

static struct plat_mem_debug_config mem_debug_config;
static struct plat_mem_debug_stats mem_debug_stats;
static struct plat_mem_debug *global_mem_debug;

static sig_atomic_t got_signal;
static jmp_buf buf;
static struct sigaction action;

static int page_size;
static char *mem_pool;
static char *mem_pool_aligned;
static char * mem_pool2;
static char *ptr_s[BLOCK_COUNT];
static char *ptr_d[BLOCK_COUNT];

static void sighandler(int signo);
static void *round_up(void *ptr, int page_size);
static void check_init(void *ptr1, void *ptr2, void *ptr3, size_t size);
static void reference_test_cmn_with_same_size(void *ptr1, void *ptr2, void *ptr3,
                                              size_t size, int writeable);
static void release_test_cmn_with_same_size(void *ptr1, void *ptr2, void *ptr3,
                                            size_t size, int writeable);

static void
sighandler(int signo)
{
    got_signal = signo;
    longjmp(buf, 1);
}

static void *
round_up(void *ptr, int page_size) {
    return ((void *)(((long)ptr + page_size - 1) & ~(page_size - 1)));
}

/**
 * This function accessed by CUnit, and only run once at the
 * start of test suite. It do some initialization.
 *
 * @return int32_t
 */
int32_t
mem_debug_init(void)
{
    int i;

    page_size = getpagesize();
    /** allocate encough memory for page alignment */
    mem_pool = (char*)plat_alloc(MEM_POOL_SIZE + 2 * page_size - 2);
    if (mem_pool == NULL) {
        printf("out of memory\n");
        plat_exit(1);
    }
    mem_pool_aligned = round_up(mem_pool, page_size);
    for (i = 0; i < BLOCK_COUNT; i++) {
        ptr_s[i] = mem_pool_aligned + i * BLOCK_SIZE;
    }

    plat_mem_debug_config_init(&mem_debug_config);
    global_mem_debug = plat_mem_debug_alloc(&mem_debug_config);
    plat_mem_debug_add_unused(global_mem_debug, (void*)mem_pool_aligned, MEM_POOL_SIZE);

    return 0;
}

/**
 * This function accessed by CUnit,and only run once at the end
 * of the test suite. It do some clean work.
 *
 * @return int32_t
 */
int32_t
mem_debug_cleanup(void)
{
    plat_mem_debug_free(global_mem_debug);
    plat_free(mem_pool);
    plat_free(mem_pool2);

    return 0;
}

void
test_mem_debug_init(void)
{
    /** check all the initialization stats */
    CU_ASSERT_EQUAL(mem_debug_config.backtrace_depth, DEFAULT_BACKTRACE_DEPTH);
    CU_ASSERT_EQUAL(mem_debug_config.log_category, PLAT_LOG_CAT_PLATFORM_MEM_DEBUG);
    CU_ASSERT_EQUAL(mem_debug_config.subobject, PLAT_MEM_SUBOBJECT_DENY);
    CU_ASSERT_PTR_NOT_NULL(global_mem_debug);

    plat_mem_debug_get_stats(global_mem_debug, &mem_debug_stats);
    CU_ASSERT_EQUAL(mem_debug_stats.reference_count, 0);
    CU_ASSERT_EQUAL(mem_debug_stats.object_count, 0);
}

void
test_check_free(void)
{
    size_t size = BLOCK_SIZE;

    /** at beginning, all regions are in free stat */
    CU_ASSERT_TRUE(plat_mem_debug_check_free(global_mem_debug,
                                             (void*)ptr_s[0], size));
    CU_ASSERT_TRUE(plat_mem_debug_check_free(global_mem_debug,
                                             (void*)ptr_s[5], size));
    CU_ASSERT_TRUE(plat_mem_debug_check_free(global_mem_debug,
                                             (void*)ptr_s[9], size));

    /**
     * when the region is referenced, check free return 0.
     * otherwise, it return 1.
     */
    plat_mem_debug_reference(global_mem_debug, (void**)ptr_s[0],
                             (void*)ptr_s[0], size, 1, 0);
    CU_ASSERT_FALSE(plat_mem_debug_check_free(global_mem_debug,
                                             (void*)ptr_s[0], size));
    plat_mem_debug_release(global_mem_debug, (void**)ptr_s[0],
                           (void*)ptr_s[0], size, 1);
    CU_ASSERT_TRUE(plat_mem_debug_check_free(global_mem_debug,
                                             (void*)ptr_s[0], size));
}

void
test_check_referenced(void)
{
    size_t size = BLOCK_SIZE;

    /** at beginning, all regions are in free stat */
    CU_ASSERT_FALSE(plat_mem_debug_check_referenced(global_mem_debug,
                                             (void*)ptr_s[2], size));
    CU_ASSERT_FALSE(plat_mem_debug_check_referenced(global_mem_debug,
                                             (void*)ptr_s[6], size));
    CU_ASSERT_FALSE(plat_mem_debug_check_referenced(global_mem_debug,
                                             (void*)ptr_s[8], size));

    /**
     * when the region is referenced, check free return 0.
     * otherwise, it return 1.
     */
    plat_mem_debug_reference(global_mem_debug, (void**)ptr_s[0],
                             (void*)ptr_s[0], size, 1, 0);
    CU_ASSERT_TRUE(plat_mem_debug_check_referenced(global_mem_debug,
                                             (void*)ptr_s[0], size));
    plat_mem_debug_release(global_mem_debug, (void**)ptr_s[0],
                           (void*)ptr_s[0], size, 1);
    CU_ASSERT_FALSE(plat_mem_debug_check_referenced(global_mem_debug,
                                             (void*)ptr_s[0], size));
}

void
test_mem_debug_stats(void)
{
    int size = BLOCK_SIZE;

    /** at beginning, no object, no reference */
    plat_mem_debug_get_stats(global_mem_debug, &mem_debug_stats);
    CU_ASSERT_EQUAL(mem_debug_stats.reference_count, 0);
    CU_ASSERT_EQUAL(mem_debug_stats.object_count, 0);

    /**
     * do several times of reference and release, check whether the
     * stats are right or not.
     */
    plat_mem_debug_reference(global_mem_debug, (void**)ptr_s[0],
                             (void*)ptr_s[0], size, 1, 0);
    plat_mem_debug_get_stats(global_mem_debug, &mem_debug_stats);
    CU_ASSERT_EQUAL(mem_debug_stats.reference_count, 1);
    CU_ASSERT_EQUAL(mem_debug_stats.object_count, 1);

    plat_mem_debug_reference(global_mem_debug, (void**)ptr_s[0],
                             (void*)ptr_s[0], size, 1, 0);
    plat_mem_debug_reference(global_mem_debug, (void**)ptr_s[0],
                             (void*)ptr_s[0], size, 1, 0);
    plat_mem_debug_get_stats(global_mem_debug, &mem_debug_stats);
    CU_ASSERT_EQUAL(mem_debug_stats.reference_count, 3);
    CU_ASSERT_EQUAL(mem_debug_stats.object_count, 1);

    plat_mem_debug_reference(global_mem_debug, (void**)ptr_s[5],
                             (void*)ptr_s[5], size, 1, 0);
    plat_mem_debug_reference(global_mem_debug, (void**)ptr_s[5],
                             (void*)ptr_s[5], size, 1, 0);
    plat_mem_debug_get_stats(global_mem_debug, &mem_debug_stats);
    CU_ASSERT_EQUAL(mem_debug_stats.reference_count, 5);
    CU_ASSERT_EQUAL(mem_debug_stats.object_count, 2);

    plat_mem_debug_release(global_mem_debug, (void**)ptr_s[0],
                           (void*)ptr_s[0], size, 1);
    plat_mem_debug_get_stats(global_mem_debug, &mem_debug_stats);
    CU_ASSERT_EQUAL(mem_debug_stats.reference_count, 4);
    CU_ASSERT_EQUAL(mem_debug_stats.object_count, 2);

    plat_mem_debug_reference(global_mem_debug, (void**)ptr_s[8],
                             (void*)ptr_s[8], size, 1, 0);
    plat_mem_debug_get_stats(global_mem_debug, &mem_debug_stats);
    CU_ASSERT_EQUAL(mem_debug_stats.reference_count, 5);
    CU_ASSERT_EQUAL(mem_debug_stats.object_count, 3);

    plat_mem_debug_release(global_mem_debug, (void**)ptr_s[8],
                           (void*)ptr_s[8], size, 1);
    plat_mem_debug_get_stats(global_mem_debug, &mem_debug_stats);
    CU_ASSERT_EQUAL(mem_debug_stats.reference_count, 4);
    CU_ASSERT_EQUAL(mem_debug_stats.object_count, 2);

    plat_mem_debug_release(global_mem_debug, (void**)ptr_s[0],
                           (void*)ptr_s[0], size, 1);
    plat_mem_debug_release(global_mem_debug, (void**)ptr_s[0],
                           (void*)ptr_s[0], size, 1);
    plat_mem_debug_get_stats(global_mem_debug, &mem_debug_stats);
    CU_ASSERT_EQUAL(mem_debug_stats.reference_count, 2);
    CU_ASSERT_EQUAL(mem_debug_stats.object_count, 1);

    plat_mem_debug_release(global_mem_debug, (void**)ptr_s[5],
                           (void*)ptr_s[5], size, 1);
    plat_mem_debug_release(global_mem_debug, (void**)ptr_s[5],
                           (void*)ptr_s[5], size, 1);
    plat_mem_debug_get_stats(global_mem_debug, &mem_debug_stats);
    CU_ASSERT_EQUAL(mem_debug_stats.reference_count, 0);
    CU_ASSERT_EQUAL(mem_debug_stats.object_count, 0);

    /**
     * ensure that all the test regions are released.
     */
    CU_ASSERT_TRUE(plat_mem_debug_check_free(global_mem_debug,
                                             (void*)ptr_s[0], size));
    CU_ASSERT_TRUE(plat_mem_debug_check_free(global_mem_debug,
                                             (void*)ptr_s[5], size));
    CU_ASSERT_TRUE(plat_mem_debug_check_free(global_mem_debug,
                                             (void*)ptr_s[8], size));
}

/**
 * use to check the regions to reference or release are not be
 * referenced.
 *
 * @param ptr1
 * @param ptr2
 * @param ptr3
 * @param size
 */
static void
check_init(void *ptr1, void *ptr2, void *ptr3, size_t size)
{
    CU_ASSERT_FALSE(plat_mem_debug_check_referenced(global_mem_debug,
                                                   (void*)ptr1, size));
    CU_ASSERT_FALSE(plat_mem_debug_check_referenced(global_mem_debug,
                                               (void*)ptr2, size));
    CU_ASSERT_FALSE(plat_mem_debug_check_referenced(global_mem_debug,
                                               (void*)ptr3, size));

    CU_ASSERT_TRUE(plat_mem_debug_check_free(global_mem_debug,
                                             (void*)ptr1, size));
    CU_ASSERT_TRUE(plat_mem_debug_check_free(global_mem_debug,
                                             (void*)ptr2, size));
    CU_ASSERT_TRUE(plat_mem_debug_check_free(global_mem_debug,
                                             (void*)ptr3, size));
}

/**
 * test referencing with 3 regins in same size. It'a only a
 * wrapping function.
 *
 * @param ptr1
 * @param ptr2
 * @param ptr3
 * @param size
 * @param writeable
 */
static void
reference_test_cmn_with_same_size(void *ptr1, void *ptr2, void *ptr3, size_t size, int writeable)
{
    check_init(ptr1, ptr2, ptr3, size);

    /**
     * reference the regions and check whether they are referenced.
     */
    plat_mem_debug_reference(global_mem_debug, (void**)ptr1,
                             (void*)ptr1, size, writeable, 0);
    CU_ASSERT_TRUE(plat_mem_debug_check_referenced(global_mem_debug,
                                                   (void*)ptr1, size));
    plat_mem_debug_reference(global_mem_debug, (void**)ptr1,
                             (void*)ptr1, size, writeable, 0);
    CU_ASSERT_TRUE(plat_mem_debug_check_referenced(global_mem_debug,
                                                   (void*)ptr1, size));

    plat_mem_debug_reference(global_mem_debug, (void**)ptr2,
                             (void*)ptr2, size, writeable, 0);
    CU_ASSERT_TRUE(plat_mem_debug_check_referenced(global_mem_debug,
                                                   (void*)ptr2, size));
    plat_mem_debug_reference(global_mem_debug, (void**)ptr2,
                             (void*)ptr2, size, writeable, 0);
    CU_ASSERT_TRUE(plat_mem_debug_check_referenced(global_mem_debug,
                                                   (void*)ptr2, size));

    plat_mem_debug_reference(global_mem_debug, (void**)ptr3,
                             (void*)ptr3, size, writeable, 0);
    CU_ASSERT_TRUE(plat_mem_debug_check_referenced(global_mem_debug,
                                                   (void*)ptr3, size));

    /** output the log message */
    plat_mem_debug_log_references(global_mem_debug, mem_debug_config.log_category,
                                  PLAT_LOG_LEVEL_TRACE, 100);

    /**
     * ensure that all the test regions are released.
     */
    plat_mem_debug_release(global_mem_debug, (void**)ptr1,
                           (void*)ptr1, size, writeable);
    plat_mem_debug_release(global_mem_debug, (void**)ptr1,
                           (void*)ptr1, size, writeable);

    plat_mem_debug_release(global_mem_debug, (void**)ptr2,
                           (void*)ptr2, size, writeable);
    plat_mem_debug_release(global_mem_debug, (void**)ptr2,
                           (void*)ptr2, size, writeable);

    plat_mem_debug_release(global_mem_debug, (void**)ptr3,
                           (void*)ptr3, size, writeable);
}

/**
 * test releasing with 3 regins in same size. It'a only a
 * wrapping function. In orde not to effect the other tests, so
 * need make sure that reference and release are paired.
 *
 * @param ptr1
 * @param ptr2
 * @param ptr3
 * @param size
 * @param writeable
 */
static void
release_test_cmn_with_same_size(void *ptr1, void *ptr2, void *ptr3, size_t size, int writeable)
{
    check_init(ptr1, ptr2, ptr3, size);

    /**
     * reference first, and then relealse. make sure the stats of
     * the reigons are right.
     */
    plat_mem_debug_reference(global_mem_debug, (void**)ptr1,
                             (void*)ptr1, size, writeable, 0);
    CU_ASSERT_TRUE(plat_mem_debug_check_referenced(global_mem_debug,
                                                   (void*)ptr1, size));
    plat_mem_debug_release(global_mem_debug, (void**)ptr1,
                           (void*)ptr1, size, writeable);
    CU_ASSERT_TRUE(plat_mem_debug_check_free(global_mem_debug,
                                             (void*)ptr1, size))

    plat_mem_debug_reference(global_mem_debug, (void**)ptr2,
                             (void*)ptr2, size, writeable, 0);
    plat_mem_debug_reference(global_mem_debug, (void**)ptr2,
                             (void*)ptr2, size, writeable, 0);
    CU_ASSERT_TRUE(plat_mem_debug_check_referenced(global_mem_debug,
                                                   (void*)ptr2, size));

    plat_mem_debug_release(global_mem_debug, (void**)ptr2,
                           (void*)ptr2, size, writeable);
    plat_mem_debug_release(global_mem_debug, (void**)ptr2,
                           (void*)ptr2, size, writeable);
    CU_ASSERT_TRUE(plat_mem_debug_check_free(global_mem_debug,
                                             (void*)ptr2, size));

    plat_mem_debug_reference(global_mem_debug, (void**)ptr3,
                             (void*)ptr3, size, writeable, 0);
    CU_ASSERT_TRUE(plat_mem_debug_check_referenced(global_mem_debug,
                                                   (void*)ptr3, size));
    plat_mem_debug_release(global_mem_debug, (void**)ptr3,
                           (void*)ptr3, size, writeable);
    CU_ASSERT_TRUE(plat_mem_debug_check_free(global_mem_debug,
                                             (void*)ptr3, size));
}

void
test_reference_with_seperated_block(void)
{
    reference_test_cmn_with_same_size(ptr_s[8], ptr_s[3], ptr_s[5], BLOCK_SIZE, 1);
    reference_test_cmn_with_same_size(ptr_s[5], ptr_s[9], ptr_s[7], BLOCK_SIZE, 1);
    reference_test_cmn_with_same_size(ptr_s[1], ptr_s[3], ptr_s[5], BLOCK_SIZE, 0);
    reference_test_cmn_with_same_size(ptr_s[4], ptr_s[7], ptr_s[0], BLOCK_SIZE, 0);
}

void
test_release_with_seperated_block(void)
{
    release_test_cmn_with_same_size(ptr_s[0], ptr_s[5], ptr_s[8], BLOCK_SIZE, 1);
    release_test_cmn_with_same_size(ptr_s[2], ptr_s[5], ptr_s[7], BLOCK_SIZE, 1);
    release_test_cmn_with_same_size(ptr_s[3], ptr_s[7], ptr_s[9], BLOCK_SIZE, 0);
    release_test_cmn_with_same_size(ptr_s[6], ptr_s[9], ptr_s[2], BLOCK_SIZE, 0);
}

void
test_reference_with_near_block(void)
{
    reference_test_cmn_with_same_size(ptr_s[0], ptr_s[1], ptr_s[2], BLOCK_SIZE, 1);
    reference_test_cmn_with_same_size(ptr_s[2], ptr_s[3], ptr_s[4], BLOCK_SIZE, 1);
    reference_test_cmn_with_same_size(ptr_s[4], ptr_s[5], ptr_s[6], BLOCK_SIZE, 0);
    reference_test_cmn_with_same_size(ptr_s[6], ptr_s[7], ptr_s[8], BLOCK_SIZE, 0);
}

void
test_release_with_near_block(void)
{
    release_test_cmn_with_same_size(ptr_s[0], ptr_s[1], ptr_s[2],BLOCK_SIZE, 1);
    release_test_cmn_with_same_size(ptr_s[2], ptr_s[3], ptr_s[4],BLOCK_SIZE, 1);
    release_test_cmn_with_same_size(ptr_s[4], ptr_s[5], ptr_s[6],BLOCK_SIZE, 0);
    release_test_cmn_with_same_size(ptr_s[6], ptr_s[7], ptr_s[8],BLOCK_SIZE, 0);
}

void
test_reference_release_with_diff_size(void)
{
    int writeable = 1;

    char *ptr1 = mem_pool_aligned;
    char *ptr2 = mem_pool_aligned + 3 * BLOCK_SIZE;
    char *ptr3 = mem_pool_aligned + 6 * BLOCK_SIZE;

    int size1 = BLOCK_SIZE;
    int size2 = 3 * BLOCK_SIZE;
    int size3 = 2 * BLOCK_SIZE;

    /**
     * make sure that all the test regions are not referenced.
     */
    CU_ASSERT_FALSE(plat_mem_debug_check_referenced(global_mem_debug,
                                                   (void*)ptr1, size1));
    CU_ASSERT_FALSE(plat_mem_debug_check_referenced(global_mem_debug,
                                               (void*)ptr2, size2));
    CU_ASSERT_FALSE(plat_mem_debug_check_referenced(global_mem_debug,
                                               (void*)ptr3, size3));

    plat_mem_debug_reference(global_mem_debug, (void**)ptr1,
                             (void*)ptr1, size1, writeable, 0);
    CU_ASSERT_TRUE(plat_mem_debug_check_referenced(global_mem_debug,
                                                   (void*)ptr1, size1));
    plat_mem_debug_reference(global_mem_debug, (void**)ptr1,
                             (void*)ptr1, size1, writeable, 0);
    CU_ASSERT_TRUE(plat_mem_debug_check_referenced(global_mem_debug,
                                                   (void*)ptr1, size1));

    plat_mem_debug_reference(global_mem_debug, (void**)ptr2,
                             (void*)ptr2, size2, writeable, 0);
    CU_ASSERT_TRUE(plat_mem_debug_check_referenced(global_mem_debug,
                                                   (void*)ptr2, size2));
    plat_mem_debug_reference(global_mem_debug, (void**)ptr2,
                             (void*)ptr2, size2, writeable, 0);
    CU_ASSERT_TRUE(plat_mem_debug_check_referenced(global_mem_debug,
                                                   (void*)ptr2, size2));

    plat_mem_debug_reference(global_mem_debug, (void**)ptr3,
                             (void*)ptr3, size3, writeable, 0);
    CU_ASSERT_TRUE(plat_mem_debug_check_referenced(global_mem_debug,
                                                   (void*)ptr3, size3));

    plat_mem_debug_release(global_mem_debug, (void**)ptr1,
                           (void*)ptr1, size1, writeable);
    plat_mem_debug_release(global_mem_debug, (void**)ptr1,
                           (void*)ptr1, size1, writeable);
    CU_ASSERT_TRUE(plat_mem_debug_check_free(global_mem_debug,
                                             (void*)ptr1, size1));

    plat_mem_debug_release(global_mem_debug, (void**)ptr2,
                           (void*)ptr2, size2, writeable);
    plat_mem_debug_release(global_mem_debug, (void**)ptr2,
                           (void*)ptr2, size2, writeable);
    CU_ASSERT_TRUE(plat_mem_debug_check_free(global_mem_debug,
                                             (void*)ptr2, size2));

    plat_mem_debug_release(global_mem_debug, (void**)ptr3,
                           (void*)ptr3, size3, writeable);
    CU_ASSERT_TRUE(plat_mem_debug_check_free(global_mem_debug,
                                             (void*)ptr3, size3));
}

/**
 * test adding another memory pool.
 */
void
test_with_two_pool(void)
{
    int i;
    char *mem_pool2_aligned;

    mem_pool2 = (char*)plat_alloc(MEM_POOL_SIZE);
    if (mem_pool2 == NULL) {
        printf("out of memory\n");
        plat_exit(1);
    }
    mem_pool2_aligned = round_up(mem_pool2, page_size);
    for (i = 0; i < BLOCK_COUNT; i++) {
        ptr_d[i] = mem_pool2_aligned + i * BLOCK_SIZE;
    }

    plat_mem_debug_add_unused(global_mem_debug, (void*)mem_pool2, MEM_POOL_SIZE);
    reference_test_cmn_with_same_size(ptr_d[8], ptr_d[3], ptr_d[5], BLOCK_SIZE, 1);
    reference_test_cmn_with_same_size(ptr_d[1], ptr_d[3], ptr_d[5], BLOCK_SIZE, 0);

    release_test_cmn_with_same_size(ptr_d[2], ptr_d[5], ptr_d[7], BLOCK_SIZE, 1);
    release_test_cmn_with_same_size(ptr_s[6], ptr_s[9], ptr_s[2], BLOCK_SIZE, 0);

    reference_test_cmn_with_same_size(ptr_s[2], ptr_s[3], ptr_s[4], BLOCK_SIZE, 1);
    reference_test_cmn_with_same_size(ptr_d[6], ptr_d[7], ptr_d[8], BLOCK_SIZE, 0);

    release_test_cmn_with_same_size(ptr_d[0], ptr_d[1], ptr_d[2],BLOCK_SIZE, 1);
    release_test_cmn_with_same_size(ptr_s[4], ptr_s[5], ptr_s[6],BLOCK_SIZE, 0);
}

/**
 * It's diffcult to test this automaticly. It need output the
 * log to the console bu reduce the level of the log. It need to
 * be checked manually. We have check it. So it doesn't output
 * the log defualt.
 */
void
test_log_reference(void)
{
    test_reference_with_seperated_block();
}

/**
 * test some error reference. When doing wrong reference, the
 * memory debug tool will output some error log. In orde not to
 * output thest log, we forbid it by increasing the log level.
 * And we check the internal stats instead.We test 2 error
 * reference:
 *  (1)overlap region
 *  (2)reference the region which is out of the memory pool.
 *  (3)the writeable attribute doesn't match when referencing
 *  and releasing.
 */
void
test_reference_failture(void)
{
    int writeable = 1;
    size_t size = BLOCK_SIZE;

    char *ptr1 = ptr_s[0];
    char *ptr2 = ptr_s[1];
    char *ptr3 = ptr_s[9];
    char *ptr4 = mem_pool + 100;

    check_init(ptr1, ptr2, ptr3, size);

    /**
     * reference incorrect regions, and check the internal stats.
     */
    plat_mem_debug_reference(global_mem_debug, (void**)ptr1,
                             (void*)ptr1, size, writeable, 0);
    CU_ASSERT_TRUE(plat_mem_debug_check_referenced(global_mem_debug,
                                                   (void*)ptr1, size));
    plat_mem_debug_reference(global_mem_debug, (void**)ptr1,
                             (void*)(ptr1 + 1), size + 1, writeable, 0);
    CU_ASSERT_FALSE(plat_mem_debug_check_referenced(global_mem_debug,
                                                    (void*)(ptr1 + 1), size + 1));
    plat_mem_debug_reference(global_mem_debug, (void**)ptr1,
                             (void*)(ptr1 - 100), size + 10, writeable, 0);
    CU_ASSERT_FALSE(plat_mem_debug_check_referenced(global_mem_debug,
                                                    (void*)(ptr1 - 100), size + 10));

    /** test overlap error reference */
    plat_mem_debug_reference(global_mem_debug, (void**)ptr2,
                             (void*)ptr2, size, writeable, 0);
    CU_ASSERT_TRUE(plat_mem_debug_check_referenced(global_mem_debug,
                                                    (void*)ptr2, size));
    plat_mem_debug_reference(global_mem_debug, (void**)ptr2,
                             (void*)(ptr2 + 100), size, writeable, 0);
    CU_ASSERT_FALSE(plat_mem_debug_check_referenced(global_mem_debug,
                                                    (void*)(ptr2 + 100), size));
    plat_mem_debug_reference(global_mem_debug, (void**)ptr2,
                             (void*)(ptr2 - 100), size + 10, writeable, 0);
    CU_ASSERT_FALSE(plat_mem_debug_check_referenced(global_mem_debug,
                                                    (void*)(ptr2 - 100), size + 10));

    plat_mem_debug_reference(global_mem_debug, (void**)ptr3,
                             (void*)ptr3, size, writeable, 0);
    CU_ASSERT_TRUE(plat_mem_debug_check_referenced(global_mem_debug,
                                                   (void*)ptr3, size));
    plat_mem_debug_reference(global_mem_debug, (void**)ptr3,
                             (void*)(ptr3 + 100), size + 1000, writeable, 0);
    CU_ASSERT_FALSE(plat_mem_debug_check_referenced(global_mem_debug,
                                                    (void*)(ptr3 + 100), size + 1000));
    plat_mem_debug_reference(global_mem_debug, (void**)ptr3,
                             (void*)(ptr3 - 100), size + 10, writeable, 0);
    CU_ASSERT_FALSE(plat_mem_debug_check_referenced(global_mem_debug,
                                                    (void*)(ptr3 - 100), size + 10));

    /** test reference the region which is out of the memory pool */
    plat_mem_debug_reference(global_mem_debug, (void**)ptr1,
                             (void*)(ptr1 - 100), 50, writeable, 0);
    CU_ASSERT_FALSE(plat_mem_debug_check_referenced(global_mem_debug,
                                                    (void*)(ptr1 - 100), 50));
    plat_mem_debug_reference(global_mem_debug, (void**)ptr1,
                             (void*)ptr4, 50, writeable, 0);
    CU_ASSERT_FALSE(plat_mem_debug_check_referenced(global_mem_debug,
                                                    (void*)ptr4, 50));

    plat_mem_debug_release(global_mem_debug, (void**)ptr1,
                           (void*)ptr1, size, writeable);
    plat_mem_debug_release(global_mem_debug, (void**)ptr2,
                           (void*)ptr2, size, writeable);
    plat_mem_debug_release(global_mem_debug, (void**)ptr3,
                           (void*)ptr3, size, writeable);

    /**
     *  test reference and release with not match writeable
     *  attribute paramater
     */
    plat_mem_debug_reference(global_mem_debug, (void**)ptr1,
                             (void*)ptr1, size, writeable, 0);
    CU_ASSERT_TRUE(plat_mem_debug_check_referenced(global_mem_debug,
                                                   (void*)ptr1, size));
    writeable = 0;
    plat_mem_debug_release(global_mem_debug, (void**)ptr1,
                           (void*)ptr1, size, writeable);
    CU_ASSERT_FALSE(plat_mem_debug_check_free(global_mem_debug,
                                              (void*)ptr1, size));
    writeable = 1;
    plat_mem_debug_release(global_mem_debug, (void**)ptr1,
                           (void*)ptr1, size, writeable);
    CU_ASSERT_TRUE(plat_mem_debug_check_free(global_mem_debug,
                                              (void*)ptr1, size));
}

/**
 * test some error release:
 * (1)overlap region
 * (2)release the region which is out of the memory pool.
 */
void
test_release_failture(void)
{
    int writeable = 1;
    size_t size = BLOCK_SIZE;

    char *ptr1 = ptr_s[0];
    char *ptr2 = ptr_s[1];
    char *ptr3 = ptr_s[9];
    char *ptr4 = mem_pool + 100;

    check_init(ptr1, ptr2, ptr3, size);

    /**
     * release incorrect regions, and check the internal stats.
     */
    plat_mem_debug_release(global_mem_debug, (void**)ptr1,
                           (void*)ptr1, size, writeable);
    plat_mem_debug_release(global_mem_debug, (void**)ptr2,
                           (void*)ptr2, size, writeable);

    /** test overlap error release */
    plat_mem_debug_reference(global_mem_debug, (void**)ptr1,
                             (void*)ptr1, size, writeable, 0);
    CU_ASSERT_TRUE(plat_mem_debug_check_referenced(global_mem_debug,
                                                    (void*)ptr1, size));
    CU_ASSERT_FALSE(plat_mem_debug_check_free(global_mem_debug,
                                              (void*)ptr1, size));
    plat_mem_debug_release(global_mem_debug, (void**)ptr1,
                          (void*)(ptr1 + 1), size + 1, writeable);
    CU_ASSERT_FALSE(plat_mem_debug_check_free(global_mem_debug,
                                              (void*)(ptr1 + 1), size + 1));
    plat_mem_debug_release(global_mem_debug, (void**)ptr1,
                          (void*)(ptr1 - 100), size + 10, writeable);
    CU_ASSERT_FALSE(plat_mem_debug_check_free(global_mem_debug,
                                              (void*)(ptr1 - 100), size + 10));

    /** test release the region which is out of the memory pool */
    plat_mem_debug_release(global_mem_debug, (void**)ptr1,
                             (void*)(ptr1 - 100), 50, writeable);
    CU_ASSERT_FALSE(plat_mem_debug_check_free(global_mem_debug,
                                                    (void*)(ptr1 - 100), 50));
    plat_mem_debug_release(global_mem_debug, (void**)ptr1,
                             (void*)ptr4, 50, writeable);
    CU_ASSERT_FALSE(plat_mem_debug_check_free(global_mem_debug,
                                                    (void*)ptr4, 50));

    plat_mem_debug_reference(global_mem_debug, (void**)ptr2,
                             (void*)ptr2, size, writeable, 0);
    CU_ASSERT_TRUE(plat_mem_debug_check_referenced(global_mem_debug,
                                                    (void*)ptr2, size));
    plat_mem_debug_release(global_mem_debug, (void**)ptr2,
                          (void*)(ptr2 - 100), size, writeable);
    CU_ASSERT_FALSE(plat_mem_debug_check_free(global_mem_debug,
                                             (void*)(ptr2 - 100), size));
    plat_mem_debug_release(global_mem_debug, (void**)ptr2,
                          (void*)(ptr2 - 100), size + 10, writeable);
    CU_ASSERT_FALSE(plat_mem_debug_check_free(global_mem_debug,
                                             (void*)(ptr2 - 100), size + 10));

    plat_mem_debug_reference(global_mem_debug, (void**)ptr3,
                             (void*)ptr3, size, writeable, 0);
    CU_ASSERT_TRUE(plat_mem_debug_check_referenced(global_mem_debug,
                                                   (void*)ptr3, size));
    plat_mem_debug_release(global_mem_debug, (void**)ptr3,
                          (void*)(ptr3 + 100), size + 1000, writeable);
    CU_ASSERT_FALSE(plat_mem_debug_check_free(global_mem_debug,
                                             (void*)(ptr3 + 100), size + 1000));
    plat_mem_debug_release(global_mem_debug, (void**)ptr3,
                          (void*)(ptr3 - 100), size + 10, writeable);
    CU_ASSERT_FALSE(plat_mem_debug_check_free(global_mem_debug,
                                             (void*)(ptr3 - 100), size + 10));

    plat_mem_debug_release(global_mem_debug, (void**)ptr1,
                           (void*)ptr1, size, writeable);
    plat_mem_debug_release(global_mem_debug, (void**)ptr2,
                           (void*)ptr2, size, writeable);
    plat_mem_debug_release(global_mem_debug, (void**)ptr3,
                           (void*)ptr3, size, writeable);
}

/**
 * test whether the memory protect function is right or not.
 */
void
test_read_write(void)
{
    int writeable = 1;
    size_t size = BLOCK_SIZE;
    char c;
    char *ptr1 = ptr_s[0];
    int status;

    /* Test overrun */
    memset(&action, 0, sizeof(action));
    action.sa_handler = &sighandler;
    status = plat_sigaction(SIGSEGV, &action, NULL);
    CU_ASSERT_TRUE(!status);

    CU_ASSERT_FALSE(plat_mem_debug_check_referenced(global_mem_debug,
                                                  (void*)ptr1, size));
    CU_ASSERT_TRUE(plat_mem_debug_check_free(global_mem_debug,
                                              (void*)ptr1, size));

    plat_mem_debug_reference(global_mem_debug, (void**)ptr1,
                            (void*)ptr1, size, writeable, 0);
    CU_ASSERT_TRUE(plat_mem_debug_check_referenced(global_mem_debug,
                                                  (void*)ptr1, size));
    /** read and write are all right */
    memset(ptr1, 0, size);
    ptr1[10] = '5';
    ptr1[size - 1] = '8';
    ptr1[0] = '9';
    c = ptr1[0];
    c = ptr1[10];
    c = ptr1[size -1];

    plat_mem_debug_release(global_mem_debug, (void**)ptr1,
                          (void*)ptr1, size, writeable);
    CU_ASSERT_TRUE(plat_mem_debug_check_free(global_mem_debug,
                                              (void*)ptr1, size));

    /*
     * Allow return from signal handler which doesn't redo the failed
     * operation and unblocks SIGSEGV.
     */
    if (!sigsetjmp(buf, 1)) {
        memset(ptr1, 0, size);
    }
    CU_ASSERT(got_signal == SIGSEGV);
    if (!sigsetjmp(buf, 1)) {
        ptr1[0] = '2';
    }
    CU_ASSERT(got_signal == SIGSEGV);
    if (!sigsetjmp(buf, 1)) {
       c = ptr1[0];
    }
    CU_ASSERT(got_signal == SIGSEGV);

    action.sa_handler = SIG_DFL;
    status = plat_sigaction(SIGSEGV, &action, NULL);
    CU_ASSERT_TRUE(!status);

    /** change the region to read only */
    writeable = 0;
    plat_mem_debug_reference(global_mem_debug, (void**)ptr1,
                            (void*)ptr1, size, writeable, 0);
    CU_ASSERT_TRUE(plat_mem_debug_check_referenced(global_mem_debug,
                                                  (void*)ptr1, size));
    c = ptr1[0];
    c = ptr1[size - 1];

    /**
     *  run here will not cause segmentation fault, because the tool
     *  can't do the read only protecting. It only record the stat
     *  in read write or read only.
     */
    ptr1[0] = '6';
    ptr1[size - 1] = '3';

    plat_mem_debug_release(global_mem_debug, (void**)ptr1,
                          (void*)ptr1, size, writeable);
    CU_ASSERT_TRUE(plat_mem_debug_check_free(global_mem_debug,
                                              (void*)ptr1, size));
}

CU_TestInfo tests_mem_debug[] = {
    { "test_mem_debug_init", test_mem_debug_init },
    { "test_check_free", test_check_free },
    { "test_check_referenced", test_check_referenced },
    { "test_mem_debug_stats", test_mem_debug_stats },
    { "test_reference_with_seperated_block", test_reference_with_seperated_block},
    { "test_release_with_seperated_block", test_release_with_seperated_block},
    { "test_reference_with_near_block", test_reference_with_near_block},
    { "test_release_with_near_block", test_release_with_near_block},
    { "test_reference_release_with_diff_size", test_reference_release_with_diff_size},
    { "test_with_two_pool", test_with_two_pool},
    { "test_log_reference", test_log_reference},
    { "test_reference_failture", test_reference_failture},
    { "test_release_failture", test_release_failture},
    { "test_read_write", test_read_write},
    CU_TEST_INFO_NULL,
};

