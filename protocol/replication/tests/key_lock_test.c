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
 * File:   $URL: svn://svn.schoonerinfotech.net/schooner-trunk/trunk/sdf/protocol/replication/tests/key_lock_test.c $
 * Author: drew
 *
 * Created on February 18, 2010
 *
 * (c) Copyright 2010, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: key_lock_test.c 15015 2010-11-10 23:09:06Z briano $
 */

/** Test replicator_key_lock functionality */

#include "sys/queue.h"

#include "platform/assert.h"
#include "platform/logging.h"
#define PLAT_OPTS_NAME(name) name ## _key_lock_test
#include "platform/opts.h"
#include "platform/shmem.h"

#include "fth/fth.h"
#include "fth/fthMbox.h"
#include "fth/fthOpt.h"

#include "common/sdftypes.h"
#include "protocol/replication/key_lock.h"

/*
 * We use a sub-category under test because test implies a huge number
 * of log messages out of simulated networking, flash, etc.
 */
PLAT_LOG_SUBCAT_LOCAL(LOG_CAT, PLAT_LOG_CAT_SDF_PROT_REPLICATION,
                      "test/case");

/* Command line arguments */
#define PLAT_OPTS_ITEMS_key_lock_test()                             \
    item("incremental", "pass when the test gets to the last known good part", \
         INCREMENTAL, ({ config->incremental = 1; 0; }), PLAT_OPTS_ARG_NO)     \
    item("ffdc_disable", "disable FFDC logging", FFDC_DISABLE,                 \
         ({ config->ffdc_disable = 1; 0; }), PLAT_OPTS_ARG_NO)                 \
    PLAT_OPTS_SHMEM(shmem)                                                     \
    PLAT_OPTS_FTH()

struct plat_opts_config_key_lock_test {
    struct plat_shmem_config shmem;

    /** @brief Hook so partialy working tests can be checked in */
    unsigned incremental : 1;

    /** @brief FFDC buffer len in bytes */
    int64_t ffdc_buffer_len;

    /** @brief disable FFDC logging */
    unsigned ffdc_disable : 1;
};

struct test_state {
    const struct plat_opts_config_key_lock_test *config;

    struct replicator_key_lock_container *lock_container;

    /** @brief Number of shared locks held */
    int lock_count_shared;

    /** @brief Number of exclusive locks held */
    int lock_count_exclusive;

    /** @brief Number of recovery locks held */
    int lock_count_recovery;

    /** @brief Number of locks waiting */
    int waiting_count;

    TAILQ_HEAD(, test_lock_attempt) attempt_list;
};

/** @brief State for first multi-threaded test */
struct thread_test_1_state {
    /** @brief Test suite state */
    struct test_state *state;

    /**
     * @brief t1 blocks
     *
     * Thread 1 blocks until before thread 2 acquires its lock, with
     * thread 2 signally thread 1 for wake up and thread 1 dispatching
     * after thread 2 blocks
     */
    struct fthMbox t1_block;

    /** @brief t1's recovery get */
    struct rklc_get *t1_get;

    /** @brief t1's recovery attempt */
    struct test_lock_attempt *t1_attempt;

    /** @brief Thread 2 lock attempt */
    struct test_lock_attempt *t2_attempt;
};

struct test_lock_attempt {
    struct test_state *state;

    /** @brief name, owned by caller */
    const char *name;

    SDF_simple_key_t key;

    enum rkl_mode mode;

    SDF_status_t status;

    struct replicator_key_lock *key_lock;

    unsigned lock_complete : 1;

    unsigned unlocked : 1;

    TAILQ_ENTRY(test_lock_attempt) attempt_list_entry;
};

static const char key1[] = "key1";
static const char key2[] = "key2";
static const char key3[] = "key3";

static void test_main(uint64_t arg);
static void thread_test_1(struct test_state *suite_state);
static void thread_test_1_t2(uint64_t arg);

static struct test_state *test_state_alloc(const struct plat_opts_config_key_lock_test *config);
static void test_state_free(struct test_state *state);
static struct test_lock_attempt *test_lock(struct test_state *state,
                                           const char *op_name,
                                           const char *key,
                                           enum rkl_mode mode);
static void test_lock_sync(struct test_state *state, const char *name,
                           const char *key, enum rkl_mode mode,
                           struct test_lock_attempt **attempt_out);
static struct test_lock_attempt *test_lock_common(struct test_state *state,
                                                  const char *name,
                                                  const char *key,
                                                  enum rkl_mode mode);
static void test_lock_attempt_free(struct test_lock_attempt *attempt);
static void test_lock_attempt_unlock(struct test_lock_attempt *attempt);
static void test_lock_attempt_lock_cb(struct plat_closure_scheduler *context,
                                      void *env, SDF_status_t status_arg,
                                      struct replicator_key_lock *key_lock);
static int test_lock_attempt_count(struct test_lock_attempt *attempt,
                                   int increment);

int
main(int argc, char **argv) {
    struct plat_opts_config_key_lock_test config;
    int status;
    struct test_state *state;

    memset(&config, 0, sizeof(config));
    plat_shmem_config_init(&config.shmem);
    config.ffdc_buffer_len = FFDC_THREAD_BUFSIZE;

    if (plat_opts_parse_key_lock_test(&config, argc, argv)) {
        return (2);
    }

    status = plat_shmem_prototype_init(&config.shmem);
    plat_assert_always(!status);

    status = plat_shmem_attach(plat_shmem_config_get_path(&config.shmem));
    plat_assert_always(!status);

    if (!config.ffdc_disable) {
        status = ffdc_initialize(0 /* read-write */, BLD_VERSION,
                                 config.ffdc_buffer_len);
        plat_assert_always(!status);
    }

    fthInit();

    state = test_state_alloc(&config);
    plat_assert_always(state);

    XResume(fthSpawn(&test_main, 40960), (uint64_t)state);
    fthSchedulerPthread(0);

    test_state_free(state);

    if (!config.ffdc_disable) {
        ffdc_detach();
    }

    status = plat_shmem_detach();
    plat_assert_always(!status);

    plat_shmem_config_destroy(&config.shmem);

    return (0);
}

/*
 * FIXME: drew 2010-02-25 Add exclusive lock which blocks on recovery lock
 * in threaded test implementation.
 */
static void
test_main(uint64_t arg) {
    struct test_state *state;
    /*
     * XXX: drew 2010-02-25 This is getting unmanagable.  Only keep enough
     * pointers arround so we can release our locks.
     */
    struct test_lock_attempt *attempt[21] = {};
    struct rklc_get *get[3] = {};

    state = (struct test_state *)arg;

    /* Uncontested exclusive - must pass */
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "op 1 exclusive lock %s", key1);
    attempt[0] = test_lock(state, "op 1 exclusive", key1,
                           RKL_MODE_EXCLUSIVE);
    plat_assert(attempt[0] && attempt[0]->lock_complete &&
                attempt[0]->status == SDF_SUCCESS);
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "op 1 exclusive lock %s complete", key1);

    /* Shared lock contests existing exclusive - must block */
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "op 2 shared lock %s", key1);
    attempt[1] = test_lock(state, "op 2 shared", key1,
                           RKL_MODE_SHARED);
    plat_assert(attempt[1] && !attempt[1]->lock_complete);

    /* Second shared lock contests existing exclusive - must block */
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "op 3 shared lock %s", key1);
    attempt[2] = test_lock(state, "op 3 shared", key1, RKL_MODE_SHARED);
    plat_assert(attempt[2] && !attempt[2]->lock_complete);

    /* Release first lock, second and third shared must both succeed */
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "op 1 unlock %s", key1);
    test_lock_attempt_unlock(attempt[0]);
    plat_assert(attempt[1]->lock_complete && attempt[1]->status == SDF_SUCCESS);
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "op 2 shared lock %s complete", key1);
    plat_assert(attempt[2]->lock_complete && attempt[2]->status == SDF_SUCCESS);
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "op 3 shared lock %s complete", key1);

    /* Exclusive lock contests existing shared - must block */
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "op 4 exclusive lock %s", key1);
    attempt[3] = test_lock(state, "op 4 exclusive", key1,
                           RKL_MODE_EXCLUSIVE);
    plat_assert(attempt[3] && !attempt[3]->lock_complete);

    /*
     * Second exclusive lock contests existing shared - must block until
     * held shared locks and previous exclusive lock complete.
     */
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "op 5 exclusive lock %s", key1);
    attempt[4] = test_lock(state, "op 5 exclusive", key1,
                           RKL_MODE_EXCLUSIVE);
    plat_assert(attempt[4] && !attempt[4]->lock_complete);

    /*
     * Releasing first of two shared locks must not unblock either
     * blocked exclusive lock
     */
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "op 3 unlock %s", key1);
    test_lock_attempt_unlock(attempt[2]);
    plat_assert(!attempt[3]->lock_complete);
    plat_assert(!attempt[4]->lock_complete);

    /* First queued exclusive lock must win on second shared lock release */
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "op 2 unlock %s", key1);
    test_lock_attempt_unlock(attempt[1]);
    plat_assert(attempt[3]->lock_complete && attempt[3]->status == SDF_SUCCESS);
    plat_assert(!attempt[4]->lock_complete);
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "op 4 exclusive lock %s complete", key1);

    /* Queued  exclusive must complete on previous exclusive release */
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "op 4 unlock %s", key1);
    test_lock_attempt_unlock(attempt[3]);
    plat_assert(attempt[4]->lock_complete && attempt[4]->status == SDF_SUCCESS);
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "op 5 exclusive lock %s complete", key1);

    /* Recovery lock started with existing exclusive lock must fail */
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "op 6 recovery lock %s", key1);
    attempt[5] = test_lock(state, "op 6 recovery", key1, RKL_MODE_RECOVERY);
    plat_assert(attempt[5] && attempt[5]->lock_complete &&
                attempt[5]->status == SDF_LOCK_RESERVED);
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "op 6 recovery lock %s failed as expected", key1);


    /*
     * Validate that an exclusive lock started before a get and
     * terminated after the get starts causes a recovery lock to
     * fail.
     */
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "get 1 start");
    get[0] = rklc_start_get(state->lock_container);
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "get 1 started");

    /* Release final exclusive lock */
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "op 5 unlock %s", key1);
    test_lock_attempt_unlock(attempt[4]);

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "op 7 recovery lock %s", key1);
    attempt[6] = test_lock(state, "op 7 recovery", key1, RKL_MODE_RECOVERY);
    plat_assert(attempt[6] && attempt[6]->lock_complete &&
                attempt[6]->status == SDF_LOCK_RESERVED);
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "op 7 recovery lock %s failed as expected", key1);

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "get 1 end");
    rklc_get_complete(get[0]);
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "get 1 complete");

    /*
     * XXX: drew 2010-02-25 Should split the independent test cases
     * out into separate functions for clarity.
     */

    /*
     * Validate that an exclusive lock started after a get and
     * terminated before the get completes causes a recovery
     * lock to fail.
     *
     * This exercises a different code path than the case
     * involving an exclusive lock that existed when the get
     * started.
     */

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "get 2 start");
    get[0] = rklc_start_get(state->lock_container);
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "get 2 started");

    /* Uncontested exclusive - must pass */
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "op 8 exclusive lock %s", key1);
    attempt[7] = test_lock(state, "op 8 exclusive", key1,
                           RKL_MODE_EXCLUSIVE);
    plat_assert(attempt[7] && attempt[7]->lock_complete &&
                attempt[7]->status == SDF_SUCCESS);
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "op 8 exclusive lock %s complete", key1);

    /* Release exclusive lock */
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "op 8 unlock %s", key1);
    test_lock_attempt_unlock(attempt[7]);

    /* Recovery lock must fail due to being contested */
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "op 9 recovery lock %s", key1);
    attempt[8] = test_lock(state, "op 9 recovery", key1, RKL_MODE_RECOVERY);
    plat_assert(attempt[8] && attempt[8]->lock_complete &&
                attempt[8]->status == SDF_LOCK_RESERVED);
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "op 9 recovery lock %s failed as expected", key1);

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "get 2 end");
    rklc_get_complete(get[0]);
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "get 2 complete");

    /*
     * Validate that acquiring, releasing, and re-acquiring
     * an exclusive lock correctly blocks the recovery operation
     * and garbage collects correctly.
     */

    /* Uncontested exclusive - must pass */
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "op 10 exclusive lock %s", key1);
    attempt[9] = test_lock(state, "op 10 exclusive", key1,
                           RKL_MODE_EXCLUSIVE);
    plat_assert(attempt[9] && attempt[9]->lock_complete &&
                attempt[9]->status == SDF_SUCCESS);
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "op 10 exclusive lock %s complete", key1);

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "get 3 start");
    get[0] = rklc_start_get(state->lock_container);
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "get 3 started");

    /* Release exclusive lock */
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "op 10 unlock %s", key1);
    test_lock_attempt_unlock(attempt[9]);

    /* Relock same key - must pass */
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "op 11 exclusive lock %s", key1);
    attempt[10] = test_lock(state, "op 11 exclusive", key1,
                            RKL_MODE_EXCLUSIVE);
    plat_assert(attempt[10] && attempt[10]->lock_complete &&
                attempt[10]->status == SDF_SUCCESS);
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "op 11 exclusive lock %s complete", key1);

    /* Release exclusive lock */
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "op 11 unlock %s", key1);
    test_lock_attempt_unlock(attempt[10]);

    /* Recovery lock must fail due to being contested */
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "op 12 recovery lock %s", key1);
    attempt[11] = test_lock(state, "op 12 recovery", key1,
                            RKL_MODE_RECOVERY);
    plat_assert(attempt[11] && attempt[11]->lock_complete &&
                attempt[11]->status == SDF_LOCK_RESERVED);
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "op 11 recovery lock %s failed as expected", key1);

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "get 3 end");
    rklc_get_complete(get[0]);
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "get 3 complete");

    /*
     * Validate that locks granted before get and released after
     * garbage collect properly.
     */

    /* Uncontested exclusive - must pass */
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "op 13 exclusive lock %s", key1);
    attempt[12] = test_lock(state, "op 13 exclusive", key1,
                            RKL_MODE_EXCLUSIVE);
    plat_assert(attempt[12] && attempt[12]->lock_complete &&
                attempt[12]->status == SDF_SUCCESS);
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "op 13 exclusive lock %s complete", key1);

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "get 4 start");
    get[0] = rklc_start_get(state->lock_container);
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "get 4 started");

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "get 4 end");
    rklc_get_complete(get[0]);
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "get 4 complete");

    /* Release exclusive lock */
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "op 13 unlock %s", key1);
    test_lock_attempt_unlock(attempt[12]);

    /*
     * Perform multiple operations and validate there are no problems
     * with reference counts other than 1
     */

    /* Uncontested exclusive - must pass */
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "op 14 exclusive lock %s", key1);
    attempt[13] = test_lock(state, "op 14 exclusive", key1,
                            RKL_MODE_EXCLUSIVE);
    plat_assert(attempt[13] && attempt[13]->lock_complete &&
                attempt[13]->status == SDF_SUCCESS);
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "op 14 exclusive lock %s complete", key1);

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "get 5 start");
    get[0] = rklc_start_get(state->lock_container);
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "get 5 started");

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "op 15 exclusive lock %s", key2);
    attempt[14] = test_lock(state, "op 15 exclusive", key2,
                            RKL_MODE_EXCLUSIVE);
    plat_assert(attempt[14] && attempt[14]->lock_complete &&
                attempt[14]->status == SDF_SUCCESS);
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "op 15 exclusive lock %s complete", key2);

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "get 6 start");
    get[1] = rklc_start_get(state->lock_container);
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "get 6 started");

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "op 16 exclusive lock %s", key3);
    attempt[15] = test_lock(state, "op 15 exclusive", key3,
                            RKL_MODE_EXCLUSIVE);
    plat_assert(attempt[15] && attempt[15]->lock_complete &&
                attempt[15]->status == SDF_SUCCESS);
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "op 16 exclusive lock %s complete", key2);

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "op 14 unlock %s", key1);
    test_lock_attempt_unlock(attempt[13]);

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "op 16 unlock %s", key3);
    test_lock_attempt_unlock(attempt[15]);

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "op 15 unlock %s", key2);
    test_lock_attempt_unlock(attempt[14]);

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "get 5 end");
    rklc_get_complete(get[0]);
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "get 5 complete");

    /* Recovery lock must fail due to being contested */
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "op 17 recovery lock %s", key1);
    attempt[16] = test_lock(state, "op 17 recovery", key1,
                            RKL_MODE_RECOVERY);
    plat_assert(attempt[16] && attempt[16]->lock_complete &&
                attempt[16]->status == SDF_LOCK_RESERVED);
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "op 17 recovery lock %s failed as expected", key1);

    /* Recovery lock must fail due to being contested */
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "op 18 recovery lock %s", key2);
    attempt[17] = test_lock(state, "op 18 recovery", key2,
                            RKL_MODE_RECOVERY);
    plat_assert(attempt[17] && attempt[17]->lock_complete &&
                attempt[17]->status == SDF_LOCK_RESERVED);
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "op 18 recovery lock %s failed as expected", key2);

    /* Recovery lock must fail due to being contested */
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "op 19 recovery lock %s", key3);
    attempt[18] = test_lock(state, "op 19 recovery", key2,
                            RKL_MODE_RECOVERY);
    plat_assert(attempt[18] && attempt[18]->lock_complete &&
                attempt[18]->status == SDF_LOCK_RESERVED);
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "op 19 recovery lock %s failed as expected", key3);

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "get 6 end");
    rklc_get_complete(get[1]);
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "get 6 complete");

    /*
     * First recovery lock must succeed, second fail immediately
     *
     * XXX: drew 2010-04-21 This only applies to SDF_REPLICATION_V1_2_WAY.
     * Other types fail with asserts.
     */
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "op 20 exclusive lock %s", key1);
    attempt[19] = test_lock(state, "op 20 exclusive", key1,
                            RKL_MODE_EXCLUSIVE);
    plat_assert(attempt[19]->lock_complete &&
                attempt[19]->status == SDF_SUCCESS);
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "op 20 exclusive lock %s complete", key1);

    /* Recovery lock started with existing exclusive lock must fail */
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "op 21 recovery lock %s", key1);
    attempt[20] = test_lock(state, "op 21 recovery", key1, RKL_MODE_RECOVERY);
    plat_assert(attempt[20] && attempt[20]->lock_complete &&
                attempt[20]->status == SDF_LOCK_RESERVED);
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "op 21 recovery lock %s failed as expected", key1);

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "op 20 unlock %s", key1);
    test_lock_attempt_unlock(attempt[19]);

    /* Validate correct garbage collection */
    plat_assert(!rklc_get_lock_count(state->lock_container));

    thread_test_1(state);

    if (state->config->incremental) {
        goto incremental_exit;
    }

incremental_exit:

    /* Terminate scheduler */
    fthKill(1);
}

/*
 * Validate that the multi-threaded case is working correctly and that
 * recovery locks correctly block exclusive locks.
 *
 * XXX: drew 2010-02-26 This will only work with a single fth scheduler
 * because that blocks t1 until after t2 has started its blocking
 * lock request.
 */

static void
thread_test_1(struct test_state *suite_state) {
    struct thread_test_1_state *case_state;

    plat_calloc_struct(&case_state);
    plat_assert(case_state);

    case_state->state = suite_state;
    fthMboxInit(&case_state->t1_block);

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "thread_test_1 get 1 start");
    case_state->t1_get = rklc_start_get(case_state->state->lock_container);
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "thread_test_1 get 1 started");

    /* Uncontended lock */
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "thread_test_1 op 1 recovery lock %s", key1);
    test_lock_sync(case_state->state, "thread_test_1 op 1 recovery", key1,
                   RKL_MODE_RECOVERY,  &case_state->t1_attempt);
    plat_assert(case_state->t1_attempt &&
                case_state->t1_attempt->lock_complete &&
                case_state->t1_attempt->status == SDF_SUCCESS);
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "thread_test_1 op 1 recovery lock %s complete", key1);

    fthResume(fthSpawn(&thread_test_1_t2, 40960), (uint64_t)case_state);

    fthMboxWait(&case_state->t1_block);
    plat_assert(case_state->t2_attempt &&
                !case_state->t2_attempt->lock_complete);

    /* Release first lock, t2 lock must succeed */
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "thread_test_1 op 1 unlock %s", key1);
    test_lock_attempt_unlock(case_state->t1_attempt);

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "thread_test_1 op 3 exclusive lock %s", key1);
    test_lock_sync(case_state->state, "thread_test_1 op 2 exclusive", key1,
                   RKL_MODE_EXCLUSIVE, &case_state->t1_attempt);
    plat_assert(case_state->t1_attempt &&
                case_state->t1_attempt->lock_complete &&
                case_state->t1_attempt->status == SDF_SUCCESS);
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "thread_test_1 op 3 exclusive lock %s complete", key1);

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "thread_test_1 op 3 unlock %s", key1);
    test_lock_attempt_unlock(case_state->t1_attempt);

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "thread_test_1 get 1 end");
    rklc_get_complete(case_state->t1_get);
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "thread_test_1 get 1 complete");

    /* Validate correct garbage collection */
    plat_assert(!rklc_get_lock_count(case_state->state->lock_container));

    plat_free(case_state);
}

static void
thread_test_1_t2(uint64_t arg) {
    struct thread_test_1_state *case_state;

    case_state = (struct thread_test_1_state *)arg;
    /* Wake main thread after blocking below */
    fthMboxPost(&case_state->t1_block, 0 /* ignored */);

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "thread_test_1 op 2 exclusive lock %s", key1);
    test_lock_sync(case_state->state, "thread_test_1 op 2 exclusive", key1,
                   RKL_MODE_EXCLUSIVE,  &case_state->t2_attempt);
    plat_assert(case_state->t2_attempt &&
                case_state->t2_attempt->lock_complete &&
                case_state->t2_attempt->status == SDF_SUCCESS);
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "thread_test_2 op 2 recovery lock %s complete", key1);

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "thread_test_1 op 2 unlock %s", key1);
    test_lock_attempt_unlock(case_state->t2_attempt);
}

/*
 * FIXME: drew 2010-04-21 SDF_REPLICATION_V1_2_WAY allows
 * attempting to acquire  #RKL_MODE_EXCLUSIVE locks on keys with an
 * existing #RKL_MODE_RECOVERY lock while other flavors do not.
 *
 * We should test all of the relevant differences.
 */
static struct test_state *
test_state_alloc(const struct plat_opts_config_key_lock_test *config) {
    struct test_state *state;
    int failed;

    failed = !plat_calloc_struct(&state);

    if (!failed) {
        state->config = config;
        /* Chosen node, sguid, vip are identifiable in messages */
        state->lock_container =
            replicator_key_lock_container_alloc(1 /* node */,
                                                2 /* sguid */,
                                                3 /* vip group */,
                                                SDF_REPLICATION_V1_2_WAY);
        failed = !state->lock_container;
        TAILQ_INIT(&state->attempt_list);
    }

    if (failed && state) {
        test_state_free(state);
        state = NULL;
    }

    plat_assert_always(state);

    return (state);
}

/* Frees state and any attempts not freed by users */
static void
test_state_free(struct test_state *state) {
    struct test_lock_attempt *attempt;
    struct test_lock_attempt *next_attempt;

    if (state) {
        if (state->lock_container) {
            rklc_free(state->lock_container);
        }

        /*
         * XXX: drew 2010-02-19 Coverity isn't smart enough to figure
         * out that state->attempt_list.lh_first is changing each time when
         * we use
         *
         * while ((attempt = TAILQ_FIRST(&state->attempt_list)))
         */
        TAILQ_FOREACH_SAFE(attempt, &state->attempt_list, attempt_list_entry,
                           next_attempt) {
            test_lock_attempt_free(attempt);
        }

        plat_free(state);
    }
}

/**
 * @brief Acquire lock
 *
 * @param name <IN> name owned by caller, referenced until
 * #test_lock_attempt_free.
 * @param key <IN> key owned by caller, not referenced after lock returns
 */
static struct test_lock_attempt *
test_lock(struct test_state *state, const char *name, const char *key,
          enum rkl_mode mode) {
    struct test_lock_attempt *attempt;
    rkl_cb_t lock_cb;

    attempt = test_lock_common(state, name, key, mode);

    lock_cb =
        rkl_cb_create(PLAT_CLOSURE_SCHEDULER_ANY_OR_SYNCHRONOUS,
                      &test_lock_attempt_lock_cb, attempt);

    if (attempt) {
        rklc_lock(attempt->state->lock_container, &attempt->key, attempt->mode,
                  lock_cb);
    }


    return (attempt);
}

static void
test_lock_sync(struct test_state *state, const char *name, const char *key,
               enum rkl_mode mode, struct test_lock_attempt **attempt_out) {
    struct test_lock_attempt *attempt;
    SDF_status_t status;
    struct replicator_key_lock *key_lock;

    attempt = test_lock_common(state, name, key, mode);
    *attempt_out = attempt;

    if (attempt) {
        key_lock = NULL;
        status = rklc_lock_sync(attempt->state->lock_container, &attempt->key,
                                attempt->mode, &key_lock);
        test_lock_attempt_lock_cb(NULL, attempt, status, key_lock);
    }
}

static struct test_lock_attempt *
test_lock_common(struct test_state *state, const char *name, const char *key,
                 enum rkl_mode mode)  {
    struct test_lock_attempt *attempt;
    size_t len;

    plat_calloc_struct(&attempt);

    if (attempt) {
        attempt->state = state;
        attempt->name = name;
        len = strlen(key);
        memcpy(attempt->key.key, key, len);
        attempt->key.len = len;
        attempt->mode = mode;

        /* XXX: drew 2010-02-19 add lock for multi-threaded test */

        (void) __sync_fetch_and_add(&attempt->state->waiting_count, 1);

        TAILQ_INSERT_TAIL(&state->attempt_list, attempt, attempt_list_entry);

    }

    plat_assert_always(attempt);

    return (attempt);
}

static void
test_lock_attempt_free(struct test_lock_attempt *attempt) {
    if (attempt) {
        plat_assert(attempt->unlocked);
        /* XXX: drew 2010-02-19 add lock for multi-threaded test */
        TAILQ_REMOVE(&attempt->state->attempt_list, attempt,
                     attempt_list_entry);
        plat_free(attempt);
    }
}

static void
test_lock_attempt_unlock(struct test_lock_attempt *attempt) {
    plat_assert_always(attempt->key_lock);
    rkl_unlock(attempt->key_lock);

    test_lock_attempt_count(attempt, -1);
    attempt->key_lock = NULL;
    attempt->unlocked = 1;
}

static void
test_lock_attempt_lock_cb(struct plat_closure_scheduler *context, void *env,
                          SDF_status_t status_arg,
                          struct replicator_key_lock *key_lock) {
    struct test_lock_attempt *attempt = (struct test_lock_attempt *)env;

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "lock %s for key '%*.*s' complete with status %s",
                 attempt->name ? attempt->name : "",
                 attempt->key.len, attempt->key.len, attempt->key.key,
                 sdf_status_to_string(status_arg));

    plat_assert_always(!attempt->key_lock);

    attempt->status = status_arg;
    attempt->key_lock = key_lock;
    attempt->lock_complete = 1;
    if (attempt->status == SDF_SUCCESS) {
        test_lock_attempt_count(attempt, 1);
    } else {
        attempt->unlocked = 1;
    }
    (void) __sync_fetch_and_sub(&attempt->state->waiting_count, 1);
}

static int
test_lock_attempt_count(struct test_lock_attempt *attempt, int increment) {
    int *counter = NULL;

    switch (attempt->mode) {
    case RKL_MODE_NONE:
        plat_fatal("impossible");
        break;
    case RKL_MODE_SHARED:
        counter = &attempt->state->lock_count_shared;
        break;
    case RKL_MODE_EXCLUSIVE:
        counter = &attempt->state->lock_count_exclusive;
        break;
    case RKL_MODE_RECOVERY:
        counter = &attempt->state->lock_count_recovery;
        break;
    }

    plat_assert_always(counter);

    return (__sync_add_and_fetch(counter, increment));
}

#include "platform/opts_c.h"
