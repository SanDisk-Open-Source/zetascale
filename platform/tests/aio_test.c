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
 * File:   $URL: svn://svn.schoonerinfotech.net/schooner-trunk/trunk/sdf/platform/tests/aio_test.c $
 * Author: drew
 *
 * Created on Mar 9, 2010
 *
 * (c) Copyright 2010, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: aio_test.c 15015 2010-11-10 23:09:06Z briano $
 */

/**
 * Test paio_api, notably paio_libaio and paio_write_merge.
 */

#define PLAT_OPTS_NAME(name) name ## _aio_test

#include <limits.h>

#include "sys/queue.h"

#include "platform/aio_api.h"
#include "platform/aio_error_bdb.h"
#include "platform/aio_error_control.h"
#include "platform/aio_libaio.h"
#include "platform/aio_wc.h"

#include "platform/alloc.h"
#include "platform/errno.h"
#include "platform/fcntl.h"
#include "platform/mman.h"
#include "platform/opts.h"
#include "platform/shmem.h"
#include "platform/string.h"
#include "platform/unistd.h"

#include "fth/fth.h"
#include "fth/fthOpt.h"
#include "fth/fthMbox.h"

PLAT_LOG_SUBCAT_LOCAL(LOG_CAT, PLAT_LOG_CAT_PLATFORM_TEST, "aio");

#define TEST_MODE_ITEMS() \
    item(TEST_MODE_LIBAIO, libaio) \
    item(TEST_MODE_WC, wc) \
    item(TEST_MODE_ERROR, error)
   
enum test_mode {
#define item(caps, lower) caps,
    TEST_MODE_ITEMS()
#undef item
};

#define TEST_OP_TYPE_ITEMS() \
    item(TEST_OP_TYPE_READ, read) \
    item(TEST_OP_TYPE_WRITE, write)

enum test_op_type {
#define item(caps, lower) caps,
    TEST_OP_TYPE_ITEMS()
#undef item
};

static const char test_default_filename[] = "aio_test_file";

enum {
    TEST_DEFAULT_LEN = 1024 * 1024,

    /** @brief Values which get good test coverage with minimal IO */
    TEST_DEFAULT_WC_IO_LIMIT = 2,
    TEST_DEFAULT_WC_BYTE_LIMIT = 10 * 4096,
    /* FIXME: drew 2010-03-19 1 does not work yet */
    TEST_DEFAULT_WC_DELAY = 0
};

enum {
    /** @brief Operation length used by #test_batch */
    TEST_BATCH_OP_LEN = 4096,
    /** @brief Length between contiguous areas in #test_batch */
    TEST_BATCH_SKIP = 4096
};

struct plat_opts_config_aio_test {
    struct plat_shmem_config shmem;
    struct paio_libaio_config paio_libaio;
    struct paio_wc_config paio_wc;
    struct paio_error_bdb_config paio_error_bdb;

    /** @brief Operational mode (sort of paio_aio_api requested) */
    enum test_mode mode;

    char filename[PATH_MAX];
    long len;

    /** @brief Hook so partialy working tests can be checked in */
    unsigned incremental : 1;
};

/* BEGIN CSTYLED */
/* cstyle does not like the nested if */
#define PLAT_OPTS_ITEMS_aio_test()                                             \
    item("file", "test file name", FILENAME,                                   \
         ({                                                                    \
          int len = strlen(optarg);                                            \
          if (len < PATH_MAX) {                                                \
              memcpy(config->filename, optarg, len);                           \
          }                                                                    \
          len < PATH_MAX ? 0 : -ENAMETOOLONG;}), PLAT_OPTS_ARG_REQUIRED)       \
    item("mode", "test mode", MODE,  test_config_parse_mode(config, optarg),   \
         PLAT_OPTS_ARG_REQUIRED)                                               \
    item("incremental", "pass when the test gets to the last known good part", \
         INCREMENTAL, ({ config->incremental = 1; 0; }), PLAT_OPTS_ARG_NO)     \
    PAIO_LIBAIO_OPTS(paio_libaio)                                              \
    PAIO_WC_OPTS(paio_wc)                                                      \
    PLAT_OPTS_SHMEM(shmem)                                                     \
    PLAT_OPTS_FTH()                                                            \
/* END CSTYLED */

/**
 * @brief Test state
 *
 * Each #test_state contains all test state, including #test_op
 * structures for each IO which has been started over the test lifetime,
 * legal goal states, and a memory mapped copy of the file(s) used for the
 * test.
 */
struct test_state {
    const struct plat_opts_config_aio_test *config;

    /** @brief api parent for all context instantiations */
    struct paio_api *api;

    /** @brief For error injection */
    struct paio_error_control *error_control;

    /** @brief Underlying api for write-combining test */
    struct paio_api *wrapped_api;

    struct paio_context *context;


    /** @brief Next operation sequence number */
    int op_seqno;

    /*
     * XXX: drew 2010-03-10 Everything down to all_ops belongs in a
     * test_fd structure, since we should be testing mutliple files
     * at once
     */

    /** @brief Open file descriptor */
    int fd;

    /**
     * @brief 'old' allowed state while pending IOs are running
     *
     * This is incrementally updated to match buf_after as individual IOs
     * complete.
     */
    char *buf_before;

    /** @brief 'after' allowed state for pending IOs */
    char *buf_after;

    /** @brief  Memory mapped file under test */
    char *buf_mmap;

    /** @brief All operations */
    TAILQ_HEAD(, test_op) all_ops;

    /** @brief Number of operations which not yet returned in getevents */
    int running_op_count;

    /** @brief Number of write operations not yet returned in getevents */
    int running_write_count;
};

enum {
    TEST_OP_SEQNO_INVALID = -1
};

enum test_op_state {
    TEST_OP_STATE_INITIAL = 0,
    TEST_OP_STATE_RUNNING,
    TEST_OP_STATE_COMPLETE,
};

enum test_op_fail {
    TEST_OP_FAIL_NONE,
    TEST_OP_FAIL_EXPECTED
};

/** @brief Op structure, one per op started during test */
struct test_op {
    enum test_op_type op_type;

    /**
     * @brief Wrapped iocb.
     *
     * First is convienent for debugging
     * (gdb) print (struct test_op *)event->obj
     */
    struct iocb iocb;

    /** @brief Failure expected or not */
    enum test_op_fail fail;

    /** Parent state */
    struct test_state *state;

    /** @brief Sequence number of op */
    int seqno;

    /** @brief Data being put or read */
    void *buf;

    /** @brief Current state */
    enum test_op_state op_state;

    /** @brief Entry in test_op->state.all_ops */
    TAILQ_ENTRY(test_op) all_ops_entry;

};

/** @brief Easily identifiable structure which is filled */
struct test_fill {
    int offset;
    int seq;
};

static void test_main(uint64_t arg);
/* Test cases */
static void test_write_read(struct test_state *state);
static void test_multi_2(struct test_state *state);
static void test_merge_left(struct test_state *state);
static void test_merge_right(struct test_state *state);
static void test_batch(struct test_state *, const char *test_name,
                       enum test_op_type op_type, int batch_arg,
                       int count_arg, long start_offset,
                       enum test_op_fail fail, struct test_op ***ops_out);
static void test_error_injection(struct test_state *state);

static struct test_state *
test_state_alloc(const struct plat_opts_config_aio_test *config);
static int test_state_init(struct test_state *state);
static void test_state_free(struct test_state *state);
static int test_alloc_api(struct test_state *state);
static void test_state_validate_buf(struct test_state *state, const char *buf,
                                    int offset_arg, int len);
static void test_fill_buf(void *buf, int len, int file_offset,
                          int seq) __attribute__((unused));
static int test_state_setup_context(struct test_state *state, int maxevents);

static int test_state_start_op(struct test_state *state, enum test_op_type kind,
                               int offset, int len, enum test_op_fail fail,
                               struct test_op **op_out);
static long test_state_getevents(struct test_state *state, int min,
                                 struct timespec *timeout);
static int test_state_destroy_context(struct test_state *state);
 
static void test_op_free(struct test_op *op);
static void test_config_init(struct plat_opts_config_aio_test *config);
static void test_config_destroy(struct plat_opts_config_aio_test *config);
static int test_config_parse_mode(struct plat_opts_config_aio_test *config,
                                  const char *optarg);
static const char *test_op_type_to_string(enum test_op_type op);

int
main(int argc, char **argv) {
    struct plat_opts_config_aio_test config;
    int status = 0;
    struct test_state *state = NULL;

    test_config_init(&config);

    if (plat_opts_parse_aio_test(&config, argc, argv)) {
        return (2);
    }

    status = plat_shmem_prototype_init(&config.shmem);
    plat_assert_always(!status);

    status = plat_shmem_attach(plat_shmem_config_get_path(&config.shmem));
    plat_assert_always(!status);

    fthInit();

    state = test_state_alloc(&config);
    plat_assert_always(state);

    status = test_state_init(state);
    if (status) {
        return (status);
    }

    XResume(fthSpawn(&test_main, 40960), (uint64_t)state);
    fthSchedulerPthread(0);

    test_state_free(state);

    status = plat_shmem_detach();
    plat_assert_always(!status);

    test_config_destroy(&config);
    return(0);
}

static void
test_main(uint64_t arg) {
    struct test_state *state = (struct test_state *)arg;
    int status = 0;

    plat_log_msg(21804, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "setup context");
    status = test_state_setup_context(state, 10 /* max ops */);
    plat_assert(!status);

    test_write_read(state);

    test_multi_2(state);

    test_merge_left(state);

    test_merge_right(state);

    if (state->config->mode == TEST_MODE_ERROR) {
        test_error_injection(state);
    }

    plat_log_msg(21811, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "destroy context");
    status = test_state_destroy_context(state);
    plat_assert(!status);

    fthKill(1);
};

/** @brief Simplest write, read, and getevents smoke test */
static void
test_write_read(struct test_state *state) {
    int status;
    struct timespec zero = { 0, 0 };

    /* Single write */
    plat_log_msg(21822, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "write_read op 1 write offset 0 len 4096");
    status = test_state_start_op(state, TEST_OP_TYPE_WRITE, 0 /* offset */,
                                 4096 /* len */, TEST_OP_FAIL_NONE,
                                 NULL /* op out */);
    plat_log_msg(21823, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "write_read op 1 write offset 0 len 4096 started");
    plat_assert(!status);
    status = test_state_getevents(state, 1 /* min */, NULL /* timeout */);
    plat_assert(status == 1);
    plat_log_msg(21824, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "write_read op 1 write offset 0 len 4096 complete");

    /* Single read */
    plat_log_msg(21825, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "write_read op 2 read offset 0 len 4096");
    status = test_state_start_op(state, TEST_OP_TYPE_READ, 0 /* offset */,
                                 4096 /* len */, TEST_OP_FAIL_NONE,
                                 NULL /* op out */);
    plat_log_msg(21826, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "write_read op 2 read offset 0 len 4096 started");
    plat_assert(!status);
    status = test_state_getevents(state, 1 /* min */, NULL /* timeout */);
    plat_assert(status == 1);
    plat_log_msg(21827, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "write_read op 2 read offset 0 len 4096 complete");


    status = test_state_getevents(state, 0 /* min */, &zero);
    plat_assert(!status);
}

/** @brief Test two non-contiguous areas */
static void
test_multi_2(struct test_state *state) {
    int status;
    int total;

    test_batch(state, "multi_2", TEST_OP_TYPE_WRITE, 2 /* batches */,
               1 /* count */, 0 /* offset */, TEST_OP_FAIL_NONE,
               NULL /* ops_out */);

    /* Get all ops */
    status = test_state_getevents(state, 2 /* min */, NULL /* timeout */);
    plat_assert(status == 2);

    /*
     * FIXME: Should retry until timeout or we get back just one of
     * 2.  This may require event control on the back end.
     */
    test_batch(state, "multi_2", TEST_OP_TYPE_READ, 2 /* batches */,
               1 /* count */, 0 /* offset */, TEST_OP_FAIL_NONE,
               NULL /* ops_out */);

    for (total = 0; total != 2; total += status) {
        status = test_state_getevents(state, 1 /* min */, NULL /* timeout */);
        plat_assert(status > 0 && status <= 2 - total);
    }

    /* FIXME: Should add log-completion option to test_batch */
}

/** @brief Merge with node to the left */
static void
test_merge_left(struct test_state *state) {
    struct test_op **first_ops;
    struct test_op **merge_ops;
    int status;

    /* Start discontiguous IOs up to limit */
    test_batch(state, "merge_left first", TEST_OP_TYPE_WRITE,
               state->config->paio_wc.io_limit /* batches */,
               1 /* one IO per */, 0 /* offset */, TEST_OP_FAIL_NONE,
               &first_ops);


    /* Start IO off the end */
    test_batch(state, "merge_left second", TEST_OP_TYPE_WRITE,
               1 /* batches */, 2 /* count */,
               state->config->paio_wc.io_limit *
               (TEST_BATCH_OP_LEN + TEST_BATCH_SKIP), TEST_OP_FAIL_NONE,
               &merge_ops);

    /*
     * FIXME: drew 2010-03 Can't validate correct merging without
     * visibility into wrapped context.
     */

    status = test_state_getevents(state,
                                  state->config->paio_wc.io_limit + 2 /* min */,
                                  NULL /* timeout */);
    plat_assert(status == state->config->paio_wc.io_limit + 2);

    plat_free(first_ops);
    plat_free(merge_ops);
}

/** @brief Merge with node to the right */
static void
test_merge_right(struct test_state *state) {
    int status;
    long offset;

    /* Start discontiguous IOs up to limit */
    test_batch(state, "merge_right first", TEST_OP_TYPE_WRITE,
               state->config->paio_wc.io_limit /* batches */,
               1 /* one IO per */, 0 /* offset */, TEST_OP_FAIL_NONE,
               NULL);


    /* Start another one with room right over */
    offset = (state->config->paio_wc.io_limit + 1) *
        (TEST_BATCH_OP_LEN + TEST_BATCH_SKIP);
    test_batch(state, "merge_right second", TEST_OP_TYPE_WRITE,
               1 /* batches */, 1 /* count */, offset, TEST_OP_FAIL_NONE,
               NULL);

    /* Start one in the middle that will join with the right node */
    offset -= TEST_BATCH_OP_LEN;
    test_batch(state, "merge_right", TEST_OP_TYPE_WRITE,
               1 /* batches */, 1 /* count */, offset, TEST_OP_FAIL_NONE,
               NULL);

    /*
     * FIXME: drew 2010-03 Can't validate correct combining without
     * visibility into wrapped context.
     */
    status = test_state_getevents(state,
                                  state->config->paio_wc.io_limit + 2 /* min */,
                                  NULL /* timeout */);
    plat_assert(status == state->config->paio_wc.io_limit + 2);
}

/**
 * @brief Start batches of operations
 *
 * @param name <IN> Prefix for logging
 * @param op_type <IN> All started operations are of this type
 * @param batch_arg <IN> number of #count_arg sized groups started
 * separated by 4K
 * @param count_arg <IN> number of 4K IOs in each consecutive set
 * @param start_offset <IN> starting offset of first IO
 * @param ops_out <OUT> when non-null an array of batch_arg * count_arg
 * started ops is stored here.  Caller frees with plat_free.
 */
static void
test_batch(struct test_state *state, const char *test_name,
           enum test_op_type op_type, int batch_arg, int count_arg,
           long start_offset, enum test_op_fail fail,
           struct test_op ***ops_out) {
    int batch;
    int count;
    long offset;
    long len;
    int status;
    struct test_op *op;
    struct test_op **ops;

    ops = ops_out ? plat_calloc(batch_arg * count_arg, sizeof (ops[0])) : NULL;
    plat_assert_iff(ops_out, ops);

    len = TEST_BATCH_OP_LEN;
    for (batch = 0; batch < batch_arg; ++batch) {
        for (count = 0; count < count_arg; ++count) {
            offset = start_offset +
                batch * (count_arg * TEST_BATCH_OP_LEN + TEST_BATCH_SKIP) +
                count * TEST_BATCH_OP_LEN;
            plat_log_msg(21828, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                         "%s total batch %d count %d start %ld op %s"
                         " current batch %d count %d offset %ld len %ld",
                         test_name, batch_arg, count_arg, start_offset,
                         test_op_type_to_string(op_type),
                         batch, count, offset, len);
            status = test_state_start_op(state, op_type, offset, len, fail,
                                         &op);
            plat_log_msg(21829, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                         "total batch %d count %d start %ld op %s started"
                         " current batch %d count %d offset %ld len %ld",
                         batch_arg, count_arg, start_offset,
                         test_op_type_to_string(op_type),
                         batch, count, offset, len);
            plat_assert(!status);
            if (ops) {
                ops[batch * count_arg + count] = op;
            }
        }
    }

    if (ops_out) {
        *ops_out = ops;
    }
}

static void
test_error_injection(struct test_state *state) {
    long status;

    plat_assert(state->config->mode == TEST_MODE_ERROR);

    status =
        paio_error_control_set_error(state->error_control,
                                     PAIO_ECT_READ_ONCE,
                                     state->config->filename,
                                     0, 8192);
    plat_assert(!status);

    /* FIXME: drew 2010-05-27 Should validate it doesn't match other areas */

    plat_log_msg(21864, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "test_error_injection op 1 read offset 0 len 4096");
    status = test_state_start_op(state, TEST_OP_TYPE_READ, 0 /* offset */,
                                 4096 /* len */, TEST_OP_FAIL_EXPECTED,
                                 NULL /* op out */);
    plat_log_msg(21865, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "test_error_injection op 1 read offset 0 len 4096 started");
    plat_assert(!status);

    status = test_state_getevents(state, 1 /* min */, NULL /* timeout */);
    plat_assert(status == 1);
    plat_log_msg(21866, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "test_error_injection op 1 read offset 0 len 4096 complete");

    /* Error region should clear */
    plat_log_msg(21867, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "test_error_injection op 2 read offset 0 len 4096");
    status = test_state_start_op(state, TEST_OP_TYPE_WRITE, 0 /* offset */,
                                 4096 /* len */, TEST_OP_FAIL_NONE,
                                 NULL /* op out */);
    plat_log_msg(21868, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "test_error_injection op 2 read offset 0 len 4096 started");
    plat_assert(!status);

    status = test_state_getevents(state, 1 /* min */, NULL /* timeout */);
    plat_assert(status == 1);
    plat_log_msg(21869, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "test_error_injection op 2 read offset 0 len 4096 complete");
}

static struct test_state *
test_state_alloc(const struct plat_opts_config_aio_test *config) {
    struct test_state *state = NULL;
    int failed = 0;

    failed = !plat_calloc_struct(&state);

    if (!failed) {
        state->config = config;
        TAILQ_INIT(&state->all_ops);
    }
    if (!failed) {
        state->buf_before = plat_calloc(1, config->len);
        failed = !state->buf_before;
    }

    if (!failed) {
        state->buf_after = plat_calloc(1, config->len);
        failed = !state->buf_after;
    }

    if (!failed) {
        failed = test_alloc_api(state);
    }

    if (failed && state) {
        test_state_free(state);
        state = NULL;
    }

    plat_assert_always(state);

    return (state);
}

/**
 * @brief Initialize test (create file, mmap, etc).
 *
 * Separate so the program can exit without asserting on
 * environmental issues
 *
 * @return 0 on success, appropriate exit code on failure
 */
static int
test_state_init(struct test_state *state) {
    int ret = 0;

    ret = plat_unlink(state->config->filename);
    if (!ret) {
    } else if (errno == ENOENT) {
        ret = 0;
    } else {
        plat_log_msg(21802, LOG_CAT,
                     PLAT_LOG_LEVEL_ERROR, "unlink(%s) failed: %s",
                     state->config->filename, plat_strerror(errno));
        ret = 1;
    }

    if (!ret) {
        state->fd = plat_open(state->config->filename,
                              O_RDWR|O_CREAT|O_EXCL, 0666);
        if (state->fd == -1) {
            plat_log_msg(20990, LOG_CAT,
                         PLAT_LOG_LEVEL_ERROR, "open(%s) failed: %s",
                         state->config->filename, plat_strerror(errno));
            ret = 1;
        }
    }

    if (!ret) {
        if (plat_ftruncate(state->fd, (off_t)state->config->len) == -1) {
            plat_log_msg(21013, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
                         "truncate(%s, %lld) failed: %s",
                         state->config->filename,
                         (long long)state->config->len,
                         plat_strerror(errno));
            ret = 1;
        }
    }

    if (!ret) {
        state->buf_mmap = plat_mmap(NULL, state->config->len,
                                    PROT_READ|PROT_WRITE, MAP_SHARED,
                                    state->fd, 0 /* offset */);
        if (state->buf_mmap == MAP_FAILED) {
            plat_log_msg(21803, LOG_CAT,
                         PLAT_LOG_LEVEL_ERROR, "mmap(%s) failed: %s",
                         state->config->filename, plat_strerror(errno));
            state->buf_mmap = NULL;
            ret = 1;
        }
    }

    return (ret);
}

static void
test_state_free(struct test_state *state) {
    struct test_op *op;
    struct test_op *op_next;

    if (state) {
        if (state->api) {
            paio_api_destroy(state->api);
        }
        if (state->wrapped_api) {
            paio_api_destroy(state->wrapped_api);
        }
        if (state->buf_before) {
            plat_free(state->buf_before);
        }
        if (state->buf_after) {
            plat_free(state->buf_after);
        }
        if (state->buf_mmap) {
            plat_munmap(state->buf_mmap, state->config->len);
        }
        if (state->fd > 0) {
            plat_close(state->fd);
            plat_unlink(state->config->filename);
        }

        /*
         * XXX: drew 2010-03-19 Coverity isn't smart enough to figure
         * out that state->all_ops.lh_first is changing each time when
         * we do this instead
         * while ((op = TAILQ_FIRST(&state->all_ops)))
         */
        TAILQ_FOREACH_SAFE(op, &state->all_ops, all_ops_entry, op_next) {
            test_op_free(op);
        }

        plat_free(state);
    }
}

static int
test_alloc_api(struct test_state *state) {
    int ret = 0;

    switch (state->config->mode) {
    case TEST_MODE_LIBAIO:
        state->api = paio_libaio_create(&state->config->paio_libaio);
        ret = state->api ? 0 : -1;
        break;

    case TEST_MODE_WC:
        state->wrapped_api = paio_libaio_create(&state->config->paio_libaio);
        ret = state->wrapped_api ? 0 : -1;

        if (!ret) {
            state->api = paio_wc_create(&state->config->paio_wc,
                                        state->wrapped_api);
            if (!state->api) {
                ret = -1;
            }
        }

        break;

    case TEST_MODE_ERROR:
        state->wrapped_api = paio_libaio_create(&state->config->paio_libaio);
        ret = state->wrapped_api ? 0 : -1;

        if (!ret) {
            state->api =
                paio_error_bdb_create(&state->error_control,
                                      &state->config->paio_error_bdb,
                                      state->wrapped_api);
            if (!state->api) {
                ret = -1;
            }
        }

    }

    return (ret);
}

/**
 * @brief Establish context
 *
 * @return 0 on success, errno on failure
 */
static int
test_state_setup_context(struct test_state *state, int maxevents) {
    int status;

    status = paio_setup(state->api, maxevents, &state->context);

    return (status == -1 ? errno : 0);
}

/**
 * @brief Start asynchronous operation
 *
 * XXX: drew 2010-03-10 Add return for test_op so that it can be
 * included in a set of specific events to wait for.
 *
 * @return 0 on success, errno on failure
 */
static int
test_state_start_op(struct test_state *state, enum test_op_type kind,
                    int offset, int len, enum test_op_fail fail,
                    struct test_op **op_out) {
    int ret;
    int status;
    struct test_op *op;
    struct iocb *iocb;

    plat_assert(offset + len <= state->config->len);

    for (ret = 0, op = TAILQ_FIRST(&state->all_ops); op && !ret;
         op = TAILQ_NEXT(op, all_ops_entry)) {
        if (op->op_state == TEST_OP_STATE_RUNNING &&
            !(op->iocb.u.c.offset + op->iocb.u.c.nbytes <= offset ||
              op->iocb.u.c.offset >= offset + len)) {
            plat_log_msg(21830, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
                         "Requested op kind %s offset %d len %d"
                         " conflicts with existing kind %s offset %d len %d"
                         " seqno %d",
                         test_op_type_to_string(kind), offset, len,
                         test_op_type_to_string(op->op_type),
                         (int)op->iocb.u.c.offset, (int)op->iocb.u.c.nbytes,
                         op->seqno);
            ret = EBUSY;
        }
    }

    if (!ret && !plat_calloc_struct(&op)) {
        ret = ENOMEM;
    }

    if (!ret) {
        op->op_type = kind;
        op->state = state;
        op->buf = plat_calloc(1, len);
        if (!op->buf) {
            ret = ENOMEM;
        }
        op->fail = fail;
        TAILQ_INSERT_TAIL(&state->all_ops, op, all_ops_entry);
    }

    if (!ret) {
        iocb = &op->iocb;
        switch (kind) {
        case TEST_OP_TYPE_READ:
            io_prep_pread(iocb, state->fd, op->buf, len, offset);
            op->seqno = TEST_OP_SEQNO_INVALID;
            break;

        case TEST_OP_TYPE_WRITE:
            op->seqno = __sync_fetch_and_add(&state->op_seqno, 1);
            test_fill_buf(op->buf, len, offset, op->seqno);
            io_prep_pwrite(iocb, state->fd, op->buf, len, offset);
            break;
        }

        status = paio_submit(state->context, 1, &iocb);
        if (status == -1) {
            ret = errno;
            plat_assert(ret);
        }
    }

    if (!ret) {
        switch (kind) {
        case TEST_OP_TYPE_READ:
            break;
        case TEST_OP_TYPE_WRITE:
            switch (state->config->mode) {
            case TEST_MODE_ERROR:
                /* Error injection assumes success and then reverts on fail */
            case TEST_MODE_WC:
                /**
                 * FIXME: drew 2010-03-10 For the write combining
                 * test we shouldn't update the after buffer until the
                 * IO is in the window of ops allowed to complete.
                 */
                /* Fall through for now */
            case TEST_MODE_LIBAIO:
                memcpy(state->buf_after + offset, op->buf, len);
                break;
            }
            (void) __sync_fetch_and_add(&state->running_write_count, 1);
            break;
        }

        op->op_state = TEST_OP_STATE_RUNNING;
        (void) __sync_fetch_and_add(&state->running_op_count, 1);

    }

    if (ret && op) {
        test_op_free(op);
        op = NULL;
    }

    if (op_out) {
        *op_out = op;
    }

    if (ret) {
        plat_log_msg(21831, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
                     "Requested op kind %s offset %d len %d failed %s",
                     test_op_type_to_string(kind), offset, len,
                     plat_strerror(ret));
    }

    return (ret);
}

/*
 * @brief Wrap paio_getevents and validate returns
 *
 * Not for error injection calls which may fail.
 *
 * XXX: drew 2010-05-27 Should just add a failure-expected field
 * to the #test_op structure and skip the ad-hoc calls to
 * #paio_get_events in #test_error_injection.
 *
 * XXX: drew 2010-03-10 Add optional array of specific #test_op
 * structures to wait for from which events are built.
 *
 * @return Number of events complete, -1 on error with error in errno
 */
static long
test_state_getevents(struct test_state *state, int min,
                     struct timespec *timeout) {
    int nr;
    struct io_event *events;
    long ret;
    int i;
    struct test_op *op;

    nr = state->running_op_count;
    events = plat_calloc(nr, sizeof (*events));
    plat_assert(events);

    ret = paio_getevents(state->context, min, nr, events, timeout);
    for (i = 0; i < ret; ++i) {
        op = PLAT_DEPROJECT(struct test_op, iocb, events[i].obj);
        if (op) {
            plat_assert(op->op_state == TEST_OP_STATE_RUNNING);
            (void) __sync_fetch_and_sub(&state->running_op_count, 1);
            op->op_state = TEST_OP_STATE_COMPLETE;

            switch (op->fail) {
            case TEST_OP_FAIL_NONE:
                plat_assert(!events[i].res2);
                break;
            case TEST_OP_FAIL_EXPECTED:
                plat_assert(events[i].res2);
                break;
            }

            if (op->iocb.aio_lio_opcode == IO_CMD_PWRITE) {
                (void) __sync_fetch_and_sub(&state->running_write_count, 1);
                if (!events[i].res2) {
                    plat_assert(events[i].res == op->iocb.u.c.nbytes);
                    plat_assert(!memcmp(state->buf_mmap + op->iocb.u.c.offset,
                                        op->buf, op->iocb.u.c.nbytes));
                }
                /*
                 * Applying test_state_validate_buf to the memory mapped
                 * file buffer has the side effect of setting the before
                 * buffer to the current state.
                 */
                test_state_validate_buf(state,
                                        state->buf_mmap + op->iocb.u.c.offset,
                                        op->iocb.u.c.offset,
                                        op->iocb.u.c.nbytes);

#ifdef notyet
                /*
                 * XXX: drew 2009-05-28 We should probably update
                 * after to match before for the specific error cases
                 * where it matters; but I think this is safe.
                 */
                memcpy(state->buf_after + op->iocb.u.c.offset
                       state->buf_mmap + op->iocb.u.c.offset,
                       op->iocp.u.c.nbytes);
#endif
            } else if (op->iocb.aio_lio_opcode == IO_CMD_PREAD) {
                if (!events[i].res2) {
                    plat_assert(events[i].res == op->iocb.u.c.nbytes);
                    test_state_validate_buf(state, op->buf, op->iocb.u.c.offset,
                                            op->iocb.u.c.nbytes);
                }
            }
        }
    }

    plat_free(events);

    return (ret);
}

static int
test_state_destroy_context(struct test_state *state) {
    int ret;

    if (state->context) {
        ret = paio_destroy(state->context) == -1 ? errno : 0;
        state->context = NULL;
    } else {
        ret = 0;
    }

    return (ret);
}

static void
test_op_free(struct test_op *op) {
    if (op) {
        plat_assert(op->op_state != TEST_OP_STATE_RUNNING);

        if (op->buf) {
            plat_free(op->buf);
        }
        TAILQ_REMOVE(&op->state->all_ops, op, all_ops_entry);
        plat_free(op);
    }
}

/**
 * @brief Validate that user provided buffer matches expected state
 *
 * Asserts on a mismatch.
 *
 * @param state <INOUT> state->buf_before is updated to reflect the
 * observed after state.
 * @param buf <IN> user-provided buffer being validated
 * @param offset_arg <IN> offset into the test file at which the
 * buffer starts
 * @param len <IN> length in bytes
 */
static void
test_state_validate_buf(struct test_state *state, const char *buf,
                        int offset_arg, int len) {
    long offset;
    int c;

    for (offset = 0; offset < len; ++offset) {
        c = buf[offset];
        /* Old state */
        if (c == state->buf_before[offset_arg + offset]) {
        /* New state */
        } else if (c == state->buf_after[offset_arg + offset]) {
            state->buf_before[offset_arg + offset] = c;
        } else {
            /* XXX: drew 2010-03-10 Should print expected and returned */
            plat_fatal("Mismatch");
        }
    }
}

/**
 * @brief Create writable data that is unique and recognizable
 *
 * #test_fill structures are used to populate the buffer with
 * appropriate offset and sequence fields.  #test_fill structures
 * are always aligned with respect to the file start, although
 * writes stopping short of the seq field end may leave a mix
 * of old and new bits.
 */
static void
test_fill_buf(void *buf, int len, int file_offset, int seq) {
    struct test_fill fill;
    int start;
    int end;
    fill.seq = seq;

    for (fill.offset = file_offset / sizeof (fill) * sizeof (fill);
         fill.offset < file_offset + len; fill.offset += sizeof (fill)) {
        if (fill.offset < file_offset) {
            start = file_offset - fill.offset;
        } else {
            start = 0;
        }

        if (fill.offset + sizeof (fill) > file_offset + len) {
            end = sizeof (fill) - file_offset + len - fill.offset;
        } else {
            end = sizeof (fill);
        }

        memcpy(buf + start + fill.offset - file_offset, (char *)&fill + start,
               end - start);
    }
}

static void
test_config_init(struct plat_opts_config_aio_test *config) {
    memset(config, 0, sizeof(*config));
    plat_shmem_config_init(&config->shmem);
    paio_libaio_config_init(&config->paio_libaio);
    paio_wc_config_init(&config->paio_wc);
    paio_error_bdb_config_init(&config->paio_error_bdb);
    config->paio_wc.io_limit = TEST_DEFAULT_WC_IO_LIMIT;
    config->paio_wc.byte_limit = TEST_DEFAULT_WC_BYTE_LIMIT;
    config->paio_wc.delay_submit_until_getevents = TEST_DEFAULT_WC_DELAY;

    strncpy(config->filename, test_default_filename, sizeof(config->filename));

    config->len = TEST_DEFAULT_LEN;
}

static void
test_config_destroy(struct plat_opts_config_aio_test *config) {
    plat_shmem_config_destroy(&config->shmem);
    paio_libaio_config_destroy(&config->paio_libaio);
    paio_wc_config_destroy(&config->paio_wc);
    paio_error_bdb_config_destroy(&config->paio_error_bdb);
}

static int
test_config_parse_mode(struct plat_opts_config_aio_test *config,
                       const char *optarg) {
    int ret;

#define item(caps, lower) \
    if (strcmp(optarg, #lower) == 0) {                                         \
        config->mode = caps;                                                   \
        ret = 0;                                                               \
    } else
    TEST_MODE_ITEMS()
#undef item
    {
#define item(caps, lower) " " #lower
        fprintf(stderr, "Unknown test mode: %s\n"
                "\toptions are"
                TEST_MODE_ITEMS() "\n", optarg);
#undef  item
        ret = -EINVAL;
    }

    return (ret);
}

static const char *
test_op_type_to_string(enum test_op_type op) {
    switch (op) {
#define item(caps, lower) case caps: return (#lower);
    TEST_OP_TYPE_ITEMS()
#undef item
    }

    plat_assert(0);
    return ("Unknown");
}

#include "platform/opts_c.h"
