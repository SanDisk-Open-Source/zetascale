/*
 * File: fcnl_msg_timeout_test.c
 * Author: Zhenwei
 * a simple timeout test case for sdfmsg
 *
 * Created on Feb 8, 2009, 5:04 PM
 *
 * Copyright Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: fcnl_msg_timeout_test.c 6355 2009-02-09 09:57:02Z syncsvn $
 */

#include "sys/time.h"
#include "fth/fth.h"
#include "platform/stdlib.h"
#include "platform/assert.h"
#include "platform/closure.h"
#include "platform/mbox_scheduler.h"
#include "sdfmsg/sdf_msg.h"
#include "sdfmsg/sdf_msg_types.h"
#include "sdfmsg/sdf_fth_mbx.h"
#include "sdfmsg/sdf_msg_wrapper.h"

#include "platform/stdio.h"
#include "common/sdftypes.h"
#include "protocol/protocol_common.h"
#include "protocol/replication/replicator.h"

#include "test_common.h"
#define MOD_BY (0x10000)
#define MILLION 1000000

struct timeval tv_send, tv_now;
int64_t timeout_us_secs;

/*  
 * We use a sub-category under test because test implies a huge number 
 * of log messages out of simulated networking, flash, etc. 
 */
PLAT_LOG_SUBCAT_LOCAL(LOG_CAT, PLAT_LOG_CAT_SDF_PROT_REPLICATION,
                      "test/case");
PLAT_LOG_SUBCAT_LOCAL(LOG_CAT_MSG, PLAT_LOG_CAT_SDF_PROT_REPLICATION,
                      "tests/msg");
#define PLAT_OPTS_NAME(name) name ## _timeout_test
#include "platform/opts.h"
#include "misc/misc.h"

#define PLAT_OPTS_ITEMS_timeout_test()                                                  \
    item("us_secs", "timeout usecs", US_SECS,                                  \
         parse_int(&config->us_secs, optarg, NULL), PLAT_OPTS_ARG_REQUIRED)

struct timeval send_tv, now;
int64_t us_secs;

struct plat_opts_config_timeout_test {
    int us_secs;
};

struct smt_config {
    /** @brief config infomation */
    int64_t timeout_usecs;
};

enum sdf_msg_timeout_state {
    /** @brief sdf_msg_timeout_test alive */
    SMT_LIVE,

    /** @brief sdf_msg_timeout_test dead */
    SMT_DEAD
};

struct sdf_msg_timeout_test {
    /** @brief state */
    enum sdf_msg_timeout_state state;

    /** @brief closure scheduler */
    struct plat_closure_scheduler *closure_scheduler;

    /** @brief closure scheduler thread */
    fthThread_t *closure_scheduler_thread;

    /** @brief config */
    struct smt_config config;

    /** @brief send_msg closure */
    sdf_replicator_send_msg_cb_t send_msg;

    /** @brief get response flag */
    int get_response_flag;
};

struct sdf_msg_request_state {
    /** @brief dynamically allocated mbx structure */
    struct sdf_fth_mbx *mbx;

    /** @brief closure applied on completion */
    sdf_msg_recv_wrapper_t response_closure;

    /** @brief Associated sdf_msg_timeout_test */
    struct sdf_msg_timeout_test *smt;
};

static void
smt_request_state_destroy(struct sdf_msg_request_state *request_state);

void
print_usage() {
    plat_log_msg(LOG_ID, LOG_CAT, LOG_TRACE,
                 "./%s --timeout us_sec",  __FILE__);
}

/**
 * @brief check whether us_secs of (new - old) is greater than
 * diff
 */
static SDF_status_t
smt_timeout_check(struct timeval *old, struct timeval *new,
                  uint64_t diff) {
    SDF_status_t status = SDF_SUCCESS;
    int64_t diff_size;
    struct timeval res;
    timersub(new, old, &res);
    diff_size =  res.tv_sec * MILLION + res.tv_usec;
    if (diff_size > 0 && diff_size > diff) {
        plat_log_msg(LOG_ID, LOG_CAT, LOG_ERR,
                     "remaining_us:%d, target timeout:%d", (int)diff_size, (int)diff);
        status = SDF_FAILURE;
    }
    return (status);
}

static void
smt_closure_scheduler_shutdown_cb(plat_closure_scheduler_t *context,
                                  void *env) {
    struct sdf_msg_timeout_test *smt =
        (struct sdf_msg_timeout_test *)env;

    /* This became invalid at some point before this call */
    smt->closure_scheduler = NULL;
}

static void
smt_shutdown_closure_scheduler(struct sdf_msg_timeout_test *smt) {
    plat_closure_scheduler_shutdown_t cb;

    plat_assert(smt && smt->closure_scheduler);
    cb = plat_closure_scheduler_shutdown_create(smt->closure_scheduler,
                                                &smt_closure_scheduler_shutdown_cb,
                                                smt);

    plat_closure_scheduler_shutdown(smt->closure_scheduler, cb);
}

/**
 * @brief A fake send_msg callback function
 */
static void
smt_send_msg_api_cb(struct plat_closure_scheduler *context, void *env,
                    struct sdf_msg_wrapper *msg_wrapper,
                    struct sdf_fth_mbx *ar_mbx, SDF_status_t *status) {
    struct sdf_msg_timeout_test *smt =
        (struct sdf_msg_timeout_test *)env;

    plat_assert(smt);
    *status = SDF_SUCCESS;
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                 "smt_send_msg_api_cb, send msg callback function");
    gettimeofday(&tv_send, NULL);
}

static struct sdf_msg_timeout_test *
sdf_msg_timeout_test_alloc(struct smt_config *config) {
    struct sdf_msg_timeout_test *ret;
    plat_assert(config);
    if (plat_calloc_struct(&ret)) {
        ret->closure_scheduler = NULL;
        ret->closure_scheduler_thread =
            fthSpawn(&plat_mbox_scheduler_main,
                     40960 /* such large heap size? */);
        ret->config = *config;
        ret->send_msg =
            sdf_replicator_send_msg_cb_create(PLAT_CLOSURE_SCHEDULER_SYNCHRONOUS,
                                              smt_send_msg_api_cb, ret);
        ret->get_response_flag = 0;
    ret->state = SMT_LIVE;
    } else {
        plat_log_msg(LOG_ID, LOG_CAT, LOG_ERR,
                     "allocate sdf_msg_timeout_test error");
        ret = NULL;
    }
    return (ret);
}

static SDF_status_t
smt_start_mbx_scheduler(struct sdf_msg_timeout_test *smt) {
    SDF_status_t ret = SDF_SUCCESS;
    smt->closure_scheduler = plat_mbox_scheduler_alloc();
    plat_assert(smt->closure_scheduler);
    fthResume(smt->closure_scheduler_thread, (uint64_t)smt->closure_scheduler);
    if (!smt->closure_scheduler_thread) {
        ret = SDF_FAILURE;
    }
    return (ret);
}

static void
smt_send_response(struct plat_closure_scheduler *context, void *env,
                  struct sdf_msg_wrapper *response) {
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                 "smt get response");
    struct sdf_msg_request_state *request_state =
        (struct sdf_msg_request_state *)env;
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                 "request_state:%p", request_state);
    /* destroy request_state */
    smt_request_state_destroy(request_state);
    request_state->smt->get_response_flag = 1;
}

static SDF_status_t
smt_send(struct sdf_msg_timeout_test *smt,
         struct sdf_msg_wrapper *send_msg_wrapper) {
    /* ignored params: sdf_msg_recv_wrapper_t response_closure */
    SDF_status_t ret = SDF_SUCCESS;
    struct sdf_msg_request_state *request_state;
    if (plat_calloc_struct(&request_state)) {
        /* request_state->response_closure = response_closure; */
        /* create wrapper and get response */
        request_state->mbx =
        sdf_fth_mbx_resp_closure_alloc(sdf_msg_recv_wrapper_create(smt->closure_scheduler,
                                                                   &smt_send_response,
                                                                   request_state),
                                       SACK_REL_YES,
                                       smt->config.timeout_usecs);
        if (!request_state->mbx) {
            ret = SDF_FAILURE_MEMORY_ALLOC;
        }
        /* print out timeout_usecs */
        plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                     "timeout usecs specified:%d", (int)smt->config.timeout_usecs);
    } else {
        ret = SDF_FAILURE_MEMORY_ALLOC;
    }

    /* send wrapper out */
    if (ret == SDF_SUCCESS) {
        plat_closure_apply(sdf_replicator_send_msg_cb, &smt->send_msg,
                           send_msg_wrapper, request_state->mbx, &ret);
    }
    if (ret == SDF_SUCCESS) {
        plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                     "send message closure out successfully");
    }
    return (ret);
}

static void
smt_destroy(struct sdf_msg_timeout_test *smt) {
    plat_assert(smt);

    if (smt->state == SMT_LIVE) {
        /* close closure scheduler */
        smt_shutdown_closure_scheduler(smt);
        if (!smt->closure_scheduler) {
            smt->state = SMT_DEAD;
            plat_free(smt);
        }
    }
}

static void
smt_request_state_destroy(struct sdf_msg_request_state *request_state) {
    plat_assert(request_state);

    if (request_state->mbx) {
        plat_free(request_state->mbx);
    }
    plat_free(request_state);
}

/**
 * @brief Daemon thread executer
 * @param arg <IN> Allocated sdf_msg_timeout_test environment
 */
static void
smt_daemon_thread(uint64_t arg) {
    struct sdf_msg_timeout_test *smt =
        (struct sdf_msg_timeout_test *)arg;
    static uint64_t count = 0;
    SDF_status_t status = SDF_SUCCESS;

    plat_assert(smt);
    while (1) {
        if (smt->get_response_flag) {
            break;
        } else {
            count++;
            if ((count & (MOD_BY - 1)) == 0) {
                /* check whether timeout */
                gettimeofday(&tv_now, NULL);
                status = smt_timeout_check(&tv_send, &tv_now, timeout_us_secs);
                if (status == SDF_FAILURE) {
                    /* timeout expired */
                    plat_log_msg(LOG_ID, LOG_CAT, LOG_ERR,
                                 "timeout expired!  Kill scheduler pthread");
                    smt_destroy(smt); /* memory leak here, sdf_msg_request_state not free */
                    fthKill(1);
                }
                plat_log_msg(LOG_ID, LOG_CAT, LOG_TRACE,
                             "count:%"PRIu64, count);
            }
            fthYield(-1);
        }
    }
    if (smt->get_response_flag) {
        smt_destroy(smt);
        plat_log_msg(LOG_ID, LOG_CAT, LOG_ERR,
                     "timeout expired!  Kill scheduler pthread");
        fthKill(1);
    }
}

/**
 * This test case send a wrapper using sdf_fth_mbx whose timeout
 * is set as a specified us, and check whether timeout is all
 * right or not, and we don't even need a simulated note.
 */
int
main(int argc, char **argv) {
    SDF_status_t status = SDF_SUCCESS;
    struct sdf_msg_timeout_test *smt = NULL;
    struct smt_config *config;
    struct plat_opts_config_timeout_test opts_config;

    memset(&opts_config, 0, sizeof (opts_config));
    int opts_status = plat_opts_parse_timeout_test(&opts_config, argc, argv);
    if (opts_status) {
        plat_opts_usage_timeout_test();
        return (1);
    }

    if (!opts_config.us_secs) {
        /* Default 1 sec */
        opts_config.us_secs = 100000;
    }

    struct plat_opts_config_replication_test_framework_sm *sm_config;
    plat_calloc_struct(&sm_config);
    plat_assert(sm_config);
 
    /* start shared memory */
    status = framework_sm_init(0, NULL, sm_config);
    plat_assert(status == SDF_SUCCESS);

    /* start fthread library */
    fthInit();

    /* allocate timeout */
    if (status == SDF_SUCCESS) {
        /* allocate a smt_config */
        if (plat_calloc_struct(&config)) {
            config->timeout_usecs = opts_config.us_secs;
            timeout_us_secs = opts_config.us_secs;
            plat_log_msg(LOG_ID, LOG_CAT,  LOG_TRACE,
                         "got usecs %d", (int)opts_config.us_secs);
        } else {
            status = SDF_FAILURE_MEMORY_ALLOC;
        }
        if (status == SDF_SUCCESS) {
            smt = sdf_msg_timeout_test_alloc(config);
            plat_assert(smt);
        }
    }

    status = smt_start_mbx_scheduler(smt);
    if (status == SDF_SUCCESS) {
        status = smt_send(smt, NULL /* wrapper */);
    }

    /* Spawn a fth thread to yield and kill if test case completed */
    XResume(fthSpawn(&smt_daemon_thread, 40960), (uint64_t)smt);
    fthSchedulerPthread(1);

    plat_free(config);
    framework_sm_destroy(sm_config);
    return (0);
}
#include "platform/opts_c.h"
