/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File:   test_flash.c
 * Author: Zhenwei Lu
 *
 * Created on Nov 12, 2008, 12:04 AM
 *
 * Copyright Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: test_flash.c 10739 2009-07-13 14:04:59Z lzwei $
 *
 */

/*
 * FIXME: drew 2009-08-18 This does not support non-persistent shards
 * which we need to get test coverage once we have the shard object
 * iteration code in.
 */

#include "utils/properties.h"

#include "platform/mbox_scheduler.h"
#include "platform/closure.h"
#include "platform/logging.h"
#include "platform/stdlib.h"
#include "platform/stdio.h"

#include "sdfmsg/sdf_msg_action.h"
#include "sdfmsg/sdf_fth_mbx.h"
#include "sdfmsg/sdf_msg.h"
#include "sdfmsg/sdf_msg_types.h"
#include "sdfmsg/sdf_msg_wrapper.h"

#include "protocol/home/home_util.h"
#include "protocol/home/home_flash.h"
#include "protocol/protocol_common.h"

#include "tlmap3.h"
#include "test_node.h"
#include "test_flash.h"
#include "test_framework.h"
#include "test_common.h"

/**
 * Define this macro to have test_flash automatically
 * generate distinct sequence numbers on all flash writes
 * (this includes deletes).
 */
#ifdef FAKE_SEQNO
static uint64_t FakeSeqno = 12345678;
#endif

#define NUM_CHIP 5
PLAT_LOG_SUBCAT_LOCAL(LOG_CAT_FLASH, PLAT_LOG_CAT_SDF_PROT_REPLICATION,
                      "test/flash");
PLAT_LOG_SUBCAT_LOCAL(LOG_CAT_EVENT, PLAT_LOG_CAT_SDF_PROT_REPLICATION,
                      "test/event");
enum flash_msg_type {
    HF_MSG_KV,
    HF_MSG_SHARD
};

typedef struct testFlashCursor {
    SDF_simple_key_t      key;
    uint64_t              seqno;
} tf_cursor_t;

struct rtf_work_state {
    /* @brief Associated test flash */
    struct replication_test_flash *test_flash;

    /* @brief Chip that handle on */
    struct rtf_chip *chip;
};

/* #define FLASH_MAX_DELAY 2000 */

#define SHARD_BUCKETS 0x1000

#ifndef FLASH_DEALY
#define FLASH_DELAY
#endif
/** @brief Current state */

#define RTF_FLASH_STATE_ITEMS() \
    /**                                                                        \
     * @brief Initial state (only on construction)                             \
     *                                                                         \
     * Prior to the replicator's initial start, the mbox dispatcher            \
     * is not running.                                                         \
     */                                                                        \
    item(RTF_STATE_INITIAL, initial)                                           \
    /** @brief Up (legal from RTF_STATE_DEAD) */                               \
    item(RTF_STATE_LIVE, live)                                                 \
    /** @brief Transitioning to dead (legal from RTF_STATE_LIVE) */            \
    item(RTF_STATE_TO_DEAD, to_dead)                                           \
    /** @brief Down (legal from RTF_STATE_TO_DEAD and RTF_STATE_INITIAL) */    \
    item(RTF_STATE_DEAD, dead)                                                 \
    /**                                                                        \
     * @brief Transitioning to RTF_STATE_SHUTDOWN                              \
     * legal from RTF_STATE_LIVE, RTF_STATE_TO_DEAD, and RTF_STATE_DEAD        \
     */                                                                        \
    item(RTF_STATE_TO_SHUTDOWN, to_shutdown)                                   \
    /** @brief Shut down (legal from all states) */                            \
    item(RTF_STATE_SHUTDOWN, shutdown)

enum rtf_state {
#define item(caps, lower) caps,
    RTF_FLASH_STATE_ITEMS()
#undef item
};

/** @brief Internal closure type implementing #rtf_shutdown_async */
PLAT_CLOSURE1(rtf_do_shutdown, replication_test_flash_shutdown_async_cb_t,
              user_cb);

/** @brief Internal closure type implementing #rtf_start */
PLAT_CLOSURE(rtf_do_start);

/** @brief Internal closure type implementing #rtf_crash_async */
PLAT_CLOSURE1(rtf_do_crash, replication_test_flash_crash_async_cb_t, user_cb);

/** @brief Internal closure type implementing #rtf_receive_msg */
PLAT_CLOSURE1(rtf_do_receive_msg, struct sdf_msg_wrapper *, msg_wrapper);

/**
 * @brief Internal closure type implementing #rtf_send_msg
 *
 * The #rtf_send_msg function always returns SDF_SUCCESS because
 * the asynchrony precludes setting it to anything else, and we always
 * want to handle failures local or remote the same way using messaging
 * layer generated message types.
 */
PLAT_CLOSURE1(rtf_do_send_msg, struct sdf_msg_wrapper *, msg_wrapper);

struct rtf_chip {
    /** @brief Associated flash */
    struct replication_test_flash *flash;

    /** @brief Chip id */
    uint32_t chip_id;
  
    /** @brief Inbound msg to be delayed on specified chip */
    struct {
        /** @brief Inbound messages */
        TAILQ_HEAD(, rtf_msg_entry) queue;

        /** @brief Event delivering the next message from queue */
        struct plat_event *next_delivery_event;
    } msg_inbound;
};

struct test_flash_entry {
    /** @brief data */
    void *data;

    /** @brief data_len */
    uint32_t data_len;

    /** @brief Expiry Time */
    SDF_time_t    exptime;

    /** @brief Creation Time */
    SDF_time_t    createtime;

    /** @brief Is this a tombstone? */
    SDF_boolean_t  is_tombstone;
};

struct rtf_shard {
    /** @brief Shard meta */
    struct SDF_shard_meta shard_meta;

    /** @brief Object number in current shard */
    uint32_t num_obj;
  
    /** @brief Shard persistence */
    TLMap3_t shard_map;

    /** @brief Sequence number of last successful, persisted update operation */
    uint64_t last_seqno;

    /** @brief Shard entry */
    TAILQ_ENTRY(rtf_shard) shard;
};


struct replication_test_flash {
    /** @brief Associated node_id */
    vnode_t node_id;

    /** @brief Flash lock */
    fthLock_t flash_lock;

    /** @brief replication test config */
    struct replication_test_config test_config;

    /** @brief shard number on local flash */
    uint32_t shard_num;

    /** @brief Flash state */
    enum rtf_state state;

    /** @brief Simulated chip */
    struct rtf_chip *chips;
  
    /** @biref Chip number per flash */
    int nchips;
  
    /* List of shards */
    TAILQ_HEAD(, rtf_shard) shards;

    /** @brief Outbound interface */
    replication_test_api_t component_api;

    /** @brief Flash scheduler */
    struct plat_closure_scheduler *closure_scheduler;

    /** @brief Closure scheduler shutdown started flag */
    unsigned closure_scheduler_shutdown_started : 1;

    /**
     * @brief number of events in fligh
     *
     * When this reaches 0 in RTF_STATAE_TO_SHUTDOWN or RTF_STATE_TO_DEAD
     * the state becomes RTN_STATE_SHUTDOWN or RTF_STATE_TO_DEAD respectively.
     */
    int ref_count;

    /**
     * @brief Completion for the executing crash
     *
     * From the call to #rtf_crash_async.
     *
     * All subsequently queued crashes apply (schedule) their
     * callback with an appropriate failure status.
     */
    replication_test_flash_crash_async_cb_t crash_cb;

    /**
     * @brief Completion for the executing shutdown
     *
     * From the call to #rtn_shutdown_async
     */
    replication_test_flash_shutdown_async_cb_t shutdown_cb;

    /** @brief get send_msg from mail_box */
    fthMbox_t inbound_mbox;
};

/** @brief Entry in replication_test_flash message structures. */
struct rtf_msg_entry {
    /* @brief Parent of this structure */
    struct replication_test_flash *test_flash;

    /* Fields for all messages */
    /**
     * @brief Message wrapper.
     */
    struct sdf_msg_wrapper *msg_wrapper;

    /** @brief Inbound queue entry (all messages) */
    TAILQ_ENTRY(rtf_msg_entry) msg_inbound_queue_entry;

    /** @brief Incremental delay from previous message */
    uint32_t us_latency;
};

/**
 * @brief create shard and mount to test flash
 * @param test_flash <IN> Test Flash
 * @param shard_meta <IN> Shard meta
 */
static SDF_boolean_t
rtf_create_shard(struct replication_test_flash *test_flash,
                 struct SDF_shard_meta *shard_meta);

/**
 * @brief delete shard from test flash
 * @param test_flash <IN> Test Flash
 * @param shard_id   <IN> Shard_id that to get shard
 */
static SDF_boolean_t
rtf_delete_shard(struct replication_test_flash *test_flash,
                 SDF_shardid_t shard_id);

/**
 * @brief shard retrieve from current flash
 * @param flash <IN> Test Flash
 * @param shard_id <IN> Shard_id that to get shard
 * @return shard ptr if found, else return NULL
 */
static struct rtf_shard *
rtf_shard_find(struct replication_test_flash *flash, SDF_shardid_t shard_id);

/**
 * @brief get reply wrapper from received message
 * @param recv_msg <IN> Received sdf msg
 * @param wrapper <IN> Received sdf_msg_wrapper
 * @return response sdf_msg_wrapper ptr
 */
static struct sdf_msg_wrapper *
rtf_msg_wrapper_reply(struct sdf_msg *recv_msg,
                      struct sdf_msg_wrapper *request_wrapper);
/**
 * @brief free sdf_msg in test_flash
 * @param msg <IN> Sdf_msg that should be free
 */
static void
rtf_msg_free(plat_closure_scheduler_t *context, void *env, struct sdf_msg *msg);

/**
 * @brief Destory test shard
 * @test_shard <IN> Test shard that should be destroyed
 * @return SDF_success if destroyed, else return SDF_failure
 */
static SDF_status_t
rtf_test_shard_destroy(struct rtf_shard *test_shard);

static void
rtf_start_impl(plat_closure_scheduler_t *context, void *env);

static void
rtf_guarantee_bootstrapped(struct replication_test_flash *test_flash);

static const char *
rtf_state_to_string(enum rtf_state state);

static void
rtf_shutdown_closure_scheduler(struct replication_test_flash *test_flash);

static void
rtf_closure_scheduler_shutdown_cb(struct plat_closure_scheduler *context,
                                  void *env);

static void
rtf_shutdown_impl(struct plat_closure_scheduler *context, void *env,
                  replication_test_flash_shutdown_async_cb_t cb);
static void
rtf_crash_impl(struct plat_closure_scheduler *context, void *env,
               replication_test_flash_crash_async_cb_t cb);

static void
rtf_crash_or_shutdown_common(struct replication_test_flash *test_flash);

#ifdef FLASH_DELAY
static void
rtf_crash_or_shutdown_messaging(struct replication_test_flash *test_flash);
#endif

static void
rtf_crash_or_shutdown_event_cb(struct plat_closure_scheduler *context,
                               void *env);
static void
rtf_crash_or_shutdown_complete(struct replication_test_flash *test_flash);

static void
rtf_crash_do_cb(struct replication_test_flash *test_flash,
                replication_test_flash_crash_async_cb_t cb);

static void
rtf_ref_count_dec(struct replication_test_flash *test_flash);

static void
rtf_ref_count_zero(struct replication_test_flash *test_flash);

static void
rtf_free(struct replication_test_flash *test_flash);

#ifdef FLASH_DELAY
static uint32_t
rtf_get_chip(struct rtf_msg_entry *entry);

static void
rtf_msg_entry_free(struct rtf_msg_entry *entry);

static void
rtf_receive_msg_impl(struct plat_closure_scheduler *context, void *env,
                     struct sdf_msg_wrapper *msg_wrapper);

static void
rtf_msg_entry_response(struct rtf_msg_entry *entry,
                       struct sdf_msg_wrapper *response_wrapper);

static void
rtf_msg_entry_error(struct replication_test_flash *test_flash,
                    struct rtf_msg_entry *entry, SDF_status_t status);

static void
rtf_msg_entry_dispatch(struct rtf_msg_entry *entry);

static void
rtf_msg_entry_add_to_chip(struct rtf_msg_entry *entry, struct rtf_chip *chip);

static void
rtf_msg_entry_remove_from_chip(struct rtf_msg_entry *entry, struct rtf_chip *chip);

static void
rtf_reset_chip_timer(struct replication_test_flash *test_flash,
                     struct rtf_chip *chip);

static void
rtf_next_delivery_cb(plat_closure_scheduler_t *context, void *env,
                     struct plat_event *event);

static void
rtf_deliver_msg(struct replication_test_flash *test_flash,
                struct rtf_chip *chip);

static void
rtf_next_delivery_event_free_cb(plat_closure_scheduler_t *context, void *env);
#endif

struct replication_test_flash *
replication_test_flash_alloc(const struct replication_test_config *test_config,
                             const replication_test_api_t *api,
                             vnode_t node_id) {
    struct replication_test_flash *ret = NULL;
    int failed;

    failed = !plat_calloc_struct(&ret);
    if (!failed) {
        ret->test_config = *test_config;
        TAILQ_INIT(&ret->shards);

        ret->node_id = node_id;
        fthLockInit(&(ret->flash_lock));
    }
#ifdef FLASH_DELAY
    int i;
    ret->chips = plat_alloc(NUM_CHIP *sizeof(struct rtf_chip));
    ret->nchips = NUM_CHIP;
    for (i = 0; i < NUM_CHIP; i++) {
        ret->chips[i].chip_id = i;
        ret->chips[i].flash = ret;
        ret->chips[i].msg_inbound.next_delivery_event = NULL;
        TAILQ_INIT(&ret->chips[i].msg_inbound.queue);
    }
#endif
    ret->closure_scheduler = plat_mbox_scheduler_alloc();

    /* initialize mailbox of flash */
    if (!failed) {
        fthMboxInit(&(ret->inbound_mbox));
    }

    if (!failed) {
        ret->component_api = *api;
    }

    if (!failed) {
        ret->shard_num = 0;
        plat_log_msg(LOG_ID, LOG_CAT_FLASH, LOG_DBG,
                     "node_id %"PRIu32" replication test flash %p allocated.", ret->node_id, ret);
    } else if (!ret) {
        plat_log_msg(LOG_ID, LOG_CAT_FLASH, LOG_ERR,
                     "node_id %"PRIu32" replication test flash %p failed.", ret->node_id, ret);
        plat_free(ret);
    }
    return (ret);
}

static void
rtf_shutdown_impl(struct plat_closure_scheduler *context, void *env,
                  replication_test_flash_shutdown_async_cb_t cb) {
    struct replication_test_flash *test_flash =
        (struct replication_test_flash *)env;
  
    plat_assert(test_flash->state != RTF_STATE_INITIAL &&
                test_flash->state != RTF_STATE_SHUTDOWN &&
                test_flash->state != RTF_STATE_TO_SHUTDOWN);
  
    plat_log_msg(LOG_ID, LOG_CAT_FLASH, LOG_DBG,
                 "node_id:%"PRIu32" shutdown flash impli", test_flash->node_id);
  
    switch (test_flash->state) {
    case RTF_STATE_INITIAL:
        plat_assert(0);
        break;
    case RTF_STATE_LIVE:
    case RTF_STATE_TO_DEAD:
    case RTF_STATE_DEAD:
        test_flash->state = RTF_STATE_TO_SHUTDOWN;
        test_flash->shutdown_cb = cb;
        rtf_crash_or_shutdown_common(test_flash);
        break;
    case RTF_STATE_TO_SHUTDOWN:
    case RTF_STATE_SHUTDOWN:
        plat_assert(0);
        break;
    }
}

/**
 * @brief Common code for flash shutdown and crash
 *
 * It is an interface for adding flash delay later, though currently
 * there is no much code here.
 */
static void
rtf_crash_or_shutdown_common(struct replication_test_flash *test_flash) {
    plat_event_free_done_t event_cb;
#ifdef FLASH_DELAY
    int i;
    struct rtf_chip *chip;
#endif
  
  
#ifdef FLASH_DELAY
    for (i = 0; i < NUM_CHIP; i++) {
        plat_assert(&test_flash->chips[i]);
        chip = &test_flash->chips[i];
        if (chip->msg_inbound.next_delivery_event) {
            /* cancel events */
            event_cb = plat_event_free_done_create(test_flash->closure_scheduler,
                                                   &rtf_crash_or_shutdown_event_cb,
                                                   test_flash);

            /* Possibly we should add ref_count for each chip, all right? */
            ++test_flash->ref_count;
            plat_event_free(chip->msg_inbound.next_delivery_event, event_cb);
            chip->msg_inbound.next_delivery_event = NULL;
        }
    }

    /* cancel messages on chips */
    rtf_crash_or_shutdown_messaging(test_flash);
#endif
    if (!test_flash->ref_count) {
        rtf_ref_count_zero(test_flash);
    }

}

/**
 * @brief Internal callback function once event canceled
 *
 */
static void
rtf_crash_or_shutdown_event_cb(struct plat_closure_scheduler *context,
                               void *env) {
    struct replication_test_flash *test_flash =
        (struct replication_test_flash *)env;
    plat_assert(test_flash->state == RTF_STATE_TO_DEAD ||
                test_flash->state == RTF_STATE_TO_SHUTDOWN);

    rtf_ref_count_dec(test_flash);
}

#ifdef FLASH_DELAY
/*
 * @brief Remove incompleted messages in test flash
 */
static void
rtf_crash_or_shutdown_messaging(struct replication_test_flash *test_flash) {
    int i;
    struct rtf_chip *chip;
    struct rtf_msg_entry *entry  __attribute__((unused));
    SDF_status_t error_type;

    plat_assert(test_flash->state == RTF_STATE_TO_DEAD ||
                test_flash->state == RTF_STATE_TO_SHUTDOWN);

    if (test_flash->state == RTF_STATE_TO_DEAD) {
        error_type = SDF_TEST_CRASH;
    } else {
        error_type = SDF_SHUTDOWN;
    }

    for (i = 0; i < NUM_CHIP; i++) {
        chip = &test_flash->chips[i];
        plat_assert(chip);

#ifdef FLASH_DELAY
        /* Remove all messages that not send yet */
        while (!TAILQ_EMPTY(&chip->msg_inbound.queue)) {
            entry = TAILQ_FIRST(&chip->msg_inbound.queue);
            rtf_msg_entry_error(test_flash, entry, error_type);
            TAILQ_REMOVE(&chip->msg_inbound.queue, entry,
                         msg_inbound_queue_entry);
        }
#endif
    }

}
#endif
/**
 * @brief Common code for flash shutdown and crash complete
 */
static void
rtf_crash_or_shutdown_complete(struct replication_test_flash *test_flash) {
    replication_test_flash_shutdown_async_cb_t shutdown_cb;
    replication_test_flash_crash_async_cb_t crash_cb;
  
    plat_assert(test_flash->state == RTF_STATE_DEAD ||
                test_flash->state == RTF_STATE_SHUTDOWN);
  
    shutdown_cb = test_flash->shutdown_cb;
    crash_cb = test_flash->crash_cb;
  
    if (test_flash->state == RTF_STATE_DEAD ||
        !replication_test_flash_crash_async_cb_is_null(&crash_cb)) {
        test_flash->crash_cb = replication_test_flash_crash_async_cb_null;
        rtf_crash_do_cb(test_flash, crash_cb);
    }
    if (test_flash->state == RTF_STATE_SHUTDOWN ||
        !replication_test_flash_shutdown_async_cb_is_null(&shutdown_cb)) {
        test_flash->shutdown_cb = replication_test_flash_shutdown_async_cb_null;
        rtf_free(test_flash);
        plat_closure_apply(replication_test_flash_shutdown_async_cb,
                           &shutdown_cb);
    }
}

/**
 * @brief Internal do crash implementation on crash complete
 */
static void
rtf_crash_do_cb(struct replication_test_flash *test_flash,
                replication_test_flash_crash_async_cb_t cb) {
    plat_log_msg(LOG_ID, LOG_CAT_FLASH, LOG_DBG,
                 "node_id %"PRIu32" crash completed",
                 test_flash->node_id);

    plat_closure_apply(replication_test_flash_crash_async_cb, &cb);
}


/**
 * @brief Decrease test flash reference
 */
static void
rtf_ref_count_dec(struct replication_test_flash *test_flash) {
    plat_assert(test_flash->ref_count > 0);
  
    --test_flash->ref_count;
    if (!test_flash->ref_count) {
        rtf_ref_count_zero(test_flash);
    }
}

/**
 * @brief Called when reference count of test flash decrease to 0
 */
static void
rtf_ref_count_zero(struct replication_test_flash *test_flash) {
    plat_assert(0 == test_flash->ref_count);
  
    if (test_flash->state == RTF_STATE_TO_DEAD) {
        test_flash->state = RTF_STATE_DEAD;
        rtf_crash_or_shutdown_complete(test_flash);
    } else if (test_flash->state == RTF_STATE_TO_SHUTDOWN) {
        if (!test_flash->closure_scheduler_shutdown_started) {
            /* shutdown closure_scheduler */
            rtf_shutdown_closure_scheduler(test_flash);
        } else {
            test_flash->state = RTF_STATE_SHUTDOWN;
            rtf_crash_or_shutdown_complete(test_flash);
        }
    }
}


/**
 * @brief Shutdown and free simulated flash,
 * Will be implemented as fthThread later
 */
void
rtf_shutdown_async(struct replication_test_flash *test_flash,
                   replication_test_flash_shutdown_async_cb_t cb) {
    rtf_do_shutdown_t do_shutdown;
    plat_log_msg(LOG_ID, LOG_CAT_FLASH, LOG_DBG,
                 "node_id %"PRIu32" shutdown flash successfully!", test_flash->node_id);
    do_shutdown =
        rtf_do_shutdown_create(test_flash->closure_scheduler,
                               &rtf_shutdown_impl,
                               test_flash);
    plat_closure_apply(rtf_do_shutdown, &do_shutdown, cb);
}

SDF_status_t
rtf_start(struct replication_test_flash *test_flash) {
    rtf_do_start_t do_start;
    do_start = rtf_do_start_create(test_flash->closure_scheduler,
                                   &rtf_start_impl, test_flash);
    plat_closure_apply(rtf_do_start, &do_start);
    /* After, so do_start is most likely to execute first */
    rtf_guarantee_bootstrapped(test_flash);
    return (SDF_SUCCESS);
}

static void
rtf_start_impl(plat_closure_scheduler_t *context, void *env) {
    struct replication_test_flash *test_flash =
        (struct replication_test_flash *)env;
    SDF_status_t status = SDF_SUCCESS;

    /* State must be at least RTN_STATE_DEAD before getting here */
    plat_assert(test_flash->state != RTF_STATE_INITIAL);

    plat_log_msg(LOG_ID, LOG_CAT_FLASH, LOG_TRACE, "node %lld rtn_start_impl",
                 (long long)test_flash->node_id);

    if (test_flash->state != RTF_STATE_DEAD) {
        plat_log_msg(LOG_ID, LOG_CAT_FLASH, LOG_TRACE,
                     "node %lld skipping start: state %s not RTN_STATE_DEAD",
                     (long long)test_flash->node_id,
                     rtf_state_to_string(status));
        status = SDF_FAILURE;
    }

    if (status == SDF_SUCCESS) {
        test_flash->state = RTF_STATE_LIVE;
    }
}
/**
 * @brief Guarantee the closure scheduler is running
 *
 * This means that a first call from consumer or producter other than
 * #rtf_start can indirect via its four line proxy and get correct
 * results.
 *
 * The caller should probably enqueue its worker closure before
 * calling this so that it executes "first" but that's not required.
 */
static void
rtf_guarantee_bootstrapped(struct replication_test_flash *test_flash) {
    /* Special case the transition between initial and starting state */
    if (test_flash->state == RTF_STATE_INITIAL &&
        __sync_bool_compare_and_swap(&test_flash->state,
                                     RTF_STATE_INITIAL /* from */,
                                     RTF_STATE_DEAD /* to */)) {

        /* start event scheduler */
        fthResume(fthSpawn(&plat_mbox_scheduler_main, 40960),
                  (uint64_t)test_flash->closure_scheduler);
    }
}

/**
 * @brief Crash simulated flash asynchronously
 *
 * Without "persistent" simulated flash, the flash component of a simulated
 * crash means that an IO fence is introduced where no new inbound messages
 * are allowed until the existing flash operations complete and have their
 * responses discarded.
 *
 * @param cb <IN> Applied when the "crash"  completes
 */
void
rtf_crash_async(struct replication_test_flash *test_flash,
                replication_test_flash_crash_async_cb_t cb) {
    rtf_do_crash_t do_crash;
    plat_log_msg(LOG_ID, LOG_CAT_FLASH, LOG_DBG,
                 "node_id %"PRIu32" crash flash", test_flash->node_id);
    do_crash =
        rtf_do_crash_create(test_flash->closure_scheduler,
                            &rtf_crash_impl,
                            test_flash);
    plat_closure_apply(rtf_do_crash, &do_crash, cb);
}

/*
 * @brief Shutdown flash asynchronous implementation
 */
static void
rtf_crash_impl(struct plat_closure_scheduler *context, void *env,
               replication_test_flash_crash_async_cb_t cb) {
    struct replication_test_flash *test_flash =
        (struct replication_test_flash *)env;
    plat_assert(test_flash->state != RTF_STATE_INITIAL &&
                test_flash->state != RTF_STATE_SHUTDOWN &&
                test_flash->state != RTF_STATE_TO_SHUTDOWN);
  
    plat_log_msg(LOG_ID, LOG_CAT_FLASH, LOG_DBG,
                 "node_id:%"PRIu32" crash flash impl", test_flash->node_id);
    switch (test_flash->state) {
    case RTF_STATE_INITIAL:
        plat_assert(0);
        break;
    case RTF_STATE_LIVE:
        test_flash->state = RTF_STATE_TO_DEAD;
        test_flash->crash_cb = cb;
        rtf_crash_or_shutdown_common(test_flash);
        break;
    case RTF_STATE_DEAD:
    case RTF_STATE_TO_DEAD:
        rtf_crash_do_cb(test_flash, cb);
        break;
    case RTF_STATE_TO_SHUTDOWN:
    case RTF_STATE_SHUTDOWN:
        plat_assert(0);
        break;
    }
}
void
rtf_crash_async_cb(plat_closure_scheduler_t *context, void *env, SDF_status_t status) {

}

/**
 * @brief Simulated version of sdf/protocol/home/home_flash.c home_flash_wrapper
 * @return a sdf_msg_wrapper to flash layer
 */
static struct sdf_msg_wrapper *
home_flash_test_kv_wrapper(struct replication_test_flash *test_flash,
                           struct sdf_msg_wrapper *msg_wrapper)
{
    char                    *key;
    size_t                   key_len;
    struct sdf_msg          *recv_msg = NULL;
    struct sdf_msg          *new_msg = NULL;
    struct sdf_msg_wrapper  *wrapper = NULL;
    struct rtf_shard        *shard;
    SDF_protocol_msg_t      *pm = NULL;
    struct test_flash_entry *flash_entry;
    TLMap3Entry_t           *pme;
    SDF_size_t               data_size = 0;
    void                    *pdata = NULL;
    SDF_status_t             status = SDF_FAILURE;
    SDF_protocol_msg_type_t  new_mtype;
    SDF_size_t               msize;
    SDF_time_t               exptime;
    SDF_time_t               createtime;
    uint64_t                 seqno;

    sdf_msg_wrapper_rwref(&recv_msg, msg_wrapper);
    pm = (SDF_protocol_msg_t *)recv_msg->msg_payload;
    SDF_shardid_t shard_id = pm->shard;

    key        = pm->key.key;
    key_len    = pm->key.len;
    exptime    = 0;
    createtime = 0;
    seqno      = SDF_SEQUENCE_NO_INVALID;
  
    /* shard search */
    shard = rtf_shard_find(test_flash, shard_id);
    if (shard) {
        flash_entry = NULL;

        switch (pm->msgtype) {
        case HFXST: /* flash exists */
            plat_log_msg(LOG_ID, LOG_CAT_FLASH, LOG_DBG,
                         "node_id %"PRIu32" fake Func:%s()  ",
                         test_flash->node_id, __FUNCTION__);
            break;
        case HFGFF: /* flash get */
            /* construct a wrapper and response to node */
            if (NULL != (pme = TLMap3Get(&(shard->shard_map), key, key_len))) {
                flash_entry = (struct test_flash_entry *)pme->contents;
                if (!flash_entry->is_tombstone) {
                    data_size   = flash_entry->data_len;
                    exptime     = flash_entry->exptime;
                    createtime  = flash_entry->createtime;
                    seqno       = pme->seqno;
                    pdata       = plat_malloc(flash_entry->data_len);
                    memcpy(pdata, flash_entry->data, flash_entry->data_len);
                    status = SDF_SUCCESS;
                } else {
                    status = SDF_OBJECT_UNKNOWN;
                    plat_log_msg(LOG_ID, LOG_CAT_FLASH, LOG_DBG,
                                 "node_id %"PRIu32" Hashmap get key %.*s is tombstone",
                                 test_flash->node_id, (int)key_len, (char *)key);
                }
            } else {
                status = SDF_OBJECT_UNKNOWN;
                plat_log_msg(LOG_ID, LOG_CAT_FLASH, LOG_DBG,
                             "node_id %"PRIu32" Hashmap get key %.*s not exist",
                             test_flash->node_id, (int)key_len, (char *)key);
            }
            break;

        case HFSET:
        case HFCIF: /* flash create */
        case HFPTF: /* flash put */
            pme = TLMap3Get(&(shard->shard_map), key, key_len);
            if (!pme) {

                pme = TLMap3Create(&(shard->shard_map), key, key_len);
                if (!pme) {
                    status = SDF_FAILURE_MEMORY_ALLOC;
                } else {

                    flash_entry = plat_calloc(1, sizeof(struct test_flash_entry));
                    plat_assert(flash_entry);
                    pme->contents = flash_entry;

                    flash_entry->data = plat_malloc(pm->data_size);
                    plat_assert(flash_entry->data);
                    flash_entry->data_len = pm->data_size;
                    memcpy(flash_entry->data, (char *)pm+sizeof(*pm),
                           flash_entry->data_len);

                    flash_entry->is_tombstone = SDF_FALSE;
                    flash_entry->exptime      = pm->exptime;
                    flash_entry->createtime   = pm->createtime;
#ifndef FAKE_SEQNO
                    pme->seqno            = pm->seqno;
#else
                    pme->seqno            = FakeSeqno++;
#endif

                    __sync_fetch_and_add(&shard->num_obj, 1);

                    plat_log_msg(LOG_ID, LOG_CAT_FLASH, LOG_DBG,
                                 "node_id %"PRIu32" Hashmap create key %.*s ok",
                                 test_flash->node_id,
                                 (int)key_len, (char *)(key));
                    status = SDF_SUCCESS;
                }
            } else if (((struct test_flash_entry *)pme->contents)->is_tombstone) {

                /*
                 * Hash table entry is just a tombstone, so I can reuse it.
                 */

                flash_entry = (struct test_flash_entry *)pme->contents;
                flash_entry->data = plat_malloc(pm->data_size);
                plat_assert(flash_entry->data);
                flash_entry->data_len = pm->data_size;
                memcpy(flash_entry->data, (char *)pm+sizeof(*pm),
                       flash_entry->data_len);

                flash_entry->is_tombstone = SDF_FALSE;
                flash_entry->exptime      = pm->exptime;
                flash_entry->createtime   = pm->createtime;
#ifndef FAKE_SEQNO
                pme->seqno            = pm->seqno;
#else
                pme->seqno            = FakeSeqno++;
#endif

                __sync_fetch_and_add(&shard->num_obj, 1);

                plat_log_msg(LOG_ID, LOG_CAT_FLASH, LOG_DBG,
                             "node_id %"PRIu32" Hashmap create key (over a tombstone) %.*s ok",
                             test_flash->node_id,
                             (int)key_len, (char *)(key));
                status = SDF_SUCCESS;
            } else if (pm->msgtype != HFSET) {
                plat_log_msg(LOG_ID, LOG_CAT_FLASH, LOG_DBG,
                             "node_id %"PRIu32" Hashmap put key %.*s exists",
                             test_flash->node_id, (int)key_len,
                             (char *)(key));

                status = SDF_OBJECT_EXISTS;
            } else {

                flash_entry = (struct test_flash_entry *)pme->contents;
                plat_free(flash_entry->data);
                flash_entry->data_len = pm->data_size;
                flash_entry->data = plat_malloc(pm->data_size);
                plat_assert(flash_entry->data);
                memcpy(flash_entry->data, (char *)pm+sizeof(*pm),
                       flash_entry->data_len);

                flash_entry->is_tombstone = SDF_FALSE;
                flash_entry->exptime    = pm->exptime;
                flash_entry->createtime = pm->createtime;
#ifndef FAKE_SEQNO
                pme->seqno            = pm->seqno;
#else
                pme->seqno            = FakeSeqno++;
#endif

                plat_log_msg(LOG_ID, LOG_CAT_FLASH, LOG_DBG,
                             "node_id %"PRIu32" Hashmap replace key %.*s ok"
                             "pm->data_size:%d, f_entry->data_size:%d",
                             test_flash->node_id, (int)key_len,
                             (char *)(key), (int)(pm->data_size),
                             (int)(flash_entry->data_len));

                status = SDF_SUCCESS;
            }
            if (status == SDF_SUCCESS) {
#ifndef FAKE_SEQNO
                shard->last_seqno = pm->seqno;
#else
                shard->last_seqno = FakeSeqno - 1;
#endif
            }
            break;
        case HZSF: /* flash delete */
            /* construct a wrapper and response to node */
            plat_log_msg(LOG_ID, LOG_CAT_FLASH, LOG_DBG,
                         "node_id %"PRIu32" Hashmap delete key:%s ",
                         test_flash->node_id, (char *)key);
            pme = TLMap3Get(&(shard->shard_map), key, key_len);
            if (pme && (!(((struct test_flash_entry *)pme->contents)->is_tombstone))) {
                /*
                 * I am responsible for freeing the data.
                 * TLMap3Delete will take care of the flash_entry structure.
                 * (But, I don't call TLMap3Delete right now because I want to
                 * keep tombstones around).
                 */
                flash_entry = (struct test_flash_entry *)pme->contents;
                plat_free(flash_entry->data);
                flash_entry->is_tombstone = SDF_TRUE;
#ifndef FAKE_SEQNO
                pme->seqno            = pm->seqno;
#else
                pme->seqno            = FakeSeqno++;
#endif
                __sync_fetch_and_sub(&shard->num_obj, 1);
                status = SDF_SUCCESS;

                /*
                 *  Sometime I need to add tombstone garbage collection.
                 */
            } else {
                status = SDF_OBJECT_UNKNOWN;
                plat_log_msg(LOG_ID, LOG_CAT_FLASH, LOG_DBG,
                             "node_id %"PRIu32" Hashmap delete key %.*s not exist",
                             test_flash->node_id, (int)key_len, (char *)key);
            }
        if (status == SDF_SUCCESS) {
#ifndef FAKE_SEQNO
            shard->last_seqno = pm->seqno;
#else
            shard->last_seqno = FakeSeqno - 1;
#endif
        }
            break;
        default:
            plat_fatal("");
        }
    } else {
        /* This is a normal situation for replicator meta storage operation */
        plat_log_msg(LOG_ID, LOG_CAT_FLASH, LOG_DIAG,
                     "node_id %"PRIu32" shard not existed, shard_id:%"PRIu64,
                     test_flash->node_id, shard_id);
        status = SDF_SHARD_NOT_FOUND;
    }
    new_mtype =
            home_flash_response_type(pm->msgtype, status);
    new_msg = home_load_msg(recv_msg->msg_dest_vnode, recv_msg->msg_src_vnode,
                            pm, new_mtype,
                            pdata /* data */, data_size /* data_size */,
                            exptime /* exptime */, createtime /* createtime */,
                            seqno,
                            status, &msize, NULL, 0, 0);

    /* xxxzzz remove me! */
#if 0
    if (pm->msgtype == HZSF) {
    plat_log_msg(LOG_ID, LOG_CAT_FLASH, LOG_ERR,
                 "===============> node_id %"PRIu32" shard_id:%"PRIu64" HZSF response=%s, key=%s, mkey=%s",
                 test_flash->node_id, shard_id,
                 SDF_Protocol_Msg_Info[new_mtype].shortname,
                 pm->key.key,
                 recv_msg->mkey);
    }
#endif

    plat_free(pdata);
    new_msg->msg_len = sizeof(*new_msg) + sizeof(SDF_protocol_msg_t) + data_size;
    new_msg->sent_id = recv_msg->sent_id;
    new_msg->msg_type = FLSH_RESPOND;
    new_msg->msg_flags |= SDF_MSG_FLAG_MBX_RESP_INCLUDED;
    new_msg->msg_dest_vnode = recv_msg->msg_src_vnode;
    new_msg->msg_src_vnode = test_flash->node_id;
    new_msg->msg_dest_service = recv_msg->msg_src_service;
    new_msg->msg_src_service = recv_msg->msg_dest_service;

    wrapper = rtf_msg_wrapper_reply(new_msg, msg_wrapper);
    sdf_msg_wrapper_rwrelease(&recv_msg, msg_wrapper);
    return (wrapper);
}

/**
 * @brief Simulated version of sdf/protocol/home/home_flash.c home_flash_shard_wrapper()
 */
static struct sdf_msg_wrapper *
home_flash_test_shard_wrapper(struct replication_test_flash *test_flash,
                              struct sdf_msg_wrapper *msg_wrapper) {
    struct sdf_msg *recv_msg = NULL;
    struct sdf_msg *new_msg = NULL;
    struct sdf_msg_wrapper *ret = NULL;
    struct SDF_shard_meta *shard_meta;
    struct rtf_shard *shard;
    SDF_protocol_msg_t *pm = NULL;
    SDF_status_t status;
    void *pdata = NULL;
    SDF_size_t data_size = 0;
    SDF_protocol_msg_type_t new_mtype;
    SDF_size_t msize;
    uint64_t   last_seqno;
    resume_cursor_t      resume_cursor;
    resume_cursor_t     *presume_cursor;
    it_cursor_t         *pitc;
    int                  i;
    int                  n_cursors;
    TLMap3Entry_t       *pme;
    uint64_t             last_bucket_no;
    uint64_t             next_bucket_no;
    tf_cursor_t         *ptfc;
    struct test_flash_entry    *flash_entry;
    SDF_time_t           exptime = 0;
    SDF_time_t           createtime = 0;
    uint64_t             seqno = SDF_SEQUENCE_NO_INVALID;
    uint32_t             flags = 0;
    char                *key = NULL;
    int                  key_len = 0;
  
    sdf_msg_wrapper_rwref(&recv_msg, msg_wrapper);
    pm = (SDF_protocol_msg_t *)recv_msg->msg_payload;
    SDF_shardid_t shard_id = pm->shard;

    status = SDF_FAILURE /* to avoid optimize failure */;
    switch (pm->msgtype) {
    case HFCSH:
        /* shard create */
        shard = rtf_shard_find(test_flash, shard_id);
        if (shard) {
            /*
             * XXX: drew 2009-08-21 the current incarnation of the
             * shardCreate interface only returns NULL on failure
             * without specifying what the error is.
             *
             * sdf/protocol/home/home_flash.c translates that
             * into SDF_FAILURE_STORAGE.  So for now we treat
             * SDF_FAILURE_STORAGE as if the shard already
             * exists.
             *
             * Currently replicator_meta_storage doesn't special case
             * multiple containers which share the same flash shard
             * so it will do this notably in the distributed meta
             * data cases
             */
            plat_log_msg(LOG_ID, LOG_CAT_FLASH, LOG_DIAG,
                         "node_id %"PRIu32" shard %"PRIu64" exists ",
                         test_flash->node_id, shard_id);
#ifdef notyet
            status = SDF_SUCCESS;
#else /* def notyet */
            status = SDF_FAILURE_STORAGE_WRITE;
#endif /* else def notyet */
        } else {
            /* create shard */
            shard_meta = plat_calloc(1, sizeof(struct SDF_shard_meta));
            memcpy(shard_meta, (char *)pm+sizeof(*pm), sizeof(struct SDF_shard_meta));
            if (rtf_create_shard(test_flash, shard_meta)) {
                plat_log_msg(LOG_ID, LOG_CAT_FLASH, LOG_DBG,
                             "node_id %"PRIu32" create shard %"PRIu64 " is successful",
                             test_flash->node_id, shard_id);
            }
            plat_free(shard_meta);
            status = SDF_SUCCESS;
        }
        break;
    case HFGLS:
        /* get latest sequence number */
        shard = rtf_shard_find(test_flash, shard_id);
        if (!shard) {
            plat_log_msg(LOG_ID, LOG_CAT_FLASH, LOG_ERR,
                         "node_id %"PRIu32" shard %"PRIu64" does not exist ",
                         test_flash->node_id, shard_id);
            status = SDF_SHARD_NOT_FOUND;
        } else {
            /* get latest sequence number */
            seqno = shard->last_seqno;
            plat_log_msg(LOG_ID, LOG_CAT_FLASH, LOG_DBG,
                         "node_id %"PRIu32" shard %"PRIu64" get latest seqno %"PRIu64" is successful",
                         test_flash->node_id, shard_id, shard->last_seqno);
            status = SDF_SUCCESS;
        }
        break;
    case HFDSH:
        shard = rtf_shard_find(test_flash, shard_id);
        if (!shard) {
            plat_log_msg(LOG_ID, LOG_CAT_FLASH, LOG_ERR,
                         "node_id %"PRIu32" shard %"PRIu64" does not exist ",
                         test_flash->node_id, shard_id);
            status = SDF_SHARD_NOT_FOUND;
        } else {
            if (rtf_delete_shard(test_flash, shard_id)) {
                plat_log_msg(LOG_ID, LOG_CAT_FLASH, LOG_DBG,
                             "node_id %"PRIu32" delete shard %"PRIu64""
                             "is successful", test_flash->node_id,
                             shard_id);
                status = SDF_SUCCESS;
            } else {
                plat_log_msg(LOG_ID, LOG_CAT_FLASH, LOG_ERR,
                             "node_id %"PRIu32" delete shard %"PRIu64" failed",
                             test_flash->node_id, shard_id);
            }
        }
        break;
    case HFGIC:
        /* get iteration cursors */
        shard = rtf_shard_find(test_flash, shard_id);
        if (!shard) {
            plat_log_msg(LOG_ID, LOG_CAT_FLASH, LOG_ERR,
                         "node_id %"PRIu32" shard %"PRIu64" does not exist ",
                         test_flash->node_id, shard_id);
            status = SDF_SHARD_NOT_FOUND;
        } else {
            /* get iteration cursors */

            if (pm->data_size == 0) {
            // this is the first call to get iteration cursors

        presume_cursor = &resume_cursor;
        resume_cursor.seqno_start  = pm->seqno;
        resume_cursor.seqno_len    = pm->seqno_len;
        resume_cursor.seqno_max    = pm->seqno_max;
        resume_cursor.cursor_state = 0; // bucket number
        resume_cursor.cursor1      = pm->seqno; // last seqno
        resume_cursor.cursor2      = 0; // unused

        } else {
            // this is a continuation of a prior call to get iteration cursors
            // get the resume_cursor
            presume_cursor = (resume_cursor_t *)((unsigned char *)pm + sizeof(SDF_protocol_msg_t));
        }

        // pack up the cursors into an opaque blob
   
            data_size = sizeof(it_cursor_t) + presume_cursor->seqno_len*sizeof(tf_cursor_t);
        pitc = plat_alloc(data_size);
        pdata = (void *)pitc;
        plat_assert(pitc);

        pitc->resume_cursor.seqno_start  = presume_cursor->seqno_start;
        pitc->resume_cursor.seqno_len    = presume_cursor->seqno_len;
        pitc->resume_cursor.seqno_max    = presume_cursor->seqno_max;

        pitc->cursor_len = sizeof(tf_cursor_t);

            ptfc = (tf_cursor_t *)pitc->cursors;
        n_cursors = 0;
        last_bucket_no = presume_cursor->cursor_state;
        last_seqno     = presume_cursor->cursor1;
        for (i = 0; i < presume_cursor->seqno_len; i++) {
            pme = TLMap3NextEnumeration(&(shard->shard_map), last_bucket_no, last_seqno, &next_bucket_no);
            if (pme == NULL) {
                break;
            }
            if ((pme->seqno >= presume_cursor->seqno_start) &&
                (pme->seqno <= presume_cursor->seqno_max)) {
                (void) strcpy(ptfc->key.key, pme->key);
                ptfc->key.len = pme->keylen;
                ptfc->seqno = pme->seqno;
                ptfc++;
                n_cursors++;
            }
            last_bucket_no = next_bucket_no;
            last_seqno     = pme->seqno;
        }

        pitc->cursor_count               = n_cursors;
        pitc->resume_cursor.cursor_state = last_bucket_no; // bucket number
        pitc->resume_cursor.cursor1      = last_seqno;     // last seqno
        pitc->resume_cursor.cursor2      = 0;              // unused

        plat_log_msg(LOG_ID, LOG_CAT_FLASH, LOG_DBG,
                     "node_id %"PRIu32" shard %"PRIu64" get iteration cursors "
                     "(seq_start=%"PRIu64", seq_len=%"PRIu64", seq_max=%"PRIu64") is successful",
                     test_flash->node_id, shard_id, pm->seqno, pm->seqno_len, pm->seqno_max);
            status = SDF_SUCCESS;
        }
        break;
    case HFGBC:
        /* get by cursor */
        shard = rtf_shard_find(test_flash, shard_id);
        if (!shard) {
            plat_log_msg(LOG_ID, LOG_CAT_FLASH, LOG_ERR,
                         "node_id %"PRIu32" shard %"PRIu64" does not exist ",
                         test_flash->node_id, shard_id);
            status = SDF_SHARD_NOT_FOUND;
        } else {
            /* get by cursor */

            plat_assert(pm->data_size == sizeof(tf_cursor_t));

        // get the resume_cursor
        ptfc = (tf_cursor_t *)((unsigned char *)pm + sizeof(SDF_protocol_msg_t));

        // look up the object
        if ((NULL == (pme = TLMap3Get(&(shard->shard_map), ptfc->key.key, ptfc->key.len)))) {
            plat_log_msg(LOG_ID, LOG_CAT_FLASH, LOG_ERR,
                         "node_id %"PRIu32" shard %"PRIu64" cursor for key '%s' is unknown",
                         test_flash->node_id, shard_id, ptfc->key.key);
            status = SDF_OBJECT_UNKNOWN;
            /* status = SDF_FLASH_STALE_CURSOR; */
        } else {
            flash_entry = (struct test_flash_entry *)pme->contents;
            if (flash_entry->is_tombstone) {
                flags |= f_tombstone;
                status = SDF_FLASH_STALE_CURSOR;
                plat_log_msg(LOG_ID, LOG_CAT_FLASH, LOG_DBG,
                             "node_id %"PRIu32" shard %"PRIu64" get by cursor key '%s' returns a tombstone",
                             test_flash->node_id, shard_id, (char *)ptfc->key.key);
            } else if (pme->seqno != ptfc->seqno) {
                status = SDF_FLASH_STALE_CURSOR;
                plat_log_msg(LOG_ID, LOG_CAT_FLASH, LOG_DBG,
                             "node_id %"PRIu32" shard %"PRIu64" get by cursor key '%s' is stale "
                             "for illegal seqno",
                             test_flash->node_id, shard_id, (char *)ptfc->key.key);
            } else {
                key     = ptfc->key.key;
                key_len = ptfc->key.len;
                seqno   = pme->seqno;
                data_size   = flash_entry->data_len;
                exptime     = flash_entry->exptime;
                createtime  = flash_entry->createtime;
                pdata       = plat_malloc(flash_entry->data_len);
                memcpy(pdata, flash_entry->data, flash_entry->data_len);
                status = SDF_SUCCESS;
                plat_log_msg(LOG_ID, LOG_CAT_FLASH, LOG_DBG,
                             "node_id %"PRIu32" shard %"PRIu64" get by cursor key '%s' returns data",
                             test_flash->node_id, shard_id, (char *)ptfc->key.key);
            }
        }
        }
        break;
    default:
        plat_fatal("");
    }
    new_mtype =
        home_flash_response_type(pm->msgtype, status);
    new_msg = home_load_msg(recv_msg->msg_dest_vnode, recv_msg->msg_src_vnode,
                            pm, new_mtype,
                            pdata  /* data */, data_size /* data_size */,
                            exptime /* exptime */, createtime /* createtime */,
                            seqno,
                            status, &msize, key, key_len, flags);
    plat_free(pdata);
    new_msg->msg_len = sizeof(*new_msg) + sizeof(SDF_protocol_msg_t) + data_size;
    new_msg->sent_id = recv_msg->sent_id;
    new_msg->msg_flags |= SDF_MSG_FLAG_MBX_RESP_INCLUDED;
    new_msg->msg_type = FLSH_RESPOND;
    new_msg->msg_src_vnode = test_flash->node_id;
    new_msg->msg_dest_vnode = recv_msg->msg_src_vnode;
    new_msg->msg_dest_service = recv_msg->msg_src_service;
    new_msg->msg_src_service = recv_msg->msg_dest_service;

    ret = rtf_msg_wrapper_reply(new_msg, msg_wrapper);
    sdf_msg_wrapper_rwrelease(&recv_msg, msg_wrapper);
    return (ret);
}

static void rtf_message_dispatch(struct replication_test_flash *test_flash,
                                 struct sdf_msg_wrapper *msg_wrapper);
/**
 * @brief synchronous receive_message for test_flash,
 * similar tiwh rtn_send_msg in test node.
 */
void rtf_receive_msg(struct replication_test_flash *test_flash,
                     struct sdf_msg_wrapper *msg_wrapper) {
    plat_assert(test_flash);
    plat_assert(msg_wrapper);
#ifdef FLASH_DELAY
    rtf_do_receive_msg_t do_receive =
        rtf_do_receive_msg_create(test_flash->closure_scheduler,
                                  &rtf_receive_msg_impl, test_flash);
    plat_closure_apply(rtf_do_receive_msg, &do_receive, msg_wrapper);
    rtf_guarantee_bootstrapped(test_flash);
#else
    rtf_message_dispatch(test_flash, msg_wrapper);
#endif
}

#ifdef FLASH_DELAY
/**
 * @brief Internal receive_msg implementation
 */
static void
rtf_receive_msg_impl(struct plat_closure_scheduler *context, void *env,
                     struct sdf_msg_wrapper *msg_wrapper) {
    struct rtf_msg_entry *entry;
    struct replication_test_flash *test_flash =
        (struct replication_test_flash *)env;
  
    plat_assert(msg_wrapper);
    entry = plat_alloc(sizeof(struct rtf_msg_entry));
    plat_assert(entry != NULL);
  
    entry->test_flash = test_flash;
    entry->msg_wrapper = msg_wrapper;
    /*
     * Fixme: Zhenwei. test flash can't utility
     * plat_prng_next_int for invisible of prng in test framework
     */
    entry->us_latency = plat_prng_next_int(test_flash->component_api.prng,
                                           test_flash->test_config.flash_timing.max_delay_us);
  
    /* state check */
    switch (test_flash->state) {
    case RTF_STATE_LIVE:
        /* dispath entry to destination chips */
        rtf_msg_entry_dispatch(entry);
        break;
    case RTF_STATE_INITIAL:
        plat_assert(0);
    case RTF_STATE_DEAD:
    case RTF_STATE_TO_DEAD:
        rtf_msg_entry_error(test_flash, entry, SDF_TEST_CRASH);
        break;
    case RTF_STATE_TO_SHUTDOWN:
    case RTF_STATE_SHUTDOWN:
        rtf_msg_entry_error(test_flash, entry, SDF_SHUTDOWN);
        break;
    }
}

/*
 * @brief Distribute rtf_msg_entry to destination chips
 * @param entry <IN> Test flash message entry
 */
static void
rtf_msg_entry_dispatch(struct rtf_msg_entry *entry) {
    uint32_t chip_id;
    chip_id = rtf_get_chip(entry);
    plat_assert(&entry->test_flash->chips[chip_id]);
#ifdef FLASH_DELAY
    rtf_msg_entry_add_to_chip(entry, &entry->test_flash->chips[chip_id]);
#endif
}

#ifdef FLASH_DELAY
/**
 * @brief Add entry to specified chip
 */
static void
rtf_msg_entry_add_to_chip(struct rtf_msg_entry *entry, struct rtf_chip *chip) {
    struct replication_test_flash *test_flash = entry->test_flash;
    int was_empty;
  
    plat_assert(chip);
    plat_assert(test_flash->state == RTF_STATE_LIVE);
  
    was_empty = TAILQ_EMPTY(&chip->msg_inbound.queue);

    plat_assert_iff(was_empty, !chip->msg_inbound.next_delivery_event);

    TAILQ_INSERT_TAIL(&chip->msg_inbound.queue, entry,
                      msg_inbound_queue_entry);
#ifdef FLASH_DELAY
    if (was_empty) {
        /* FIXME: We should dequeue immediately if we aren't doing the delay */
        rtf_reset_chip_timer(test_flash, chip);
    }
#else /* def FLASH_DELAY */
    /* The previous message should have been immediately dequeued */
    plat_assert(was_empty);
#endif /* else def FLASH_DELAY */
}

/**
 * @brief Remove entry from sepcific chip
 */
static void
rtf_msg_entry_remove_from_chip(struct rtf_msg_entry *entry, struct rtf_chip *chip) {
    struct replication_test_flash *test_flash = entry->test_flash;
    plat_event_free_done_t free_done_cb;
  
    plat_assert(!TAILQ_EMPTY(&chip->msg_inbound.queue));

    free_done_cb =
        plat_event_free_done_create(test_flash->closure_scheduler,
                                    &rtf_next_delivery_event_free_cb,
                                    test_flash);

    if (entry == TAILQ_FIRST(&chip->msg_inbound.queue) &&
        chip->msg_inbound.next_delivery_event) {
        ++test_flash->ref_count;
        plat_event_free(chip->msg_inbound.next_delivery_event,
                        free_done_cb);
        chip->msg_inbound.next_delivery_event = NULL;
    }

    TAILQ_REMOVE(&chip->msg_inbound.queue, entry,
                 msg_inbound_queue_entry);

    if (!TAILQ_EMPTY(&chip->msg_inbound.queue) &&
        !chip->msg_inbound.next_delivery_event &&
        test_flash->state == RTF_STATE_LIVE) {
        rtf_reset_chip_timer(test_flash, chip);
    }

    plat_assert_imply(TAILQ_EMPTY(&chip->msg_inbound.queue),
                      !chip->msg_inbound.next_delivery_event);

    rtf_msg_entry_free(entry);
}

/**
 * @brief Encaplation of #rtf_ref_count_dec,
 * Question: Why not adding ref_count for each chip?
 */
static void
rtf_next_delivery_event_free_cb(plat_closure_scheduler_t *context, void *env) {
    struct replication_test_flash *test_flash =
        (struct replication_test_flash *)env;

    rtf_ref_count_dec(test_flash);
}

/*
 * @brief reset chip and send request out.
 */
static void
rtf_reset_chip_timer(struct replication_test_flash *test_flash,
                     struct rtf_chip *chip) {
    plat_assert(test_flash && chip);
    plat_assert(test_flash->state == RTF_STATE_LIVE);
    plat_assert(chip && !chip->msg_inbound.next_delivery_event);
    plat_assert(chip && !TAILQ_EMPTY(&chip->msg_inbound.queue));
  
#ifdef FLASH_DELAY
    struct rtf_work_state *work_state;
    struct timeval now;
    plat_event_fired_t fired;
    struct rtf_msg_entry *entry;
    struct timeval when;

    entry = TAILQ_FIRST(&chip->msg_inbound.queue);

    plat_closure_apply(plat_timer_dispatcher_gettime,
                       &test_flash->component_api.gettime, &now);
    plat_log_msg(LOG_ID, LOG_CAT_EVENT, LOG_TRACE,
                 "node_id %lld NOW:usec:%lu, sec:%lu, nw_ltcy:%"PRIu32,
                 (long long)test_flash->node_id,
                 now.tv_usec, now.tv_sec, entry->us_latency);
#if 0
    when.tv_sec = now.tv_sec + (now.tv_usec + entry->us_latency) / MILLION;
    when.tv_usec = (now.tv_usec + entry->us_latency) % MILLION;
#endif
    when.tv_sec = (entry->us_latency)/ MILLION;
    when.tv_usec = (entry->us_latency) % MILLION;

    plat_calloc_struct(&work_state);
    work_state->chip = chip;
    work_state->test_flash = test_flash;
    fired = plat_event_fired_create(test_flash->closure_scheduler,
                                    &rtf_next_delivery_cb, work_state);
    chip->msg_inbound.next_delivery_event =
            plat_timer_dispatcher_timer_alloc(test_flash->component_api.timer_dispatcher,
                                              "flash_next_delivery", LOG_CAT_EVENT,
                                              fired, 1 /* free_count */,
                                              &when, PLAT_TIMER_RELATIVE,
                                              NULL /* rank_ptr */);
#else
  
#endif
}

/** @brief Timer fired (all common code) */
static void
rtf_next_delivery_cb(plat_closure_scheduler_t *context, void *env,
                     struct plat_event *event) {
   struct rtf_work_state *work_state = (struct rtf_work_state *)env;
   struct replication_test_flash *test_flash = work_state->test_flash;
   struct rtf_chip *chip = work_state->chip;
   rtf_deliver_msg(test_flash, chip);
   plat_free(work_state);
}

/** @brief Deliver first message on specific chip */
static void
rtf_deliver_msg(struct replication_test_flash *test_flash,
                struct rtf_chip *chip) {
    struct rtf_msg_entry *entry;
    struct sdf_msg_wrapper *msg_wrapper = NULL;

    if (!TAILQ_EMPTY(&chip->msg_inbound.queue)) {
        entry = TAILQ_FIRST(&chip->msg_inbound.queue);
        msg_wrapper = entry->msg_wrapper;
        entry->msg_wrapper = NULL;
        rtf_msg_entry_remove_from_chip(entry, chip);
        /* XXX: drew 2009-01-13 should uniquely identify message sent */
        plat_log_msg(LOG_ID, LOG_CAT_FLASH, LOG_DBG,
                     "node_id %"PRIu32" send msg", test_flash->node_id);
        rtf_message_dispatch(test_flash, msg_wrapper);
    } else {
        plat_log_msg(LOG_ID, LOG_CAT_FLASH, LOG_DBG,
                     "inbound queue empty in node:%"PRIu32" chip:%"PRIu32,
                     test_flash->node_id, chip->chip_id);
    }
}
#endif

/**
 * @brief Signal error detection for #rtf_msg_entry
 *
 * A SDF_MSG_ERROR response is generated for entry and it's removed from
 * all internal structures just as if a regular response had been received
 * for it.
 *
 * @param entry <IN> message entry
 * @param error_type <IN> type of error
 */
static void
rtf_msg_entry_error(struct replication_test_flash *test_flash,
                    struct rtf_msg_entry *entry, SDF_status_t error_type) {
    struct sdf_msg_wrapper *response_wrapper;
    struct sdf_msg *response_msg;
    struct sdf_msg_error_payload *response_payload;
    sdf_msg_wrapper_free_local_t local_free;

    response_msg = sdf_msg_alloc(sizeof (*response_payload));
    plat_assert(response_msg);

    response_msg->msg_flags |= SDF_MSG_FLAG_MBX_RESP_INCLUDED;
    response_payload =
        (struct sdf_msg_error_payload *)&response_msg->msg_payload;
    response_payload->error = error_type;

    /* Wrapper as a sdf_msg_wrapper */
    local_free =
        sdf_msg_wrapper_free_local_create(PLAT_CLOSURE_SCHEDULER_ANY_OR_SYNCHRONOUS,
                                          &rtf_msg_free, NULL);

    response_wrapper =
        sdf_msg_wrapper_local_alloc(response_msg, local_free,
                                    SMW_MUTABLE_FIRST,
                                    SMW_TYPE_RESPONSE,
                                    test_flash->node_id /* src */,
                                    SDF_SDFMSG /* src service */,
                                    test_flash->node_id /* dest */,
                                    FLSH_RESPOND /* possibly error */,
                                    SDF_MSG_ERROR,
                                    sdf_msg_wrapper_get_response_mbx(entry->msg_wrapper));
    plat_assert(response_wrapper);
    rtf_msg_entry_response(entry, response_wrapper);
}

/** @brief Deliver response for given entry which is a request */
static void
rtf_msg_entry_response(struct rtf_msg_entry *entry,
                       struct sdf_msg_wrapper *response_wrapper) {
    SDF_status_t status;
    plat_assert(response_wrapper);
    if (response_wrapper) {
        plat_log_msg(LOG_ID, LOG_CAT_FLASH, LOG_TRACE,
                     "send msg response to %"PRIu32, entry->test_flash->node_id);
        plat_closure_apply(sdf_replicator_send_msg_cb,
                           &entry->test_flash->component_api.send_msg,
                           response_wrapper,
                           NULL /* no response expected */, &status);
    }
    rtf_msg_entry_free(entry);
}
#endif

/**
 * @brief Receive message
 */
static void
rtf_message_dispatch(struct replication_test_flash *test_flash,
                     struct sdf_msg_wrapper *msg_wrapper) {
    /* combind two wrapper togother */
    struct sdf_msg *request_msg = NULL;
    struct sdf_msg_wrapper *response_wrapper = NULL;
    enum flash_msg_type msg_type = -1;

    SDF_protocol_msg_t *request_pm = NULL;
    sdf_msg_wrapper_rwref(&request_msg, msg_wrapper);
    request_pm = (SDF_protocol_msg_t *)request_msg->msg_payload;

    plat_assert(msg_wrapper);
    switch (request_pm->msgtype) {
#define item(caps, type, response_good, response_bad) \
    case caps:                                                                 \
        msg_type = type;                                                       \
        break;
        HF_MSG_ITEMS()
#undef item
    default:
        plat_log_msg(LOG_ID, LOG_CAT_FLASH, LOG_DBG,
                     "node_id %"PRIu32" Unsupported meesage type",
                     test_flash->node_id);
    }
    /* send message according to msg_type */
    switch (msg_type) {
    case HF_MSG_KV:   /* key/value operations */
        response_wrapper = home_flash_test_kv_wrapper(test_flash, msg_wrapper);
        break;
    case HF_MSG_SHARD: /* shard operations */
        response_wrapper = home_flash_test_shard_wrapper(test_flash, msg_wrapper);
        break;
    }
    sdf_msg_wrapper_rwrelease(&request_msg, msg_wrapper);

    /* request wrapper should be freed */
    sdf_msg_wrapper_ref_count_dec(msg_wrapper);

    SDF_status_t status;
    plat_assert(response_wrapper);
    plat_closure_apply(sdf_replicator_send_msg_cb,
                       &test_flash->component_api.send_msg,
                       response_wrapper,
                       NULL /* no response expected */, &status);
}

static void
rtf_msg_free(plat_closure_scheduler_t *context, void *env, struct sdf_msg *msg) {
    sdf_msg_free(msg);
}

static struct rtf_shard *
rtf_shard_find(struct replication_test_flash *flash, SDF_shardid_t shard_id) {
    struct rtf_shard *ret = NULL;
    struct rtf_shard *temp_shard = NULL;
    struct rtf_shard *next = NULL;
    TAILQ_FOREACH_SAFE(temp_shard, &flash->shards, shard, next) {
        if (temp_shard->shard_meta.sguid == shard_id) {
            ret = temp_shard;
            plat_log_msg(LOG_ID, LOG_CAT_FLASH, LOG_DBG,
                         "node_id %"PRIu32" shard_id:%"PRIu64", temp_shard:%p",
                         flash->node_id, shard_id, temp_shard);
            break;
        }
    }
    return (ret);
}

static SDF_boolean_t
rtf_create_shard(struct replication_test_flash *test_flash,
                 struct SDF_shard_meta *shard_meta) {
    SDF_boolean_t ret = SDF_FALSE;
    struct rtf_shard *temp_shard = NULL;
    temp_shard = plat_calloc(1, sizeof(struct rtf_shard));
    if (temp_shard) {
        memset(&temp_shard->shard_meta, 0, sizeof(struct SDF_shard_meta));
        memcpy(&temp_shard->shard_meta, shard_meta, sizeof(struct SDF_shard_meta));
        TLMap3Init(&(temp_shard->shard_map), SHARD_BUCKETS, NULL);
        temp_shard->last_seqno = 0;
        temp_shard->num_obj = 0;
        /* mount to flash */
        TAILQ_INSERT_TAIL(&test_flash->shards, temp_shard, shard);
        __sync_fetch_and_add(&test_flash->shard_num, 1);
        plat_log_msg(LOG_ID, LOG_CAT_FLASH, LOG_DBG,
                     "node_id %"PRIu32" create shard:%"PRIu64,
                     test_flash->node_id, shard_meta->sguid);
        ret = SDF_TRUE;
    } else {
            plat_log_msg(LOG_ID, LOG_CAT_FLASH, LOG_ERR,
                         "node_id %"PRIu32" allocate flash shard error.",
                         test_flash->node_id);
    }
    return (ret);
}

static SDF_boolean_t
rtf_delete_shard(struct replication_test_flash *test_flash,
                 SDF_shardid_t shard_id) {
    /* Is flash alive? */
    SDF_boolean_t ret = SDF_FALSE;
    struct rtf_shard *temp_shard = NULL;
    struct rtf_shard *next = NULL;
    fthWaitEl_t *lock = fthLock(&test_flash->flash_lock, 1, NONE);
    TAILQ_FOREACH_SAFE(temp_shard, &test_flash->shards, shard, next) {
        if (temp_shard->shard_meta.sguid == shard_id) {
            TAILQ_REMOVE(&test_flash->shards, temp_shard, shard);
            rtf_test_shard_destroy(temp_shard);
            __sync_fetch_and_sub(&test_flash->shard_num, 1);
            ret = SDF_SUCCESS;
            plat_log_msg(LOG_ID, LOG_CAT_FLASH, LOG_DBG,
                         "node_id %"PRIu32" shard_id:%"PRIu64", temp_shard:%p",
                         test_flash->node_id, shard_id, temp_shard);
            break;
        }
    }
    fthUnlock(lock);
    return (ret);
}

/**
 * @brief Allocate response_wrapper
 *
 * @param response_msg <IN> Consumed by this function
 * @param request_wrapper <IN> Original request to which response_msg
 * is a reply.
 * @return wrapper for the response to pass to component_api.send_msg
 */
static struct sdf_msg_wrapper *
rtf_msg_wrapper_reply(struct sdf_msg *response_msg,
                      struct sdf_msg_wrapper *request_wrapper) {
    sdf_msg_wrapper_free_local_t local_free;
    struct sdf_msg_wrapper *ret = NULL;

    local_free =
        sdf_msg_wrapper_free_local_create(PLAT_CLOSURE_SCHEDULER_ANY_OR_SYNCHRONOUS,
                                          &rtf_msg_free,
                                          NULL);

    ret =
        sdf_msg_wrapper_local_alloc(response_msg,  local_free,
                                    SMW_MUTABLE_FIRST,
                                    SMW_TYPE_RESPONSE,
                                    response_msg->msg_src_vnode /* src */,
                                    response_msg->msg_src_service,
                                    response_msg->msg_dest_vnode /* dest */,
                                    response_msg->msg_dest_service /* dest */,
                                    FLSH_RESPOND,
                                    sdf_msg_wrapper_get_response_mbx(request_wrapper));

    if (!ret) {
        plat_log_msg(LOG_ID, LOG_CAT_FLASH, LOG_ERR,
                     "reply wrapper failure");
        plat_assert_always(0);
    }
    plat_log_msg(LOG_ID, LOG_CAT_FLASH, LOG_DBG,
                 "allocate wrapper:%p", ret);
    return (ret);
}

void
rtf_shard_enumerate(struct replication_test_flash *test_flash) {
    struct rtf_shard *temp_shard = NULL;
    struct rtf_shard *next = NULL;
    TAILQ_FOREACH_SAFE(temp_shard, &test_flash->shards, shard, next) {
        plat_log_msg(LOG_ID, LOG_CAT_FLASH, LOG_DBG,
                     "node_id %"PRIu32" shard:%p, shard_id:%"PRIu64,
                     test_flash->node_id, temp_shard,
                     temp_shard->shard_meta.sguid);
    }
}

static SDF_status_t
rtf_test_shard_destroy(struct rtf_shard *test_shard) {
    /* enumerate Hashmap and free object */
    TLMap3Destroy(&(test_shard->shard_map));
    test_shard->num_obj = 0;
    plat_free(test_shard);
    return (SDF_TRUE);
}

static void
rtf_shutdown_closure_scheduler(struct replication_test_flash *test_flash) {
    plat_closure_scheduler_shutdown_t cb;

    plat_assert(test_flash->state == RTF_STATE_LIVE ||
                test_flash->state == RTF_STATE_TO_SHUTDOWN);

    cb = plat_closure_scheduler_shutdown_create(test_flash->closure_scheduler,
                                                &rtf_closure_scheduler_shutdown_cb,
                                                test_flash);
    test_flash->closure_scheduler_shutdown_started = 1;
    ++test_flash->ref_count;
    plat_closure_scheduler_shutdown(test_flash->closure_scheduler, cb);
}

static void
rtf_closure_scheduler_shutdown_cb(struct plat_closure_scheduler *context,
                                  void *env) {
    struct replication_test_flash *test_flash =
        (struct replication_test_flash *)env;

    /* Set closure scheduler NULL */
    test_flash->closure_scheduler = NULL;
    rtf_ref_count_dec(test_flash);
}

static const char *
rtf_state_to_string(enum rtf_state state) {
    switch (state) {
#define item(caps, lower)             \
    case caps:                        \
        return (#lower);
    RTF_FLASH_STATE_ITEMS()
#undef item
    default:
        plat_assert(0);
    }
}

static void
rtf_free(struct replication_test_flash *test_flash) {
    plat_assert(test_flash != NULL);
    plat_assert(test_flash->state == RTF_STATE_SHUTDOWN ||
                (test_flash->state == RTF_STATE_INITIAL &&
                 !test_flash->closure_scheduler));
    plat_assert(!test_flash->ref_count);
    plat_assert(!test_flash->closure_scheduler);
  
    /* shutdown simulated flash */
    struct rtf_shard *temp_shard = NULL;
    struct rtf_shard *next;
    rtf_shard_enumerate(test_flash);
    fthWaitEl_t *lock = fthLock(&test_flash->flash_lock, 1, NULL);
    TAILQ_FOREACH_SAFE(temp_shard, &test_flash->shards, shard, next) {
        plat_log_msg(LOG_ID, LOG_CAT_FLASH, LOG_DBG,
                     "flash:%p, shard:%p, node: %u", test_flash, temp_shard, test_flash->node_id);
        TAILQ_REMOVE(&test_flash->shards, temp_shard, shard);
        rtf_test_shard_destroy(temp_shard);
        --test_flash->shard_num;
    }
#ifdef FLASH_DELAY
    /* destroy simulated chips */
    plat_free(test_flash->chips);
#endif
    fthUnlock(lock);
    plat_free(test_flash);
}

#ifdef FLASH_DELAY
/*
 * @brief Get destination chip according to received wrapper
 * @return chip_id
 */
static uint32_t
rtf_get_chip(struct rtf_msg_entry *entry) {
    uint64_t ret;
    struct replication_test_flash *test_flash = entry->test_flash;
    struct sdf_msg_wrapper *wrapper = entry->msg_wrapper;
    struct sdf_msg *msg = NULL;
    SDF_protocol_msg_t *pm = NULL;
    SDF_shardid_t shard;

    sdf_msg_wrapper_rwref(&msg, wrapper);
    pm = (SDF_protocol_msg_t *)msg->msg_payload;
    shard = pm->shard;
    /* take shard is as key in view of no syndrome in create/shard ops */
    ret = shard % (test_flash->nchips);
    sdf_msg_wrapper_rwrelease(&msg, wrapper);
    return (ret);
}
static void
rtf_msg_entry_free(struct rtf_msg_entry *entry) {
    if (entry->msg_wrapper) {
        sdf_msg_wrapper_ref_count_dec(entry->msg_wrapper);
    }
    entry->msg_wrapper = NULL;
    entry->test_flash = NULL;
    entry->us_latency = -1;
    plat_free(entry);
}
#endif
