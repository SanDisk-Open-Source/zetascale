#ifndef REPLICATION_TEST_FLASH_H
#define REPLICATION_TEST_FLASH_H 1

/*
 * File: sdr/protocol/replication/tests/test_flash.h
 *
 * Author: drew
 *
 * Created on November 3, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: test_flash.h 7551 2009-05-22 18:42:34Z briano $
 */

/**
 * Simulated flash interface.  Initially, this wraps the real flash code
 * but provides the message based API implenented by sdf/protocol/home_flash.c.
 *
 * The differences are that:
 * 1) a simulated messaging layer is used which does not require round trips
 *    through a pthread that makes reproduceable timings difficult for
 *    pseudo-random tests
 *
 * 2) a configurable simulated delay (#replication_test_config
 *    flash_timing field) is applied to operations before they start,
 *    thus causing the potential ordering issues which exist (the
 *    pool of worker threads in home_flash.c and flash software + hardware
 *    implementation make no guarantees about when IOs will compelte)
 *    to manifest more frequently in the simulated environment.
 */



#include "platform/closure.h"
#include "platform/defs.h"
#include "platform/time.h"

#include "common/sdftypes.h"
#include "fth/fth.h"


#include "test_api.h"
#include "test_framework.h"
#include "utils/hashmap.h"

// #define FLASH_WORKER

/**
 * @brief Table of all supported  messags
 *
 * item(caps, type, response_good, response_bad)
 */
#define HF_MSG_ITEMS() \
    /* Key, value operations */                                                \
    item(HFXST, HF_MSG_KV, FHXST, FHNXS)                                       \
    item(HFGFF, HF_MSG_KV, FHDAT, FHGTF)                                       \
    item(HFSET, HF_MSG_KV, FHSTC, FHSTF)                                       \
    item(HFPTF, HF_MSG_KV, FHPTC, FHPTF)                                       \
    item(HZSF, HF_MSG_KV, FHDEC, FHDEF)                                       \
    /* Shard operations */                                                     \
    item(HFCSH, HF_MSG_SHARD, FHCSC, FHCSF)                                    \
    item(HFDSH, HF_MSG_SHARD, FHDSC, FHDSF)                                    \
    item(HFGLS, HF_MSG_SHARD, FHGLC, FHGLF)                                    \
    item(HFGIC, HF_MSG_SHARD, FHGIC, FHGIF)                                    \
    item(HFGBC, HF_MSG_SHARD, FHGCC, FHGCF)



struct sdf_msg_wrapper;


struct replication_test_config;

#define NUM_SHARD_MAX 100
#define NUM_SHARD_RESERVED 10

enum flash_status {
    /** @brief flash_crash */
    FLASH_CRASH = 1 << 0,
    /** @brief flash_start */
    FLASH_START = 1 << 1,
};

/** @brief shutdown callback */
PLAT_CLOSURE(replication_test_flash_shutdown_async_cb);

/** @brief crash callback */
/* Fixme: remove status to in homony with node_crash and shutdown */
PLAT_CLOSURE(replication_test_flash_crash_async_cb);

__BEGIN_DECLS

/** @brief Create simulated flash */
struct replication_test_flash *
replication_test_flash_alloc(const struct replication_test_config *test_config,
                             const replication_test_api_t *api,
                             vnode_t node_id);

/**
 * @brief Shutdown and free simulated flash
 */
void
rtf_shutdown_async(struct replication_test_flash *test_flash,
                   replication_test_flash_shutdown_async_cb_t cb);

void
rtf_crash_async_cb(plat_closure_scheduler_t *context, void *env, SDF_status_t status);

/**
 * @brief Start or restart flash operation.
 *
 * @return SDF_SUCCESS on success, other on failure (ex: node already
 * started)
 */
SDF_status_t
rtf_start(struct replication_test_flash *test_flash);

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
                replication_test_flash_crash_async_cb_t cb);

/**
 * @brief Receive message
 *
 * One reference count of msg_wrapper is consumed
 */
void
rtf_receive_msg(struct replication_test_flash *test_flash,
                struct sdf_msg_wrapper *msg_wrapper);

/**
 * @brief shard enumeration
 */
void
rtf_shard_enumerate(struct replication_test_flash *test_flash);
__END_DECLS

#endif /* ndef REPLICATION_TEST_FLASH_H */
