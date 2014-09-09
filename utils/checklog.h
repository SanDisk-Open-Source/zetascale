/*
 * File:   checklog.h
 * Author: Darryl Ouye
 *
 * Created on August 28, 2014
 *
 * SanDisk Proprietary Material, © Copyright 2014 SanDisk, all rights reserved.
 * http://www.sandisk.com
 *
 */
#ifndef __CHECKLOG_H
#define __CHECKLOG_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <inttypes.h>
#include "common/zstypes.h"

// zscheck logfile default
#define  ZSCHECK_LOG_DEFAULT "/tmp/zscheck.log"

// zscheck tests
#define ZSCHECK_TEST_LABEL          0x1
#define ZSCHECK_TEST_SUPERBLOCK     0x2
#define ZSCHECK_TEST_SHARD_DESC     0x4
#define ZSCHECK_TEST_SHARD_PROP     0x8
#define ZSCHECK_TEST_SEG_LIST       0x10
#define ZSCHECK_TEST_CLASS_DESC     0x20
#define ZSCHECK_TEST_CKPT_DESC      0x40
#define ZSCHECK_TEST_PAGE_HDR       0x80
#define ZSCHECK_TEST_SLAB_META      0x100
#define ZSCHECK_TEST_SLAB_DATA      0x200
#define ZSCHECK_TEST_POT_BM         0x400
#define ZSCHECK_TEST_SLAB_BM        0x800

// zscheck test property
#define ZS_META_FAILURE "ZS_META_FAILURE"

// Operational modes
#define ZS_OP_MODE "ZS_OP_MODE"   // property
#define ZSRUN "ZSRUN"             // normal run
#define ZSCHECK "ZSCHECK"         // check
#define ZSTEST "ZSTEST"           // test

typedef enum {
    ZS_RUN_MODE = 0,
    ZS_CHECK_MODE = 1,
} ZS_operational_mode_t;

typedef enum {
    ZSCHECK_LABEL = 0,
    ZSCHECK_SUPERBLOCK,
    ZSCHECK_SHARD_DESCRIPTOR,
    ZSCHECK_SHARD_PROPERTIES,
    ZSCHECK_SEGMENT_LIST,
    ZSCHECK_CLASS_DESCRIPTOR,
    ZSCHECK_CKPT_DESCRIPTOR,
    ZSCHECK_LOG_PAGE_HEADER,
    ZSCHECK_SLAB_METADATA,
    ZSCHECK_SLAB_DATA,
    ZSCHECK_POT_BITMAP,
    ZSCHECK_SLAB_BITMAP,
} ZS_check_entity_t;

typedef enum {
    ZSCHECK_SUCCESS = 0,
    ZSCHECK_READ_ERROR,
    ZSCHECK_WRITE_ERROR,
    ZSCHECK_LABEL_ERROR,
    ZSCHECK_MAGIC_ERROR,
    ZSCHECK_CHECKSUM_ERROR,
} ZS_check_error_t;

typedef struct {
    ZS_check_entity_t entity;
    uint64_t id;
    ZS_check_error_t error;
    char *msg;
} ZS_check_log_entry_t;
    
int zscheck_init_log(char *file);
int zscheck_close_log();

void zscheck_log_msg(
    ZS_check_entity_t entity,
    uint64_t id,
    ZS_check_error_t error,
    char *msg
    );

#ifdef __cplusplus
}
#endif

#endif // __CHECKLOG_H



