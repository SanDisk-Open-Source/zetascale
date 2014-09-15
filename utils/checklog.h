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
#define  ZSCHECK_LOG_DEFAULT "/tmp/zsck.log"

typedef enum {
    ZSCHECK_NO_CHECK = 0,
    ZSCHECK_NO_INIT,
    ZSCHECK_BTREE_CHECK,
} ZS_check_mode_t;

typedef enum {
    ZSCHECK_LABEL = 0,
    ZSCHECK_SUPERBLOCK,
    ZSCHECK_SHARD_DESCRIPTOR,
    ZSCHECK_SHARD_PROPERTIES,
    ZSCHECK_SEGMENT_LIST,
    ZSCHECK_CLASS_DESCRIPTOR,
    ZSCHECK_CKPT_DESCRIPTOR,
    ZSCHECK_FLOG_RECORD,
    ZSCHECK_LOG_PAGE_HEADER,
    ZSCHECK_SLAB_METADATA,
    ZSCHECK_SLAB_DATA,
    ZSCHECK_POT_BITMAP,
    ZSCHECK_SLAB_BITMAP,
    ZSCHECK_BTREE_NODE,
    ZSCHECK_CONTAINER_META,
    ZSCHECK_SHARD_SPACE_MAP,
} ZS_check_entity_t;

typedef enum {
    ZSCHECK_SUCCESS = 0,
    ZSCHECK_READ_ERROR,
    ZSCHECK_WRITE_ERROR,
    ZSCHECK_LABEL_ERROR,
    ZSCHECK_MAGIC_ERROR,
    ZSCHECK_CHECKSUM_ERROR,
    ZSCHECK_BTREE_ERROR,
    ZSCHECK_CONTAINER_META_ERROR,
    ZSCHECK_SHARD_SPACE_MAP_ERROR,
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



