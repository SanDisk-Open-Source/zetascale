/*
 * File:   checklog.c
 * Author: Darryl Ouye
 *
 * Created on August 28, 2014
 *
 * SanDisk Proprietary Material, © Copyright 2014 SanDisk, all rights reserved.
 * http://www.sandisk.com
 *
 */
#include <stdio.h>
#include <string.h>
#include <sys/time.h>
#include "platform/logging.h"
#include "properties.h"
#include "common/zstypes.h"

static char *logfile = NULL;
static FILE *fp = NULL;

int zscheck_init_log(char *file)
{
    if (file)
        logfile = file;
    else
        logfile = ZSCHECK_LOG_DEFAULT;

    fp = fopen(logfile, "a");

    if (fp)
        return 0;
    else {
        perror("Failed to open logfile: ");
        return -1;
    }
}
    
int zscheck_close_log()
{
    if (fp) {
      fflush(fp);
      fclose(fp);
      fp = NULL;
      return 0;
    }
    else
      return -1;
}

void zscheck_log_msg(
    ZS_check_entity_t entity,
    uint64_t id,
    ZS_check_error_t error,
    char *msg
    )
{
    struct timeval now;
    char *time_str;
    char *entity_str = "";
    char *error_str = "";

    gettimeofday(&now, NULL);
    time_str = plat_log_timeval_to_string(&now);

    switch (entity) {
    case ZSCHECK_LABEL:
        entity_str = "ZSCHECK_LABEL";
        break;

    case ZSCHECK_SUPERBLOCK:
        entity_str = "ZSCHECK_SUPERBLOCK";
        break;

    case ZSCHECK_SHARD_DESCRIPTOR:
        entity_str = "ZSCHECK_SHARD_DESCRIPTOR";
        break;

    case ZSCHECK_SHARD_PROPERTIES:
        entity_str = "ZSCHECK_SHARD_PROPERTIES";
        break;

    case ZSCHECK_SEGMENT_LIST:
        entity_str = "ZSCHECK_SEGMENT_LIST";
        break;

    case ZSCHECK_CLASS_DESCRIPTOR:
        entity_str = "ZSCHECK_CLASS_DESCRIPTOR";
        break;

    case ZSCHECK_CKPT_DESCRIPTOR:
        entity_str = "ZSCHECK_CKPT_DESCRIPTOR";
        break;

    case ZSCHECK_FLOG_RECORD:
        entity_str = "ZSCHECK_FLOG_RECORD";
        break;

    case ZSCHECK_LOG_PAGE_HEADER:
        entity_str = "ZSCHECK_LOG_PAGE_HEADER";
        break;

    case ZSCHECK_SLAB_METADATA:
        entity_str = "ZSCHECK_SLAB_METADATA";
        break;

    case ZSCHECK_SLAB_DATA:
        entity_str = "ZSCHECK_SLAB_DATA";
        break;

    case ZSCHECK_POT:
        entity_str = "ZSCHECK_POT";
        break;

    case ZSCHECK_POT_BITMAP:
        entity_str = "ZSCHECK_POT_BITMAP";
        break;

    case ZSCHECK_SLAB_BITMAP:
        entity_str = "ZSCHECK_SLAB_BITMAP";
        break;

    case ZSCHECK_BTREE_NODE:
        entity_str = "ZSCHECK_BTREE_NODE";
        break;

    case ZSCHECK_CONTAINER_META:
        entity_str = "ZSCHECK_CONTAINER_META";
        break;

    case ZSCHECK_SHARD_SPACE_MAP:
        entity_str = "ZSCHECK_CSHARD_SPACE_MAP";
        break;

    case ZSCHECK_OBJECT_TABLE:
        entity_str = "ZSCHECK_OBJECT_TABLE";
        break;

    case ZSCHECK_STORM_LOG:
        entity_str = "ZSCHECK_STORM_LOG";
        break;

    default: 
        entity_str = "ZSCHECK_UNKNOWN";
        break;
    }

    switch (error) {
    case ZSCHECK_SUCCESS:
        error_str = "ZSCHECK_SUCCESS";
        break;

    case ZSCHECK_INFO:
        error_str = "ZSCHECK_INFO";
        break;

    case ZSCHECK_READ_ERROR:
        error_str = "ZSCHECK_READ_ERROR";
        break;

    case ZSCHECK_WRITE_ERROR:
        error_str = "ZSCHECK_WRITE_ERROR";
        break;

    case ZSCHECK_LABEL_ERROR:
        error_str = "ZSCHECK_LABEL_ERROR";
        break;

    case ZSCHECK_MAGIC_ERROR:
        error_str = "ZSCHECK_MAGIC_ERROR";
        break;

    case ZSCHECK_CHECKSUM_ERROR:
        error_str = "ZSCHECK_CHECKSUM_ERROR";
        break;

    case ZSCHECK_BTREE_ERROR:
        error_str = "ZSCHECK_BTREE_ERROR";
        break;

    case ZSCHECK_LSN_ERROR:
        error_str = "ZSCHECK_LSN_ERROR";
        break;

    case ZSCHECK_CONTAINER_META_ERROR:
        error_str = "ZSCHECK_CONTAINER_META_ERROR";
        break;

    case ZSCHECK_SHARD_SPACE_MAP_ERROR:
        error_str = "ZSCHECK_SHARD_SPACE_MAP_ERROR";
        break;

    default:
        error_str = "ZSHECK_UNKNOWN";
        break;

    }

    if (fp) {
        fprintf(fp, "%s %s %lu %s %s\n", time_str, entity_str, id, error_str, msg);
        fflush(fp);
    }
}
